#!/usr/bin/env bash
# Run PySpark + Pinot benchmark tests (testing/*.py)
set -euo pipefail

ROOT="$(cd "$(dirname "$0")" && pwd)"
PINOT_CONTROLLER_URL="${PINOT_CONTROLLER_URL:-http://localhost:19000}"
PINOT_BROKER_URL="${PINOT_BROKER_URL:-http://localhost:8099}"

log() { echo "[INFO] $1"; }
ok()  { echo "[SUCCESS] $1"; }
die() { echo "[ERROR] $1" >&2; exit 1; }

wait_http() {
    local url="$1" label="$2"
    log "Waiting for $label ($url)..."
    until curl -sf "$url" >/dev/null 2>&1; do sleep 3; done
    ok "$label is healthy."
}

# --- Prerequisites ---
command -v docker >/dev/null || die "Docker is required."
command -v curl >/dev/null || die "curl is required."
command -v python3 >/dev/null || die "python3 is required."
[[ -f "$ROOT/testing/pyspark_test.py" ]] || die "Missing testing/pyspark_test.py"
[[ -f "$ROOT/testing/pinot_test.py" ]] || die "Missing testing/pinot_test.py"
[[ -d "$ROOT/silver/titles" ]] || die "Missing silver/titles parquet (run etl_job.py first)"
[[ -d "$ROOT/gold/category_wise_movies" ]] || die "Missing gold/category_wise_movies parquet (run etl_job.py first)"

# --- PySpark benchmark ---
log "Starting Spark cluster..."
docker compose up -d spark-master spark-worker
# Free RAM before heavy Spark job if Pinot is already up
docker compose stop pinot-server pinot-broker pinot-controller pinot-zookeeper >/dev/null 2>&1 || true

log "Waiting for Spark master UI..."
until curl -sf "http://localhost:8080" >/dev/null 2>&1; do sleep 3; done
ok "Spark master is up."

log "Running testing/pyspark_test.py..."
docker exec -i spark-master /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --driver-memory 1g --executor-memory 4g --executor-cores 4 \
    ./testing/pyspark_test.py
ok "PySpark benchmark finished."

log "Stopping Spark to free resources for Pinot..."
docker compose stop spark-worker spark-master >/dev/null 2>&1 || true

# --- Pinot setup (needed for pinot_test.py) ---
[[ -f "$ROOT/pinot/register_tables.sh" ]] || die "Missing pinot/register_tables.sh"
[[ -f "$ROOT/load_to_olap.py" ]] || die "Missing load_to_olap.py"

log "Starting Pinot cluster..."
docker compose up -d pinot-zookeeper pinot-controller pinot-broker pinot-server
wait_http "$PINOT_CONTROLLER_URL/health" "Pinot controller" \
    || wait_http "$PINOT_CONTROLLER_URL/" "Pinot controller"
wait_http "$PINOT_BROKER_URL/health" "Pinot broker"

log "Registering Pinot schemas and tables..."
PINOT_CONTROLLER="$PINOT_CONTROLLER_URL" bash "$ROOT/pinot/register_tables.sh"

log "Running Pinot batch ingestion..."
python3 "$ROOT/load_to_olap.py"

# --- Pinot benchmark ---
log "Running testing/pinot_test.py..."
python3 "$ROOT/testing/pinot_test.py"
ok "Pinot benchmark finished."

ok "All tests finished."
echo "Controller UI: $PINOT_CONTROLLER_URL"
echo "Broker health: $PINOT_BROKER_URL/health"
echo "Broker SQL:    POST $PINOT_BROKER_URL/query/sql"
