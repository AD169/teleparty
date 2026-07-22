#!/usr/bin/env bash
# IMDb lakehouse -> Apache Pinot ingestion pipeline
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
[[ -f "$ROOT/pinot/register_tables.sh" ]] || die "Missing pinot/register_tables.sh"
[[ -f "$ROOT/load_to_olap.py" ]] || die "Missing load_to_olap.py"

# --- Spark ETL (optional; uncomment to rebuild silver/gold) ---

log "Starting Spark and running etl_job.py..."
docker compose up -d spark-master spark-worker
docker compose stop pinot-server pinot-broker pinot-controller pinot-zookeeper >/dev/null 2>&1 || true
docker exec -i spark-master /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --driver-memory 1g --executor-memory 4g --executor-cores 4 \
    ./etl_job.py

docker compose stop spark-worker spark-master >/dev/null 2>&1 || true

# --- Pinot ---
log "Starting Pinot cluster..."
docker compose up -d pinot-zookeeper pinot-controller pinot-broker pinot-server
wait_http "$PINOT_CONTROLLER_URL/health" "Pinot controller" \
    || wait_http "$PINOT_CONTROLLER_URL/" "Pinot controller"
wait_http "$PINOT_BROKER_URL/health" "Pinot broker"

log "Registering Pinot schemas and tables..."
PINOT_CONTROLLER="$PINOT_CONTROLLER_URL" bash "$ROOT/pinot/register_tables.sh"

log "Running Pinot batch ingestion..."
python3 "$ROOT/load_to_olap.py"

# --- Verify ---
RECORD_COUNT="$(
  curl -sf -X POST "$PINOT_BROKER_URL/query/sql" \
    -H "Content-Type: application/json" \
    -d '{"sql":"SELECT COUNT(*) AS cnt FROM silver_titles"}' \
    | python3 -c 'import json,sys; r=json.load(sys.stdin); print(r.get("resultTable",{}).get("rows",[[0]])[0][0])'
)"

ok "Pipeline finished."
echo "silver_titles count: $RECORD_COUNT"
echo "Controller UI: $PINOT_CONTROLLER_URL"
echo "Broker health: $PINOT_BROKER_URL/health"
echo "Broker SQL:    POST $PINOT_BROKER_URL/query/sql"
