"""
Batch-ingest silver/gold Parquet into Apache Pinot (parallel table writes).
"""
from __future__ import annotations

import os
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

ROOT = Path(__file__).resolve().parent
PINOT_IMAGE = os.environ.get("PINOT_IMAGE", "apachepinot/pinot:1.2.0")
# Principals/akas parquet parts are ~120–170MB compressed; segment gen needs several GB heap.
PINOT_INGEST_JAVA_OPTS = os.environ.get(
    "PINOT_INGEST_JAVA_OPTS",
    "-Xms1g -Xmx4g -XX:+UseG1GC -XX:MaxGCPauseMillis=200",
)
PINOT_INGEST_MEMORY = os.environ.get("PINOT_INGEST_MEMORY", "5g")
# How many LaunchDataIngestionJob containers to run at once.
# Keep modest: each heavy table needs ~5g and the Pinot cluster is already up.
PINOT_INGEST_PARALLELISM = int(os.environ.get("PINOT_INGEST_PARALLELISM", "3"))

# (table, container memory). Smaller tables get less RAM so more can run in parallel.
TABLES: list[tuple[str, str]] = [
    ("silver_ratings", "1g"),
    ("gold_top_category_wise_cast", "2g"),
    ("silver_names", "3g"),
    ("silver_titles", "3g"),
    ("silver_akas", PINOT_INGEST_MEMORY),
    ("gold_category_wise_movies", PINOT_INGEST_MEMORY),
    ("silver_principals", PINOT_INGEST_MEMORY),
]


def compose_network() -> str:
    out = subprocess.check_output(
        [
            "docker",
            "inspect",
            "-f",
            "{{range $k, $_ := .NetworkSettings.Networks}}{{$k}}{{end}}",
            "pinot-controller",
        ],
        text=True,
    ).strip()
    network = out.split()[0] if out else ""
    if not network:
        raise RuntimeError("Could not resolve Docker network for pinot-controller.")
    return network


def java_opts_for_memory(memory: str) -> str:
    """Derive a heap cap from the container memory limit (leave headroom for native/OS)."""
    override = os.environ.get("PINOT_INGEST_JAVA_OPTS")
    if override:
        return override
    raw = memory.strip().lower()
    if raw.endswith("g"):
        total_gb = float(raw[:-1])
        if total_gb <= 1:
            return "-Xms256m -Xmx512m -XX:+UseG1GC -XX:MaxGCPauseMillis=200"
        heap_gb = max(1, int(total_gb) - 1)
        return f"-Xms512m -Xmx{heap_gb}g -XX:+UseG1GC -XX:MaxGCPauseMillis=200"
    return PINOT_INGEST_JAVA_OPTS


def ingest_table(network: str, table: str, memory: str) -> tuple[str, float]:
    job_spec = f"/opt/pinot/imdb/pinot/ingestion/{table}.yaml"
    java_opts = java_opts_for_memory(memory)
    print(f"[INFO] Ingesting {table} (mem={memory}) ...", flush=True)
    started = time.monotonic()
    subprocess.run(
        [
            "docker",
            "run",
            "--rm",
            "--network",
            network,
            "--memory",
            memory,
            "-v",
            f"{ROOT}:/opt/pinot/imdb",
            "-e",
            f"JAVA_OPTS={java_opts}",
            PINOT_IMAGE,
            "LaunchDataIngestionJob",
            "-jobSpecFile",
            job_spec,
        ],
        check=True,
    )
    elapsed = time.monotonic() - started
    print(f"[OK] Ingested {table} in {elapsed:.1f}s", flush=True)
    return table, elapsed


def main() -> int:
    (ROOT / "pinot_segments").mkdir(parents=True, exist_ok=True)
    network = compose_network()
    workers = max(1, min(PINOT_INGEST_PARALLELISM, len(TABLES)))
    print(f"[INFO] Using Docker network: {network}")
    print(f"[INFO] Parallel ingest workers: {workers}")

    started = time.monotonic()
    errors: list[tuple[str, BaseException]] = []
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {
            pool.submit(ingest_table, network, table, memory): table
            for table, memory in TABLES
        }
        for fut in as_completed(futures):
            table = futures[fut]
            try:
                fut.result()
            except Exception as exc:  # noqa: BLE001 - surface per-table failures
                errors.append((table, exc))
                print(f"[ERROR] Failed ingesting {table}: {exc}", flush=True)

    if errors:
        failed = ", ".join(t for t, _ in errors)
        print(f"[FAILED] Ingestion incomplete ({failed}).", flush=True)
        return 1

    print(
        f"[SUCCESS] All tables ingested into Pinot in {time.monotonic() - started:.1f}s.",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
