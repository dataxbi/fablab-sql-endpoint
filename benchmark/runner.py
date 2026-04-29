"""
benchmark/runner.py
Main TPC-DS benchmark runner for Fabric SQL endpoints.

Execution order per scale-factor block (default):
  1. Pause + Resume Fabric capacity  → flush caches for cold start
  2. Cold block: run all (endpoint × query) once
  3. Warm block: run all (endpoint × query) N times
  4. Pause Fabric capacity

With --warm-only:
  - Skip pause/resume, skip cold block, skip final pause.
  - Only runs the warm block (capacity must already be Active).

Usage:
    py benchmark/runner.py [--config benchmark/config.yaml] [--sf SF10 SF100]
                           [--endpoints warehouse_frag lakehouse_frag]
                           [--warm-only] [--dry-run]
"""

import argparse
import logging
import os
import sys
from pathlib import Path

import yaml
from dotenv import load_dotenv

# Ensure project root is on sys.path so sibling packages are importable
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from benchmark.connection import get_connection
from benchmark.utils import RunResult, Timer, save_results, setup_logging
from provision.capacity_manager import pause_capacity, resume_capacity

load_dotenv()
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Query execution
# ---------------------------------------------------------------------------

def _execute_query(
    conn,
    sql: str,
    timeout_sec: int,
) -> tuple[int, str]:
    """
    Run a SQL query on an open connection.
    Returns (rows_returned, status). status is 'success', 'error' or 'timeout'.
    """
    try:
        cursor = conn.cursor()
        cursor.timeout = timeout_sec  # pyodbc SQL_ATTR_QUERY_TIMEOUT (seconds)
        cursor.execute(sql)
        rows = cursor.fetchall()
        return len(rows), "success"
    except Exception as exc:
        msg = str(exc)
        if "timeout" in msg.lower() or "HYT00" in msg:
            return 0, "timeout"
        return 0, f"error: {msg}"


def run_query(
    endpoint_id: str,
    endpoint_cfg: dict,
    query_id: str,
    sql: str,
    cache_mode: str,
    repetition: int,
    scale_factor: str,
    timeout_sec: int,
) -> RunResult:
    server = os.path.expandvars(endpoint_cfg["server"])
    database = os.path.expandvars(endpoint_cfg["database"])
    schema = endpoint_cfg.get("schema")

    sql_exec = sql.replace("_S_", f"[{schema}]") if schema else sql

    with get_connection(server, database) as conn:
        with Timer() as t:
            rows, status = _execute_query(conn, sql_exec, timeout_sec)

    error_msg = status if status not in ("success", "timeout") else ""
    final_status = "error" if error_msg else status

    result = RunResult(
        endpoint=endpoint_id,
        scale_factor=scale_factor,
        query_id=query_id,
        cache_mode=cache_mode,
        repetition=repetition,
        elapsed_ms=round(t.elapsed_ms, 2),
        rows_returned=rows,
        status=final_status,
        error_message=error_msg,
    )
    logger.info(
        "[%s] %s | %s | %s | rep=%d | %.0f ms | rows=%d | %s",
        scale_factor, endpoint_id, query_id, cache_mode,
        repetition, result.elapsed_ms, rows, final_status,
    )
    return result


# ---------------------------------------------------------------------------
# Main benchmark loop
# ---------------------------------------------------------------------------

def run_benchmark(
    config: dict,
    scale_factors: list[str],
    dry_run: bool,
    endpoints_filter: list[str] | None = None,
    warm_only: bool = False,
) -> list[RunResult]:
    cap_cfg = config["capacity"]
    endpoints = config["endpoints"]
    if endpoints_filter:
        unknown = set(endpoints_filter) - set(endpoints)
        if unknown:
            logger.warning("Unknown endpoint(s) in --endpoints filter: %s", ", ".join(sorted(unknown)))
        endpoints = {k: v for k, v in endpoints.items() if k in endpoints_filter}
        if not endpoints:
            logger.error("No matching endpoints after applying --endpoints filter: %s", endpoints_filter)
            return []
    query_files = config["queries"]
    warm_reps = config.get("warm_repetitions", 3)
    timeout_sec = config.get("query_timeout_sec", 300)
    results_dir = config.get("results_dir", "results")

    # Load SQL query text once
    queries: dict[str, str] = {}
    for qid, path in query_files.items():
        queries[qid] = Path(path).read_text(encoding="utf-8")

    all_results: list[RunResult] = []

    for sf in scale_factors:
        logger.info("=" * 60)
        logger.info("Starting scale factor block: %s", sf)
        logger.info("=" * 60)

        if not dry_run and not warm_only:
            # Pause first to flush all in-memory caches, then resume.
            # This guarantees a true cold start even if the capacity was already Active.
            # pause_capacity() is a no-op if already Paused.
            logger.info("Pausing Fabric capacity to flush caches for cold block (%s)...", sf)
            pause_capacity(
                subscription_id=os.path.expandvars(cap_cfg["subscription_id"]),
                resource_group=os.path.expandvars(cap_cfg["resource_group"]),
                capacity_name=os.path.expandvars(cap_cfg["capacity_name"]),
                timeout_sec=cap_cfg.get("pause_timeout_sec", 600),
            )
            logger.info("Resuming Fabric capacity for %s block...", sf)
            resume_capacity(
                subscription_id=os.path.expandvars(cap_cfg["subscription_id"]),
                resource_group=os.path.expandvars(cap_cfg["resource_group"]),
                capacity_name=os.path.expandvars(cap_cfg["capacity_name"]),
                timeout_sec=cap_cfg.get("resume_timeout_sec", 600),
            )

        # --- Cold block (1 repetition each) ---
        if not warm_only:
            logger.info("Cold block (%s)...", sf)
            for ep_id, ep_cfg in endpoints.items():
                for qid, sql in queries.items():
                    if dry_run:
                        logger.info("[DRY-RUN] cold | %s | %s | %s", sf, ep_id, qid)
                        continue
                    result = run_query(ep_id, ep_cfg, qid, sql, "cold", 1, sf, timeout_sec)
                    all_results.append(result)

        # --- Warm block (N repetitions each) ---
        logger.info("Warm block (%s, %d reps)...", sf, warm_reps)
        for rep in range(1, warm_reps + 1):
            for ep_id, ep_cfg in endpoints.items():
                for qid, sql in queries.items():
                    if dry_run:
                        logger.info("[DRY-RUN] warm | %s | %s | %s | rep=%d", sf, ep_id, qid, rep)
                        continue
                    result = run_query(ep_id, ep_cfg, qid, sql, "warm", rep, sf, timeout_sec)
                    all_results.append(result)

        if not dry_run and not warm_only:
            logger.info("Pausing Fabric capacity after %s block...", sf)
            pause_capacity(
                subscription_id=os.path.expandvars(cap_cfg["subscription_id"]),
                resource_group=os.path.expandvars(cap_cfg["resource_group"]),
                capacity_name=os.path.expandvars(cap_cfg["capacity_name"]),
                timeout_sec=cap_cfg.get("pause_timeout_sec", 600),
            )

        logger.info("Scale factor %s complete.", sf)

    if all_results:
        csv_path, json_path = save_results(all_results, results_dir)
        logger.info("Results saved to: %s", csv_path)
        logger.info("Results saved to: %s", json_path)

    return all_results


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    setup_logging()

    parser = argparse.ArgumentParser(description="TPC-DS SQL endpoint benchmark runner.")
    parser.add_argument(
        "--config",
        default="benchmark/config.yaml",
        help="Path to config.yaml (default: benchmark/config.yaml)",
    )
    parser.add_argument(
        "--sf",
        nargs="+",
        default=None,
        help="Scale factors to run (e.g. --sf SF10 SF100). Defaults to all in config.",
    )
    parser.add_argument(
        "--endpoints",
        nargs="+",
        default=None,
        metavar="ENDPOINT_ID",
        help=(
            "Run only these endpoint IDs (e.g. --endpoints warehouse_frag lakehouse_frag). "
            "Defaults to all endpoints in config.yaml."
        ),
    )
    parser.add_argument(
        "--warm-only",
        action="store_true",
        help=(
            "Skip capacity pause/resume, skip the cold block, and skip the final pause. "
            "Runs only the warm block. Capacity must already be Active."
        ),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print what would be executed without actually running queries or touching capacity.",
    )
    args = parser.parse_args()

    with open(args.config, encoding="utf-8") as f:
        config = yaml.safe_load(f)

    scale_factors = args.sf or config.get("scale_factors", [])
    if not scale_factors:
        logger.error("No scale factors specified. Use --sf or set scale_factors in config.yaml.")
        sys.exit(1)

    run_benchmark(
        config,
        scale_factors,
        dry_run=args.dry_run,
        endpoints_filter=args.endpoints,
        warm_only=args.warm_only,
    )


if __name__ == "__main__":
    main()
