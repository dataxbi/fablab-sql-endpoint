"""
fragmentation/02_copy_into_wh.py
Load benchmark_frag.store_sales in Fabric Warehouse with many small Parquet files
to simulate table fragmentation via CSV COPY INTO.

NOTE: Fabric Warehouse COPY INTO does NOT support GZIP-compressed CSV files
(codec error -5 / invalid input stream). Plain CSV is required.

Approach:
  1. Split store_sales.csv (WSL) into {CHUNK_ROWS}-row chunks, stripping trailing
     pipe characters. Output: plain .csv files (no compression).
  2. Upload .csv chunks to OneLake via azcopy (AZCOPY_AUTO_LOGIN_TYPE=AZCLI).
  3. Run COPY INTO for each .csv file using {WORKERS} parallel WH connections.
     Each COPY INTO execution is one transaction → one small Parquet file in WH.

Usage:
    # Full pipeline (split + upload + COPY INTO)
    .venv\\Scripts\\python.exe fragmentation/02_copy_into_wh.py

    # Skip split (chunks already exist in WSL as .csv)
    .venv\\Scripts\\python.exe fragmentation/02_copy_into_wh.py --skip-split

    # Skip split+upload (files already in OneLake)
    .venv\\Scripts\\python.exe fragmentation/02_copy_into_wh.py --skip-split --skip-upload

    # Test COPY INTO with the first file only, then exit
    .venv\\Scripts\\python.exe fragmentation/02_copy_into_wh.py --skip-split --skip-upload --test

    # Dry run: print SQL without executing
    .venv\\Scripts\\python.exe fragmentation/02_copy_into_wh.py --skip-split --skip-upload --dry-run

Prerequisites:
    - WSL 2 with Ubuntu installed.
    - store_sales.csv for SF100 at ~/tpcds-data/sf100/store_sales.csv inside WSL.
    - azcopy on PATH; `az login` active (AZCOPY_AUTO_LOGIN_TYPE=AZCLI).
    - benchmark_frag.store_sales exists in WH_01 (run 00_setup_wh_frag.sql first).
    - FABRIC_WORKSPACE_ID and FABRIC_LAKEHOUSE_ID set in .env or as CLI args.
    - Active Fabric capacity.
"""

import argparse
import json
import logging
import os
import queue
import subprocess
import sys
import threading
import time
from pathlib import Path

from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from benchmark.connection import get_warehouse_connection

load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(threadName)s]: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------
DEFAULT_CHUNK_ROWS = 10_000
DEFAULT_WORKERS = 8
DEFAULT_WSL_SRC_CSV = "~/tpcds-data/sf100/store_sales.csv"
DEFAULT_WSL_CHUNKS_DIR = "~/tpcds-data/sf100/ss_frag"
DEFAULT_ONELAKE_FOLDER = "frag_input/store_sales"
DEFAULT_CHECKPOINT_FILE = Path(__file__).parent / ".copy_into_checkpoint.json"

# OneLake endpoints
_BLOB_BASE = "https://onelake.blob.fabric.microsoft.com"
_DFS_BASE = "https://onelake.dfs.fabric.microsoft.com"

# Well-known locations where azcopy may be installed but not on PATH
_AZCOPY_FALLBACK_DIRS = [
    Path.home() / "bin",
    Path("C:/tools"),
    Path("C:/azcopy"),
]


def _find_azcopy() -> str:
    """Return the azcopy executable path, searching fallback dirs if not on PATH."""
    import shutil
    exe = shutil.which("azcopy")
    if exe:
        return exe
    for d in _AZCOPY_FALLBACK_DIRS:
        candidate = d / "azcopy.exe"
        if candidate.exists():
            return str(candidate)
    raise FileNotFoundError(
        "azcopy not found on PATH or in known locations. "
        "Install from https://aka.ms/downloadazcopy-v10-windows and add to PATH."
    )


# ---------------------------------------------------------------------------
# WSL helpers
# ---------------------------------------------------------------------------

def _wsl_run(bash_script: str, check: bool = True) -> subprocess.CompletedProcess:
    """Execute a bash script inside WSL, streaming output to the logger."""
    logger.debug("WSL: %s", bash_script[:300])
    result = subprocess.run(
        ["wsl", "--", "bash", "-c", bash_script],
        check=check, text=True, capture_output=True,
    )
    if result.stdout.strip():
        for line in result.stdout.strip().splitlines():
            logger.info("  WSL: %s", line)
    if result.stderr.strip():
        for line in result.stderr.strip().splitlines():
            logger.debug("  WSL stderr: %s", line)
    return result


def _wsl_output(bash_script: str) -> str:
    """Execute bash in WSL and return stdout stripped."""
    return subprocess.run(
        ["wsl", "--", "bash", "-c", bash_script],
        check=True, text=True, capture_output=True,
    ).stdout.strip()


def _wsl_chunks_to_win_path(wsl_dir: str) -> str:
    """Convert a WSL path to a Windows UNC path using wslpath."""
    return _wsl_output(f"wslpath -w {wsl_dir}")


# ---------------------------------------------------------------------------
# Checkpoint helpers
# ---------------------------------------------------------------------------

def _load_checkpoint(path: Path) -> dict:
    if path.exists():
        with path.open(encoding="utf-8") as f:
            return json.load(f)
    return {"completed_files": []}


def _save_checkpoint(path: Path, completed: list[str]) -> None:
    tmp = path.with_suffix(".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump({"completed_files": completed}, f)
    tmp.replace(path)


# ---------------------------------------------------------------------------
# Phase 1: Split + Gzip
# ---------------------------------------------------------------------------

def phase_split(wsl_src_csv: str, wsl_chunks_dir: str, chunk_rows: int, dry_run: bool) -> None:
    """Split store_sales.csv into chunk_rows-row chunks, stripping trailing pipes.

    Fabric Warehouse COPY INTO does not support GZIP-compressed CSV files, so
    chunks are stored as plain .csv files (no compression).
    """
    logger.info("Phase 1: Split — chunk_rows=%d", chunk_rows)

    # Check how many .csv files already exist
    count_csv = int(_wsl_output(
        f"ls {wsl_chunks_dir}/part_*.csv 2>/dev/null | wc -l"
    ) or "0")
    if count_csv > 0:
        logger.info("Phase 1 skipped: %d .csv chunks already found in %s.",
                    count_csv, wsl_chunks_dir)
        return

    if dry_run:
        logger.info("[DRY-RUN] Would split %s → %s (chunk_rows=%d).",
                    wsl_src_csv, wsl_chunks_dir, chunk_rows)
        return

    logger.info("Running WSL split + sed (strip trailing |). This may take ~5-10 minutes...")
    # Write a helper script via stdin to avoid shell quoting issues through
    # Python subprocess → WSL. sed strips the trailing '|' that dsdgen
    # appends to each row so COPY INTO sees exactly 23 fields.
    helper_script = "/tmp/split_chunk.sh"
    helper_content = (
        b"#!/bin/bash\n"
        b"set -e\n"
        b"mkdir -p \"$2\"\n"
        b"sed 's/|[[:space:]]*$//' \"$1\" | split -l \"$3\" "
        b"  --numeric-suffixes=1 --suffix-length=5 "
        b"  --additional-suffix=.csv - \"$2/part_\"\n"
        b"echo \"Split done.\"\n"
    )
    subprocess.run(
        ["wsl", "--", "bash", "-c",
         f"cat > {helper_script} && chmod +x {helper_script}"],
        input=helper_content, check=True,
    )
    _wsl_run(f"{helper_script} {wsl_src_csv} {wsl_chunks_dir} {chunk_rows}")
    count = int(_wsl_output(
        f"ls {wsl_chunks_dir}/part_*.csv 2>/dev/null | wc -l"
    ))
    logger.info("Phase 1 complete: %d chunks ready.", count)


# ---------------------------------------------------------------------------
# Phase 2: Upload to OneLake
# ---------------------------------------------------------------------------

def phase_upload(wsl_chunks_dir: str, blob_url: str, dry_run: bool) -> None:
    """Upload .csv chunks from WSL to OneLake via azcopy."""
    logger.info("Phase 2: Uploading .csv chunks to OneLake...")

    win_path = _wsl_chunks_to_win_path(wsl_chunks_dir)
    logger.info("  Source: %s", win_path)
    logger.info("  Target: %s", blob_url)

    if dry_run:
        logger.info("[DRY-RUN] Would azcopy from %s to %s/", win_path, blob_url)
        return

    env = os.environ.copy()
    env["AZCOPY_AUTO_LOGIN_TYPE"] = "AZCLI"
    t0 = time.monotonic()
    azcopy = _find_azcopy()
    logger.info("  Using azcopy: %s", azcopy)
    subprocess.run(
        [
            azcopy, "copy",
            f"{win_path}\\*.csv",
            f"{blob_url}/",
            "--recursive=false",
            "--trusted-microsoft-suffixes=*.fabric.microsoft.com",
        ],
        check=True,
        env=env,
    )
    elapsed = time.monotonic() - t0
    logger.info("Phase 2 complete: upload finished in %.0f s.", elapsed)


# ---------------------------------------------------------------------------
# Phase 3: Parallel COPY INTO
# ---------------------------------------------------------------------------

def _build_copy_into_sql(dfs_url: str, filename: str) -> str:
    # Fabric Warehouse COPY INTO does not support GZIP-compressed CSV files;
    # plain CSV with auto-detected encoding is used instead.
    return (
        f"COPY INTO benchmark_frag.store_sales "
        f"FROM '{dfs_url}/{filename}' "
        f"WITH (FILE_TYPE = 'CSV', FIELDTERMINATOR = '|')"
    )


def _worker_thread(
    worker_id: int,
    file_queue: queue.Queue,
    completed: list[str],
    errors: list[tuple[str, str]],
    completed_lock: threading.Lock,
    checkpoint_file: Path,
    dfs_url: str,
    progress_counter: list[int],
    total_files: int,
    start_time: float,
) -> None:
    """Worker: open one WH connection and process files from the queue until empty."""
    logger.info("Worker %d starting — opening WH connection...", worker_id)
    try:
        conn = get_warehouse_connection()
    except Exception as exc:
        logger.error("Worker %d: failed to connect — %s", worker_id, exc)
        return

    logger.info("Worker %d connected.", worker_id)
    try:
        while True:
            try:
                fname = file_queue.get(block=False)
            except queue.Empty:
                break

            sql = _build_copy_into_sql(dfs_url, fname)
            try:
                conn.execute(sql)
                with completed_lock:
                    completed.append(fname)
                    progress_counter[0] += 1
                    done = progress_counter[0]
                    elapsed = time.monotonic() - start_time
                    rate = done / elapsed if elapsed > 0 else 0
                    eta_min = (total_files - done) / rate / 60 if rate > 0 else 0
                    if done % 100 == 0 or done <= 5:
                        _save_checkpoint(checkpoint_file, completed)
                        logger.info(
                            "Progress: %d/%d (%.1f%%) — %.1f files/s — ETA %.0f min",
                            done, total_files,
                            done / total_files * 100,
                            rate, eta_min,
                        )
            except Exception as exc:
                with completed_lock:
                    errors.append((fname, str(exc)))
                logger.error("Worker %d: COPY INTO failed for %s: %s", worker_id, fname, exc)

            file_queue.task_done()
    finally:
        conn.close()
        logger.info("Worker %d done.", worker_id)


def phase_copy_into(
    wsl_chunks_dir: str,
    dfs_url: str,
    workers: int,
    checkpoint_file: Path,
    truncate_first: bool,
    dry_run: bool,
    test_only: bool,
    total_chunks: int = 0,
) -> None:
    """Run parallel COPY INTO for all .csv chunks."""
    logger.info("Phase 3: COPY INTO with %d parallel workers.", workers)

    # Try to get the file list from WSL; fall back to a Python-generated list for
    # detached/CI environments where the wsl binary may not be accessible.
    raw = _wsl_output(
        f"ls {wsl_chunks_dir}/part_*.csv 2>/dev/null | xargs -I{{}} basename {{}}"
    )
    all_files = [f.strip() for f in raw.splitlines() if f.strip()]
    if not all_files:
        if total_chunks and total_chunks > 0:
            all_files = [f"part_{i:05d}.csv" for i in range(1, total_chunks + 1)]
            logger.warning(
                "WSL unavailable — using generated file list: part_00001.csv … part_%05d.csv (%d files).",
                total_chunks, total_chunks,
            )
        else:
            logger.error("No .csv files found in %s — run Phase 1 first.", wsl_chunks_dir)
            sys.exit(1)

    logger.info("Total .csv files: %d", len(all_files))

    if test_only:
        logger.info("TEST MODE: running COPY INTO for 1 file only.")
        all_files = all_files[:1]

    checkpoint = _load_checkpoint(checkpoint_file)
    completed_set = set(checkpoint["completed_files"])
    pending = [f for f in all_files if f not in completed_set]

    if completed_set and not test_only:
        logger.info("Resuming checkpoint: %d done, %d pending.", len(completed_set), len(pending))

    if not pending:
        logger.info("Nothing to do — all files already in checkpoint.")
        return

    if dry_run:
        logger.info("[DRY-RUN] Would run %d COPY INTO statements with %d workers.",
                    len(pending), workers)
        logger.info("[DRY-RUN] Example SQL:\n  %s",
                    _build_copy_into_sql(dfs_url, pending[0]))
        return

    # Truncate before loading (unless resuming or explicitly skipped)
    if truncate_first and not completed_set:
        logger.info("Truncating benchmark_frag.store_sales before loading...")
        conn = get_warehouse_connection()
        try:
            conn.execute("TRUNCATE TABLE benchmark_frag.store_sales")
            logger.info("Truncate complete.")
        finally:
            conn.close()

    # Fill queue
    file_q: queue.Queue = queue.Queue()
    for f in pending:
        file_q.put(f)

    completed: list[str] = list(completed_set)
    errors: list[tuple[str, str]] = []
    completed_lock = threading.Lock()
    progress_counter = [0]
    start_time = time.monotonic()

    threads = [
        threading.Thread(
            target=_worker_thread,
            args=(
                i, file_q, completed, errors, completed_lock,
                checkpoint_file, dfs_url, progress_counter, len(pending), start_time,
            ),
            name=f"worker-{i}",
            daemon=True,
        )
        for i in range(workers)
    ]

    for t in threads:
        t.start()

    try:
        file_q.join()
    except KeyboardInterrupt:
        logger.info("Interrupted — saving checkpoint...")
        with completed_lock:
            _save_checkpoint(checkpoint_file, completed)
        sys.exit(1)

    for t in threads:
        t.join()

    # Final checkpoint save
    with completed_lock:
        _save_checkpoint(checkpoint_file, completed)

    elapsed = time.monotonic() - start_time
    logger.info(
        "Phase 3 complete: %d succeeded, %d failed in %.0f s (%.1f files/s).",
        len(completed) - len(completed_set),
        len(errors),
        elapsed,
        len(pending) / elapsed if elapsed > 0 else 0,
    )

    if errors:
        logger.warning("%d COPY INTO failures — re-run to retry (checkpoint saved):", len(errors))
        for fname, err in errors[:10]:
            logger.warning("  %s: %s", fname, err)
        if len(errors) > 10:
            logger.warning("  ... and %d more.", len(errors) - 10)
    elif not test_only:
        logger.info("All files loaded successfully. Removing checkpoint file.")
        if checkpoint_file.exists():
            checkpoint_file.unlink()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Load benchmark_frag.store_sales in WH_01 via CSV COPY INTO. "
            "Splits store_sales.csv into 10K-row plain CSV chunks in WSL, uploads to "
            "OneLake, then runs parallel COPY INTO (one per file = one Parquet file). "
            "Note: Fabric Warehouse COPY INTO does not support GZIP-compressed CSV."
        ),
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--skip-split", action="store_true",
        help="Skip WSL split phase (plain .csv chunks already exist).",
    )
    parser.add_argument(
        "--skip-upload", action="store_true",
        help="Skip azcopy upload phase (chunks already in OneLake).",
    )
    parser.add_argument(
        "--no-truncate", action="store_true",
        help="Do not TRUNCATE the table before loading (use when resuming).",
    )
    parser.add_argument(
        "--workers", type=int, default=DEFAULT_WORKERS,
        help="Number of parallel WH connections for COPY INTO.",
    )
    parser.add_argument(
        "--total-chunks", type=int, default=28800,
        help=(
            "Total number of CSV chunk files (part_00001.csv … part_NNNNN.csv). "
            "Used as fallback when WSL is unavailable to list the chunks directory."
        ),
    )
    parser.add_argument(
        "--chunk-rows", type=int, default=DEFAULT_CHUNK_ROWS,
        help="Rows per CSV chunk (used in split phase).",
    )
    parser.add_argument(
        "--wsl-src-csv", default=DEFAULT_WSL_SRC_CSV,
        help="WSL path to the source store_sales.csv.",
    )
    parser.add_argument(
        "--wsl-chunks-dir", default=DEFAULT_WSL_CHUNKS_DIR,
        help="WSL directory where split chunks are stored.",
    )
    parser.add_argument(
        "--onelake-folder", default=DEFAULT_ONELAKE_FOLDER,
        help="OneLake Files sub-path for the chunks.",
    )
    parser.add_argument(
        "--workspace-id",
        default=os.getenv("FABRIC_WORKSPACE_ID", "f67c4250-c938-4c8d-b931-e446a0e04a01"),
        help="Fabric workspace GUID (or FABRIC_WORKSPACE_ID env var).",
    )
    parser.add_argument(
        "--lakehouse-id",
        default=os.getenv("FABRIC_LAKEHOUSE_ID", "d10ce80e-78a7-488e-981e-6227dbc63ed7"),
        help="Lakehouse item GUID (or FABRIC_LAKEHOUSE_ID env var).",
    )
    parser.add_argument(
        "--checkpoint-file", type=Path, default=DEFAULT_CHECKPOINT_FILE,
        help="JSON file used to checkpoint COPY INTO progress.",
    )
    parser.add_argument(
        "--test", action="store_true",
        help="Run COPY INTO for the first file only, then exit.",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Print what would be done without executing.",
    )
    args = parser.parse_args()

    blob_url = f"{_BLOB_BASE}/{args.workspace_id}/{args.lakehouse_id}/Files/{args.onelake_folder}"
    dfs_url = f"{_DFS_BASE}/{args.workspace_id}/{args.lakehouse_id}/Files/{args.onelake_folder}"

    logger.info("=== 02_copy_into_wh.py ===")
    logger.info("OneLake folder : %s", args.onelake_folder)
    logger.info("WH workers     : %d", args.workers)
    logger.info("Chunk rows     : %d", args.chunk_rows)
    logger.info("Checkpoint     : %s", args.checkpoint_file)

    if not args.skip_split:
        phase_split(args.wsl_src_csv, args.wsl_chunks_dir, args.chunk_rows, args.dry_run)
    else:
        logger.info("Phase 1 (split+gzip): skipped.")

    if not args.skip_upload:
        phase_upload(args.wsl_chunks_dir, blob_url, args.dry_run)
    else:
        logger.info("Phase 2 (upload): skipped.")

    phase_copy_into(
        wsl_chunks_dir=args.wsl_chunks_dir,
        dfs_url=dfs_url,
        workers=args.workers,
        checkpoint_file=args.checkpoint_file,
        truncate_first=not args.no_truncate,
        dry_run=args.dry_run,
        test_only=args.test,
        total_chunks=args.total_chunks,
    )


if __name__ == "__main__":
    main()
