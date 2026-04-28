"""
fragmentation/01_insert_wh.py
Insert all rows from benchmark.store_sales into benchmark_frag.store_sales in small batches,
creating many small Parquet files to simulate a fragmented table in Fabric Warehouse.

Each INSERT generates a separate Parquet file in the Warehouse's underlying storage.
Batches are read from benchmark.store_sales using ORDER BY + OFFSET/FETCH for deterministic,
resumable iteration.

Progress is saved to a JSON checkpoint file after every INSERT. If the script is interrupted,
re-running it continues from the last completed batch without losing progress.

Usage:
    py fragmentation/01_insert_wh.py [--batch-size 10000] [--dry-run]
                                     [--checkpoint-file fragmentation/.wh_insert_checkpoint.json]

Prerequisites:
    - benchmark_frag.store_sales must exist and be empty (run 00_setup_wh_frag.sql first).
    - benchmark.store_sales must be populated (SF100).
    - WAREHOUSE_SERVER and WAREHOUSE_DATABASE env vars must be set (or .env loaded).
    - Active Fabric capacity.
"""

import argparse
import json
import logging
import os
import sys
import time
from pathlib import Path

from dotenv import load_dotenv

# Ensure project root is on sys.path so benchmark package is importable
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from benchmark.connection import get_warehouse_connection
from benchmark.utils import setup_logging

load_dotenv()
logger = logging.getLogger(__name__)

DEFAULT_BATCH_SIZE = 10_000
DEFAULT_CHECKPOINT_FILE = Path(__file__).parent / ".wh_insert_checkpoint.json"

# ORDER BY columns used for deterministic OFFSET/FETCH pagination.
# These three columns form the composite key of store_sales.
_ORDER_BY = "ss_sold_date_sk, ss_item_sk, ss_ticket_number"


# ---------------------------------------------------------------------------
# Checkpoint helpers
# ---------------------------------------------------------------------------

def _load_checkpoint(path: Path) -> dict:
    if path.exists():
        with path.open(encoding="utf-8") as f:
            return json.load(f)
    return {"offset": 0, "total_rows": None, "batches_done": 0}


def _save_checkpoint(path: Path, offset: int, total_rows: int, batches_done: int) -> None:
    tmp = path.with_suffix(".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(
            {"offset": offset, "total_rows": total_rows, "batches_done": batches_done},
            f,
        )
    tmp.replace(path)


# ---------------------------------------------------------------------------
# Main logic
# ---------------------------------------------------------------------------

def run_inserts(batch_size: int, checkpoint_file: Path, dry_run: bool) -> None:
    checkpoint = _load_checkpoint(checkpoint_file)
    offset: int = checkpoint["offset"]
    total_rows: int | None = checkpoint["total_rows"]
    batches_done: int = checkpoint["batches_done"]

    if offset > 0:
        logger.info(
            "Resuming from checkpoint: offset=%d, batches_done=%d",
            offset, batches_done,
        )

    conn = get_warehouse_connection()
    try:
        cursor = conn.cursor()

        # Determine total rows on first run
        if total_rows is None:
            logger.info("Counting rows in benchmark.store_sales ...")
            cursor.execute("SELECT COUNT(*) FROM benchmark.store_sales")
            total_rows = cursor.fetchone()[0]
            logger.info("Total rows to insert: %d", total_rows)

        total_batches = (total_rows + batch_size - 1) // batch_size
        logger.info(
            "Will run %d batches of %d rows each (offset start: %d).",
            total_batches - batches_done, batch_size, offset,
        )

        while offset < total_rows:
            batch_num = batches_done + 1
            sql = (
                f"INSERT INTO benchmark_frag.store_sales "
                f"SELECT * FROM benchmark.store_sales "
                f"ORDER BY {_ORDER_BY} "
                f"OFFSET {offset} ROWS FETCH NEXT {batch_size} ROWS ONLY"
            )

            if dry_run:
                logger.info(
                    "[DRY-RUN] batch %d/%d — OFFSET %d FETCH %d",
                    batch_num, total_batches, offset, batch_size,
                )
            else:
                t0 = time.monotonic()
                cursor.execute(sql)
                elapsed = time.monotonic() - t0

                offset += batch_size
                batches_done += 1
                _save_checkpoint(checkpoint_file, offset, total_rows, batches_done)

                pct = min(offset / total_rows * 100, 100)
                if batches_done % 100 == 0 or batches_done == 1:
                    logger.info(
                        "Batch %d/%d done — rows so far: %d/%d (%.1f%%) — %.1f s/batch",
                        batches_done, total_batches, min(offset, total_rows), total_rows,
                        pct, elapsed,
                    )
                continue

            # dry-run: advance offset without executing
            offset += batch_size
            batches_done += 1

        if dry_run:
            logger.info("[DRY-RUN] Would have run %d batches total.", total_batches)
        else:
            logger.info(
                "All %d batches complete. %d rows inserted into benchmark_frag.store_sales.",
                batches_done, total_rows,
            )
            # Remove checkpoint on successful completion
            if checkpoint_file.exists():
                checkpoint_file.unlink()
                logger.info("Checkpoint file removed.")

    except KeyboardInterrupt:
        logger.warning("Interrupted by user. Progress saved to checkpoint: %s", checkpoint_file)
        raise
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    setup_logging()

    parser = argparse.ArgumentParser(
        description=(
            "Insert all rows from benchmark.store_sales into benchmark_frag.store_sales "
            "in small batches to create Parquet file fragmentation in Fabric Warehouse."
        )
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=DEFAULT_BATCH_SIZE,
        help=f"Rows per INSERT batch (default: {DEFAULT_BATCH_SIZE:,}). "
             "Each batch creates one Parquet file.",
    )
    parser.add_argument(
        "--checkpoint-file",
        type=Path,
        default=DEFAULT_CHECKPOINT_FILE,
        help=f"Path to the JSON checkpoint file (default: {DEFAULT_CHECKPOINT_FILE}). "
             "Allows resuming after interruption.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print what would be executed without running any INSERT statements.",
    )
    args = parser.parse_args()

    logger.info(
        "Starting WH fragmentation insert: batch_size=%d, checkpoint=%s, dry_run=%s",
        args.batch_size, args.checkpoint_file, args.dry_run,
    )
    run_inserts(args.batch_size, args.checkpoint_file, args.dry_run)


if __name__ == "__main__":
    main()
