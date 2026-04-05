"""
benchmark/utils.py
Shared utilities: timer, result model, logging setup and serialisation helpers.
"""

import csv
import json
import logging
import os
import time
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

def setup_logging(level: int = logging.INFO) -> None:
    logging.basicConfig(
        level=level,
        format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


# ---------------------------------------------------------------------------
# Timer context manager
# ---------------------------------------------------------------------------

class Timer:
    """Context manager that measures wall-clock elapsed time in milliseconds."""

    def __init__(self) -> None:
        self.elapsed_ms: float = 0.0
        self._start: float = 0.0

    def __enter__(self) -> "Timer":
        self._start = time.perf_counter()
        return self

    def __exit__(self, *_) -> None:
        self.elapsed_ms = (time.perf_counter() - self._start) * 1000


# ---------------------------------------------------------------------------
# Result model
# ---------------------------------------------------------------------------

@dataclass
class RunResult:
    endpoint: str
    scale_factor: str
    query_id: str
    cache_mode: str        # "cold" | "warm"
    repetition: int
    elapsed_ms: float
    rows_returned: int
    status: str            # "success" | "error" | "timeout"
    error_message: str = ""
    run_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    timestamp: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )


# ---------------------------------------------------------------------------
# Serialisation
# ---------------------------------------------------------------------------

_CSV_FIELDS = [
    "run_id", "timestamp", "endpoint", "scale_factor", "query_id",
    "cache_mode", "repetition", "elapsed_ms", "rows_returned",
    "status", "error_message",
]


def save_results(results: list[RunResult], results_dir: str) -> tuple[str, str]:
    """
    Write results to CSV and JSON files in results_dir.
    Returns (csv_path, json_path).
    """
    Path(results_dir).mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    csv_path = os.path.join(results_dir, f"benchmark_{ts}.csv")
    json_path = os.path.join(results_dir, f"benchmark_{ts}.json")

    rows = [asdict(r) for r in results]

    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=_CSV_FIELDS)
        writer.writeheader()
        writer.writerows(rows)

    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(rows, f, indent=2, ensure_ascii=False)

    return csv_path, json_path
