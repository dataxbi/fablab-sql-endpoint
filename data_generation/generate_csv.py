"""
data_generation/generate_csv.py
TPC-DS data generator wrapper.
Invokes dsdgen for the specified scale factors and organises output CSV files
under data/sfXX/. Data files are git-ignored and never committed.

Requirements:
  - dsdgen compiled binary available on PATH or at DSDGEN_PATH env var
  - Output directory defaults to <project_root>/data/

Usage:
    py data_generation/generate_csv.py --sf 10 [--sf 100] [--sf 1000] [--out data]
    py data_generation/generate_csv.py --sf 10 --dsdgen /path/to/dsdgen
"""

import argparse
import logging
import os
import shutil
import subprocess
from pathlib import Path

logger = logging.getLogger(__name__)

# TPC-DS table names produced by dsdgen
TPCDS_TABLES = [
    "call_center", "catalog_page", "catalog_returns", "catalog_sales",
    "customer", "customer_address", "customer_demographics", "date_dim",
    "household_demographics", "income_band", "inventory", "item",
    "promotion", "reason", "ship_mode", "store", "store_returns",
    "store_sales", "time_dim", "warehouse", "web_page", "web_returns",
    "web_sales", "web_site",
]


def _find_dsdgen() -> str:
    """Return path to dsdgen binary (env var > PATH > common locations)."""
    if path := os.getenv("DSDGEN_PATH"):
        return path
    if found := shutil.which("dsdgen"):
        return found
    # Common location after building tpcds-kit on Linux/macOS
    candidate = Path(__file__).resolve().parents[1] / "tools" / "dsdgen"
    if candidate.exists():
        return str(candidate)
    raise FileNotFoundError(
        "dsdgen not found. Install tpcds-kit, compile dsdgen and either:\n"
        "  - Add it to PATH, or\n"
        "  - Set DSDGEN_PATH=/path/to/dsdgen"
    )


def generate(scale_factor: int, output_base: Path, dsdgen_bin: str) -> Path:
    """
    Run dsdgen for a given scale factor.
    CSV files are written to output_base/sfXX/.
    Returns the output directory path.
    """
    sf_dir = output_base / f"sf{scale_factor}"
    sf_dir.mkdir(parents=True, exist_ok=True)

    logger.info("Generating TPC-DS data for SF%d into %s ...", scale_factor, sf_dir)

    cmd = [
        dsdgen_bin,
        "-SCALE", str(scale_factor),
        "-DIR", str(sf_dir),
        "-TERMINATE", "Y",   # add | terminator at end of each row
        "-FORCE",            # overwrite existing files
        "-QUIET",
    ]
    logger.debug("Running: %s", " ".join(cmd))
    result = subprocess.run(cmd, capture_output=True, text=True, cwd=sf_dir)

    if result.returncode != 0:
        raise RuntimeError(
            f"dsdgen failed for SF{scale_factor}:\n{result.stderr.strip()}"
        )

    # Rename .dat files to .csv for consistency
    for dat_file in sf_dir.glob("*.dat"):
        dat_file.rename(dat_file.with_suffix(".csv"))

    csv_files = list(sf_dir.glob("*.csv"))
    logger.info(
        "SF%d complete: %d CSV files in %s", scale_factor, len(csv_files), sf_dir
    )
    return sf_dir


def main() -> None:
    logging.basicConfig(
        level=logging.INFO, format="%(levelname)s: %(message)s"
    )

    project_root = Path(__file__).resolve().parents[1]

    parser = argparse.ArgumentParser(
        description="Generate TPC-DS CSV data using dsdgen."
    )
    parser.add_argument(
        "--sf",
        type=int,
        nargs="+",
        required=True,
        help="Scale factor(s) to generate, e.g. --sf 10 100 1000",
    )
    parser.add_argument(
        "--out",
        default=str(project_root / "data"),
        help="Base output directory (default: <project_root>/data)",
    )
    parser.add_argument(
        "--dsdgen",
        default=None,
        help="Path to dsdgen binary (overrides DSDGEN_PATH env var and PATH lookup)",
    )
    args = parser.parse_args()

    dsdgen_bin = args.dsdgen or _find_dsdgen()
    output_base = Path(args.out)

    for sf in args.sf:
        generate(sf, output_base, dsdgen_bin)

    logger.info("All scale factors generated. Data location: %s", output_base)


if __name__ == "__main__":
    main()
