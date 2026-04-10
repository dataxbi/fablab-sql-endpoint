"""
data_generation/upload_to_onelake.py
Split large TPC-DS CSVs, gzip the chunks, and upload everything to OneLake.

Runs from Windows. Split and gzip steps are executed inside WSL; upload uses
azcopy with Azure CLI credentials.

Usage:
    py data_generation/upload_to_onelake.py \\
        --sf 100 \\
        --src "\\\\wsl$\\Ubuntu-24.04\\home\\<user>\\tpcds-data" \\
        --workspace-id <workspace-id> \\
        --lakehouse-id <lakehouse-id>

    # Skip split/gzip if already done (re-upload only):
    py data_generation/upload_to_onelake.py --sf 100 ... --skip-split

Environment variables (override defaults):
    FABRIC_WORKSPACE_ID   Fabric workspace GUID
    FABRIC_LAKEHOUSE_ID   Lakehouse item GUID
"""

import argparse
import logging
import os
import subprocess
import sys
from pathlib import Path, PurePosixPath

from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

# Tables large enough to require split+gzip (others upload directly as CSV).
SPLIT_TABLES = {"store_sales", "catalog_sales", "web_sales", "inventory"}

# Row chunk size for split. Each chunk is ~100-200 MB uncompressed.
SPLIT_ROWS = 2_000_000


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _run(cmd: list[str], check: bool = True, **kwargs) -> subprocess.CompletedProcess:
    logger.debug("Running: %s", " ".join(str(c) for c in cmd))
    return subprocess.run(cmd, check=check, text=True, **kwargs)


def _wsl_path(windows_path: str) -> str:
    """Convert a Windows UNC path (\\wsl$\\...) to a WSL-native path."""
    p = windows_path.replace("\\", "/")
    # \\wsl$\Ubuntu-24.04\home\... → /home/...
    if p.startswith("//wsl$/") or p.startswith("//wsl.localhost/"):
        parts = p.lstrip("/").split("/", 2)  # ['wsl$', 'distro', 'rest']
        return "/" + parts[2] if len(parts) == 3 else "/"
    # /mnt/c/... paths pass through unchanged
    return p


def _wsl_run(bash_script: str) -> None:
    """Execute a bash script string inside WSL."""
    _run(["wsl", "--", "bash", "-c", bash_script])


def _azcopy_available() -> bool:
    result = subprocess.run(["azcopy", "--version"], capture_output=True, text=True)
    return result.returncode == 0


# ---------------------------------------------------------------------------
# Step 1 — Split large CSVs in WSL
# ---------------------------------------------------------------------------

def split_large_tables(src_dir_wsl: str, sf: int) -> None:
    """Split large CSV files into SPLIT_ROWS-row chunks inside WSL."""
    sf_dir = f"{src_dir_wsl}/sf{sf}"
    split_dir = f"{sf_dir}/split"

    for table in sorted(SPLIT_TABLES):
        src_csv = f"{sf_dir}/{table}.csv"
        out_dir = f"{split_dir}/{table}"
        script = (
            f"set -e; "
            f"mkdir -p {out_dir}; "
            f"if ls {out_dir}/part_*.csv 2>/dev/null | head -1 | grep -q .; then "
            f"  echo 'SKIP: {table} already split'; "
            f"else "
            f"  echo 'Splitting {table}...'; "
            f"  split -l {SPLIT_ROWS} --additional-suffix=.csv {src_csv} {out_dir}/part_; "
            f"  echo 'Done splitting {table}'; "
            f"fi"
        )
        logger.info("Splitting %s (SF%s)...", table, sf)
        _wsl_run(script)


# ---------------------------------------------------------------------------
# Step 2 — Gzip chunks in WSL
# ---------------------------------------------------------------------------

def gzip_chunks(src_dir_wsl: str, sf: int) -> None:
    """Gzip all split CSV chunks in parallel (4 workers)."""
    split_dir = f"{src_dir_wsl}/sf{sf}/split"

    for table in sorted(SPLIT_TABLES):
        table_dir = f"{split_dir}/{table}"
        script = (
            f"set -e; "
            f"CSV_COUNT=$(find {table_dir} -name '*.csv' 2>/dev/null | wc -l); "
            f"if [ \"$CSV_COUNT\" -eq 0 ]; then "
            f"  echo 'SKIP: {table} — no uncompressed chunks found'; "
            f"else "
            f"  echo \"Compressing $CSV_COUNT chunks for {table}...\"; "
            f"  find {table_dir} -name '*.csv' | xargs -P4 gzip; "
            f"  echo 'Done compressing {table}'; "
            f"fi"
        )
        logger.info("Gzipping %s chunks (SF%s)...", table, sf)
        _wsl_run(script)


# ---------------------------------------------------------------------------
# Step 3 — Upload to OneLake with azcopy
# ---------------------------------------------------------------------------

def upload_to_onelake(
    src_dir_windows: str,
    sf: int,
    workspace_id: str,
    lakehouse_id: str,
) -> None:
    """Upload CSV files (small tables) and split/*.csv.gz (large tables) to OneLake."""
    if not _azcopy_available():
        logger.error("azcopy not found on PATH. Install from https://aka.ms/downloadazcopy-v10-windows")
        sys.exit(1)

    env = os.environ.copy()
    env["AZCOPY_AUTO_LOGIN_TYPE"] = "AZCLI"

    base_url = (
        f"https://onelake.blob.fabric.microsoft.com"
        f"/{workspace_id}/{lakehouse_id}/Files/tpcds/sf{sf}"
    )
    trusted = "--trusted-microsoft-suffixes=*.fabric.microsoft.com"
    sf_dir = str(Path(src_dir_windows) / f"sf{sf}")

    # -- Small tables: plain CSV files in sf_dir root --
    logger.info("Uploading small tables (plain CSV) to OneLake sf%s...", sf)
    _run(
        [
            "azcopy", "copy",
            f"{sf_dir}\\*.csv",
            f"{base_url}/",
            "--recursive=false",
            trusted,
        ],
        env=env,
    )

    # -- Large tables: split/*.csv.gz --
    split_dir = str(Path(sf_dir) / "split")
    logger.info("Uploading split+gzip chunks to OneLake sf%s/split/...", sf)
    _run(
        [
            "azcopy", "copy",
            split_dir,
            f"{base_url}/",
            "--recursive",
            trusted,
        ],
        env=env,
    )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Split large TPC-DS CSVs, gzip the chunks, and upload to OneLake. "
            "Split/gzip run in WSL; upload uses azcopy with Azure CLI credentials."
        )
    )
    parser.add_argument(
        "--sf", type=int, required=True,
        help="Scale factor (e.g. 10, 100)",
    )
    parser.add_argument(
        "--src",
        default=r"\\wsl$\Ubuntu-24.04\home\nelson\tpcds-data",
        help=(
            "Windows path to the TPC-DS data root (e.g. \\\\wsl$\\Ubuntu-24.04\\home\\nelson\\tpcds-data). "
            "Must be accessible as a UNC path from Windows."
        ),
    )
    parser.add_argument(
        "--workspace-id",
        default=os.getenv("FABRIC_WORKSPACE_ID"),
        help="Fabric workspace GUID (env: FABRIC_WORKSPACE_ID)",
    )
    parser.add_argument(
        "--lakehouse-id",
        default=os.getenv("FABRIC_LAKEHOUSE_ID"),
        help="Lakehouse item GUID (env: FABRIC_LAKEHOUSE_ID)",
    )
    parser.add_argument(
        "--skip-split", action="store_true",
        help="Skip split+gzip steps (use if chunks already prepared)",
    )
    parser.add_argument(
        "--skip-upload", action="store_true",
        help="Skip azcopy upload (useful for testing split/gzip only)",
    )
    parser.add_argument(
        "--verbose", "-v", action="store_true",
        help="Enable DEBUG logging",
    )
    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    if not args.workspace_id:
        parser.error("--workspace-id is required (or set FABRIC_WORKSPACE_ID)")
    if not args.lakehouse_id:
        parser.error("--lakehouse-id is required (or set FABRIC_LAKEHOUSE_ID)")

    src_wsl = _wsl_path(args.src)
    logger.info("=== upload_to_onelake | SF%s ===", args.sf)
    logger.info("Source (Windows): %s", args.src)
    logger.info("Source (WSL):     %s", src_wsl)
    logger.info("Workspace ID:     %s", args.workspace_id)
    logger.info("Lakehouse ID:     %s", args.lakehouse_id)

    if not args.skip_split:
        split_large_tables(src_wsl, args.sf)
        gzip_chunks(src_wsl, args.sf)
    else:
        logger.info("Skipping split+gzip (--skip-split)")

    if not args.skip_upload:
        upload_to_onelake(args.src, args.sf, args.workspace_id, args.lakehouse_id)
    else:
        logger.info("Skipping upload (--skip-upload)")

    logger.info("=== Done ===")


if __name__ == "__main__":
    main()
