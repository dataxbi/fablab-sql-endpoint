# Fragmentation Experiment

This directory contains the scripts for the **small-file fragmentation** benchmark experiment.
The goal is to measure how Parquet file fragmentation affects query performance on both
**Fabric Warehouse** (`WH_01`) and **Fabric Lakehouse SQL endpoint** (`LH_01`).

Both endpoints get a new schema `benchmark_frag` with the same 8 tables as the baseline
benchmarks. The 7 dimension tables are copied compactly. Only `store_sales` is fragmented:
all **287,997,099 SF100 rows** are present, but split across many small Parquet files.

| Endpoint | Schema | `store_sales` fragmentation |
|----------|--------|-----------------------------|
| Lakehouse baseline | `benchmark_default` | Compact (post-OPTIMIZE, ~100–200 files) |
| **Lakehouse frag** | `benchmark_frag` | ~28,800 files × 10,000 rows |
| Warehouse baseline | `benchmark` | Compact (CTAS bulk, ~100–200 files) |
| **Warehouse frag** | `benchmark_frag` | ~28,800 files × 10,000 rows |

---

## Scripts

### `00_setup_wh_frag.sql` — Warehouse setup (run once)

Creates `benchmark_frag` schema, an **empty** `store_sales` table, and compact dimension tables.

```bash
sqlcmd -S <WH_SERVER> -d WH_01 -G \
       -i fragmentation/00_setup_wh_frag.sql -l 3600
```

> **Note**: `store_sales` is intentionally left empty. Fill it with `01_insert_wh.py`.

---

### `00_setup_lh_frag.ipynb` — Lakehouse setup (run once, in Fabric)

Fabric PySpark notebook. Drops `benchmark_frag.store_sales` (clean Delta log), copies dimension tables from `benchmark_default` and writes
`store_sales` with `maxRecordsPerFile=10000` in a single Spark job.

**Upload and run** this notebook in LH_01 on Fabric. Estimated runtime: ~30–60 minutes.

---

### `01_insert_wh.py` — ~~Warehouse insert loop~~ (deprecated: too slow)

> **Deprecated**: OFFSET/FETCH degrades fatally on Fabric Warehouse as the offset grows
> (50+ s/batch at batch 1,200). Use `02_copy_into_wh.py` instead.

---

### `02_copy_into_wh.py` — Warehouse COPY INTO via CSV (recommended)

Faster approach: splits `store_sales.csv` (SF100) in WSL into 10K-row gzipped chunks,
uploads them to OneLake, then runs `COPY INTO` in parallel (one statement per file).
Each `COPY INTO` = one transaction = one small Parquet file in WH.

**Prerequisites:**
- `store_sales.csv` for SF100 at `~/tpcds-data/sf100/store_sales.csv` inside WSL
- `azcopy` on PATH; `az login` active
- `benchmark_frag.store_sales` exists (run `00_setup_wh_frag.sql` first)
- `FABRIC_WORKSPACE_ID` and `FABRIC_LAKEHOUSE_ID` set in `.env`

```bash
# Full pipeline: split → gzip → upload → COPY INTO
py fragmentation/02_copy_into_wh.py

# Skip split+gzip if chunks already exist in WSL:
py fragmentation/02_copy_into_wh.py --skip-split

# Skip upload too (files already in OneLake):
py fragmentation/02_copy_into_wh.py --skip-split --skip-upload

# Test with 1 file before launching full run:
py fragmentation/02_copy_into_wh.py --skip-split --skip-upload --test

# Resume after interruption (checkpoint is in .copy_into_checkpoint.json):
py fragmentation/02_copy_into_wh.py --skip-split --skip-upload --no-truncate
```

**Estimated runtimes** (SF100, 287M rows, 28,800 files, 8 workers):

| Phase | Estimated time |
|-------|---------------|
| Split + gzip (WSL, 8 workers) | ~20–30 min |
| azcopy upload to OneLake (~6–8 GB) | ~10–15 min |
| 28,800 × COPY INTO (8 parallel workers) | ~2 h |
| **Total** | **~3–4 h** |

**Checkpoint**: COPY INTO progress saved to `fragmentation/.copy_into_checkpoint.json`.
Re-run with `--skip-split --skip-upload --no-truncate` to resume after interruption.

---

## Running the fragmentation benchmark

Once both setups are complete, run the benchmark against only the fragmented endpoints:

```bash
py benchmark/runner.py --endpoints warehouse_frag lakehouse_frag
```

Results will be saved to `results/benchmark_{timestamp}.csv/.json` alongside the baseline results.

Compare fragmented vs. baseline latencies in `analysis/analyze_results.ipynb`.

---

## Files generated (git-ignored)

- `fragmentation/.wh_insert_checkpoint.json` — OFFSET/FETCH insert progress (deprecated)
- `fragmentation/.copy_into_checkpoint.json` — COPY INTO progress checkpoint
