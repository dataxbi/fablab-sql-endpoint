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
| **Lakehouse frag** | `benchmark_frag` | ~288,000 files × 1,000 rows |
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

Fabric PySpark notebook. Copies dimension tables from `benchmark_default` and writes
`store_sales` with `maxRecordsPerFile=1000` in a single Spark job.

**Upload and run** this notebook in LH_01 on Fabric. Estimated runtime: ~30–60 minutes.

---

### `01_insert_wh.py` — Warehouse insert loop (long-running, resumable)

Inserts all rows from `benchmark.store_sales` into `benchmark_frag.store_sales` in batches
using `OFFSET/FETCH`. Each INSERT creates a separate Parquet file.

```bash
# Dry run (no actual inserts — counts batches only)
py fragmentation/01_insert_wh.py --dry-run

# Default: 10,000 rows/batch (~28,800 batches, ~24–40 h total)
py fragmentation/01_insert_wh.py

# Custom batch size
py fragmentation/01_insert_wh.py --batch-size 5000
```

**Checkpoint**: progress is saved to `fragmentation/.wh_insert_checkpoint.json` after each
batch. If the script is interrupted (Ctrl+C or session end), re-run the same command and it
will resume from the last completed batch.

To start over from scratch, delete the checkpoint file:
```bash
Remove-Item fragmentation/.wh_insert_checkpoint.json
```

**Estimated runtimes** (SF100, 288M rows):

| `--batch-size` | Batches | Estimated time |
|----------------|---------|----------------|
| 10,000 (default) | ~28,800 | ~24–40 h |
| 50,000 | ~5,760 | ~5–8 h |
| 100,000 | ~2,880 | ~3–5 h |

> Larger batches = fewer files = less fragmentation. Use the default for maximum fragmentation.

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

- `fragmentation/.wh_insert_checkpoint.json` — WH insert progress checkpoint
