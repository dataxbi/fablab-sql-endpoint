# GitHub Copilot Instructions — FabLab SQL Endpoint Benchmark

## Project Overview

This project benchmarks the performance of **Microsoft Fabric SQL endpoints**: Fabric Lakehouse SQL endpoint vs. Fabric Warehouse, using representative TPC-DS queries across multiple data scales and table configurations.

The goal is to produce objective latency metrics that inform decisions about which Fabric SQL engine to use for a given workload pattern.

---

## Repository Layout

```
fablab-sql-endpoint/
├── .github/copilot-instructions.md   ← this file
├── specs/especificaciones.md         ← authoritative spec (Spanish)
├── provision/
│   ├── setup_fabric.py               ← create workspace/lakehouse/warehouse via az rest
│   └── capacity_manager.py           ← pause/resume Fabric capacity with polling
├── data_generation/
│   └── generate_csv.py               ← dsdgen wrapper, outputs native CSV
├── ingestion/
│   ├── 01_lakehouse_ingest.ipynb     ← Spark: CSV → Delta (3 configs)
│   ├── 02_warehouse_ingest.sql       ← T-SQL CTAS cross-DB: LH_01 → WH_01
│   └── table_configs.py              ← Delta table config definitions + StructType schemas
├── sql/
│   ├── q01_simple_agg.sql            ← TPC-DS Q29 style
│   ├── q02_large_join.sql            ← TPC-DS Q19 style
│   ├── q03_top_n_selective.sql       ← TPC-DS Q6/Q42 style
│   ├── q04_complex_tpcds.sql         ← TPC-DS Q72/Q14 style (CTE + subquery)
│   └── q05_window_function.sql       ← TPC-DS Q35/Q86 style (RANK/ROW_NUMBER)
├── benchmark/
│   ├── runner.py                     ← main benchmark executor
│   ├── config.yaml                   ← test matrix definition
│   ├── connection.py                 ← pyodbc connection management
│   └── utils.py                      ← timer, logging, result serialization
├── fragmentation/
│   ├── 00_setup_wh_frag.sql          ← DDL empty store_sales + CTAS 7 dimensions → benchmark_frag (WH)
│   ├── 00_setup_lh_frag.ipynb        ← Spark: compact dimensions + fragmented store_sales (LH)
│   ├── 01_insert_wh.py               ← OFFSET/FETCH INSERT loop with checkpoint (WH)
│   └── README.md
├── results/                          ← CSV/JSON output (committed to repo)
├── analysis/
│   └── analyze_results.ipynb         ← comparison charts and statistics
├── .env.example                      ← environment variable template
├── .gitignore
└── requirements.txt
```

---

## Fabric Endpoints Under Test

| Endpoint ID | Type | Delta Config |
|-------------|------|--------------|
| `lakehouse_default` | Lakehouse SQL endpoint | No partition, no V-order |
| `lakehouse_partitioned` | Lakehouse SQL endpoint | PARTITION BY `ss_sold_date_sk` |
| `lakehouse_vorder` | Lakehouse SQL endpoint | V-Order enabled at write time |
| `warehouse` | Fabric Warehouse | Standard configuration |
| `lakehouse_frag` | Lakehouse SQL endpoint | Fragmented — `store_sales` with ~288K Parquet files (1K rows each) |
| `warehouse_frag` | Fabric Warehouse | Fragmented — `store_sales` with ~28.8K Parquet files (~10K rows each) |

> **Note on OPTIMIZE**: after ingesting each configuration, `OPTIMIZE` (without ZORDER) is run on all tables for Parquet file compaction. This is standard Delta maintenance, not a benchmark configuration. ZORDER was discarded due to disproportionate cost at scale.

---

## Test Matrix

- **Scale factors**: SF100 only (~100 GB). SF10 (~10 GB) was used solely for ingestion validation and is not included in final results.
- **Queries**: Q1–Q5 (see `sql/`)
- **Cache modes**: cold (first run after capacity resume), warm (3 repetitions on hot capacity)
- **Total executions**: 80 (cold: 20, warm: 60) + 1 capacity pause/resume cycle

### Execution Order
```
1. Resume Fabric capacity → poll until Active
2. Cold block: run all (endpoint × query) once — true cold cache
3. Warm block: run all (endpoint × query) 3 times — hot cache
4. Pause Fabric capacity → poll until Paused
```

---

## Data Generation

- Tool: **dsdgen** (TPC-DS kit), outputs native CSV — no format conversion
- Script: `data_generation/generate_csv.py`
- Data stored in `data/sfXX/` (git-ignored, never committed)
- Generation and ingestion time are **not measured** in the benchmark

---

## Key Design Decisions

1. **Cold cache via capacity pause/resume** — the only reliable way to flush all in-memory caches in Fabric. 1 cycle total (SF100 only).
2. **Secrets via environment variables only** — never hardcoded. See `.env.example`.
3. **Configurable resource names** — workspace, lakehouse and warehouse names are all configurable via CLI args (`--workspace`, `--lh`, `--wh`) or env vars (`FABRIC_WORKSPACE_NAME`, `FABRIC_LAKEHOUSE_NAME`, `FABRIC_WAREHOUSE_NAME`).
4. **capacity_manager.py is a standalone module** — reused by both `setup_fabric.py` and `benchmark/runner.py`. It polls the Fabric REST API until the capacity reaches the expected state, with a configurable timeout (default: 10 minutes).
5. **Authentication via Azure CLI** — `az login` only, no service principals or secrets in code.
6. **Read-only queries** — all benchmark queries are SELECT statements, ensuring compatibility with both endpoints without permission differences.
7. **Timeout per query** — configurable in `config.yaml` to prevent slow queries from blocking the suite.
8. **pyarrow removed** — data format is native CSV from dsdgen; no Parquet conversion needed.
9. **Schema-per-config for Lakehouse** — each of the 3 Lakehouse configs writes to its own schema within the same Lakehouse database: `benchmark_default`, `benchmark_partitioned`, `benchmark_vorder`. SQL queries have no schema prefix; the runner executes `USE {schema}` immediately after connecting for Lakehouse endpoints. The `warehouse` endpoint has no `schema` field and uses its default schema.
10. **Explicit StructType schemas for ingestion** — all ingestion scripts define full `StructType` schemas for every TPC-DS table (never `inferSchema`). This ensures consistent column names and types (e.g. `LongType` for SK keys, `DecimalType(7,2)` for monetary columns). The partition column in `benchmark_partitioned` is `ss_sold_date_sk` (real column name, not positional `_c0`). Schema definitions live in `ingestion/table_configs.py`.
11. **`overwriteSchema=True` on Delta writes** — the lakehouse ingestion notebook uses `.option('overwriteSchema', 'true')` on all Delta write operations. This is required when tables already exist with a prior schema (e.g. `_c0` columns from a previous `inferSchema` run), since Delta rejects schema changes by default. Combined with `mode('overwrite')`, tables are fully recreated with the correct schema.
12. **SF10 for ingestion validation only** — SF10 data was ingested to validate the pipeline (dsdgen → CSV → Delta → OPTIMIZE) and verify query execution. SF10 results are not included in the final benchmark report. The benchmark runner runs exclusively on SF100.
13. **Warehouse ingestion via T-SQL CTAS cross-database** — the `com.microsoft.fabric.spark.write` Spark connector is only available inside native Fabric notebooks, not in external Livy sessions. Warehouse tables are populated using `CREATE TABLE benchmark.<table> AS SELECT * FROM [LH_01].[benchmark_default].<table>` executed via `sqlcmd` against the WH_01 SQL endpoint. This leverages Fabric's native cross-database query capability (same workspace, 3-part naming). Script: `ingestion/02_warehouse_ingest.sql`.
14. **Fragmentation experiment uses a new schema `benchmark_frag`** in both endpoints. Dimensions are copied compactly (CTAS in WH, Spark write in LH). `store_sales` is created fragmented: Lakehouse via a single Spark job with `maxRecordsPerFile=1000` (~288K files of 1K rows); Warehouse via `01_insert_wh.py` OFFSET/FETCH loop inserting from `benchmark.store_sales` into an initially empty `benchmark_frag.store_sales` (~28.8K files of 10K rows). The original `benchmark` / `benchmark_default` schemas are untouched and serve as baseline.
15. **`runner.py --endpoints` flag** — pass one or more endpoint IDs to run only those endpoints, e.g. `py benchmark/runner.py --endpoints warehouse_frag lakehouse_frag`. If omitted, all endpoints in config.yaml run. This avoids re-running baseline endpoints when measuring fragmentation.
16. **`fragmentation/01_insert_wh.py` checkpoint** — the script saves progress to a JSON file (default: `fragmentation/.wh_insert_checkpoint.json`) after each INSERT. If interrupted, re-running the script continues from where it left off. Use `--checkpoint-file` to override the path.

---

## Metrics Captured Per Execution

| Field | Type | Description |
|-------|------|-------------|
| `run_id` | UUID | Unique execution identifier |
| `timestamp` | datetime | Start time of the execution |
| `endpoint` | string | One of the 4 endpoint IDs above |
| `scale_factor` | string | `SF10`, `SF100` |
| `query_id` | string | `q01`–`q05` |
| `cache_mode` | string | `cold` or `warm` |
| `repetition` | int | Repetition number (1, 2, 3) |
| `elapsed_ms` | float | Client-side elapsed time in milliseconds |
| `rows_returned` | int | Number of rows returned |
| `status` | string | `success`, `error`, or `timeout` |
| `error_message` | string | Error message if status is not `success` |

Results are written to `results/benchmark_{timestamp}.csv` and `results/benchmark_{timestamp}.json`.

---

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `FABRIC_WORKSPACE_NAME` | `FabLab_SQL_Endpoint` | Fabric workspace name |
| `FABRIC_LAKEHOUSE_NAME` | `LH_01` | Lakehouse item name |
| `FABRIC_WAREHOUSE_NAME` | `WH_01` | Warehouse item name |
| `FABRIC_CAPACITY_ID` | — | Fabric capacity GUID (get via `az rest` against Fabric API) |
| `FABRIC_CAPACITY_NAME` | — | ARM capacity name (for suspend/resume) |
| `AZURE_SUBSCRIPTION_ID` | — | Azure subscription ID |
| `AZURE_RESOURCE_GROUP` | — | Resource group containing the capacity |
| `LAKEHOUSE_SERVER` | — | Lakehouse SQL endpoint FQDN |
| `LAKEHOUSE_DATABASE` | — | Lakehouse database name |
| `WAREHOUSE_SERVER` | — | Warehouse SQL endpoint FQDN |
| `WAREHOUSE_DATABASE` | — | Warehouse database name |
| `TENANT_ID` | — | Azure AD tenant ID |

---

## Git Authorship Convention

| File type | Commit author | Co-author |
|-----------|--------------|-----------|
| `specs/`, `.github/copilot-instructions.md` | Nelson López `<nelson.lopez@dataxbi.com>` | `Co-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>` |
| All code (`provision/`, `benchmark/`, `sql/`, `data_generation/`, `ingestion/`, `analysis/`) | `GitHub Copilot <223556219+Copilot@users.noreply.github.com>` | — |

### Documentation update protocol
Every time a plan change is approved:
1. Update `specs/especificaciones.md`
2. Update this file accordingly
3. Commit documentation **before** committing code

---

## Code Style Guidelines

- Python 3.14 (use the `py` launcher on Windows: `py -m ...`)
- Virtual environment: `.venv/` (created with `py -m venv .venv`)
- Dependencies managed in `requirements.txt`
- Follow PEP 8; use type hints where practical
- Use `python-dotenv` to load `.env` — never `os.environ` directly for secrets
- Use `logging` module (not `print`) in all scripts except interactive CLIs
- All scripts must support `--help` and document their arguments
- SQL files must include a header comment with query ID, TPC-DS inspiration, and a brief description
