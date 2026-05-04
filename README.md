# FabLab SQL Endpoint Benchmark

Performance benchmark comparing **Microsoft Fabric SQL endpoints** using TPC-DS-inspired queries.

## What this benchmarks

Six endpoints are tested, including a fragmentation experiment:

| Endpoint | Type | Delta config |
|----------|------|--------------|
| `lakehouse_default` | Lakehouse SQL endpoint | No partition, no V-Order |
| `lakehouse_partitioned` | Lakehouse SQL endpoint | `PARTITION BY ss_sold_date_sk` |
| `lakehouse_vorder` | Lakehouse SQL endpoint | V-Order enabled at write time |
| `warehouse` | Fabric Warehouse | Standard configuration |
| `lakehouse_frag` | Lakehouse SQL endpoint | Fragmented — `store_sales` in ~28.8K small Parquet files |
| `warehouse_frag` | Fabric Warehouse | Fragmented — `store_sales` in ~28.8K small Parquet files |

Five TPC-DS-inspired queries are run at **SF100 (~100 GB)** scale in two cache modes:
- **Cold** — first run after capacity resume (true cold cache, guaranteed by pause/resume cycle)
- **Warm** — 3 repetitions on a hot, active capacity

---

## Repository layout

```
fablab-sql-endpoint/
├── provision/          ← One-time Fabric resource setup + capacity manager
├── data_generation/    ← TPC-DS data generation (dsdgen) + OneLake upload
├── ingestion/          ← Spark notebooks to load CSVs → Delta / Warehouse
├── sql/                ← Five benchmark SQL queries (q01–q05)
├── benchmark/          ← Main runner, config, connection and utilities
├── fragmentation/      ← Fragmentation experiment: setup scripts + README
├── analysis/           ← Results analysis notebook (charts + statistics)
├── results/            ← Output CSV/JSON files (committed to repo)
├── specs/              ← Authoritative project specification (Spanish)
├── .env.example        ← Environment variable template
└── requirements.txt
```

---

## Documentation by folder

| Folder | README | What it covers |
|--------|--------|----------------|
| [`provision/`](provision/README.md) | ✅ | Create Fabric workspace/Lakehouse/Warehouse; capacity pause/resume |
| [`data_generation/`](data_generation/README.md) | ✅ | Generate TPC-DS CSVs with dsdgen; split, gzip and upload to OneLake |
| [`ingestion/`](ingestion/README.md) | ✅ | Load CSVs into 3 Lakehouse schemas and the Warehouse via Spark |
| [`benchmark/`](benchmark/README.md) | ✅ | Run the benchmark; config reference; output format |
| [`fragmentation/`](fragmentation/README.md) | ✅ | Create fragmented `store_sales` in Lakehouse and Warehouse; run fragmentation benchmark |
| [`results/`](results/) | — | Output CSV/JSON files — committed to the repo |

---

## End-to-end workflow

```
1. provision/setup_fabric.py              → create workspace, Lakehouse, Warehouse
2. data_generation/generate_csv.py        → generate TPC-DS data with dsdgen
3. data_generation/upload_to_onelake.py   → split, gzip, upload to OneLake
4. ingestion/01_lakehouse_ingest.ipynb    → load data into 3 Lakehouse schemas
5. ingestion/02_warehouse_ingest.sql      → load data into Warehouse (CTAS)
6. benchmark/runner.py                    → run benchmark (cold + warm blocks)
7. fragmentation/00_setup_lh_frag.ipynb  → create fragmented store_sales in Lakehouse
8. fragmentation/00_setup_wh_frag.sql    → create benchmark_frag schema in Warehouse
9. fragmentation/02_copy_into_wh.py      → load fragmented store_sales into Warehouse
10. benchmark/runner.py --endpoints lakehouse_frag warehouse_frag  → run fragmentation benchmark
11. analysis/analyze_results.ipynb        → compare results across endpoints
```

---

## Prerequisites

- Python 3.14+ (`py` launcher on Windows)
- [Azure CLI](https://learn.microsoft.com/cli/azure/install-azure-cli) — `az login` for authentication
- [Microsoft ODBC Driver 18 for SQL Server](https://learn.microsoft.com/sql/connect/odbc/download-odbc-driver-for-sql-server)
- [azcopy v10](https://aka.ms/downloadazcopy-v10-windows) — for OneLake upload
- WSL 2 with Ubuntu — for dsdgen compilation and CSV splitting

```powershell
# Install Python dependencies
pip install -r requirements.txt

# Authenticate
az login
```

Copy `.env.example` to `.env` and fill in the values printed by `provision/setup_fabric.py`.

---

## Results

Benchmark output files are committed to [`results/`](results/):

| File | Scale | Description |
|------|-------|-------------|
| `benchmark_20260410T160604.csv` | SF100 | Run 0 — 4 base endpoints, Q1–Q5, cold + warm (80 rows) |
| `benchmark_20260429T171514.csv` | SF100 | Run A — `lakehouse_frag` + `warehouse_frag`, warm only (30 rows) |
| `benchmark_20260430T063923.csv` | SF100 | Failed run — all endpoints timed out (capacity not ready) |
| `benchmark_20260430T065849.csv` | SF100 | Run B — 5 endpoints (no `lakehouse_frag`), warm only (75 rows) |

Open `analysis/analyze_results.ipynb` to generate comparison charts from any results file.
