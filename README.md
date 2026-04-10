# FabLab SQL Endpoint Benchmark

Performance benchmark comparing **Microsoft Fabric SQL endpoints** using TPC-DS-inspired queries.

## What this benchmarks

Four endpoints are tested under identical conditions:

| Endpoint | Type | Delta config |
|----------|------|--------------|
| `lakehouse_default` | Lakehouse SQL endpoint | No partition, no V-Order |
| `lakehouse_partitioned` | Lakehouse SQL endpoint | `PARTITION BY ss_sold_date_sk` |
| `lakehouse_vorder` | Lakehouse SQL endpoint | V-Order enabled at write time |
| `warehouse` | Fabric Warehouse | Standard configuration |

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
├── analysis/           ← Results analysis notebook (charts + statistics)
├── results/            ← Output CSV/JSON files (git-ignored)
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

---

## End-to-end workflow

```
1. provision/setup_fabric.py       → create workspace, Lakehouse, Warehouse
2. data_generation/generate_csv.py → generate TPC-DS data with dsdgen
3. data_generation/upload_to_onelake.py → split, gzip, upload to OneLake
4. ingestion/01_lakehouse_ingest.ipynb  → load data into 3 Lakehouse schemas
5. ingestion/02_warehouse_ingest.sql    → load data into Warehouse (CTAS)
6. benchmark/runner.py             → run benchmark (cold + warm blocks)
7. analysis/analyze_results.ipynb  → compare results across endpoints
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

The benchmark produces `results/benchmark_<timestamp>.csv` and `.json`.
Open `analysis/analyze_results.ipynb` to generate comparison charts.
