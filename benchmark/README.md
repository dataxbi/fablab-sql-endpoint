# benchmark/

Main benchmark executor for the Fabric SQL endpoint performance study.
Runs TPC-DS queries against four endpoints, measures client-side latency,
and writes results to `results/`.

## Files

| File | Role |
|------|------|
| `runner.py` | Main entry point — orchestrates the full benchmark loop |
| `config.yaml` | Test matrix: endpoints, queries, scale factors, timeouts |
| `connection.py` | pyodbc connection management with Azure CLI token auth |
| `utils.py` | Shared utilities: timer, result model, CSV/JSON serialisation |

---

## Quick start

```powershell
# Activate virtual environment
.venv\Scripts\activate

# Full benchmark run (reads config.yaml and .env)
py benchmark/runner.py

# Dry run — logs every planned execution without connecting to Fabric
py benchmark/runner.py --dry-run

# Run a specific scale factor only
py benchmark/runner.py --sf SF100

# Custom config file
py benchmark/runner.py --config benchmark/config.yaml
```

---

## Execution order

The runner follows this sequence for each scale-factor block:

```
For each SF in scale_factors:
  1. Pause Fabric capacity   → flush all in-memory caches (cold guarantee)
  2. Resume Fabric capacity  → poll until Active
  3. Cold block: run all (endpoint × query) once
  4. Warm block: run all (endpoint × query) × warm_repetitions
  5. Pause Fabric capacity   → cost saving
```

> **Why pause before resume?** Pausing is the only reliable way to flush Fabric's
> in-memory cache. Even if the capacity is already Active, a pause+resume cycle
> guarantees a true cold start for the first measurement block.

---

## config.yaml

Defines the full test matrix. All `${VAR}` placeholders are expanded from
environment variables at runtime.

Key sections:

| Section | Description |
|---------|-------------|
| `capacity` | Azure subscription/RG/capacity name for pause/resume |
| `endpoints` | 4 SQL endpoints with server, database and schema |
| `scale_factors` | List of SF blocks to run (e.g. `[SF100]`) |
| `queries` | Map of query IDs to SQL file paths |
| `warm_repetitions` | Number of warm-cache repetitions (default: 3) |
| `query_timeout_sec` | Per-query timeout in seconds (default: 300) |
| `results_dir` | Output directory for result files (default: `results/`) |

---

## Endpoints under test

| Endpoint ID | Type | Delta config |
|-------------|------|--------------|
| `lakehouse_default` | Lakehouse SQL endpoint | No partition, no V-Order |
| `lakehouse_partitioned` | Lakehouse SQL endpoint | `PARTITION BY ss_sold_date_sk` |
| `lakehouse_vorder` | Lakehouse SQL endpoint | V-Order enabled at write time |
| `warehouse` | Fabric Warehouse | Standard configuration |

For Lakehouse endpoints, the runner issues `USE [{schema}]` immediately after
connecting so queries resolve to the correct schema without requiring a schema prefix
in the SQL files.

---

## Authentication

Connections use **Azure CLI token injection** (`az account get-access-token`) — no
passwords or service principals. Run `az login` once before executing the benchmark.

The ODBC driver (`ODBC Driver 18 for SQL Server`) must be installed separately.
Set `ODBC_DRIVER` in `.env` to override the default driver name.

---

## Output

Results are written to two files in `results/` after each full run:

```
results/
├── benchmark_20260410T160604.csv
└── benchmark_20260410T160604.json
```

Each row/object captures:

| Field | Description |
|-------|-------------|
| `run_id` | UUID — unique per execution |
| `timestamp` | UTC start time |
| `endpoint` | One of the 4 endpoint IDs |
| `scale_factor` | `SF10`, `SF100`, … |
| `query_id` | `q01`–`q05` |
| `cache_mode` | `cold` or `warm` |
| `repetition` | Repetition number (1, 2, 3) |
| `elapsed_ms` | Client-side wall-clock time in milliseconds |
| `rows_returned` | Number of rows fetched |
| `status` | `success`, `error`, or `timeout` |
| `error_message` | Error detail if status ≠ `success` |

---

## Prerequisites

```powershell
az login                            # Azure CLI authentication
pip install -r requirements.txt     # pyodbc, pyyaml, python-dotenv
```

Copy `.env.example` to `.env` and fill in the connection strings printed by
`provision/setup_fabric.py`.
