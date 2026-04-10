# Ingestion

This folder contains the scripts and notebooks to load TPC-DS data into the four benchmark endpoints:
three Lakehouse Delta configurations and the Fabric Warehouse.

Ingestion time is **not measured** in the benchmark. Run it once per scale factor and re-use the data.

---

## Prerequisites

Before running ingestion, ensure:

1. **TPC-DS CSV files are generated** — see [`data_generation/`](../data_generation/).
   Expected location: `data/sf100/` (or `data/sf10/` for validation runs).

2. **Fabric resources are provisioned** — workspace `FabLab_SQL_Endpoint`, lakehouse `LH_01`
   (schema-enabled), and warehouse `WH_01` must exist.
   See [`provision/setup_fabric.py`](../provision/setup_fabric.py).

3. **CSV files are uploaded to OneLake** — the ingestion notebook reads from
   `Files/data/sf100/` inside `LH_01`. Upload the generated CSVs there via the Fabric UI or
   `azcopy`/`az storage` before running the notebook.

4. **Environment variables are set** — copy `.env.example` to `.env` at the repo root and fill
   in `LAKEHOUSE_SERVER`, `WAREHOUSE_SERVER`, `WAREHOUSE_DATABASE`, and `TENANT_ID`.

5. **Fabric capacity is active** — the benchmark capacity must be in `Active` state.
   Use `provision/capacity_manager.py` to resume it if paused.

---

## Step 1 — Lakehouse ingestion (3 Delta configurations)

**Script**: `01_lakehouse_ingest.ipynb`

This notebook reads CSV files from OneLake and writes three Delta table sets into `LH_01`:

| Schema | Configuration | Details |
|--------|---------------|---------|
| `benchmark_default` | No partition, no V-Order | Baseline Delta tables |
| `benchmark_partitioned` | Partitioned by `ss_sold_date_sk` | One partition per date key on `store_sales` |
| `benchmark_vorder` | V-Order enabled | Spark write config `spark.sql.parquet.vorder.enabled=true` |

Each schema contains the same 8 tables: `store_sales`, `date_dim`, `item`, `store`, `customer`,
`customer_demographics`, `promotion`, `household_demographics`.

After each configuration, `OPTIMIZE` (without ZORDER) is run on all tables to compact Parquet
files produced by Spark. This is standard Delta maintenance and is **not** a benchmark variable.

**Explicit schemas** (`StructType`) are used for all tables — never `inferSchema`. The canonical
schema definitions live in [`table_configs.py`](table_configs.py) and are inlined into the
notebook's cell 2 for Fabric compatibility.

**Key write options applied**:
- `.mode('overwrite')` — full table replacement on each run
- `.option('overwriteSchema', 'true')` — required if tables already exist with a different schema
  (e.g. `_c0` columns from a prior `inferSchema` run)

### How to run

**Option A — Fabric UI (recommended for SF100)**

1. Open `LH_01` in the Fabric workspace.
2. Open the notebook `01_lakehouse_ingest` (already deployed).
3. Verify cell 1 sets `SCALE_FACTOR = "sf100"` and the data path matches `Files/data/sf100/`.
4. Click **Run all**. Expected duration: ~20–25 minutes on F64 with Workspace Pool.

**Option B — CLI (re-deploy and submit)**

```bash
# From repo root (requires .env loaded and az login active)
py C:\Users\nlope\.copilot\session-state\<session-id>\files\deploy_notebook.py
```

> **Note**: The `deploy_notebook.py` script in the session workspace handles `updateDefinition`
> (to sync local changes to Fabric) and submits a job run via the Fabric Jobs API.
> Use `useWorkspacePool: True` for SF100 — Starter Pool will cause premature job failure.

### Verify

After the notebook completes, confirm column names are correct (no `_c0`):

```sql
-- Run against the Lakehouse SQL endpoint
SELECT TOP 1 ss_sold_date_sk, ss_item_sk, ss_sales_price
FROM benchmark_default.store_sales;
```

Expected SF100 row counts:

| Table | Rows |
|-------|------|
| `store_sales` | 287,997,099 |
| `customer` | 2,000,000 |
| `customer_demographics` | 1,920,800 |
| `date_dim` | 73,049 |
| `item` | 204,000 |
| `store` | 402 |
| `promotion` | 1,000 |
| `household_demographics` | 7,200 |

---

## Step 2 — Warehouse ingestion (CTAS from Lakehouse)

**Script**: `02_warehouse_ingest.sql`

Populates `WH_01` by copying all 8 tables from `LH_01.benchmark_default` using
**CTAS cross-database queries** within the same Fabric workspace. This avoids the need for
the Fabric Spark connector (which only works inside native Fabric notebooks).

```sql
-- Example of what the script does for each table:
CREATE TABLE benchmark.store_sales
AS SELECT * FROM [LH_01].[benchmark_default].[store_sales];
```

> **Prerequisite**: the `benchmark` schema must exist in `WH_01` before running.
> If it doesn't, create it first:
> ```sql
> IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'benchmark')
>     EXEC('CREATE SCHEMA benchmark');
> ```

### How to run

```bash
sqlcmd -S <WAREHOUSE_SERVER> -d WH_01 -G -i ingestion/02_warehouse_ingest.sql -l 7200
```

Replace `<WAREHOUSE_SERVER>` with the value of `WAREHOUSE_SERVER` from your `.env`.

The `-G` flag uses Azure Active Directory authentication (consistent with `az login`).
The `-l 7200` timeout (2 hours) is needed for SF100 — `store_sales` alone takes ~15–20 minutes
to copy via CTAS.

The script prints progress messages and finishes with a row-count verification query.

---

## File reference

| File | Purpose |
|------|---------|
| `01_lakehouse_ingest.ipynb` | PySpark notebook — CSV → Delta for all 3 Lakehouse configs |
| `02_warehouse_ingest.sql` | T-SQL script — CTAS from Lakehouse → Warehouse |
| `table_configs.py` | Source of truth for StructType schemas and `TableConfig` definitions |

---

## Re-running ingestion

To re-run ingestion from scratch (e.g. after a schema change or data corruption):

1. The notebook uses `.mode('overwrite')` + `.option('overwriteSchema', 'true')` — simply
   re-run it. Tables will be fully replaced.
2. The SQL script issues `DROP TABLE IF EXISTS` before each `CREATE TABLE AS SELECT` — it is
   also idempotent.

> **Warning**: `overwriteSchema=True` drops and recreates the table, which removes any manual
> Delta table properties or constraints you may have set outside of the notebook.
