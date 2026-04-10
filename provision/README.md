# provision/

One-time provisioning scripts for the Fabric resources required by the benchmark.

## Files

| File | Role |
|------|------|
| `setup_fabric.py` | One-time script: creates the workspace, Lakehouse and Warehouse via the Fabric API |
| `capacity_manager.py` | Reusable module: pauses/resumes the Fabric capacity with polling |

---

## setup_fabric.py

Creates (or verifies the existence of) the three required Fabric resources and prints the SQL
connection strings so you can copy them into your `.env` file.

**When to run:** once at project setup, or when recreating the environment from scratch.

```bash
py provision/setup_fabric.py [--workspace NAME] [--lh NAME] [--wh NAME] [--capacity-id ID]
```

### Arguments

| Argument | Environment variable | Default |
|----------|----------------------|---------|
| `--workspace` | `FABRIC_WORKSPACE_NAME` | `FabLab_SQL_Endpoint` |
| `--lh` | `FABRIC_LAKEHOUSE_NAME` | `LH_01` |
| `--wh` | `FABRIC_WAREHOUSE_NAME` | `WH_01` |
| `--capacity-id` | `FABRIC_CAPACITY_ID` | *(optional)* |

### Example output

```
============================================================
PROVISIONING COMPLETE
============================================================
Workspace     : FabLab_SQL_Endpoint  (id: f67c4250-...)

Lakehouse     : LH_01  (id: d10ce80e-...)
  Server      : evxidzw3ig3u...datawarehouse.fabric.microsoft.com
  Database    : d10ce80e-...

Warehouse     : WH_01  (id: ...)
  Server      : evxidzw3ig3u...datawarehouse.fabric.microsoft.com
  Database    : WH_01
============================================================
Copy the values above into your .env file.
```

> The script is idempotent: if a resource already exists it is detected and not recreated.

---

## capacity_manager.py

Library module — **not meant to be run directly**. It exposes the functions used by
`benchmark/runner.py` during benchmark execution:

| Function | Description |
|----------|-------------|
| `resume_capacity(...)` | Resumes the capacity and blocks until state is `Active` |
| `pause_capacity(...)` | Pauses the capacity and blocks until state is `Paused` |
| `get_capacity_state(...)` | Returns the current state of the capacity |

Both functions poll every 15 s until the expected state is reached or the timeout expires
(default: 10 minutes).

### Why this is needed

Pausing and resuming the Fabric capacity is the only reliable way to flush all in-memory
caches for *cold* cache measurements. The runner calls these functions at the start of each
scale-factor block (`SF10`, `SF100`).

---

## Prerequisites

```bash
az login   # Azure CLI authentication — no service principals or secrets required
```

Credentials are read from the environment — see `.env.example` at the repository root.
