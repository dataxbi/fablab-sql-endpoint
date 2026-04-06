# Data Generation

## Requirements

- **dsdgen** binary (from [tpcds-kit](https://github.com/databricks/tpcds-kit))
- Python 3.14+ with project virtual environment active

---

## Option A — Windows with WSL (recommended for Windows users)

`dsdgen` is a C program designed for Linux/macOS. The easiest way to compile
and run it on Windows is via **WSL** (Windows Subsystem for Linux).

### Step 1 — Install WSL 2 + Ubuntu

Open PowerShell as Administrator and run:

```powershell
wsl --install -d Ubuntu-24.04
```

Restart your machine when prompted, then open the **Ubuntu** app to complete
the initial Linux setup (username + password).

### Step 2 — Install build dependencies

Inside the WSL terminal:

```bash
sudo apt update && sudo apt install -y build-essential flex bison git
```

### Step 3 — Clone and compile tpcds-kit

```bash
cd ~
git clone https://github.com/databricks/tpcds-kit.git
cd tpcds-kit/tools
make OS=LINUX
```

The `dsdgen` binary will be at `~/tpcds-kit/tools/dsdgen`.

### Step 4 — Locate the project from WSL

Your Windows filesystem is mounted at `/mnt/c/` inside WSL.
The project root is typically:

```bash
cd /mnt/c/Users/<your-username>/fablab-sql-endpoint
```

### Step 5 — Generate the data

Still inside WSL, with the project directory as your working directory:

```bash
# SF10 only (~10 GB — recommended first run to validate everything)
python data_generation/generate_csv.py --sf 10 \
    --dsdgen ~/tpcds-kit/tools/dsdgen \
    --out data

# SF10 + SF100 + SF1000
python data_generation/generate_csv.py --sf 10 100 1000 \
    --dsdgen ~/tpcds-kit/tools/dsdgen \
    --out data
```

> **Tip**: if the WSL `python` command is not found, use `python3` or activate
> the project virtual environment first:
> ```bash
> source .venv/bin/activate
> ```

### Approximate generation times

| Scale factor | Size   | Estimated time |
|-------------|--------|----------------|
| SF10        | ~10 GB | ~5 minutes     |
| SF100       | ~100 GB | ~40 minutes   |
| SF1000      | ~1 TB  | ~7 hours       |

Times vary with CPU speed and disk I/O. SF1000 is best run overnight.

---

## Option B — Native Linux or macOS

```bash
git clone https://github.com/databricks/tpcds-kit.git
cd tpcds-kit/tools
make OS=LINUX   # or OS=MACOS
```

Copy or symlink the resulting `dsdgen` binary to a directory on your PATH,
or set the `DSDGEN_PATH` environment variable to point to it directly.

---

## Usage (all platforms)

```bash
# Activate the virtual environment first
.venv\Scripts\activate          # Windows PowerShell
source .venv/bin/activate       # Linux / macOS / WSL

# Generate SF10 only
py data_generation/generate_csv.py --sf 10

# Generate SF10, SF100 and SF1000
py data_generation/generate_csv.py --sf 10 100 1000

# Custom output directory
py data_generation/generate_csv.py --sf 10 --out D:\tpcds_data

# Explicit path to dsdgen binary (required in WSL if not on PATH)
py data_generation/generate_csv.py --sf 10 --dsdgen ~/tpcds-kit/tools/dsdgen
```

---

## Output structure

```
data/
├── sf10/
│   ├── store_sales.csv
│   ├── date_dim.csv
│   ├── item.csv
│   └── ... (24 tables total)
├── sf100/
│   └── ...
└── sf1000/
    └── ...
```

> **Note**: The `data/` directory is excluded from git (see `.gitignore`).
> Generated files can be several hundred GB for SF100 and SF1000.
> After generating, upload the CSV files to the Lakehouse Files section
> (`Files/tpcds/sfXX/`) before running the ingestion notebooks.
