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
sudo apt update && sudo apt install -y build-essential flex bison git python3-full
```

### Step 3 — Clone and compile tpcds-kit

> **Important**: clone into your Linux home directory (`~`), **not** into
> `/mnt/c/...`. The Windows filesystem does not support the file permissions
> that `git` and `make` require.

```bash
cd ~
git clone https://github.com/databricks/tpcds-kit.git
cd tpcds-kit/tools
make OS=LINUX
```

The `dsdgen` binary will be at `~/tpcds-kit/tools/dsdgen`.

### Step 4 — Create a Linux-native Python virtual environment

The project `.venv` was created on Windows and does not work in WSL.
Create a dedicated venv inside the Linux filesystem instead.

`python3-full` (already installed in Step 2) includes `venv` support.
The data generation script only needs two packages from the full
`requirements.txt` — the rest (pyodbc, jupyter, etc.) are only needed on
Windows for running the benchmark and analysis:

```bash
python3 -m venv ~/venv-tpcds
source ~/venv-tpcds/bin/activate
pip install pyyaml python-dotenv
```

Activate this venv every time you open a new WSL terminal for data generation:

```bash
source ~/venv-tpcds/bin/activate
```

### Step 5 — Generate the data

> **Important**: use a Linux output directory (e.g. `~/tpcds-data`).
> Writing directly to `/mnt/c/...` can cause permission errors with dsdgen.

```bash
cd /mnt/c/Users/<your-username>/source/repos/fablab-sql-endpoint

# SF10 only (~10 GB — recommended first run to validate everything)
python data_generation/generate_csv.py \
    --sf 10 \
    --dsdgen ~/tpcds-kit/tools/dsdgen \
    --out ~/tpcds-data

# SF10 + SF100 + SF1000
python data_generation/generate_csv.py \
    --sf 10 100 1000 \
    --dsdgen ~/tpcds-kit/tools/dsdgen \
    --out ~/tpcds-data
```

After generation, upload the CSV files from `~/tpcds-data` to the Fabric
Lakehouse Files section (`Files/tpcds/sfXX/`) before running the ingestion
notebooks.

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
