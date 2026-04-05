# Data Generation

## Requirements

- **dsdgen** binary (from [tpcds-kit](https://github.com/databricks/tpcds-kit))
- Python 3.14+ with project virtual environment active

## Building dsdgen (Linux/macOS)

```bash
git clone https://github.com/databricks/tpcds-kit.git
cd tpcds-kit/tools
make OS=LINUX   # or OS=MACOS
```

Copy or symlink the resulting `dsdgen` binary to a directory on your PATH,
or set the `DSDGEN_PATH` environment variable to point to it directly.

## Usage

```bash
# Activate the virtual environment first
.venv\Scripts\activate          # Windows
source .venv/bin/activate       # Linux/macOS

# Generate SF10 only
py data_generation/generate_csv.py --sf 10

# Generate SF10, SF100 and SF1000
py data_generation/generate_csv.py --sf 10 100 1000

# Custom output directory
py data_generation/generate_csv.py --sf 10 --out D:\tpcds_data

# Custom dsdgen path
py data_generation/generate_csv.py --sf 10 --dsdgen /opt/tpcds/dsdgen
```

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
