"""
ingestion/table_configs.py
Definitions of the three Lakehouse Delta table configurations used in the benchmark.
Imported by the Lakehouse ingestion notebook (01_lakehouse_ingest.ipynb).

After ingesting each configuration, OPTIMIZE (without ZORDER) is run on all tables
for Parquet file compaction. ZORDER was discarded due to disproportionate cost at SF1000.
"""

from dataclasses import dataclass, field

# TPC-DS tables used in the benchmark queries
BENCHMARK_TABLES = [
    "store_sales",
    "date_dim",
    "item",
    "store",
    "customer",
    "customer_demographics",
    "promotion",
    "household_demographics",
]


@dataclass
class TableConfig:
    name: str
    description: str
    partition_by: list[str] = field(default_factory=list)  # column names
    vorder_enabled: bool = False
    schema_name: str = "benchmark"  # Lakehouse schema


# The three configurations under test
CONFIGS: dict[str, TableConfig] = {
    "default": TableConfig(
        name="default",
        description="No partition, no V-order",
    ),
    "partitioned": TableConfig(
        name="partitioned",
        description="PARTITION BY ss_sold_date_sk on fact table",
        partition_by=["ss_sold_date_sk"],
    ),
    "vorder": TableConfig(
        name="vorder",
        description="V-Order write optimisation enabled (Fabric-native)",
        vorder_enabled=True,
    ),
}
