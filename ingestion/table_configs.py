"""
ingestion/table_configs.py
Definitions of the four Lakehouse Delta table configurations used in the benchmark.
Imported by the Lakehouse ingestion notebook (01_lakehouse_ingest.ipynb).
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

# Columns to use for Z-order optimisation (high-cardinality join/filter columns)
ZORDER_COLUMNS = {
    "store_sales": ["ss_item_sk", "ss_store_sk", "ss_sold_date_sk"],
    "item":        ["i_item_sk", "i_category"],
    "store":       ["s_store_sk"],
    "customer":    ["c_customer_sk"],
}


@dataclass
class TableConfig:
    name: str
    description: str
    partition_by: list[str] = field(default_factory=list)  # column names
    zorder_by: dict[str, list[str]] = field(default_factory=dict)  # table → columns
    vorder_enabled: bool = False
    schema_name: str = "benchmark"  # Lakehouse schema


# The four configurations under test
CONFIGS: dict[str, TableConfig] = {
    "default": TableConfig(
        name="default",
        description="No partition, no Z-order, no V-order",
    ),
    "partitioned": TableConfig(
        name="partitioned",
        description="PARTITION BY ss_sold_date_sk on fact table",
        partition_by=["ss_sold_date_sk"],
    ),
    "zorder": TableConfig(
        name="zorder",
        description="Z-ORDER on high-cardinality join and filter columns",
        zorder_by=ZORDER_COLUMNS,
    ),
    "vorder": TableConfig(
        name="vorder",
        description="V-Order write optimisation enabled (Fabric-native)",
        vorder_enabled=True,
    ),
}
