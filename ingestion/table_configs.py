"""
ingestion/table_configs.py
Definitions of the three Lakehouse Delta table configurations and explicit TPC-DS StructType
schemas used in the benchmark ingestion scripts.

Explicit schemas (never inferSchema) ensure consistent column names and types across SF10
and SF100, regardless of CSV content. Imported by ingestion notebooks and scripts.

After ingesting each configuration, OPTIMIZE (without ZORDER) is run on all tables
for Parquet file compaction. ZORDER was discarded due to disproportionate cost at SF1000.
"""

from dataclasses import dataclass, field
from pyspark.sql.types import (
    DecimalType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

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

# Tables whose CSV data is split into multiple gzip chunks (too large for a single file)
SPLIT_TABLES = {"store_sales", "catalog_sales", "inventory", "web_sales"}


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

# ---------------------------------------------------------------------------
# Explicit StructType schemas for all 8 TPC-DS benchmark tables.
# These definitions replace inferSchema to guarantee deterministic column
# names (matching TPC-DS spec) and types across SF10 and SF100 ingestions.
# ---------------------------------------------------------------------------
SCHEMAS: dict[str, StructType] = {
    "store_sales": StructType([
        StructField("ss_sold_date_sk",       LongType(),       True),
        StructField("ss_sold_time_sk",       LongType(),       True),
        StructField("ss_item_sk",            LongType(),       False),
        StructField("ss_customer_sk",        LongType(),       True),
        StructField("ss_cdemo_sk",           LongType(),       True),
        StructField("ss_hdemo_sk",           LongType(),       True),
        StructField("ss_addr_sk",            LongType(),       True),
        StructField("ss_store_sk",           LongType(),       True),
        StructField("ss_promo_sk",           LongType(),       True),
        StructField("ss_ticket_number",      LongType(),       False),
        StructField("ss_quantity",           IntegerType(),    True),
        StructField("ss_wholesale_cost",     DecimalType(7,2), True),
        StructField("ss_list_price",         DecimalType(7,2), True),
        StructField("ss_sales_price",        DecimalType(7,2), True),
        StructField("ss_ext_discount_amt",   DecimalType(7,2), True),
        StructField("ss_ext_sales_price",    DecimalType(7,2), True),
        StructField("ss_ext_wholesale_cost", DecimalType(7,2), True),
        StructField("ss_ext_list_price",     DecimalType(7,2), True),
        StructField("ss_ext_tax",            DecimalType(7,2), True),
        StructField("ss_coupon_amt",         DecimalType(7,2), True),
        StructField("ss_net_paid",           DecimalType(7,2), True),
        StructField("ss_net_paid_inc_tax",   DecimalType(7,2), True),
        StructField("ss_net_profit",         DecimalType(7,2), True),
    ]),
    "date_dim": StructType([
        StructField("d_date_sk",             LongType(),    False),
        StructField("d_date_id",             StringType(),  False),
        StructField("d_date",                StringType(),  True),
        StructField("d_month_seq",           IntegerType(), True),
        StructField("d_week_seq",            IntegerType(), True),
        StructField("d_quarter_seq",         IntegerType(), True),
        StructField("d_year",                IntegerType(), True),
        StructField("d_dow",                 IntegerType(), True),
        StructField("d_moy",                 IntegerType(), True),
        StructField("d_dom",                 IntegerType(), True),
        StructField("d_qoy",                 IntegerType(), True),
        StructField("d_fy_year",             IntegerType(), True),
        StructField("d_fy_quarter_seq",      IntegerType(), True),
        StructField("d_fy_week_seq",         IntegerType(), True),
        StructField("d_day_name",            StringType(),  True),
        StructField("d_quarter_name",        StringType(),  True),
        StructField("d_holiday",             StringType(),  True),
        StructField("d_weekend",             StringType(),  True),
        StructField("d_following_holiday",   StringType(),  True),
        StructField("d_first_dom",           IntegerType(), True),
        StructField("d_last_dom",            IntegerType(), True),
        StructField("d_same_day_ly",         IntegerType(), True),
        StructField("d_same_day_lq",         IntegerType(), True),
        StructField("d_current_day",         StringType(),  True),
        StructField("d_current_week",        StringType(),  True),
        StructField("d_current_month",       StringType(),  True),
        StructField("d_current_quarter",     StringType(),  True),
        StructField("d_current_year",        StringType(),  True),
    ]),
    "item": StructType([
        StructField("i_item_sk",        LongType(),        False),
        StructField("i_item_id",        StringType(),      False),
        StructField("i_rec_start_date", StringType(),      True),
        StructField("i_rec_end_date",   StringType(),      True),
        StructField("i_item_desc",      StringType(),      True),
        StructField("i_current_price",  DecimalType(7,2),  True),
        StructField("i_wholesale_cost", DecimalType(7,2),  True),
        StructField("i_brand_id",       IntegerType(),     True),
        StructField("i_brand",          StringType(),      True),
        StructField("i_class_id",       IntegerType(),     True),
        StructField("i_class",          StringType(),      True),
        StructField("i_category_id",    IntegerType(),     True),
        StructField("i_category",       StringType(),      True),
        StructField("i_manufact_id",    IntegerType(),     True),
        StructField("i_manufact",       StringType(),      True),
        StructField("i_size",           StringType(),      True),
        StructField("i_formulation",    StringType(),      True),
        StructField("i_color",          StringType(),      True),
        StructField("i_units",          StringType(),      True),
        StructField("i_container",      StringType(),      True),
        StructField("i_manager_id",     IntegerType(),     True),
        StructField("i_product_name",   StringType(),      True),
    ]),
    "store": StructType([
        StructField("s_store_sk",         LongType(),       False),
        StructField("s_store_id",         StringType(),     False),
        StructField("s_rec_start_date",   StringType(),     True),
        StructField("s_rec_end_date",     StringType(),     True),
        StructField("s_closed_date_sk",   LongType(),       True),
        StructField("s_store_name",       StringType(),     True),
        StructField("s_number_employees", IntegerType(),    True),
        StructField("s_floor_space",      IntegerType(),    True),
        StructField("s_hours",            StringType(),     True),
        StructField("s_manager",          StringType(),     True),
        StructField("s_market_id",        IntegerType(),    True),
        StructField("s_geography_class",  StringType(),     True),
        StructField("s_market_desc",      StringType(),     True),
        StructField("s_market_manager",   StringType(),     True),
        StructField("s_division_id",      IntegerType(),    True),
        StructField("s_division_name",    StringType(),     True),
        StructField("s_company_id",       IntegerType(),    True),
        StructField("s_company_name",     StringType(),     True),
        StructField("s_street_number",    StringType(),     True),
        StructField("s_street_name",      StringType(),     True),
        StructField("s_street_type",      StringType(),     True),
        StructField("s_suite_number",     StringType(),     True),
        StructField("s_city",             StringType(),     True),
        StructField("s_county",           StringType(),     True),
        StructField("s_state",            StringType(),     True),
        StructField("s_zip",              StringType(),     True),
        StructField("s_country",          StringType(),     True),
        StructField("s_gmt_offset",       DecimalType(5,2), True),
        StructField("s_tax_precentage",   DecimalType(5,2), True),
    ]),
    "customer": StructType([
        StructField("c_customer_sk",            LongType(),    False),
        StructField("c_customer_id",            StringType(),  False),
        StructField("c_current_cdemo_sk",       LongType(),    True),
        StructField("c_current_hdemo_sk",       LongType(),    True),
        StructField("c_current_addr_sk",        LongType(),    True),
        StructField("c_first_shipto_date_sk",   LongType(),    True),
        StructField("c_first_sales_date_sk",    LongType(),    True),
        StructField("c_salutation",             StringType(),  True),
        StructField("c_first_name",             StringType(),  True),
        StructField("c_last_name",              StringType(),  True),
        StructField("c_preferred_cust_flag",    StringType(),  True),
        StructField("c_birth_day",              IntegerType(), True),
        StructField("c_birth_month",            IntegerType(), True),
        StructField("c_birth_year",             IntegerType(), True),
        StructField("c_birth_country",          StringType(),  True),
        StructField("c_login",                  StringType(),  True),
        StructField("c_email_address",          StringType(),  True),
        StructField("c_last_review_date_sk",    LongType(),    True),
    ]),
    "customer_demographics": StructType([
        StructField("cd_demo_sk",             LongType(),    False),
        StructField("cd_gender",              StringType(),  True),
        StructField("cd_marital_status",      StringType(),  True),
        StructField("cd_education_status",    StringType(),  True),
        StructField("cd_purchase_estimate",   IntegerType(), True),
        StructField("cd_credit_rating",       StringType(),  True),
        StructField("cd_dep_count",           IntegerType(), True),
        StructField("cd_dep_employed_count",  IntegerType(), True),
        StructField("cd_dep_college_count",   IntegerType(), True),
    ]),
    "promotion": StructType([
        StructField("p_promo_sk",           LongType(),        False),
        StructField("p_promo_id",           StringType(),      False),
        StructField("p_start_date_sk",      LongType(),        True),
        StructField("p_end_date_sk",        LongType(),        True),
        StructField("p_item_sk",            LongType(),        True),
        StructField("p_cost",               DecimalType(15,2), True),
        StructField("p_response_tgt",       IntegerType(),     True),
        StructField("p_promo_name",         StringType(),      True),
        StructField("p_channel_dmail",      StringType(),      True),
        StructField("p_channel_email",      StringType(),      True),
        StructField("p_channel_catalog",    StringType(),      True),
        StructField("p_channel_tv",         StringType(),      True),
        StructField("p_channel_radio",      StringType(),      True),
        StructField("p_channel_press",      StringType(),      True),
        StructField("p_channel_event",      StringType(),      True),
        StructField("p_channel_demo",       StringType(),      True),
        StructField("p_channel_details",    StringType(),      True),
        StructField("p_purpose",            StringType(),      True),
        StructField("p_discount_active",    StringType(),      True),
    ]),
    "household_demographics": StructType([
        StructField("hd_demo_sk",          LongType(),    False),
        StructField("hd_income_band_sk",   LongType(),    True),
        StructField("hd_buy_potential",    StringType(),  True),
        StructField("hd_dep_count",        IntegerType(), True),
        StructField("hd_vehicle_count",    IntegerType(), True),
    ]),
}

