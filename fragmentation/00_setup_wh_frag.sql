-- Query ID: 00_setup_wh_frag
-- Description: Set up the benchmark_frag schema in WH_01 for the fragmentation experiment.
--   - Creates schema benchmark_frag (idempotent).
--   - Creates store_sales as an EMPTY table (schema cloned from benchmark.store_sales via SELECT TOP 0).
--     Data will be inserted incrementally by fragmentation/01_insert_wh.py.
--   - Copies the 7 dimension tables compactly via CTAS from benchmark.<table>.
--
-- Run against the WH_01 SQL endpoint:
--   sqlcmd -S <WH_SERVER> -d WH_01 -G -i fragmentation/00_setup_wh_frag.sql -l 3600
--
-- Prerequisites:
--   - Schema 'benchmark' must exist and be fully populated (SF100).
--   - Run ingestion/02_warehouse_ingest.sql first if needed.

SET NOCOUNT ON;
PRINT 'Setting up benchmark_frag schema in WH_01...';

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'benchmark_frag')
    EXEC('CREATE SCHEMA benchmark_frag');
PRINT 'Schema benchmark_frag ready.';

-- store_sales: empty table (same schema, no data — filled by 01_insert_wh.py)
IF OBJECT_ID('benchmark_frag.store_sales') IS NOT NULL DROP TABLE benchmark_frag.store_sales;
PRINT 'Creating benchmark_frag.store_sales (empty)...';
CREATE TABLE benchmark_frag.store_sales AS SELECT * FROM benchmark.store_sales WHERE 1 = 0;
PRINT 'Done: store_sales (empty)';

-- Dimension tables: compact copies via CTAS
IF OBJECT_ID('benchmark_frag.date_dim') IS NOT NULL DROP TABLE benchmark_frag.date_dim;
PRINT 'Creating benchmark_frag.date_dim...';
CREATE TABLE benchmark_frag.date_dim AS SELECT * FROM benchmark.date_dim;
PRINT 'Done: date_dim';

IF OBJECT_ID('benchmark_frag.item') IS NOT NULL DROP TABLE benchmark_frag.item;
PRINT 'Creating benchmark_frag.item...';
CREATE TABLE benchmark_frag.item AS SELECT * FROM benchmark.item;
PRINT 'Done: item';

IF OBJECT_ID('benchmark_frag.store') IS NOT NULL DROP TABLE benchmark_frag.store;
PRINT 'Creating benchmark_frag.store...';
CREATE TABLE benchmark_frag.store AS SELECT * FROM benchmark.store;
PRINT 'Done: store';

IF OBJECT_ID('benchmark_frag.customer') IS NOT NULL DROP TABLE benchmark_frag.customer;
PRINT 'Creating benchmark_frag.customer...';
CREATE TABLE benchmark_frag.customer AS SELECT * FROM benchmark.customer;
PRINT 'Done: customer';

IF OBJECT_ID('benchmark_frag.customer_demographics') IS NOT NULL DROP TABLE benchmark_frag.customer_demographics;
PRINT 'Creating benchmark_frag.customer_demographics...';
CREATE TABLE benchmark_frag.customer_demographics AS SELECT * FROM benchmark.customer_demographics;
PRINT 'Done: customer_demographics';

IF OBJECT_ID('benchmark_frag.promotion') IS NOT NULL DROP TABLE benchmark_frag.promotion;
PRINT 'Creating benchmark_frag.promotion...';
CREATE TABLE benchmark_frag.promotion AS SELECT * FROM benchmark.promotion;
PRINT 'Done: promotion';

IF OBJECT_ID('benchmark_frag.household_demographics') IS NOT NULL DROP TABLE benchmark_frag.household_demographics;
PRINT 'Creating benchmark_frag.household_demographics...';
CREATE TABLE benchmark_frag.household_demographics AS SELECT * FROM benchmark.household_demographics;
PRINT 'Done: household_demographics';

PRINT 'Verifying dimension row counts...';
SELECT 'store_sales (empty)'      AS tbl, COUNT(*) AS rows FROM benchmark_frag.store_sales             UNION ALL
SELECT 'date_dim',                         COUNT(*) FROM benchmark_frag.date_dim                        UNION ALL
SELECT 'item',                             COUNT(*) FROM benchmark_frag.item                            UNION ALL
SELECT 'store',                            COUNT(*) FROM benchmark_frag.store                           UNION ALL
SELECT 'customer',                         COUNT(*) FROM benchmark_frag.customer                        UNION ALL
SELECT 'customer_demographics',            COUNT(*) FROM benchmark_frag.customer_demographics           UNION ALL
SELECT 'promotion',                        COUNT(*) FROM benchmark_frag.promotion                       UNION ALL
SELECT 'household_demographics',           COUNT(*) FROM benchmark_frag.household_demographics;

PRINT 'Setup complete. Run fragmentation/01_insert_wh.py to populate store_sales.';
