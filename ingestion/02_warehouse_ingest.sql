-- Query ID: 02_warehouse_ingest
-- Description: Warehouse SF100 ingestion via CTAS cross-database query.
--   Copies all 8 benchmark tables from [LH_01].[benchmark_default] into
--   the WH_01 benchmark schema using CREATE TABLE AS SELECT.
--   Run against the WH_01 SQL endpoint via sqlcmd.
--
-- Usage:
--   sqlcmd -S <WH_SERVER> -d WH_01 -G -i 02_warehouse_ingest.sql -l 7200
--
-- Prerequisites:
--   - Schema 'benchmark' must exist in WH_01:
--       IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name='benchmark') EXEC('CREATE SCHEMA benchmark')
--   - LH_01 must be accessible (same workspace, cross-DB query)
--   - Lakehouse benchmark_default schema must be fully ingested (SF100)

SET NOCOUNT ON;
PRINT 'Starting WH ingestion via CTAS from LH_01.benchmark_default...';

IF OBJECT_ID('benchmark.store_sales') IS NOT NULL DROP TABLE benchmark.store_sales;
PRINT 'Creating benchmark.store_sales...';
CREATE TABLE benchmark.store_sales AS SELECT * FROM [LH_01].[benchmark_default].[store_sales];
PRINT 'Done: store_sales';

IF OBJECT_ID('benchmark.date_dim') IS NOT NULL DROP TABLE benchmark.date_dim;
PRINT 'Creating benchmark.date_dim...';
CREATE TABLE benchmark.date_dim AS SELECT * FROM [LH_01].[benchmark_default].[date_dim];
PRINT 'Done: date_dim';

IF OBJECT_ID('benchmark.item') IS NOT NULL DROP TABLE benchmark.item;
PRINT 'Creating benchmark.item...';
CREATE TABLE benchmark.item AS SELECT * FROM [LH_01].[benchmark_default].[item];
PRINT 'Done: item';

IF OBJECT_ID('benchmark.store') IS NOT NULL DROP TABLE benchmark.store;
PRINT 'Creating benchmark.store...';
CREATE TABLE benchmark.store AS SELECT * FROM [LH_01].[benchmark_default].[store];
PRINT 'Done: store';

IF OBJECT_ID('benchmark.customer') IS NOT NULL DROP TABLE benchmark.customer;
PRINT 'Creating benchmark.customer...';
CREATE TABLE benchmark.customer AS SELECT * FROM [LH_01].[benchmark_default].[customer];
PRINT 'Done: customer';

IF OBJECT_ID('benchmark.customer_demographics') IS NOT NULL DROP TABLE benchmark.customer_demographics;
PRINT 'Creating benchmark.customer_demographics...';
CREATE TABLE benchmark.customer_demographics AS SELECT * FROM [LH_01].[benchmark_default].[customer_demographics];
PRINT 'Done: customer_demographics';

IF OBJECT_ID('benchmark.promotion') IS NOT NULL DROP TABLE benchmark.promotion;
PRINT 'Creating benchmark.promotion...';
CREATE TABLE benchmark.promotion AS SELECT * FROM [LH_01].[benchmark_default].[promotion];
PRINT 'Done: promotion';

IF OBJECT_ID('benchmark.household_demographics') IS NOT NULL DROP TABLE benchmark.household_demographics;
PRINT 'Creating benchmark.household_demographics...';
CREATE TABLE benchmark.household_demographics AS SELECT * FROM [LH_01].[benchmark_default].[household_demographics];
PRINT 'Done: household_demographics';

PRINT 'Verifying row counts...';
SELECT 'store_sales'             AS tbl, COUNT(*) AS rows FROM benchmark.store_sales             UNION ALL
SELECT 'date_dim',                        COUNT(*) FROM benchmark.date_dim                        UNION ALL
SELECT 'item',                            COUNT(*) FROM benchmark.item                            UNION ALL
SELECT 'store',                           COUNT(*) FROM benchmark.store                           UNION ALL
SELECT 'customer',                        COUNT(*) FROM benchmark.customer                        UNION ALL
SELECT 'customer_demographics',           COUNT(*) FROM benchmark.customer_demographics           UNION ALL
SELECT 'promotion',                       COUNT(*) FROM benchmark.promotion                       UNION ALL
SELECT 'household_demographics',          COUNT(*) FROM benchmark.household_demographics;
