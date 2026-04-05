-- q02_large_join.sql
-- Query ID  : Q2
-- TPC-DS ref: Q19 (adapted)
-- Description: Large star-schema join — total sales by item brand and store state,
--              joining the fact table with four dimension tables. Tests the engine's
--              ability to handle multi-table joins with GROUP BY and ORDER BY.

SELECT
    i.i_brand_id,
    i.i_brand,
    i.i_manufact_id,
    i.i_manufact,
    s.s_state,
    d.d_year,
    COUNT(*)                        AS num_sales,
    SUM(ss.ss_ext_sales_price)      AS total_ext_sales_price,
    SUM(ss.ss_net_paid)             AS total_net_paid
FROM store_sales          ss
JOIN date_dim             d  ON ss.ss_sold_date_sk    = d.d_date_sk
JOIN item                 i  ON ss.ss_item_sk         = i.i_item_sk
JOIN store                s  ON ss.ss_store_sk        = s.s_store_sk
JOIN customer             c  ON ss.ss_customer_sk     = c.c_customer_sk
WHERE d.d_year = 2002
  AND i.i_category IN ('Books', 'Electronics', 'Sports')
GROUP BY
    i.i_brand_id,
    i.i_brand,
    i.i_manufact_id,
    i.i_manufact,
    s.s_state,
    d.d_year
ORDER BY
    total_net_paid DESC,
    i.i_brand
LIMIT 100;
