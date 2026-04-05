-- q03_top_n_selective.sql
-- Query ID  : Q3
-- TPC-DS ref: Q6 / Q42 (adapted)
-- Description: Top N with selective filters — top 10 items by net revenue within
--              a specific category and date range. Tests predicate pushdown and
--              the ability to efficiently prune data with selective WHERE clauses.

SELECT
    i.i_item_id,
    i.i_item_desc,
    i.i_category,
    i.i_class,
    SUM(ss.ss_ext_sales_price)   AS revenue,
    SUM(ss.ss_net_paid)          AS net_paid,
    COUNT(*)                     AS num_transactions
FROM store_sales ss
JOIN date_dim    d ON ss.ss_sold_date_sk = d.d_date_sk
JOIN item        i ON ss.ss_item_sk      = i.i_item_sk
WHERE i.i_category = 'Electronics'
  AND d.d_year = 2001
  AND d.d_qoy  = 4
GROUP BY
    i.i_item_id,
    i.i_item_desc,
    i.i_category,
    i.i_class
ORDER BY
    revenue DESC
LIMIT 10;
