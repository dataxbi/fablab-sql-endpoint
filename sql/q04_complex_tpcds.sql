-- q04_complex_tpcds.sql
-- Query ID  : Q4
-- TPC-DS ref: Q72 / Q14 (adapted)
-- Description: Complex TPC-DS-style query — uses multiple CTEs, correlated subqueries
--              and joins across six tables to find items where sales in a promotion
--              period significantly exceed sales outside it. Tests the optimizer's
--              handling of CTEs, subqueries and complex predicate evaluation.

WITH promo_sales AS (
    SELECT
        ss.ss_item_sk,
        SUM(ss.ss_ext_sales_price) AS promo_revenue
    FROM store_sales ss
    JOIN date_dim    d ON ss.ss_sold_date_sk = d.d_date_sk
    JOIN promotion   p ON ss.ss_promo_sk     = p.p_promo_sk
    WHERE d.d_year = 2002
      AND d.d_qoy  = 2
      AND p.p_channel_tv = 'Y'
    GROUP BY ss.ss_item_sk
),
total_sales AS (
    SELECT
        ss.ss_item_sk,
        SUM(ss.ss_ext_sales_price) AS total_revenue
    FROM store_sales ss
    JOIN date_dim    d ON ss.ss_sold_date_sk = d.d_date_sk
    WHERE d.d_year = 2002
      AND d.d_qoy  = 2
    GROUP BY ss.ss_item_sk
)
SELECT
    i.i_item_id,
    i.i_item_desc,
    i.i_category,
    ps.promo_revenue,
    ts.total_revenue,
    ROUND(
        CASE WHEN ts.total_revenue = 0 THEN 0
             ELSE 100.0 * ps.promo_revenue / ts.total_revenue
        END, 2
    )                               AS promo_pct
FROM promo_sales   ps
JOIN total_sales   ts ON ps.ss_item_sk = ts.ss_item_sk
JOIN item           i ON ps.ss_item_sk = i.i_item_sk
JOIN customer_demographics cd
    ON EXISTS (
        SELECT 1
        FROM store_sales ss2
        JOIN household_demographics hd ON ss2.ss_hdemo_sk = hd.hd_demo_sk
        WHERE ss2.ss_item_sk = ps.ss_item_sk
          AND cd.cd_gender   = 'F'
          AND hd.hd_vehicle_count > 0
    )
WHERE ts.total_revenue > 0
ORDER BY
    promo_pct DESC,
    i.i_item_id
LIMIT 50;
