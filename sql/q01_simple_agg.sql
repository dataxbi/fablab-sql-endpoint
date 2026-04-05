-- q01_simple_agg.sql
-- Query ID  : Q1
-- TPC-DS ref: Q29 (simplified)
-- Description: Simple aggregation — total sales amount and transaction count
--              grouped by store and calendar month. Tests GROUP BY + aggregate
--              functions on the main fact table with a small date dimension join.

SELECT
    s.s_store_name,
    s.s_state,
    d.d_year,
    d.d_moy                          AS month_of_year,
    COUNT(*)                         AS num_transactions,
    SUM(ss.ss_net_paid)              AS total_net_paid,
    SUM(ss.ss_net_profit)            AS total_net_profit,
    AVG(ss.ss_net_paid)              AS avg_net_paid
FROM store_sales ss
JOIN date_dim     d ON ss.ss_sold_date_sk = d.d_date_sk
JOIN store        s ON ss.ss_store_sk     = s.s_store_sk
WHERE d.d_year = 2001
GROUP BY
    s.s_store_name,
    s.s_state,
    d.d_year,
    d.d_moy
ORDER BY
    d.d_year,
    d.d_moy,
    total_net_paid DESC;
