-- q05_window_function.sql
-- Query ID  : Q5
-- TPC-DS ref: Q35 / Q86 (adapted)
-- Description: Window / analytical function query — ranks customers by total spend
--              within their state, and computes a running total and percentile.
--              Tests the engine's window function execution (RANK, SUM OVER,
--              NTILE) on a moderately sized result set.

WITH customer_spend AS (
    SELECT
        c.c_customer_sk,
        c.c_customer_id,
        c.c_first_name,
        c.c_last_name,
        c.c_birth_country,
        SUM(ss.ss_net_paid) AS total_spend
    FROM _S_.store_sales ss
    JOIN _S_.customer    c  ON ss.ss_customer_sk     = c.c_customer_sk
    JOIN _S_.date_dim    d  ON ss.ss_sold_date_sk    = d.d_date_sk
    WHERE d.d_year BETWEEN 2000 AND 2002
    GROUP BY
        c.c_customer_sk,
        c.c_customer_id,
        c.c_first_name,
        c.c_last_name,
        c.c_birth_country
)
SELECT
    c_customer_id,
    c_first_name,
    c_last_name,
    c_birth_country,
    total_spend,
    RANK()       OVER (PARTITION BY c_birth_country ORDER BY total_spend DESC) AS country_rank,
    ROW_NUMBER() OVER (ORDER BY total_spend DESC)                               AS overall_rank,
    SUM(total_spend) OVER (
        PARTITION BY c_birth_country
        ORDER BY total_spend DESC
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    )                                                                           AS running_total,
    NTILE(10) OVER (ORDER BY total_spend DESC)                                  AS spend_decile
FROM customer_spend
ORDER BY
    overall_rank
OFFSET 0 ROWS FETCH NEXT 200 ROWS ONLY;
