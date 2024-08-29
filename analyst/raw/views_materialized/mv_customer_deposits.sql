CREATE MATERIALIZED VIEW
  `views.mv_customer_deposits` AS (
      -- join the customer and accounts tables
      -- group by all the customer columns
      -- project all the customer columns and sum of account balances
    SELECT
      c.*,
      SUM(a.balance) AS total_balance
    FROM
      `raw.customers` c
    JOIN
      `raw.accounts` a
    ON
      c.customer_id = a.customer_id
    GROUP BY
      1,
      2,
      3,
      4,
      5,
      6,
      7,
      8,
      9,
      10 )