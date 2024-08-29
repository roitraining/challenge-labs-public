CREATE MATERIALIZED VIEW
  `views.mv_churn_risk` AS(
  SELECT
    c.*,
    COUNT(t.account_id) AS num_transactions
  FROM
    `raw.customers` c
  JOIN
    `raw.accounts` a
  ON
    c.customer_id = a.customer_id
  JOIN
    `raw.transactions` t
  ON
    t.account_id = a.account_id
  WHERE
    DATE(t.transaction_datetime) >= DATE_ADD("2023-10-15", INTERVAL -90 DAY)
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
    10)