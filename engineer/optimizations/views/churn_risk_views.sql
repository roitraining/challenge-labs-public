CREATE MATERIALIZED VIEW
  `views.trans_count_mv` AS(
  WITH
    denorm AS (
    SELECT
      *
    FROM
      `optimized.transaction_nested`,
      UNNEST(transactions))
  SELECT
    customer_id,
    first_name,
    last_name,
    email,
    phone_number,
    address,
    city,
    province,
    postal_code,
    primary_branch,
    COUNT(transaction_id) AS num_transactions
  FROM
    denorm
  WHERE
    DATE(transaction_datetime) >= DATE_ADD("2023-10-15", INTERVAL -90 DAY)
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
    10);
CREATE VIEW
  `views.churn_risk_opt` AS (
  SELECT
    *
  FROM
    views.trans_count_mv
  WHERE
    num_transactions < 5 )