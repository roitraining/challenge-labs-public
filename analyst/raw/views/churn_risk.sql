  -- CUSTOMERS AT RISK OF CHURN AS OF 2023-10-15
  --
  -- STRATEGY
  -- 1. Join the customer, accounts, and transaction tables
  -- 2. Filter by transaction date using DATE() and DATE_SUB()
  -- 3. Group by all the customer columns
  -- 4. Filter by count of transactions using HAVING
  --
  -- Difficulty: 2/5
CREATE OR REPLACE VIEW
  `views.churn_risk` AS(
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
    10
  HAVING
    COUNT(t.transaction_id) < 5)