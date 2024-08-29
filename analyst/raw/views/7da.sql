  -- 7 DAY ACTIVE CUSTOMERS
  -- Difficulty: 4/5
  
CREATE OR REPLACE VIEW
  `views.7da` AS (
  WITH
    transactions_w_customerids AS (
    SELECT
      DATE(t.transaction_datetime) AS transaction_date,
      a.customer_id
    FROM
      `raw.transactions` t
    JOIN
      `raw.accounts` a
    ON
      a.account_id = t.account_id
    WHERE
      DATE(t.transaction_datetime) >= DATE_ADD("2023-10-15", INTERVAL -89 day) ),
    date_customer_with_one_transaction AS (
    SELECT
      transaction_date,
      customer_id
    FROM
      transactions_w_customerids
    GROUP BY
      transaction_date,
      customer_id )
  SELECT
    DATE_ADD(transaction_date, INTERVAL i DAY) report_date,
    COUNT(DISTINCT  customer_id) unique_7day_active_customers
  FROM
    date_customer_with_one_transaction,
    UNNEST(GENERATE_ARRAY(0, 6)) i
  GROUP BY
    report_date
  HAVING
    report_date <= "2023-10-15"
  ORDER BY
    report_date DESC)