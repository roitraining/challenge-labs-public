CREATE OR REPLACE VIEW
  views.upsell_candidates AS (
  WITH
    customers_accounts AS (
      -- join customers and accounts tables
      -- group by customer_id
      -- project customer_id and array of account types
    SELECT
      c.customer_id,
      ARRAY_AGG(account_type) AS accts
    FROM
      raw.customers c
    JOIN
      raw.accounts a
    ON
      c.customer_id = a.customer_id
    GROUP BY
      customer_id),
    savings_only AS (
      -- query the customer_accounts intermediate table
      -- filter by customers that have an array of length 1
      -- filter by customers where first array element is "Savings"
      -- project customer_id
    SELECT
      customer_id
    FROM
      customers_accounts
    WHERE
      ARRAY_LENGTH(accts) = 1
      AND accts[ORDINAL(1)] = "Savings"),
    transactions_accounts AS (
      -- join the accounts and transactions tables
      -- filter by transactions in the last 180 days
      -- filter by account_type = "Savings" or "Checking"
      -- group by account_id, customer_id, and account_type
      -- project customer_id and num_transactions
    SELECT
      COUNT(transaction_id) AS num_transactions,
      customer_id,
    FROM
      raw.accounts a
    JOIN
      raw.transactions t
    ON
      a.account_id = t.account_id
    WHERE
      DATE(t.transaction_datetime) >= DATE_ADD("2023-10-15", INTERVAL -179 DAY)
      AND (account_type = "Savings"
        OR account_type="Checking")
    GROUP BY
      customer_id ),
    percentiles AS (
      -- use PERCENT_RANK() window function to get percentile for each customer
      -- project customer_id, num_transactions, and percentile
    SELECT
      customer_id,
      num_transactions,
      PERCENT_RANK() OVER (ORDER BY num_transactions) AS percentile
    FROM
      transactions_accounts),
    top_half AS (
      -- filter percentile table for percentile > 0.5
      -- project customer_id, num_transactions, and percentile
    SELECT
      *
    FROM
      percentiles
    WHERE
      percentile > 0.5 )
    -- join savings_only and top_half tables
    -- project customer_id, num_transactions, and percentile
  SELECT
    s.customer_id,
    num_transactions,
    percentile
  FROM
    savings_only s
  JOIN
    top_half t
  ON
    s.customer_id = t.customer_id
  ORDER BY
    num_transactions DESC)