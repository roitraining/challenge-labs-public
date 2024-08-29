  -- HIGH VALUE CUSTOMERS
  --
  -- STRATEGY
  -- 1. Join the customer, accounts, and transaction tables
  -- 2. Filter by transaction date using DATE() and DATE_SUB()
  -- 3. Group by all the customer columns
  -- 4. Filter by count of transactions using HAVING
  --
  -- Difficulty: 2/5
CREATE OR REPLACE VIEW
  `views.high_value_customers` AS (
  WITH
    customer_deposits AS (
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
      10 ),
    customer_loans AS (
      -- join the customer and accounts tables
      -- group by all the customer_id column
      -- project all the customer_id and sum of account balances
    SELECT
      c.customer_id,
      SUM(l.loan_amount) AS total_loans
    FROM
      `raw.customers` c
    JOIN
      `raw.loans` l
    ON
      c.customer_id = l.customer_id
    GROUP BY
      1),
    -- join the customer_deposits and customer_loans tables
    -- project all the customer_deposits columns and total_loans
    -- calculate the overall value rank using
    -- 1 - ((percent_rank of total_loans + percent_rank of total_balance) / 2) * 100
    -- order by value_rank descending
    ranked_customers AS (
    SELECT
      c.*,
      l.total_loans,
      ROUND( 1 -( PERCENT_RANK() OVER (ORDER BY l.total_loans DESC ) + PERCENT_RANK() OVER (ORDER BY c.total_balance DESC ) ) / 2, 2 ) * 100 AS percentile
    FROM
      customer_deposits c
    JOIN
      customer_loans l
    ON
      l.customer_id = c.customer_id)
  SELECT
    *
  FROM
    ranked_customers
  WHERE
    percentile > 90
  ORDER BY
    percentile DESC)