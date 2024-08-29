ddls = {}

ddls["loans"] = """
CREATE MATERIALIZED VIEW
  views_composer.loans_mv AS (
  SELECT
    FORMAT_DATE('%m/%Y', loan_date) AS month_year,
    COUNT(*) AS new_loan_count,
    SUM(l.loan_amount) AS new_loan_total
  FROM
    `optimized_composer.loans_partitioned_clustered` l
  WHERE
    loan_date >= DATE_SUB("2023-10-15", INTERVAL 12 MONTH)
  GROUP BY
    month_year)
"""

ddls["loans_by_month"] = """
CREATE MATERIALIZED VIEW
  views_composer.loans_by_branch_mv AS (
  SELECT
    FORMAT_DATE('%m/%Y', loan_date) AS month_year,
    branch_id AS branch_id,
    COUNT(*) AS new_loan_count,
    SUM(l.loan_amount) AS new_loan_total
  FROM
    `optimized_composer.loans_partitioned_clustered` l
  WHERE
    loan_date >= DATE_SUB("2023-10-15", INTERVAL 12 MONTH)
  GROUP BY
    month_year,
    branch_id)
"""

ddls["churn"] = """
CREATE MATERIALIZED VIEW
  `views_composer.trans_count_mv` AS(
  WITH
    denorm AS (
    SELECT
      *
    FROM
      `optimized_composer.transaction_nested`,
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
  `views_composer.churn_risk_opt` AS (
  SELECT
    *
  FROM
    views_composer.trans_count_mv
  WHERE
    num_transactions < 5 )
"""

ddls["value"] = """
CREATE OR REPLACE VIEW
  `views_composer.high_value_customers_opt` AS(
  WITH
    sums AS (
    SELECT
      * EXCEPT(accounts,
        loans,
        transactions),
      (
      SELECT
        SUM(balance)
      FROM
        UNNEST(accounts)) AS account_balance,
      (
      SELECT
        SUM(loan_amount)
      FROM
        UNNEST(loans)) AS loan_amount
    FROM
      `optimized_composer.customer_info_nested` ),
    ranked_customers AS (
    SELECT
      *,
      ROUND( 1 -( PERCENT_RANK() OVER (ORDER BY loan_amount DESC ) + PERCENT_RANK() OVER (ORDER BY account_balance DESC ) ) / 2, 2 ) * 100 AS percentile
    FROM
      sums )
  SELECT
    *
  FROM
    ranked_customers
  WHERE
    percentile > 90
  ORDER BY
    percentile DESC)
"""

ddls["upsell"] = """
CREATE OR REPLACE VIEW
  `views_composer.upsell_candidates_opt` AS (
  WITH
    t1 AS (
    SELECT
      customer_id,
      t.*,
    IF
      (ARRAY_LENGTH(accounts) = 1
        AND accounts[ORDINAL(1)].account_type = "Savings",1,0 ) AS savings_only,
      (
      SELECT
        account_type
      FROM
        UNNEST(accounts)
      WHERE
        account_id=t.account_id) AS type
    FROM
      `optimized_composer.customer_info_nested`,
      UNNEST(transactions) AS t
    WHERE
      DATE(t.transaction_datetime) >= DATE_ADD("2023-10-15", INTERVAL -179 DAY)),
    t2 AS (
    SELECT
      customer_id,
      savings_only,
      COUNT(*) AS num_transactions
    FROM
      t1
    WHERE
      type IN ('Savings',
        'Checking')
    GROUP BY
      customer_id,
      savings_only),
    t3 AS (
    SELECT
      customer_id,
      savings_only,
      num_transactions,
      PERCENT_RANK() OVER (ORDER BY num_transactions) AS percentile
    FROM
      t2 ),
    t4 AS (
    SELECT
      *
    FROM
      t3
    WHERE
      percentile > 0.5)
  SELECT
    customer_id,
    num_transactions,
    percentile
  FROM
    t4
  WHERE
    savings_only = 1
  ORDER BY
    num_transactions DESC)
"""
