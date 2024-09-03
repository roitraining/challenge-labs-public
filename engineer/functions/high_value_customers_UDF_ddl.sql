CREATE OR REPLACE VIEW
  `views.high_value_customers_opt` AS(
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
      `optimized.customer_info_nested` ),
    ranked_customers AS (
    SELECT
      *,
      functions.composite_percentile( PERCENT_RANK() OVER (ORDER BY account_balance),
        PERCENT_RANK() OVER (ORDER BY loan_amount) ) AS percentile
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