CREATE OR REPLACE VIEW
  `views.high_value_customers` AS (
  WITH
    ranked_customers AS (
    SELECT
      c.*,
      l.total_loans,
      functions.percentile( PERCENT_RANK() OVER (ORDER BY l.total_loans DESC ) + PERCENT_RANK() OVER (ORDER BY c.total_balance DESC ) / 2) AS percentile
  FROM
    `views.mv_customer_deposits` c
  JOIN
    `views.mv_customer_loans` l
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