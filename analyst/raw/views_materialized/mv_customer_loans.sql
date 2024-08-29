CREATE MATERIALIZED VIEW
  `views.mv_customer_loans` AS (
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
    1)