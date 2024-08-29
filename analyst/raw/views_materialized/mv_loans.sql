CREATE MATERIALIZED VIEW
  views.mv_loans AS (
  SELECT
    FORMAT_DATE('%m/%Y', loan_date) AS month_year,
    COUNT(*) AS new_loan_count,
    SUM(l.loan_amount) AS new_loan_total
  FROM
    `raw.loans` l
  WHERE
    loan_date >= DATE_SUB("2023-10-15", INTERVAL 12 MONTH)
  GROUP BY
    month_year)