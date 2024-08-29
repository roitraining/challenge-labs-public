CREATE MATERIALIZED VIEW
  views.mv_loans_by_branch AS (
  SELECT
    FORMAT_DATE('%m/%Y', loan_date) AS month_year,
    e.branch_id AS branch_id,
    COUNT(*) AS new_loan_count,
    SUM(l.loan_amount) AS new_loan_total
  FROM
    `raw.loans` l
  JOIN
    `raw.employees` e
  ON
    e.employee_id = l.employee_id
  WHERE
    loan_date >= DATE_SUB("2023-10-15", INTERVAL 12 MONTH)
  GROUP BY
    month_year,
    branch_id)