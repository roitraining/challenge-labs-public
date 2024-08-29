CREATE OR REPLACE VIEW
  views.loans_opt AS (
  SELECT
    FORMAT_DATE('%m/%Y', loan_date) AS month_year,
    COUNT(*) AS new_loan_count,
    SUM(l.loan_amount) AS new_loan_total
  FROM
    `optimized.loans_partitioned_clustered` l
  WHERE
    loan_date >= DATE_SUB("2023-10-15", INTERVAL 12 MONTH)
  GROUP BY
    month_year
  ORDER BY
    month_year);

CREATE OR REPLACE VIEW
  views.loans_by_branch_opt AS (
  SELECT
    FORMAT_DATE('%m/%Y', loan_date) AS month_year,
    branch_id AS branch_id,
    COUNT(*) AS new_loan_count,
    SUM(loan_amount) AS new_loan_total
  FROM
    `optimized.loans_partitioned_clustered` l
  WHERE
    loan_date >= DATE_SUB("2023-10-15", INTERVAL 12 MONTH)
  GROUP BY
    month_year,
    branch_id
  ORDER BY
    month_year,
    branch_id)