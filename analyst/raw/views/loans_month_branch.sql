  -- LOANS BY MONTH, ALL BRANCHES, 12 MONTH PERIOD ENDING 2023-10-15
  --
  -- STRATEGY
  -- 1. Join the loans table to the employees table to get branch_ids
  -- 2. Extract month_year from loan_date using FORMAT_DATE()
  -- 3. Filter by loan_date using DATE_SUB()
  -- 4. Group by month_year and branch_id
  -- 5. Order by month_year and branch_id
  --
  -- Difficulty: 1/5
CREATE OR REPLACE VIEW
  views.loans_by_branch AS (
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
    branch_id
  ORDER BY
    month_year,
    branch_id)