  -- LOANS BY MONTH, ALL BRANCHES, 12 MONTH PERIOD ENDING 2023-10-15
  --
  -- STRATEGY
  -- 1. Extract month_year from loan_date using FORMAT_DATE()
  -- 2. Filter by loan_date using DATE_SUB()
  -- 3. Group by month_year
  -- 4. Order by month_year
  --
  -- Difficulty: 1/5
CREATE OR REPLACE VIEW
  views.loans AS (
  SELECT
    FORMAT_DATE('%m/%Y', loan_date) AS month_year,
    COUNT(*) AS new_loan_count,
    SUM(l.loan_amount) AS new_loan_total
  FROM
    `raw.loans` l
  WHERE
    loan_date >= DATE_SUB("2023-10-15", INTERVAL 12 MONTH)
  GROUP BY
    month_year
  ORDER BY
    month_year)