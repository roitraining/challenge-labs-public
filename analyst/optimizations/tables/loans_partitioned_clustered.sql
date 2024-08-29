CREATE OR REPLACE TABLE
  optimized.loans_partitioned_clustered ( 
    `loan_id` STRING NOT NULL,
    `customer_id` STRING NOT NULL,
    `employee_id` STRING NOT NULL,
    `loan_type` STRING,
    `loan_amount` NUMERIC,
    `loan_date` DATE,
    `branch_id` STRING,
    )
PARTITION BY
  loan_date
CLUSTER BY
  branch_id AS (
  SELECT
    l.*,
    e.branch_id
  FROM
    `raw.loans` l
  JOIN
    `raw.employees` e
  ON
    l.employee_id = e.employee_id)