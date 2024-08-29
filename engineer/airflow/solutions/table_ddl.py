ddls = {}

ddls["loans"] = """
CREATE OR REPLACE TABLE
  optimized_composer.loans_partitioned_clustered ( 
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
    `raw_composer.loans` l
  JOIN
    `raw_composer.employees` e
  ON
    l.employee_id = e.employee_id)

"""

ddls["transactions"] = """
  CREATE TEMP TABLE denorm AS (
  SELECT
    c.*,
    a.* EXCEPT (customer_id),
    t.* EXCEPT (account_id)
  FROM
    `raw_composer.customers` c
  JOIN
    `raw_composer.accounts` a
  ON
    c.customer_id = a.customer_id
  JOIN
    `raw_composer.transactions` t
  ON
    t.account_id = a.account_id);
CREATE OR REPLACE TABLE
  `optimized_composer.transaction_nested` AS (
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
    account_id,
    account_type,
    balance,
    date_opened,
    ARRAY_AGG(STRUCT(transaction_id,
        transaction_type,
        amount,
        transaction_datetime)) AS transactions
  FROM
    denorm
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
    10,
    11,
    12,
    13,
    14)
  """

ddls["customer_info_nested"] = """
CREATE TEMP TABLE cust_accounts AS (
  SELECT
    customer_id,
    ARRAY_AGG(STRUCT( account_id,
        account_type,
        balance,
        date_opened )) AS accounts
  FROM
    `raw_composer.accounts` a
  GROUP BY
    customer_id); 
    
CREATE TEMP TABLE cust_loans AS (
  SELECT
    customer_id,
    ARRAY_AGG(STRUCT( loan_id,
        employee_id,
        loan_type,
        loan_amount,
        loan_date )) AS loans
  FROM
    `raw_composer.loans`
  GROUP BY
    customer_id); 
    
CREATE TEMP TABLE transactions AS (
  SELECT
    customer_id,
    ARRAY_AGG(STRUCT( transaction_id,
        t.account_id,
        transaction_type,
        amount,
        transaction_datetime )) AS transactions
  FROM
    `raw_composer.transactions` t
  JOIN
    raw_composer.accounts a
  ON
    a.account_id = t.account_id
  GROUP BY
    customer_id);

CREATE OR REPLACE TABLE
  `optimized_composer.customer_info_nested` AS (
  SELECT
    c.*,
    ca.accounts,
    cl.loans,
    t.transactions
  FROM
    transactions t
  RIGHT JOIN
    `raw_composer.customers` c
  ON
    t.customer_id = c.customer_id
  LEFT JOIN
    cust_accounts ca
  ON
    ca.customer_id = c.customer_id
  LEFT JOIN
    cust_loans cl
  ON
    cl.customer_id = c.customer_id)
"""