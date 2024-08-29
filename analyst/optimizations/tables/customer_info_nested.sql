CREATE TEMP TABLE cust_accounts AS (
  SELECT
    customer_id,
    ARRAY_AGG(STRUCT( account_id,
        account_type,
        balance,
        date_opened )) AS accounts
  FROM
    `raw.accounts` a
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
    `raw.loans`
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
    `raw.transactions` t
  JOIN
    raw.accounts a
  ON
    a.account_id = t.account_id
  GROUP BY
    customer_id);
CREATE OR REPLACE TABLE
  `optimized.customer_info_nested` AS (
  SELECT
    c.*,
    ca.accounts,
    cl.loans,
    t.transactions
  FROM
    transactions t
  RIGHT JOIN
    `raw.customers` c
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