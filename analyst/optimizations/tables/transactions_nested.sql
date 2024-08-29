  CREATE TEMP TABLE denorm AS (
  SELECT
    c.*,
    a.* EXCEPT (customer_id),
    t.* EXCEPT (account_id)
  FROM
    `raw.customers` c
  JOIN
    `raw.accounts` a
  ON
    c.customer_id = a.customer_id
  JOIN
    `raw.transactions` t
  ON
    t.account_id = a.account_id);
CREATE OR REPLACE TABLE
  `optimized.transaction_nested` AS (
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