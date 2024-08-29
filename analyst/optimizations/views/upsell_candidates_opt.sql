CREATE OR REPLACE VIEW
  `views.upsell_candidates_opt` AS (
  WITH
    t1 AS (
    SELECT
      customer_id,
      t.*,
    IF
      (ARRAY_LENGTH(accounts) = 1
        AND accounts[ORDINAL(1)].account_type = "Savings",1,0 ) AS savings_only,
      (
      SELECT
        account_type
      FROM
        UNNEST(accounts)
      WHERE
        account_id=t.account_id) AS type
    FROM
      `optimized.customer_info_nested`,
      UNNEST(transactions) AS t
    WHERE
      DATE(t.transaction_datetime) >= DATE_ADD("2023-10-15", INTERVAL -179 DAY)),
    t2 AS (
    SELECT
      customer_id,
      savings_only,
      COUNT(*) AS num_transactions
    FROM
      t1
    WHERE
      type IN ('Savings',
        'Checking')
    GROUP BY
      customer_id,
      savings_only),
    t3 AS (
    SELECT
      customer_id,
      savings_only,
      num_transactions,
      PERCENT_RANK() OVER (ORDER BY num_transactions) AS percentile
    FROM
      t2 ),
    t4 AS (
    SELECT
      *
    FROM
      t3
    WHERE
      percentile > 0.5)
  SELECT
    customer_id,
    num_transactions,
    percentile
  FROM
    t4
  WHERE
    savings_only = 1
  ORDER BY
    num_transactions DESC)