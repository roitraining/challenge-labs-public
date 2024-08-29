  -- SUMMARY BY BRANCH
  --
  -- STRATEGY
  -- 1. Get count of customers per branch
  -- 2. Rank branches by number of customers
  -- 3. Get sum of balances per branch
  -- 4. Rank branches by total deposits
  -- 5. Get count of loans per branch
  -- 6. Rank branches by number of loans
  -- 7. Get sum of loan amounts per branch
  -- 8. Rank branches by total loan amounts
  -- 9. Get count of credit cards per branch
  -- 10. Rank branches by number of credit cards
  -- 11. Join all branch summary tables
  --
  -- Difficulty: 3/5

CREATE OR REPLACE VIEW `views.branch_summary` AS (
WITH
  branch_customer_counts AS (
    -- get count of customers per branch
    -- grouping customers by primary_branch col
    -- project branch_id and number of customers
  SELECT
    primary_branch AS branch_id,
    COUNT(customer_id) AS num_cust,
  FROM
    `raw.customers`
  GROUP BY
    primary_branch ),
  branch_customer_ranks AS (
    -- rank branches by number of customers
    -- using rank window function
    -- project branch_id, number of customers, and rank
  SELECT
    branch_id,
    num_cust,
    RANK() OVER (ORDER BY num_cust DESC ) AS rank
  FROM
    branch_customer_counts),
  branch_deposit_sums AS (
    -- get sum of balances per branch
    -- joining accounts and customers tables
    -- grouping by primary_branch col
    -- project branch_id and total deposits
  SELECT
    primary_branch AS branch_id,
    SUM(balance) AS total_dep
  FROM
    `raw.accounts` a
  JOIN
    raw.customers c
  ON
    a.customer_id = c.customer_id
  GROUP BY
    primary_branch ),
  branch_deposit_ranks AS (
    -- rank branches by total deposits
    -- using rank window function
    -- project branch_id, total deposits, and rank
  SELECT
    branch_id,
    total_dep,
    RANK() OVER (ORDER BY total_dep DESC ) AS rank
  FROM
    branch_deposit_sums ),
  branch_loan_counts AS (
    -- get count of loans per branch
    -- joining loans, employees, and branches tables
    -- grouping by branch_id col
  SELECT
    b.branch_id AS branch_id,
    COUNT(*) AS num_loans
  FROM
    `raw.loans` l
  JOIN
    `raw.employees` e
  ON
    l.employee_id = e.employee_id
  JOIN
    `raw.branches` b
  ON
    e.branch_id = b.branch_id
  GROUP BY
    b.branch_id ),
  branch_loan_ranks AS (
    -- rank branches by number of loans
    -- using rank window function
    -- project branch_id, number of loans, and rank
  SELECT
    branch_id,
    num_loans,
    RANK() OVER (ORDER BY num_loans DESC ) AS rank
  FROM
    branch_loan_counts ),
  b_loan_totals AS (
    -- get sum of loan amounts per branch
    -- joining loans, employees, and branches tables
    -- grouping by branch_id col
    -- project branch_id and total loan amounts
  SELECT
    b.branch_id AS branch_id,
    SUM(l.loan_amount) AS total_loans
  FROM
    `raw.loans` l
  JOIN
    `raw.employees` e
  ON
    l.employee_id = e.employee_id
  JOIN
    `raw.branches` b
  ON
    e.branch_id = b.branch_id
  GROUP BY
    b.branch_id ),
  b_loan_totals_rank AS (
    -- rank branches by total loan amounts
    -- using rank window function
    -- project branch_id, total loan amounts, and rank
  SELECT
    branch_id,
    total_loans,
    RANK() OVER (ORDER BY total_loans DESC ) AS rank
  FROM
    b_loan_totals ),
  branch_card_counts AS (
    -- get count of credit cards per branch
    -- joining credit cards and customers tables
    -- grouping by branch_id col
    -- project branch_id and number of credit cards
  SELECT
    c.primary_branch AS branch_id,
    COUNT(*) AS num_cards
  FROM
    `raw.credit_cards` cc
  JOIN
    `raw.customers` c
  ON
    cc.customer_id = c.customer_id
  GROUP BY
    branch_id ),
  branch_card_ranks AS (
    -- rank branches by number of credit cards
    -- using rank window function
    -- project branch_id, number of credit cards, and rank
  SELECT
    branch_id,
    num_cards,
    RANK() OVER (ORDER BY num_cards DESC ) AS rank
  FROM
    branch_card_counts )
  -- join all branch summary tables
  -- project branch cols and count cols and rank cols
SELECT
  b.*,
  c.num_cust,
  c.rank AS cust_rank,
  d.total_dep,
  d.rank AS dep_rank,
  l.num_loans,
  l.rank AS loan_rank,
  lt.total_loans,
  lt.rank AS loan_totals_rank,
  cc.num_cards,
  cc.rank AS cards_rank
FROM
  `raw.branches` b
JOIN
  branch_customer_ranks c
ON
  b.branch_id = c.branch_id
JOIN
  branch_deposit_ranks d
ON
  c.branch_id = d.branch_id
JOIN
  branch_loan_ranks l
ON
  c.branch_id = l.branch_id
JOIN
  b_loan_totals_rank lt
ON
  c.branch_id = lt.branch_id
JOIN
  branch_card_ranks cc
ON
  c.branch_id = cc.branch_id)