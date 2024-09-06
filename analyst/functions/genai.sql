WITH
  customer_info AS(
  SELECT
    *
  FROM
    `views.upsell_candidates_opt` u
  JOIN
    `raw.customers` c
  ON
    c.customer_id = u.customer_id ),
  prompts AS (
  SELECT
      CONCAT( "Please write an email a customer named: ", 
          c.first_name, 
          " The email should be professional but convivial, and should recommend that the user ", 
          "would benefit from adding a checking account to their savings account. ", 
          "Tailor the email based on the following information... ",
          "Percentile ranking of transaction volume relative to other customers with savings and/or checking:",
          round(c.percentile * 100),
          "Total number of transaction in 6 month period: ",
          c.num_transactions,
          "Primary banking branch: ",
          b.branch_name,
          "[additional context/direction goes here]"
      ) AS prompt
  FROM
    customer_info c
  JOIN
    `raw.branches` b
  ON
    b.branch_id = c.primary_branch)
SELECT
  *
FROM
  ML.GENERATE_TEXT( MODEL `models.upsell`,
    TABLE prompts,
    STRUCT( 0.9 AS temperature,
      300 AS max_output_tokens,
      0.5 AS top_p,
      40 AS top_k,
      TRUE AS flatten_json_output) )