SELECT
  date,
  customer_id,
  functions.translate(feedback.feedback) AS french_feedback
FROM
  raw.feedback