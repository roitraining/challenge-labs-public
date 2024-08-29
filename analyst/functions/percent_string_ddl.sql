CREATE OR REPLACE FUNCTION
  `functions.percent_string` (decimal float64)
  RETURNS string AS (CONCAT(ROUND(decimal, 2) * 100,"%"))