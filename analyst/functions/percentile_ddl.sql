CREATE OR REPLACE FUNCTION
  `functions.percentile` (decimal float64)
  RETURNS float64 AS ( ROUND(1 - decimal, 2) * 100)