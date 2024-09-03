CREATE OR REPLACE FUNCTION
  `functions.composite_percentile` (inp_a float64, inp_b float64)
  RETURNS float64 AS (ROUND((inp_a*100 + inp_b*100)/2,2))