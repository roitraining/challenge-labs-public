CREATE OR REPLACE FUNCTION
	`functions.translate`(x STRING) RETURNS STRING
REMOTE WITH CONNECTION `us.translate`
OPTIONS (endpoint = 'https://translate-nmwkl6pwxa-uc.a.run.app', max_batching_rows = 10);