CREATE OR REPLACE MODEL
  `models.upsell` REMOTE
WITH CONNECTION `us.upsell`  OPTIONS(ENDPOINT = 'gemini-2.0-flash')
