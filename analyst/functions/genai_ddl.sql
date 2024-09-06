CREATE OR REPLACE MODEL
  `models.upsell` REMOTE
WITH CONNECTION `us.upsell`  OPTIONS(ENDPOINT = 'gemini-1.5-flash-001')