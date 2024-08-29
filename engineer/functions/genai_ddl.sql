CREATE OR REPLACE MODEL
  `models.upsell` REMOTE
WITH CONNECTION `us.upsell`  OPTIONS(ENDPOINT = 'text-bison')