-- INITIAL
CREATE OR REPLACE TABLE l3_marketing.scd_activities
PARTITION BY DATE(dim_end_datetime)
CLUSTER BY status
AS
WITH base AS (
    SELECT *
    FROM activities
  WHERE run_date < CURRENT_DATE()
)
SELECT
  updated_at_wib AS dim_start_datetime
  , CASE WHEN rn = 1 THEN DATETIME(NULL) ELSE LAG(updated_at_wib) OVER(PARTITION BY _id ORDER BY updated_at_wib DESC) END AS dim_end_datetime
  , (rn = 1) AS dim_is_active
  , * EXCEPT(rn)
FROM (
  SELECT
    * EXCEPT(rn)
    , ROW_NUMBER() OVER(PARTITION BY _id ORDER BY updated_at_wib DESC) AS rn
  FROM (
    SELECT 
      *
      , ROW_NUMBER() OVER(PARTITION BY _id, DATE(updated_at_wib) ORDER BY updated_at_wib DESC, run_date ASC) AS rn
    FROM (
      SELECT *
      FROM base
      UNION ALL
      SELECT * REPLACE(created_at_wib AS updated_at_wib)
      FROM base
      QUALIFY ROW_NUMBER() OVER(PARTITION BY _id ORDER BY updated_at_wib) = 1 
    )
  )
  WHERE rn = 1
)
;