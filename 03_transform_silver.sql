-- PIPELINE: ELT
-- PHASE: silver
CREATE OR REPLACE TABLE `{{PROJECT_ID}}.{{ELT_DATASET_SILVER}}.bikeshare_trips_clean` AS
WITH base AS (
  SELECT
    trip_id, bike_id, start_time, end_time,
    start_station_id, start_station_name,
    end_station_id, end_station_name,
    subscriber_type, duration_minutes
  FROM `{{PROJECT_ID}}.{{ELT_DATASET_RAW}}.bikeshare_trips_raw`
),
dedup AS (
  SELECT *
  FROM (
    SELECT base.*,
           ROW_NUMBER() OVER (PARTITION BY trip_id ORDER BY end_time DESC) AS rn
    FROM base
  )
  WHERE COALESCE(trip_id, '') <> '' AND rn = 1
),
typed AS (
  SELECT
    trip_id,
    CAST(bike_id AS STRING) AS bike_id,
    start_time, end_time,
    CAST(start_station_id AS STRING) AS start_station_id,
    INITCAP(TRIM(start_station_name)) AS start_station_name,
    CAST(end_station_id AS STRING) AS end_station_id,
    INITCAP(TRIM(end_station_name)) AS end_station_name,
    INITCAP(TRIM(subscriber_type)) AS subscriber_type,
    TIMESTAMP_DIFF(end_time, start_time, MINUTE) AS duration_min_calc
  FROM dedup
  WHERE start_time IS NOT NULL AND end_time IS NOT NULL
    AND end_time >= start_time
),
rules AS (
  SELECT
    *,
    CASE WHEN duration_min_calc BETWEEN 1 AND 24*60 THEN duration_min_calc ELSE NULL END AS duration_min_clean
  FROM typed
)
SELECT * EXCEPT(duration_min_calc)
REPLACE(duration_min_clean AS duration_min_clean)
FROM rules;
