-- PIPELINE: ELT
-- PHASE: gold
CREATE OR REPLACE TABLE `{{PROJECT_ID}}.{{ELT_DATASET_GOLD}}.dim_station` AS
SELECT DISTINCT
  start_station_id AS station_id,
  ANY_VALUE(start_station_name) AS station_name
FROM `{{PROJECT_ID}}.{{ELT_DATASET_SILVER}}.bikeshare_trips_clean`
UNION DISTINCT
SELECT DISTINCT
  end_station_id AS station_id,
  ANY_VALUE(end_station_name) AS station_name
FROM `{{PROJECT_ID}}.{{ELT_DATASET_SILVER}}.bikeshare_trips_clean`
WHERE end_station_id IS NOT NULL;

CREATE OR REPLACE TABLE `{{PROJECT_ID}}.{{ELT_DATASET_GOLD}}.dim_date` AS
WITH d AS (
  SELECT DISTINCT DATE(start_time) AS dt
  FROM `{{PROJECT_ID}}.{{ELT_DATASET_SILVER}}.bikeshare_trips_clean`
)
SELECT
  dt AS date_key,
  EXTRACT(YEAR FROM dt) AS year,
  EXTRACT(MONTH FROM dt) AS month,
  EXTRACT(DAY FROM dt) AS day,
  EXTRACT(DAYOFWEEK FROM dt) AS dow
FROM d;

CREATE OR REPLACE TABLE `{{PROJECT_ID}}.{{ELT_DATASET_GOLD}}.fact_trip` AS
SELECT
  trip_id,
  bike_id,
  DATE(start_time) AS date_key,
  start_station_id,
  end_station_id,
  duration_min_clean AS duration_min
FROM `{{PROJECT_ID}}.{{ELT_DATASET_SILVER}}.bikeshare_trips_clean`
WHERE duration_min_clean IS NOT NULL;
