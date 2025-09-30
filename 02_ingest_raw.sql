-- PIPELINE: ELT
-- PHASE: ingest
CREATE OR REPLACE TABLE `{{PROJECT_ID}}.{{ELT_DATASET_RAW}}.bikeshare_trips_raw` AS
SELECT
  trip_id,
  bike_id,
  start_time,
  end_time,
  start_station_id,
  start_station_name,
  end_station_id,
  end_station_name,
  subscriber_type,
  duration_minutes
FROM `bigquery-public-data.austin_bikeshare.bikeshare_trips`
WHERE start_time BETWEEN TIMESTAMP('{{DATE_START}}') AND TIMESTAMP('{{DATE_END}}');
