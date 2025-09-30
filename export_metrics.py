import argparse
import json
import os
from typing import Dict

import pandas as pd
from google.cloud import bigquery

def load_config(path: str) -> Dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def bq_client(project_id: str, location: str) -> bigquery.Client:
    client = bigquery.Client(project=project_id)
    client.location = location
    return client

def export_pipeline_metrics(cfg: Dict) -> pd.DataFrame:
    project = cfg["PROJECT_ID"]
    region = cfg["REGION"].lower()
    date_start = cfg["DATE_START"]
    date_end = cfg["DATE_END"]
    price_per_tib = float(cfg.get("PRICE_PER_TIB_USD", 6.25))

    client = bq_client(project, cfg["REGION"])
    jobs_table = f"`region-{region}`.INFORMATION_SCHEMA.JOBS_BY_PROJECT"

    sql = f'''
    WITH jobs AS (
      SELECT creation_time, end_time, total_bytes_billed, job_type, query, error_result, labels
      FROM {jobs_table}
      WHERE creation_time BETWEEN TIMESTAMP('{date_start}') AND TIMESTAMP('{date_end} 23:59:59')
        AND job_type IN ('QUERY','LOAD')
        AND (
          REGEXP_CONTAINS(COALESCE(query, ''), r'PIPELINE:\\s*(ETL|ELT)')
          OR EXISTS (SELECT 1 FROM UNNEST(labels) l WHERE l.key = 'pipeline')
        )
    ),
    base AS (
      SELECT
        creation_time,
        end_time,
        total_bytes_billed,
        job_type,
        REGEXP_EXTRACT(COALESCE(query, ''), r'PIPELINE:\\s*(ETL|ELT)') AS pipeline_q,
        REGEXP_EXTRACT(COALESCE(query, ''), r'PHASE:\\s*([a-zA-Z_]+)') AS phase_q,
        (SELECT ANY_VALUE(l.value) FROM UNNEST(labels) l WHERE l.key = 'pipeline') AS pipeline_l,
        (SELECT ANY_VALUE(l.value) FROM UNNEST(labels) l WHERE l.key = 'phase') AS phase_l
      FROM jobs
      WHERE error_result IS NULL
    ),
    norm AS (
      SELECT
        COALESCE(pipeline_q, pipeline_l, 'UNLABELED') AS pipeline,
        COALESCE(phase_q, phase_l, 'unlabeled') AS phase,
        creation_time,
        end_time,
        total_bytes_billed,
        job_type
      FROM base
    )
    SELECT
      pipeline,
      phase,
      COUNT(*) AS jobs,
      SUM(TIMESTAMP_DIFF(end_time, creation_time, SECOND)) AS total_seconds,
      SUM(total_bytes_billed) AS total_bytes_billed,
      SUM(total_bytes_billed)/POW(1024,4) AS total_tib_billed,
      (SUM(total_bytes_billed)/POW(1024,4)) * {price_per_tib} AS custo_usd
    FROM norm
    GROUP BY pipeline, phase
    ORDER BY pipeline, phase;
    '''
    df = client.query(sql).to_dataframe()
    return df

def export_quality_metrics(cfg: Dict) -> pd.DataFrame:
    project = cfg["PROJECT_ID"]
    elt_gold = f"{project}.{cfg['ELT_DATASET_GOLD']}"
    etl_ds = f"{project}.{cfg['ETL_DATASET']}"

    client = bq_client(project, cfg["REGION"])

    sql = f'''
    WITH base AS (
      SELECT 'ELT' AS pipeline, * FROM `{elt_gold}.fact_trip`
      UNION ALL
      SELECT 'ETL' AS pipeline, * FROM `{etl_ds}.fact_trip`
    ),
    nulos AS (
      SELECT
        pipeline,
        AVG(CASE WHEN trip_id IS NULL THEN 1 ELSE 0 END)*100 AS pct_null_trip_id,
        AVG(CASE WHEN date_key IS NULL THEN 1 ELSE 0 END)*100 AS pct_null_date_key,
        AVG(CASE WHEN start_station_id IS NULL THEN 1 ELSE 0 END)*100 AS pct_null_start_station,
        AVG(CASE WHEN end_station_id IS NULL THEN 1 ELSE 0 END)*100 AS pct_null_end_station,
        AVG(CASE WHEN duration_min IS NULL THEN 1 ELSE 0 END)*100 AS pct_null_duration
      FROM base
      GROUP BY pipeline
    ),
    dups AS (
      SELECT pipeline, 100.0 * SUM(cnt>1)/COUNT(*) AS pct_duplicidade
      FROM (
        SELECT pipeline, trip_id, COUNT(*) AS cnt
        FROM base
        GROUP BY pipeline, trip_id
      )
      GROUP BY pipeline
    ),
    conf AS (
      SELECT
        pipeline,
        100.0 * AVG(CASE WHEN duration_min BETWEEN 1 AND 24*60 THEN 1 ELSE 0 END) AS pct_duration_ok
      FROM base
      GROUP BY pipeline
    )
    SELECT
      n.pipeline,
      n.pct_null_trip_id, n.pct_null_date_key, n.pct_null_start_station, n.pct_null_end_station, n.pct_null_duration,
      d.pct_duplicidade,
      c.pct_duration_ok
    FROM nulos n
    JOIN dups d ON d.pipeline = n.pipeline
    JOIN conf c ON c.pipeline = n.pipeline
    ORDER BY n.pipeline;
    '''
    df = client.query(sql).to_dataframe()
    return df

def main():
    ap = argparse.ArgumentParser(description="Exporta métricas (pipeline e qualidade) para CSV")
    ap.add_argument("--config", required=True, help="Caminho para config.json")
    args = ap.parse_args()

    cfg = load_config(args.config)
    os.makedirs("outputs", exist_ok=True)

    metrics_df = export_pipeline_metrics(cfg)
    metrics_csv = cfg.get("METRICS_OUTPUT_CSV", "./outputs/pipeline_metrics.csv")
    metrics_df.to_csv(metrics_csv, index=False)
    print(f"[OK] Métricas do pipeline salvas em: {metrics_csv}")

    quality_df = export_quality_metrics(cfg)
    quality_csv = cfg.get("QUALITY_OUTPUT_CSV", "./outputs/quality_metrics.csv")
    quality_df.to_csv(quality_csv, index=False)
    print(f"[OK] Métricas de qualidade salvas em: {quality_csv}")

if __name__ == "__main__":
    main()
