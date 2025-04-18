#!/usr/bin/env python
# coding: utf-8

from datetime import datetime
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'scripts')))

import data_ingestion_util as diu

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryDeleteTableOperator, 
    BigQueryCreateExternalTableOperator, 
    BigQueryInsertJobOperator
)
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


# load env configs
AIRFLOW_HOME = "/opt/airflow"
GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
GCS_BUCKET = os.environ.get("GCP_GCS_BUCKET")
GCP_BIGQUERY_DATASET = os.environ.get("GCP_BIGQUERY_DATASET")

# read configs from airflow
TICKERS = '{{ dag_run.conf["tickers"] }}'
START_MONTH = '{{ dag_run.conf["start_month"] }}'
END_MONTH = '{{ dag_run.conf["end_month"] }}'

# default dataset names
FNAME_TRADE_DATA = 'stock_trading_data'
OUTPUT_FILE_TRADE_DATA = f'data/{FNAME_TRADE_DATA}.csv'
FNAME_COUNTRY_DATA = 'stock_country_data'
OUTPUT_FILE_COUNTRY_DATA = f'data/{FNAME_COUNTRY_DATA}.csv'
FNAME_RETURN_DATA = 'stock_return_data'


data_elt_workflow = DAG(
    "data_extract_load_transform_dag",
    schedule_interval="0 6 2 * *",
    start_date=datetime(2025, 4, 1),
    catchup=False
)

with data_elt_workflow:

    trading_data_download_clean = PythonOperator(
        task_id="trading_data_download_clean",
        python_callable=diu.trading_data_download_clean,
        op_kwargs={
            "tickers": TICKERS,
            "start_month": START_MONTH,
            "end_month": END_MONTH,
            "output_path": f"{AIRFLOW_HOME}/{OUTPUT_FILE_TRADE_DATA}"
        }
    )

    local_to_gcs_task1 = PythonOperator(
        task_id="local_to_gcs_task1",
        python_callable=diu.upload_to_gcs,
        op_kwargs={
            "bucket": GCS_BUCKET,
            "object_name": f"{OUTPUT_FILE_TRADE_DATA}",
            "local_file": f"{AIRFLOW_HOME}/{OUTPUT_FILE_TRADE_DATA}"
        },
    )

    delete_existing_table1 = BigQueryDeleteTableOperator(
        task_id="delete_existing_external_table1",
        deletion_dataset_table=f"{GCP_PROJECT_ID}.{GCP_BIGQUERY_DATASET}.{FNAME_TRADE_DATA}_external",
        ignore_if_missing=True,  # avoids failure if table doesn't exist yet
    )

    bigquery_external_table_task1 = BigQueryCreateExternalTableOperator(
        task_id="bigquery_external_table_task1",
        table_resource={
            "tableReference": {
                "projectId": GCP_PROJECT_ID,
                "datasetId": GCP_BIGQUERY_DATASET,
                "tableId": f"{FNAME_TRADE_DATA}_external",
            },
            "externalDataConfiguration": {
                "sourceFormat": "CSV",
                "sourceUris": [f"gs://{GCS_BUCKET}/{OUTPUT_FILE_TRADE_DATA}"],
                "autodetect": True
            },
        },
    )

    create_partitioned_table_task = BigQueryInsertJobOperator(
        task_id='create_partitioned_table_task',
        configuration={
            "query": {
                "query": f"""
                    CREATE OR REPLACE TABLE `{GCP_PROJECT_ID}.{GCP_BIGQUERY_DATASET}.{FNAME_TRADE_DATA}_partitioned`
                    PARTITION BY `Date`
                    CLUSTER BY `Ticker`
                    AS
                    SELECT * FROM `{GCP_PROJECT_ID}.{GCP_BIGQUERY_DATASET}.{FNAME_TRADE_DATA}_external`
                """,
                "useLegacySql": False,
            }
        },
        location="US"
    )

    country_data_download_clean = PythonOperator(
        task_id="country_data_download_clean",
        python_callable=diu.country_data_download_clean,
        op_kwargs={
            "tickers": TICKERS,
            "output_path": f"{AIRFLOW_HOME}/{OUTPUT_FILE_COUNTRY_DATA}"
        }
    )

    local_to_gcs_task2 = PythonOperator(
        task_id="local_to_gcs_task2",
        python_callable=diu.upload_to_gcs,
        op_kwargs={
            "bucket": GCS_BUCKET,
            "object_name": f"{OUTPUT_FILE_COUNTRY_DATA}",
            "local_file": f"{AIRFLOW_HOME}/{OUTPUT_FILE_COUNTRY_DATA}"
        }
    )

    delete_existing_table2 = BigQueryDeleteTableOperator(
        task_id="delete_existing_external_table2",
        deletion_dataset_table=f"{GCP_PROJECT_ID}.{GCP_BIGQUERY_DATASET}.{FNAME_COUNTRY_DATA}_external",
        ignore_if_missing=True,  # avoids failure if table doesn't exist yet
    )

    bigquery_external_table_task2 = BigQueryCreateExternalTableOperator(
        task_id="bigquery_external_table_task2",
        table_resource={
            "tableReference": {
                "projectId": GCP_PROJECT_ID,
                "datasetId": GCP_BIGQUERY_DATASET,
                "tableId": f"{FNAME_COUNTRY_DATA}_external",
            },
            "externalDataConfiguration": {
                "sourceFormat": "CSV",
                "sourceUris": [f"gs://{GCS_BUCKET}/{OUTPUT_FILE_COUNTRY_DATA}"],
                "autodetect": False,
                "csvOptions": {
                    "skipLeadingRows": 1
                },
                "schema": {
                    "fields": [
                            {"name": "Ticker", "type": "STRING", "mode": "REQUIRED"},
                            {"name": "Name", "type": "STRING", "mode": "NULLABLE"},
                            {"name": "Country", "type": "STRING", "mode": "NULLABLE"},
                    ]
                },
            },
        },
    )

    create_materialized_table_task = BigQueryInsertJobOperator(
        task_id='create_materialized_table_task',
        configuration={
            "query": {
                "query": f"""
                    CREATE OR REPLACE TABLE `{GCP_PROJECT_ID}.{GCP_BIGQUERY_DATASET}.{FNAME_COUNTRY_DATA}_materialized`
                    AS
                    SELECT * FROM `{GCP_PROJECT_ID}.{GCP_BIGQUERY_DATASET}.{FNAME_COUNTRY_DATA}_external`
                """,
                "useLegacySql": False,
            }
        },
        location="US"
    )

    spark_data_transform_task = SparkSubmitOperator(
        task_id='spark_data_transform_task',
        application='/opt/airflow/scripts/data_transform_script.py',
        conn_id='spark_default',
        conf={
            'spark.master': 'spark://spark-master:7077',
            'spark.hadoop.google.cloud.auth.service.account.json.keyfile': '/.google/credentials/google_credentials.json',
            'spark.executorEnv.GOOGLE_APPLICATION_CREDENTIALS': '/.google/credentials/google_credentials.json',
        },
        application_args=[
            '--project_id', GCP_PROJECT_ID,
            '--dataset_id', GCP_BIGQUERY_DATASET,
            '--gcs_bucket', GCS_BUCKET,
            '--output_table', FNAME_RETURN_DATA,
        ],
    )

    trading_data_download_clean >> local_to_gcs_task1 >> delete_existing_table1 >> bigquery_external_table_task1 >> create_partitioned_table_task >> spark_data_transform_task
    country_data_download_clean >> local_to_gcs_task2 >> delete_existing_table2 >> bigquery_external_table_task2 >> create_materialized_table_task >> spark_data_transform_task