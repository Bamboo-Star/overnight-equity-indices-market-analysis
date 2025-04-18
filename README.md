# Overnight Stock Price Analysis
## Problem Description

## Project Architecture

Terraform --> Data --> GCP (Optimize) --> Spark (Transform) --> Dashboard --> Analysis
Airflow-Docker orchestration

## Steps to Reproduce

### Terraform
Terraform to create infrastructure resources according to configs in the .env file.

1. In Google Cloud, 
    create a new project named: "overnight-stock-price-analysis"
2. In Google Cloud "IAM & Admin" / "Service accounts", 
    create a new service account and grant the below access roles
    * BigQuery Admin
    * Dataproc Administrator
    * Storage Admin

4. In Google Cloud "IAM & Admin" / "Service accounts" / "Service account: xxxx" / "Keys", 
    1. Click "Add key" to create a new json key
    2. Save the key to a google_credentials.json file
    3. Place the file in the project folder
5. In the project folder .env_example file, 
    update the `<<proj_dir>>` to your own project directory and rename the file to .env
6. In the project terraform folder,
    run the below commands in sequence
    ```
    terraform init
    export $(grep -v '^#' ../.env | xargs)
    terraform plan
    terraform apply
    ```
7. Enable the corresponding access if anything goes wrong. 
    Retry the failed commands until success
8. Now the cloud storage bucket and bigquery schema should be created.
9. Destroy the cloud infra after use
    ```
    terraform destroy
    ```


### Airflow
Airflow to orchestrate data ingestion and transformation processes.

1. In the project airflow folder docker-compose.yaml,
    replace the project directory `/Users/xiaozhuxin/Desktop/` with your own project directory.
2. Open the "Docker Desktop" App
3. In bash, build and bring up docker compose in the airflow directory
    ```
    cd <<proj_dir>>/overnight-stock-price-analysis/airflow
    docker-compose build
    docker-compose up -d
    ```
4. In browswer, 
    open 'localhost:8080' and login using username=password=admin
5. Trigger the data_ingestion_bigquery_dag using the below config
    ```
    {
        "tickers": "['QQQ','XIU.TO','EWW','EWZ','000001.SS','^HSI','ES3.SI','^N225','^KS11','^BSESN','STW.AX','^FCHI','^GDAXI','IMIB.MI','^TA125.TA']",
        "start_month": "2020-01",
        "end_month": "2025-01"
    }
    ```
6. Now the below 3 tables should be created in BigQuery
    * stock_trading_data_external
    * stock_trading_data_partitioned
    * stock_country_data_external
7. Trigger the data_transformation_spark_dag without any config
8. Now the below 2 additional table should be created
    * stock_return_data
    * stock_return_data_partitioned
9. Stop the docker compose after use
    ```
    docker-compose down
    ```