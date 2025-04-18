import argparse
import os
from pyspark.sql import SparkSession

# parse arguments
parser = argparse.ArgumentParser()

parser.add_argument('--project_id', required=True)
parser.add_argument('--dataset_id', required=True)
parser.add_argument('--gcs_bucket', required=True)
parser.add_argument('--output_table', required=True)

args = parser.parse_args()

gcp_credential = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')
project_id = args.project_id
dataset_id = args.dataset_id
gcs_bucket = args.gcs_bucket
output_table = args.output_table

# start a spark session
spark = SparkSession.builder \
    .appName("ReadFromBigQuery") \
    .master("local[*]") \
    .config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.36.0") \
    .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
    .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", gcp_credential) \
    .getOrCreate()

spark.conf.set("parentProject", project_id)
spark.conf.set("spark.hadoop.google.cloud.project.id", project_id)

# read data from bigquery
df_trading = spark.read.format("bigquery") \
    .option("project", project_id) \
    .option("dataset", dataset_id) \
    .option("table", "stock_trading_data_partitioned") \
    .option("credentialsFile", gcp_credential) \
    .load()

df_trading.registerTempTable('stock_trading_data')

df_country = spark.read.format("bigquery") \
    .option("project", project_id) \
    .option("dataset", dataset_id) \
    .option("table", "stock_country_data_materialized") \
    .option("credentialsFile", gcp_credential) \
    .load()

df_country.registerTempTable('stock_country_data')

# join tables and transform to return
df_return = spark.sql("""
    SELECT
        std.Ticker, 
        scd.Name,
        scd.Country,
        std.Date, 
        std.Close/std.Open as Return_Intrad,
        std.Open/(LAG(std.Close,1) OVER (PARTITION BY std.Ticker ORDER BY std.Date asc)) as Return_Ovrnigt
    FROM 
        stock_trading_data std
    LEFT JOIN 
        stock_country_data scd
    ON std.Ticker = scd.Ticker
    """)

df_return.registerTempTable('stock_daliy_return')

# obtain culmulative return and positive return flag
df_return_cumul = spark.sql("""
    SELECT
        *,
        EXP(SUM(LOG(IntradReturn)) OVER (PARTITION BY Ticker ORDER BY Date)) as IntradReturnCumul,
        EXP(SUM(LOG(OvernigtReturn)) OVER (PARTITION BY Ticker ORDER BY Date)) as OvernigtReturnCumul,
        (CASE WHEN IntradReturn >= 1 THEN 1 ELSE 0 END) as IntradReturnPositive,
        (CASE WHEN OvernigtReturn >= 1 THEN 1 ELSE 0 END) as OvernigtReturnPositive
    FROM 
        stock_daliy_return
    WHERE OvernigtReturn IS NOT NULL
    """)

# write the return table to bigquery
df_return_cumul.write \
    .format('bigquery') \
    .option("temporaryGcsBucket", gcs_bucket) \
    .option('table', output_table) \
    .mode("overwrite") \
    .save()

# stop the spark session
spark.stop()