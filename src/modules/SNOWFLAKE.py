from pyspark.sql import *
from pyspark.sql.functions import *


# spark = SparkSession \
#     .builder \
#     .appName("DemoJob") \
#     .config('spark.jars.packages','net.snowflake:snowflake-jdbc:3.11.1,net.snowflake:spark-snowflake_2.11:2.5.7-spark_2.4')\
#     .getOrCreate()
# df_raw = spark.read.format("csv").load("C:\\Users\\abhishek.dd\\Desktop\\Anmol\\git\\src\\output_files\\raw_log_details.csv")
#



def write_to_snowflake():
    SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"
    snowflake_database = "twinkaldb"
    snowflake_schema = "public"
    target_table_name = "curated_log_details"
    snowflake_options = {
        "sfUrl": "jn94146.ap-south-1.aws.snowflakecomputing.com",
        "sfUser": "sushantsangle",
        "sfPassword": "Stanford@01",
        "sfDatabase": snowflake_database,
        "sfSchema": snowflake_schema,
        "sfWarehouse": "curated_snowflake"
    }
    spark = SparkSession.builder \
        .appName("Demo_Project").enableHiveSupport().getOrCreate()
    df_raw = spark.read.format("csv").option("header","True").load(
        "s3://twinkal-db//raw_logs.csv")
    raw = df_raw.select("*")
    raw.write.format("snowflake")\
        .options(**snowflake_options) \
        .option("dbtable", "raw_log_details") \
        .option("header", "true") \
        .mode("overwrite") \
        .save()
    df_cleans = spark.read.format("csv").option("header","True").load(
        "s3://atul-db//cleansed_logs.csv")
    cleans = df_cleans.select("*")
    cleans.write.format("snowflake") \
        .options(**snowflake_options) \
        .option("dbtable", "cleans_log_details") \
        .option("header", "true") \
        .mode("overwrite") \
        .save()
    df_curate = spark.read.format("csv").option("header","True").load(
        "s3://atul-db//curated_logs.csv")
    curate = df_curate.select("*")
    curate.write.format("snowflake") \
        .options(**snowflake_options) \
        .option("dbtable", "curate_log_details") \
        .option("header", "true") \
        .mode("overwrite") \
        .save()
    df_per = spark.read.format("csv").option("header","True").load(
        "s3://atul-db//log_agg_per_device.csv")
    per = df_per.select("*")
    per.write.format("snowflake") \
        .options(**snowflake_options) \
        .option("dbtable", "log_agg_per_details") \
        .option("header", "true") \
        .mode("overwrite") \
        .save()
    df_across = spark.read.format("csv").option("header","True").load(
        "s3://atul-db//log_agg_across_device.csv")
    across = df_across.select("*")
    across.write.format("snowflake") \
        .options(**snowflake_options) \
        .option("dbtable", "log_agg_across_details") \
        .option("header", "true") \
        .mode("overwrite") \
        .save()

write_to_snowflake()


