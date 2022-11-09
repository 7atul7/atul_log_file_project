# -*- coding: utf-8 -*-
"""curated_layer.ipynb

Automatically generated by Colaboratory.

Original file is located at
    https://colab.research.google.com/drive/1Tu3eBUE_ze_8-tL3ZGvU_-EwpBJ4AxoW
"""

!pip3 install pyspark

import sys
import time

from pyspark import *
from pyspark.sql import *
from pyspark.sql import HiveContext
from pyspark.sql.functions import *
from pyspark.sql.functions import col, when
from pyspark.sql import functions as F
import logging
import re

class Setup:
    spark = SparkSession.builder.appName("log_file_project1").config('spark.ui.port', '4050').config(
        "spark.master", "local").config('spark.jars.packages','net.snowflake:snowflake-jdbc:3.13.23,net.snowflake:spark-snowflake_2.12:2.11.0-spark_3.3').enableHiveSupport().getOrCreate()


    curated_df = spark.read.csv(
        "/content/drive/MyDrive/project_output_files/cleansed_log_details/part-00000-1f9f0ea6-af4c-415d-920f-c27cc698342d-c000.csv", header=True,inferSchema=True)

    
    def __init__(self):
        sc = self.spark.sparkContext
        sc.setLogLevel("Error")

    def read_from_s3_clean(self):
        try:
            self.curated_df = self.spark.read.csv("/content/drive/MyDrive/project_output_files/cleansed_log_details/part-00000-1f9f0ea6-af4c-415d-920f-c27cc698342d-c000.csv",header=True,inferSchema=True)
      
        except Exception as err:
            logging.error('Exception was thrown in connection %s' % err)
            print("Error is {}".format(err))
            sys.exit(1)
        else:
          pass
            


class Curated(Setup):

    def drop_referer(self):
        self.curated_df = self.curated_df.drop("referer")
        self.curated_df.show()

    def write_to_s3(self):
        self.curated_df.write.csv("/content/drive/MyDrive/project_output_files/curated_log_details/", mode="overwrite", header=True)

    def write_to_hive(self):
        sqlContext = HiveContext(self.spark.sparkContext)
        sqlContext.sql('DROP TABLE IF EXISTS curate_log_details')
        self.curated_df.write.saveAsTable("curate_log_details")
        self.spark.sql("select count(*) from curate_log_details")



class Agg(Curated):




    # add column hour,Get,Post,Head
    def add_temp_columns(self):
        df_temp = self.curated_df.withColumn("No_get", when(col("method") == "GET", "GET")) \
            .withColumn("No_post", when(col("method") == "POST", "POST")) \
            .withColumn("No_Head", when(col("method") == "HEAD", "HEAD")) \
            .withColumn("day", to_date(col("datetime"))) \
            .withColumn("hour", hour(col("datetime"))) \
            .withColumn("day_hour", concat(col("day"), lit(" "), col("hour")))

        check_null_count= df_temp.select([count(when(col(c).isNull(), c)).alias(c) for c in df_temp.columns])
        # check_null_count.show()

        # df_temp.show()
        return df_temp

    # perform aggregation per device
    def agg_per_device(self, df_temp):
        df_agg_per_device = df_temp.select("row_id", "day_hour", "client/ip", "no_get", "no_post", "no_head") \
            .groupBy("day_hour", "client/ip") \
            .agg(count("row_id").alias("count_of_records"),
                 count(col("No_get")).alias("no_get"),
                 count(col("No_post")).alias("no_post"),
                 count(col("No_head")).alias("no_head")) 

        print("aggregations per device")
        df_agg_per_device.show()
        return df_agg_per_device

    # perform aggregation across device
    def agg_across_device(self,df_temp):
        df_agg_across_device = df_temp.select("*") \
            .groupBy("day_hour") \
            .agg(
                count("client/ip").alias("no_of_clients"),
                count("row_id").alias("count_of_records"),
                count(col("No_get")).alias("no_get"),
                count(col("No_post")).alias("no_post"),
                count(col("No_head")).alias("no_head")
                )
        print("aggregations across device")
        df_agg_across_device.show()
        return df_agg_across_device

    # write to s3 curated-layer-aggregations
    def write_to_s3_agg(self,df_agg_per_device,df_agg_across_device):
        df_agg_per_device.write.csv("/content/drive/MyDrive/project_output_files/aggregations/per_device/", header=True,
                                    mode='overwrite')
        df_agg_across_device.write.csv("/content/drive/MyDrive/project_output_files/aggregations/across_device/",
                                       header=True, mode='overwrite')

    # write to hive
    def write_to_hive(self,df_agg_per_device,df_agg_across_device):
        sqlContext = HiveContext(self.spark.sparkContext)
        sqlContext.sql('DROP TABLE IF EXISTS log_agg_per_device')
        df_agg_per_device.write.saveAsTable('log_agg_per_device')
        print("count of aggregation per device")
        self.spark.sql("select count(*) from log_agg_per_device").show()
        sqlContext.sql('DROP TABLE IF EXISTS log_agg_across_device')
        df_agg_across_device.write.saveAsTable('log_agg_across_device')
        print("count of aggregation across device")
        self.spark.sql("select count(*) from log_agg_across_device").show()

    


if __name__ == '__main__':
    try:
        setup = Setup()
    except Exception as e:
        logging.error('Error at %s', 'Setup Object creation', exc_info=e)
        sys.exit(1)

    try:
        setup.read_from_s3_clean()
    except Exception as e:
        logging.error('Error at %s', 'read from s3 clean', exc_info=e)
        sys.exit(1)

    try:
        curated = Curated()
    except Exception as e:
        logging.error('Error at %s', 'curated Object creation', exc_info=e)
        sys.exit(1)

    try:
        curated.drop_referer()
    except Exception as e:
        logging.error('Error at %s', 'drop referer', exc_info=e)
        sys.exit(1)

    try:
        curated.write_to_s3()
    except Exception as e:
        logging.error('Error at %s', 'write to s3', exc_info=e)
        sys.exit(1)
    
    try:
        curated.write_to_hive()
    except Exception as e:
        logging.error('Error at %s', 'write to hive', exc_info=e)
        sys.exit(1)

    # Agg
    try:
        agg = Agg()
    except Exception as e:
        logging.error('Error at %s', 'error creating Object agg', exc_info=e)
        sys.exit(1)


    try:
        df_temp = agg.add_temp_columns()
    except Exception as e:
        logging.error('Error at %s', 'add_temp_columns', exc_info=e)
        sys.exit(1)

    try:
        df_temp_agg_per_device = agg.agg_per_device(df_temp)
    except Exception as e:
        logging.error('Error at %s', 'agg_per_device', exc_info=e)
        sys.exit(1)

    try:
        df_temp_agg_across_device= agg.agg_across_device(df_temp)
    except Exception as e:
        logging.error('Error at %s', 'agg_per_device', exc_info=e)
        sys.exit(1)

    try:
        agg.write_to_s3_agg(df_temp_agg_per_device,df_temp_agg_across_device)
    except Exception as e:
        logging.error('Error at %s', 'write to s3_agg', exc_info=e)
        sys.exit(1)

    try:
        agg.write_to_hive(df_temp_agg_per_device,df_temp_agg_across_device)
    except Exception as e:
        logging.error('Error at %s', 'write to s3_agg', exc_info=e)
        sys.exit(1)

    