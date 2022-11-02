import sys
import time

from pyspark import *
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.functions import col, when
from pyspark.sql import functions as F
import logging
import re


class Session:
    spark = SparkSession.builder.appName("log_file_project1").config('spark.ui.port', '4050').config(
        "spark.master", "local").enableHiveSupport().getOrCreate()
    curated = spark.read.csv(
        "s3a://layers/clean-layer/cleansed.csv/part-00000-65b1e49f-2d58-479a-93b3-0e58cf8450b2-c000.csv",
        header=True, inferSchema=True)

    

    
    def __init__(self):
        sc = self.spark.sparkContext
        sc.setLogLevel("Error")

    def data_from_s3_cleansed(self):
        try:
            self.curated = self.spark.read.csv(
                "s3a://layers/clean-layer/cleansed.csv/part-00000-65b1e49f-2d58-479a-93b3-0e58cf8450b2-c000.csv",
                header=True, inferSchema=True)
            
        except Exception as err:
            logging.error('Exception was thrown in connection %s' % err)
            print("Error is {}".format(err))
            sys.exit(1)
        else:
            self.curated_df.printSchema()
            # self.curated_df.show()
            # raw_df.show(10, truncate=False)


class Curated(Session):

    def drop_referer(self):
        self.curated = self.curated.drop("referer")
        self.curated.show()

    def save_to_s3(self):
        self.curated.write.csv("s3a://layers/curated-layer/", mode="overwrite", header=True)


class Agg(Curated):
    pass

    # check distinct no. of user device ie ip
    def check_distinct_user(self):
        self.curated.select("client/ip").distinct().count()

    def cal_agg(self):
        # add column hour,Get,Post,Head
        df_temp = self.curated_df.withColumn("No_get", when(col("method(GET)") == "GET", "GET")) \
            .withColumn("No_post", when(col("method(GET)") == "POST", "POST")) \
            .withColumn("No_Head", when(col("method(GET)") == "HEAD", "HEAD")) \
            .withColumn("hour", hour(col("datetime")))
        df_temp.show()

        # perform aggregation per device
        agg_per_device = df_temp.select("*").groupBy("client/ip").agg(
            count("row_id").alias("row_id"), sum("hour").alias("day_hour"), count("client/ip").alias("count_client/ip"),
            count(col("No_get")).alias("no_get"), count(col("No_post")).alias("no_post"),
            count(col("No_head")).alias("no_head"))

        agg_per_device.show()

        # perform aggregation across device
        agg_across_device = df_temp.select("*").agg(count("row_id").alias("row_id"), first("hour").alias("day_hour"),
                                                      count("client/ip").alias("count_client/ip"),
                                                      count(col("No_get")).alias("no_get"),
                                                      count(col("No_post")).alias("no_post"),
                                                      count(col("No_head")).alias("no_head"))

        agg_across_device.show()

        # write to s3 curated-layer
        agg_per_device.write.csv("s3a://layers/curated-layer/aggregation/per_device/", header=True,mode='overwrite')
        agg_across_device.write.csv("s3a://layers/curated-layer/aggregation/across_device/",
                                       header=True,mode='overwrite')

    # write to

    # create external table pointing to s3


if __name__ == '__main__':
    try:
        session = Session()
    except Exception as e:
        logging.error('Error at %s', 'Setup Object creation', exc_info=e)
        sys.exit(1)

    try:
        session.data_from_s3_cleansed()
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
        curated.save_to_s3()
    except Exception as e:
        logging.error('Error at %s', 'save data to s3', exc_info=e)
        sys.exit(1)

    # Agg
    try:
        agg = Agg()
    except Exception as e:
        logging.error('Error at %s', 'error creating Object agg', exc_info=e)
        sys.exit(1)

    try:
        agg.check_distinct_user()
    except Exception as e:
        logging.error('Error at %s', 'check distinct user', exc_info=e)
        sys.exit(1)

    try:
        agg.cal_agg()
    except Exception as e:
        logging.error('Error at %s', 'cal_agg', exc_info=e)
        sys.exit(1)
