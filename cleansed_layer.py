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
    spark = SparkSession.builder.appName("log_file_project1").config('spark.ui.port', '4050').config("spark.master","local").enableHiveSupport().getOrCreate()
    cleansed = spark.read.csv("s3a://layers/raw-layer/raw.csv/part-00000-09e58db2-0d5b-407b-a308-30d472cc0908-c000.csv",header=True)
    

    def __init__(self):
        sc = self.spark.sparkContext
        sc.setLogLevel("Error")

    def read_raw_data_from_s3(self):
        try:
            
            self.cleansed = self.spark.read.csv("s3a://layers/raw-layer/raw.csv/part-00000-09e58db2-0d5b-407b-a308-30d472cc0908-c000.csv",header=True)
        except Exception as err:
            logging.error('Exception was thrown in connection %s' % err)
            print("Error is {}".format(err))
            sys.exit(1)
        else:
            self.cleansed.printSchema()
            self.cleansed.show()
            # raw_df.show(10, truncate=False)

class CleansedLayer(Session):
    #changing date time format
    def datetime_format(self):
        self.cleansed = self.cleansed.withColumn("datetime", split(self.clean_df["datetime"], ' ').getItem(0)).withColumn("datetime",to_timestamp("datetime",'dd/MMM/yyyy:hh:mm:ss'))\
                                                                                                                .withColumn("datetime",to_timestamp("datetime",'MMM/dd/yyyy:hh:mm:ss'))
        # self.clean_df.show()

    def referer_present(self):
        self.cleansed = self.clean_df.withColumn("referer_present(Y/N)",
                                      when(col("referer") == None, "N") \
                                      .otherwise("Y"))
        self.cleansed.show()

    def save_to_s3(self):
        self.cleansed.write.csv("s3a://layers/clean-layer/cleansed.csv", mode="append",header=True)

    def save_to_hive_table(self):
        pass
        self.cleansed.write.saveAsTable('cleansed_table')


if __name__ == "__main__":
    #Session class
    session = Session()
    try:
        session.read_raw_data_from_s3()
    except Exception as e:
        logging.error('Error at %s', 'Reading from S3 raw bucket', exc_info=e)
        sys.exit(1)

    #Cleansed
    try:
       cleansedLayer = CleansedLayer()
    except Exception as e:
        logging.error('Error at %s', 'Error at the time of cleansed object creation', exc_info=e)
        sys.exit(1)

    try:
        cleansedLayer.datetime_format()
    except Exception as e:
        logging.error('Error at %s', 'Error at the tiume of changing datetime format', exc_info=e)
        sys.exit(1)

    try:
        cleansedLayer.referer_present()
    except Exception as e:
        logging.error('Error at %s', 'Error at  the time of referer present', exc_info=e)
        sys.exit(1)

    try:

        cleansed.save_to_s3(
        logging.info("Writing data to  Cleansed_Layer in S3 Successfull!")
    except Exception as e:
        logging.error('Error at %s', 'save_to_s3', exc_info=e)
        sys.exit(1)

    try:
        clean.save_to_hive_table()
        logging.info("Saving data cleansed_data into hive table successfull")
    except Exception as e:
       logging.error('Error at %s', 'save to hive table', exc_info=e)