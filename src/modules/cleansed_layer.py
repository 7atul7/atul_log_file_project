# -*- coding: utf-8 -*-
"""project_cleansed_layer.ipynb

Automatically generated by Colaboratory.

Original file is located at
    https://colab.research.google.com/drive/1TitEoKyjNhzzYJbnTEJmOqmcMZU30aeh

**cleansed_Layer**
"""

!pip3 install pyspark

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
    cleansed = spark.read.csv("/content/drive/MyDrive/project_output_files/raw_log_details/part-00000-ebee025e-bcaa-46e3-8260-d928ed0a7619-c000.csv",header=True)
    

    def __init__(self):
        sc = self.spark.sparkContext
        sc.setLogLevel("Error")

    def read_raw_data_from_s3(self):
        try:
            
            self.cleansed = self.spark.read.csv("/content/drive/MyDrive/project_output_files/raw_log_details/part-00000-ebee025e-bcaa-46e3-8260-d928ed0a7619-c000.csv",header=True)
        except Exception as err:
            logging.error('Exception was thrown in connection %s' % err)
            print("Error is {}".format(err))
            sys.exit(1)
        else:
            # self.cleansed.printSchema()
            self.cleansed.show()
            # raw_df.show(10, truncate=False)

class CleansedLayer(Session):
    #changing date time format
    def datetime_format(self):
        self.cleansed = self.cleansed.withColumn("datetime", split(self.cleansed["datetime"], ' ').getItem(0)).withColumn("datetime",to_timestamp("datetime",'dd/MMM/yyyy:HH:mm:ss'))\
                                                                                                                .withColumn("datetime",to_timestamp("datetime",'MMM/dd/yyyy:HH:mm:ss'))
        # self.clean_df.show()

    def referer_present(self):
        self.cleansed = self.cleansed.withColumn("referer_present(Y/N)",
                                      when(col("referer") == "NA", "N") \
                                      .otherwise("Y"))
        self.cleansed.printSchema()


    def save_to_s3(self):
        self.cleansed.coalesce(1).write.csv("/content/drive/MyDrive/project_output_files/cleansed_log_details/", mode="overwrite",header=True)

    def save_to_hive_table(self):
        pass
        # self.cleansed.coalesce(1).write.saveAsTable('cleansed_table9')
        print("count of cleansed_hive_table")
        self.spark.sql("select count(*) from cleansed_table9").show()
        
        


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
        cleansedLayer.save_to_s3()
        logging.info("Writing data to  Cleansed_Layer in S3 Successfull!")
    except Exception as e:
        logging.error('Error at %s', 'save_to_s3', exc_info=e)
        sys.exit(1)

    try:
        cleansedLayer.save_to_hive_table()
        logging.info("Saving data cleansed_data into hive table successfull")
    except Exception as e:
       logging.error('Error at %s', 'save to hive table', exc_info=e)

