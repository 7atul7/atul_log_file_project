import sys
import time

from pyspark import *
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.functions import col, when
import logging
import re


class Session:
    spark = SparkSession.builder.appName("Demo-Project-Stage-I").config('spark.ui.port', '4050').config("spark.master",
                                                                                                        "local").enableHiveSupport().getOrCreate()
    raw = spark.read.text("s3://mkc-atul-destination-bucket/tutorial/mkc-atul-topics-latest/299999.text")

    

    
    def __init__(self):
        sc = self.spark.sparkContext
        sc.setLogLevel("Error")

    def data_from_s3_bucket(self):
        try:
            self.raw = self.spark.read.text("s3://mkc-atul-destination-bucket/tutorial/mkc-atul-topics-latest/299999.text")
        except Exception as err:
            logging.error('Exception was thrown in connection %s' % err)
            print("Error is {}".format(err))
            sys.exit(1)
        else:
            self.raw.printSchema()
            # raw_df.show(10, truncate=False)


class Cleansed(Session):
    def extract_data(self):
        regex_pattern = r'(.+?)\ - - \[(.+?)\] \"(.+?)\ (.+?)\ (.+?\/.+?)\" (.+?) (.+?) (.+?) \"(.+?)\"'

        self.raw = self.raw.withColumn("row_id", monotonically_increasing_id()) \
            .select("row_id", regexp_extract('value', regex_pattern, 1).alias('client/ip'),
                    regexp_extract('value', regex_pattern, 2).alias('datetime'),
                    regexp_extract('value', regex_pattern, 3).alias('method(GET)'),
                    regexp_extract('value', regex_pattern, 4).alias('request'),
                    regexp_extract('value', regex_pattern, 6).alias('status_code'),
                    regexp_extract('value', regex_pattern, 7).alias('size'),
                    regexp_extract('value', regex_pattern, 8).alias('referer'),
                    regexp_extract('value', regex_pattern, 9).alias('user_agent'))
        self.raw.show(truncate=False)

    def remove_special_character(self):
        # Removing special characters from data like ?,/,+,=
        self.raw = self.raw.withColumn('request', regexp_replace('request', '%|-|\?=', ''))

    def bytes_to_kb(self):
        self.raw = self.raw.withColumn('size', round(self.raw.size / 1024, 2))
        return self.raw

    def replace_null_with_None(self):
        self.raw = self.raw.select(
            [when(col(c) == "-", None).otherwise(col(c)).alias(c) for c in self.raw.columns])
        self.raw.show()
        return self.raw

    def count_null_values(self):
        return self.raw.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in self.raw.columns])

    def save_to_s3(self):
        self.raw_df.write.csv("s3a://layers/raw-layer/", mode="append",header=True)

    def save_to_hive_table(self):
        pass
        self.raw.write.saveAsTable('Raw_Data')


if __name__ == "__main__":
    # Session
    Session = Session()
    try:
        Session.data_from_s3_bucket()
    except Exception as e:
        logging.error('Error at %s', 'Reading from S3 Sink', exc_info=e)
        sys.exit(1)

    # Cleansed
    try:
        cleansed = Cleansed()
    except Exception as e:
        logging.error('Error at %s', 'Cleaning Object Creation', exc_info=e)
        sys.exit(1)
    try:
        cleansed.extract_data()
    except Exception as e:
        logging.error('Error at %s', 'extract_column_regex', exc_info=e)
        sys.exit(1)

    try:
        cleansed.remove_special_character()
    except Exception as e:
        logging.error('Error at %s', 'remove_special_character', exc_info=e)
        sys.exit(1)

    try:
        cleansed.bytes_to_kb()
    except Exception as e:
        logging.error('Error at %s', 'size_to_kb', exc_info=e)
        sys.exit(1)

    try:
        cleansed.replace_null_with_None()
    except Exception as e:
        logging.error('Error at %s', 'remove empty string with null', exc_info=e)
        sys.exit(1)

    try:
        cleansed.count_null_values()
    except Exception as e:
        logging.error('Error at %s', 'count null each column', exc_info=e)
        sys.exit(1)

    try:

        cleansed.save_to_s3()
        logging.info("Writing  Raw_Layer to S3 Successful.")
    except Exception as e:
        logging.error('Error at %s', 'write_to_s3', exc_info=e)
        sys.exit(1)

    try:
        cleansed.save_to_hive_table()
        logging.info("storing data into hive tables successful")
    except Exception as e:
        logging.error('Error at %s', 'write to hive', exc_info=e)