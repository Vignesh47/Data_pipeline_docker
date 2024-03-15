#importing required Libraries for data ingestion


import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging
import logging.config
import os

class Ingest:

    def __init__(self,spark):
        self.spark = spark
        # Creating Object for Spark function
        spark = SparkSession.builder.appName("My new app").enableHiveSupport().getOrCreate()
        logging.info("Spark has been initiated.......")


    def ingest_data(self):
        try:
            logger = logging.getLogger("Ingest")

            # define the schema for the JSON data
            schema = StructType([
                StructField("abstract", StructType([
                    StructField("_value", StringType())
                ])),
                StructField("label", StructType([
                    StructField("_value", StringType())
                ])),
                StructField("numberOfSignatures", LongType())
            ])

            logging.info("Schema has been  Created")

            # read the JSON file into a DataFrame

            input_data_path = os.path.join(os.getcwd(), "data", "input_data.json")
            petition_df = self.spark.read.json(input_data_path, schema)
            print(petition_df)

            logging.info("File has been Loaded successfully")
        except Exception as exp:

            logging.error("Ingestion Process has been failed.>" + str(exp))
            raise Exception(" File is not available in the input path ")

        return petition_df

