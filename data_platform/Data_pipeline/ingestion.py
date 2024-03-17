#importing required Libraries for data ingestion

#import pyspark
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
        spark = SparkSession.builder.appName("Data Transformation").enableHiveSupport().getOrCreate()
        logging.info("Spark has been initiated.......")


    def ingest_data(self):
        try:
            logger = logging.getLogger("Ingest")
            base_data_path = os.path.join(os.getcwd(),"data_platform", "data", "Base_data.xlsx")
            vechile_data_path = os.path.join(os.getcwd(),"data_platform", "data", "vehicle_line_mapping.csv")
            options_data_path = os.path.join(os.getcwd(), "data_platform", "data", "options_data.csv")

            # read the CSV file into a DataFramE
            base_data_df= self.spark.read.format("com.crealytics.spark.excel").option("header", "true") \
                        .option("inferSchema", "true").load(base_data_path)
            vechile_df = self.spark.read.csv(vechile_data_path,header=True)
            options_df =self.spark.read.csv(options_data_path,header=True)

            print(base_data_df,vechile_df,options_df)

            logging.info("File has been Loaded successfully")
        except Exception as exp:

            logging.error("Ingestion Process has been failed.>" + str(exp))
            raise Exception(" File is not available in the input path ")

        return base_data_df,vechile_df,options_df

