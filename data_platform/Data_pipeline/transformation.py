from pyspark.sql import SparkSession
import re
from pyspark.sql.window import Window
from pyspark.ml.feature import Tokenizer, StopWordsRemover
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging
import logging.config



class Transform:

    def __init__(self,spark):
        self.spark = spark
    def transform_data(self, raw_data):
        try:
            print(raw_data)
            logger = logging.getLogger("Transform")
            # select the required columns and rename them to use the values from the label field as column names
            base_data_df2 = base_data_df.withColumn("sales_price", base_data_df["sales_price"].cast("float"))
            base_data_df2 = base_data_df2.withColumn("production_cost",when(col("sales_price") <= 0, 0).otherwise(None).cast("float"))

            # show the result
            base_data_df2.show()
            logging.info("Transformation completed")

        except Exception as exp:
            logging.error("Transformation Process has been failed.>" + str(exp))
            raise Exception(" Raw data is not available. ")
        return base_data_df2