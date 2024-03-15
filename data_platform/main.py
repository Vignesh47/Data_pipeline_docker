# Data Transformation Data pipeline using Spark


#importing Librarires
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
import logging
import logging.config


#importing ingestion, Transformation and Storage file
from Data_pipeline import ingestion, transformation, storage
from testing import test_ingestion


class data_pipeline:
    logging.config.fileConfig("/Users/darkstorm/PycharmProjects/Mydatapipeline/Configs/logging.config")
    def pipeline(self):
        try:
            logging.info("Pipeline has been Initiated successfully")
            ingest = ingestion.Ingest(self.spark)
            raw_data = ingest.ingest_data()
            logging.info("File ingestion has been successfully completed")
            transform = transformation.Transform(self.spark)
            base_data = transform.transform_data(raw_data)
            logging.info("Base --->>>> Data has been successfully cleaned")
            store = storage.Storage(self.spark)
            store.storage_data(base_data)
            logging.info("Integration--->>>> Data has been stored successfully")
        except Exception as exp:
            logging.error("Pipeline failed due to >"+str(exp))
            sys.exit(1)


    def spark_build(self):
        self.spark = SparkSession.builder.appName("Aviva_Petition_Data").getOrCreate()

if __name__ == "__main__":
    pipeline_new = data_pipeline()
    pipeline_new.spark_build()
    pipeline_new.pipeline()




