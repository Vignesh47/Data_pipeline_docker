import unittest
import os
from pyspark.sql.types import StringType
from pyspark.sql.functions import *
from pyspark.sql.types import *
from Data_pipeline import transformation
from pyspark.ml.feature import Tokenizer, StopWordsRemover

from Data_pipeline import ingestion
from pyspark.sql import SparkSession

class data_pipline_test(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.appName("Aviva Data Pipeline unittesting").getOrCreate()
        schema = StructType([
            StructField("abstract", StructType([
                StructField("_value", StringType())
            ])),
            StructField("label", StructType([
                StructField("_value", StringType())
            ])),
            StructField("numberOfSignatures", LongType())
        ])
        input_data_path = os.path.join(os.getcwd(), "input_test_data.json")
        cls.raw_test_data = cls.spark.read.json(input_data_path, schema)
        print(cls.raw_test_data)
        cls.transform = transformation.Transform(cls.spark)

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_ingestion_count(self):
        len_df = self.raw_test_data.count()
        print(self.raw_test_data)
        print("hi",len_df)
        self.assertNotEqual(0, len_df)
    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_transform_data(self):
        # call the transform_data method on the transformer instance with the sample raw_data
        result = self.transform.transform_data(self.raw_test_data)
        cols = len(result.columns)
        # Check for null values in each column
        for col_name in result.columns:
            null_counts = result.select([count(when(isnull(col_name), col_name)).alias(col_name)])
            self.assertEqual(null_counts.collect()[0][0], 0)
        self.assertEqual(cols,21)
        return

if __name__ == '__main__':
    unittest.main()
