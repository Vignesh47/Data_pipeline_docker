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
            df_schema = raw_data.select(col("abstract._value").alias("abstract"),
                col("numberOfSignatures").alias("numberOfSignatures"),
                col("label._value").alias("label"))

            # Create a new column with a unique identifier using row_number()
            df_with_id = df_schema.select(row_number().over(Window.orderBy("numberofSignatures")).alias("petition_id"), '*')

            # Step 2: Define a UDF to clean the text data
            # Clean text
            df_clean = df_with_id.select('Petition_id',(lower(regexp_replace('abstract', "[^a-zA-Z\\s]", "")).alias('Clean_text')))

            # Tokenize text
            tokenizer = Tokenizer(inputCol='Clean_text', outputCol='Words_token')
            df_words_token = tokenizer.transform(df_clean).select('Petition_id', 'Words_token')

            # Remove stop words
            remover = StopWordsRemover(inputCol='Words_token', outputCol='Words_clean')
            df_words_no_stopw = remover.transform(df_words_token).select('Petition_id', 'Words_clean')

            # Filter length word > 5
            filter_length_udf = udf(lambda row: [x for x in row if len(x) >= 5], ArrayType(StringType()))
            df_final_words = df_words_no_stopw.withColumn('Words_new', filter_length_udf(col('Words_clean'))).select(
                'Petition_id', 'Words_new')

            # explode the 'words_new' column to transform each row into multiple rows, each containing a single word
            df_explode = df_final_words.select("Petition_id", explode(col('Words_new')).alias('Word'))

            # group by the 'petition_id' and 'word' columns and use the 'count' function to count the occurrence of each word with each petition_id
            df_word_occurance = df_explode.groupBy('Petition_id', 'Word').count()

            # extract the unique values from the 'word' column, list the top 20 most common words in descending order, and convert them into separate columns with their counts for each petition_id
            most_common_words = [row['Word'] for row in df_word_occurance.groupBy('Word').count().orderBy(desc('count')).limit(20).collect()]
            final_petition_data = df_word_occurance.groupBy('Petition_id').pivot('Word', most_common_words).sum('count').na.fill(0).orderBy('Petition_id')

            # show the result
            final_petition_data.show()
            logging.info("Transformation completed")

        except Exception as exp:
            logging.error("Transformation Process has been failed.>" + str(exp))
            raise Exception(" Raw data is not available. ")
        return final_petition_data