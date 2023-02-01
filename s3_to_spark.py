import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.functions import col
from pyspark.sql.functions import *

load_dotenv()

os.environ["PYSPARK_SUBMIT_ARGS"] = "--packages com.amazonaws:aws-java-sdk-s3:1.12.196,org.apache.hadoop:hadoop-aws:3.3.1,com.datastax.spark:spark-cassandra-connector_2.12:3.2.0 pyspark-shell"

class Spark_DAG:

    def __init__(self) -> None:


        # Create the Spark configuration
        conf = SparkConf()\
            .setAppName('S3toSpark') \
                .setMaster('local[*]')

        sc=SparkContext(conf=conf)

        # Configuration for the setting to read from the S3 bucket
        accessKeyId=os.environ["aws_access_key_id"]
        secretAccessKey=os.environ["aws_secret_access_key"]
        hadoopConf = sc._jsc.hadoopConfiguration()
        hadoopConf.set('fs.s3a.access.key', accessKeyId)
        hadoopConf.set('fs.s3a.secret.key', secretAccessKey)
        # Enables the package to authenticate with AWS
        hadoopConf.set('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider')

        # Create out Spark session
        # self.spark=SparkSession(sc)

        self.spark = SparkSession\
                                .builder\
                                .config('spark.app.name', 'spark_data_cleaning')\
                                .getOrCreate()

    def s3_to_parquet(self):

        # Read from the S3 bucket
        self.df = self.spark.read.option('multiline', 'true').json('s3a://mypinterestbucket/*.json')
        self.df.show()
        self.df.printSchema()

    def save_df_to_parquet(self):
        self.df.write.parquet('/home/labinotpllana1997/Documents/AiCore Projects/data_engineering_project/parquet')
        df_restored = self.spark\
            .read\
                .parquet('/home/labinotpllana1997/Documents/AiCore Projects/data_engineering_project/parquet')
        df_restored.show()
        df_restored.printSchema()

    def clean_data(self):
        #Load up Dataframe
        df_restored = self.spark\
            .read\
                .parquet('/home/labinotpllana1997/Documents/AiCore Projects/data_engineering_project/parquet')

        new_df = df_restored.withColumn("follower_count", when(col('follower_count').like("%k"), (regexp_replace('follower_count', 'k', '').cast('int')*1000))\
    .when(col('follower_count').like("%M"), (regexp_replace('follower_count', 'M', '').cast('int')*1000000))\
    .when(col('follower_count').like("%B"), (regexp_replace('follower_count', 'B', '').cast('int')*1000000000))\
    .otherwise((regexp_replace('follower_count', ' ', '').cast('int'))))
        
        #replace any empty values with null values
        new_df = new_df.select([F.when(F.col(c)=="",None).otherwise(F.col(c)).alias(c) for c in new_df.columns])

        # All data has been downloaded, so the column 'downloaded can be dropped
        new_df = new_df.drop('downloaded')

        # Tidy up the column header
        new_df = new_df\
            .withColumnRenamed('is_image_or_video', 'image_or_video?')

        #Cleaning sa_location column by removing repetitive 'Local save in' substring
        new_df = new_df.withColumn("save_location", F.regexp_replace("save_location", "Local save in ", ""))
        
        # order data starting from highest follower count
        new_df\
            .orderBy('follower_count', ascending=False)\
            .show()
        new_df.printSchema()


if __name__ == '__main__':
    spark_job = Spark_DAG()
    # spark_job.s3_to_parquet()
    # spark_job.save_df_to_parquet()
    spark_job.clean_data()
    # spark_job.save_dataframe()




















# from pyspark import SparkContext
# from pyspark.sql import SparkSession
# sc = SparkContext.getOrCreate()
# spark = SparkSession.builder.appName('PySpark DataFrame From RDD').getOrCreate()
# rdd = sc.parallelize([('C',85,76,87,91), ('B',85,76,87,91), ("A", 85,78,96,92), ("A", 92,76,89,96)], 4)
# #print(type(rdd))
# sub = ['Division','English','Mathematics','Physics','Chemistry']
# marks_df = spark.createDataFrame(rdd, schema=sub)
# #print(type(marks_df))
# #marks_df.printSchema()
# marks_df.show()