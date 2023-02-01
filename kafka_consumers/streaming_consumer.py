import os
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructType, StructField, IntegerType

# Download spark sql kafka package from Maven repository
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1 pyspark-shell'

# Kafka topic
kafka_topic_name = 'MyFirstKafkaTopic'

# Kafka bootstrap server
kafka_bootstrap_servers = 'localhost:9092'



class Spark_Stream:
    def __init__(self) -> None:
        '''
        Create the spark session 
        '''
        self.spark =  SparkSession.builder\
            .appName('KafkaStreaming')\
                .getOrCreate()

    def create_stream(self):
        '''
        Constructing a streaming DataFrame that reads from topic
        '''

        self.stream_df = self.spark \
                .readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
                .option("subscribe", kafka_topic_name) \
                .option("startingOffsets", "earliest") \
                .load()
        print('Stream run in process')

    def dataframe_schema(self):
        '''
        This selects the value park of the kafka message and turns into a string
        '''

        self.stream_df = self.stream_df.selectExpr('CAST(value as STRING)')

        # Creates the Schema for the dataframe
        schema_for_df = StructType([StructField("category", StringType()),
                                    StructField("description", StringType()),
                                    StructField("downloaded", IntegerType()),
                                    StructField("follower_count", StringType()),
                                    StructField("image_src", StringType()),
                                    StructField("index", IntegerType()),
                                    StructField("is_image_or_video", StringType()),
                                    StructField("save_location", StringType()),
                                    StructField("tag_list", StringType()),
                                    StructField("title", StringType()),
                                    StructField("unique_id", StringType()),
                                    ])

        # Converts the JSON file into multiple columns                             
        self.stream_df = self.stream_df.withColumn("value", F.from_json(self.stream_df["value"], schema_for_df))\
            .select(F.col("value.*"))


    def clean_stream(self):
        '''
        Clean the follower_count column, remove the K representing thousand and add three zeros instead
        '''

        self.stream_df = self.stream_df.withColumn("follower_count",
            F.when(F.col("follower_count").like("%k"), 
            (F.regexp_replace("follower_count", "k", "").cast("int")*1000))\
                .when(F.col("follower_count").like("%M"), 
            (F.regexp_replace("follower_count", "M", "").cast("int")*1000000))\
                .when(F.col("follower_count").like("%B"), 
            (F.regexp_replace("follower_count", "B", "").cast("int")*1000000000))\
                .otherwise((F.regexp_replace("follower_count", " ", "").cast("int"))))

        # Clean the location save column
        self.stream_df = self.stream_df.withColumn('save_location', 
            F.when(F.col('save_location').startswith('Local save in'),
            F.regexp_replace('save_location', 'Local save in ', '')))

        # Make the downloaded column a boolean
        self.stream_df = self.stream_df.withColumn('downloaded', F.col('downloaded').cast('boolean'))\
            .withColumn('index', F.col('index').cast('int'))

    def output_to_console(self):
        '''
        output the messages to the console and terminate after 30 seconds
        '''
        self.stream_df.writeStream\
            .format('console')\
                .outputMode('update')\
                    .start()\
                        .awaitTermination(30)


    def run_stream(self):
        self.create_stream()
        self.dataframe_schema()
        self.clean_stream()
        self.output_to_console()

if __name__=='__main__':
    spark_stream_session = Spark_Stream()
    spark_stream_session.run_stream()