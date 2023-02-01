from kafka import KafkaConsumer
from json import loads, dumps
from dotenv import load_dotenv
import datetime
import boto3
import time
import os

load_dotenv()

s3_bucket = os.environ["S3_BUCKET"]
s3_client = boto3.client('s3')

data_batch_consumer = KafkaConsumer(
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda message: loads(message),
    auto_offset_reset='earliest'
)

data_batch_consumer.subscribe(topics="MyFirstKafkaTopic")

for message in data_batch_consumer:
    # Create timestamp for S3 key value
    timestamp = time.time()
    # Turn timestamp into string for key value
    timestamp_str = datetime.datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d')
    s3_key = f'{message[6]["index"]}_{timestamp_str}'
    print(s3_key)
    print(dumps(message[6]))
    s3_client.put_object(Body=dumps(message[6]), Bucket=s3_bucket, Key=f'{s3_key}.json')