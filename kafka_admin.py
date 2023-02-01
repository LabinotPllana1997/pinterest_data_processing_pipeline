from kafka import KafkaAdminClient
from kafka.admin import NewTopic
from kafka.cluster import ClusterMetadata

# Create a Kafka client to enable the adminstration to be performed
admin_client = KafkaAdminClient(
    bootstrap_servers="localhost:9092",
    client_id="Kafka Administration"
)

# The topics which are to be passed and created. Must be passed as a list
topics = []
topics.append(NewTopic(name="", num_partitions=3, replication_factor=1))

# Topics are now created using the client
admin_client.create_topics(new_topics=topics)

admin_client.list_topics()