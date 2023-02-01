from kafka import KafkaClient
from kafka.cluster import ClusterMetadata

# Creates a connection to retrieve metadata about the cluster
metadata_cluster_conn =  ClusterMetadata(
    bootstrap_servers="localhost:9092")


# retrieves the metadata about the cluster and brokers
print(metadata_cluster_conn.brokers())

#Create a connection to the KafkaBroker to check that it is running properly
client_conn = KafkaClient(
    bootstrap_servers="localhost:9092",  #The specific broker address that is connected to 
    client_id="Broker test"  # Creates an id for this client to reference this connection
)


# Check that the connection to the server is working and running
print(client_conn.bootstrap_connected())

# Check the Kafka version number
print(client_conn.check_version())