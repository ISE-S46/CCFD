import os
import pandas as pd
from kafka import KafkaProducer
import json
import time

# Load the preprocessed test dataset
csv_path = "/app/Test_Process_Data.csv" # Path to the CSV file inside the container
df = pd.read_csv(csv_path)

# Get Kafka broker address from environment variable
kafka_broker = os.getenv("KAFKA_BROKER", "localhost:9092")

# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers=kafka_broker,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# Kafka topic
topic = "credit-card-transactions"

# Simulate real-time transactions
for index, row in df.iterrows():
    # Convert row to dictionary
    transaction = row.to_dict()
    
    # Send transaction to Kafka topic
    producer.send(topic, value=transaction)
    
    # Print the transaction (for debugging)
    print(f"Sent transaction {index + 1}: {transaction}")
    
    # Simulate a delay (e.g., 1 second per transaction)
    time.sleep(1)

# Flush and close the producer
producer.flush()
producer.close()