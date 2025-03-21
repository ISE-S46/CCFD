from kafka import KafkaConsumer
import json
from pyspark.sql import SparkSession
from pyspark.ml.classification import GBTClassificationModel
from pyspark.ml.feature import VectorAssembler
import os

# Get Kafka broker address from environment variable
kafka_broker = os.getenv("KAFKA_BROKER", "kafka:9092")

# Initialize Spark session
spark = SparkSession.builder.appName("RealTimeFraudDetection").getOrCreate()

# Load the trained GBT model
model_path = "/app/fraud_detection_model"  # Path to the saved model
model = GBTClassificationModel.load(model_path)

# Define the feature columns
feature_columns = [
    "amt", "hour", "day_of_week", "distance", 
    "category_0", "category_1", "category_2", "category_3", 
    "category_4", "category_5", "category_6", "category_7", 
    "category_8", "category_9", "category_10", "category_11", 
    "category_12"
]

# Create a VectorAssembler to assemble the feature columns into a single vector column
assembler = VectorAssembler(inputCols=feature_columns, outputCol="features")

# Initialize Kafka consumer
consumer = KafkaConsumer(
    "credit-card-transactions",  # Kafka topic
    bootstrap_servers=kafka_broker,  # Kafka broker address
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),  # Deserialize JSON messages
    auto_offset_reset="earliest",  # Start reading from the earliest message
    group_id="fraud-detection-group"  # Consumer group ID
)

# Process transactions
print("Listening for transactions...")
for message in consumer:
    transaction = message.value 
    
    # Convert the transaction to a DataFrame
    transaction_df = spark.createDataFrame([transaction])  # Wrap the dictionary in a list

    # Assemble the features
    transaction_df = assembler.transform(transaction_df)
    
    # Apply the fraud detection model
    prediction = model.transform(transaction_df)
    
    # Check if fraud was detected
    is_fraud = prediction.select("prediction").collect()[0][0]
    if is_fraud == 1:
        print(f"Fraud detected in transaction: {transaction}")
        # Trigger an alert
    else:
        print(f"Legitimate transaction: {transaction}")
    
    