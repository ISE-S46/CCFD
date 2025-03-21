from kafka import KafkaConsumer
import json
from pyspark.sql import SparkSession
from pyspark.ml.classification import GBTClassificationModel
from pyspark.ml.feature import VectorAssembler
import psycopg2
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

# Function to insert fraud cases into PostgreSQL
def insert_fraud_case(transaction, prediction, probability):
    try:
        # Connect to PostgreSQL
        conn = psycopg2.connect(
            dbname="fraud_db",
            user="admin",
            password="password",
            host="postgres",  # Use the service name from docker-compose
            port="5432"
        )
        cursor = conn.cursor()

        # Insert the fraud case into the database
        cursor.execute("""
            INSERT INTO fraud_cases (transaction_id, amount, hour, day_of_week, distance, is_fraud, prediction, probability)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            transaction.get("trans_num"),
            transaction.get("amt"),
            transaction.get("hour"),
            transaction.get("day_of_week"),
            transaction.get("distance"),
            transaction.get("is_fraud"),
            prediction,
            float(probability)
        ))

        # Commit the transaction
        conn.commit()
        cursor.close()
        conn.close()
        print("Fraud case inserted into PostgreSQL.")
    except Exception as e:
        print(f"Error inserting fraud case into PostgreSQL: {e}")

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
    transaction = message.value  # Get the transaction data
    
    # Convert the transaction to a DataFrame
    transaction_df = spark.createDataFrame([transaction])  # Wrap the dictionary in a list

    # Assemble the features
    transaction_df = assembler.transform(transaction_df)
    
    # Apply the fraud detection model
    prediction = model.transform(transaction_df)
    
    # Check if fraud was detected
    is_fraud = prediction.select("prediction").collect()[0][0]
    probability = prediction.select("probability").collect()[0][0][1]  # Probability of fraud

    if is_fraud == 1:
        print(f"Fraud detected in transaction: {transaction}")
        # Insert the fraud case into PostgreSQL
        insert_fraud_case(transaction, is_fraud, probability)
    else:
        print(f"Legitimate transaction: {transaction}")