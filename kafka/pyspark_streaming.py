import os
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

# Initialize Spark session
spark = SparkSession.builder.appName("RealTimeFraudDetection").getOrCreate()
ssc = StreamingContext(spark.sparkContext, batchDuration=10)  # 10-second batches

# Create a Kafka stream
kafka_stream = KafkaUtils.createDirectStream(
    ssc,
    topics=["credit-card-transactions"],
    kafkaParams={"metadata.broker.list": "localhost:9092"}
)

# Process each RDD in the stream
def process_rdd(rdd):
    if not rdd.isEmpty():
        # Convert RDD to DataFrame
        df = spark.read.json(rdd)
        
        # Apply your fraud detection model here
        # For example, use df.withColumn("is_fraud", model.predict(df.features))
        
        # Show the processed data
        df.show()

kafka_stream.foreachRDD(process_rdd)

# Start the streaming context
ssc.start()
ssc.awaitTermination()