from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, hour, dayofweek,
    to_date, datediff, floor, radians, cos, sin, atan2, sqrt,
    lit, udf, when
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType
)
from pyspark.ml import PipelineModel
import os
import sys

KAFKA_BROKER_URL = os.getenv('KAFKA_BROKER_URL', 'kafka:29092')
TOPIC_NAME = os.getenv('KAFKA_TOPIC_NAME', 'raw_transactions')
MODEL_PATH = '/app/Pipeline'
CHECKPOINT_PATH = os.getenv('CONSUMER_CHECKPOINT_PATH', '/tmp/checkpoints')

SPARK_MASTER_URL = os.getenv('SPARK_MASTER_URL', 'spark://spark-master:7077')
SPARK_DRIVER_MEMORY = os.getenv('SPARK_DRIVER_MEMORY', '1g')
SPARK_EXECUTOR_MEMORY = os.getenv('SPARK_EXECUTOR_MEMORY', '1g')
SPARK_EXECUTOR_CORES = os.getenv('SPARK_EXECUTOR_CORES', '1')
SPARK_SQL_SHUFFLE_PARTITIONS = os.getenv('SPARK_SQL_SHUFFLE_PARTITIONS', '2')
SPARK_DRIVER_MAX_RESULT_SIZE = os.getenv('SPARK_DRIVER_MAX_RESULT_SIZE', '1g')
SPARK_KAFKA_PACKAGE_VERSION = os.getenv('SPARK_KAFKA_PACKAGE_VERSION', '4.0.0')
SPARK_SERIALIZER = os.getenv('SPARK_SERIALIZER', 'org.apache.spark.serializer.KryoSerializer')
SPARK_STREAMING_TRIGGER_TIME = os.getenv('SPARK_STREAMING_TRIGGER_TIME', '10 seconds')
SPARK_STREAMING_MAX_OFFSETS_PER_TRIGGER = os.getenv('SPARK_STREAMING_MAX_OFFSETS_PER_TRIGGER', '1000')
CONSUMER_FRAUD_THRESHOLD = float(os.getenv('CONSUMER_FRAUD_THRESHOLD', '0.75'))

# Create a custom temp directory for Spark
custom_temp_dir = os.getenv('SPARK_LOCAL_DIRS', '/tmp/spark-custom')
os.makedirs(custom_temp_dir, exist_ok=True)
os.makedirs(CHECKPOINT_PATH, exist_ok=True)

# Set Java system properties before creating SparkSession
os.environ['SPARK_LOCAL_DIRS'] = custom_temp_dir

print("Setting up Spark Session...")

try:
    spark = SparkSession.builder \
        .appName("FraudDetectionStreaming") \
        .master(SPARK_MASTER_URL) \
        .config("spark.jars.packages", f"org.apache.spark:spark-sql-kafka-0-10_2.13:{SPARK_KAFKA_PACKAGE_VERSION}") \
        .config("spark.driver.memory", SPARK_DRIVER_MEMORY) \
        .config("spark.executor.memory", SPARK_EXECUTOR_MEMORY) \
        .config("spark.executor.cores", SPARK_EXECUTOR_CORES) \
        .config("spark.driver.maxResultSize", SPARK_DRIVER_MAX_RESULT_SIZE) \
        .config("spark.local.dir", custom_temp_dir) \
        .config("spark.sql.warehouse.dir", f"{custom_temp_dir}/warehouse") \
        .config("spark.sql.streaming.metricsEnabled", "true") \
        .config("spark.sql.adaptive.enabled", "false") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "false") \
        .config("spark.serializer", SPARK_SERIALIZER) \
        .getOrCreate()
    
    print("Spark Session created successfully in local mode")
except Exception as e:
    print(f"Failed to create Spark session: {e}")
    sys.exit(1)

spark.sparkContext.setLogLevel("ERROR")  # Reduce logging to ERROR only

transaction_schema = StructType([
    StructField("trans_date_trans_time", StringType()),
    StructField("cc_num", StringType()),
    StructField("merchant", StringType()),
    StructField("category", StringType()),
    StructField("amt", DoubleType()),
    StructField("first", StringType()),
    StructField("last", StringType()),
    StructField("gender", StringType()),
    StructField("street", StringType()),
    StructField("city", StringType()),
    StructField("state", StringType()),
    StructField("zip", IntegerType()),
    StructField("lat", DoubleType()),
    StructField("long", DoubleType()),
    StructField("city_pop", IntegerType()),
    StructField("job", StringType()),
    StructField("dob", StringType()),
    StructField("trans_num", StringType()),
    StructField("unix_time", IntegerType()),
    StructField("merch_lat", DoubleType()),
    StructField("merch_long", DoubleType()),
    StructField("is_fraud", IntegerType())
])


print("Loading Pipeline model...")

try:
    loaded_pipeline_model = PipelineModel.load(MODEL_PATH)
    print("Pipeline model loaded successfully.")
except Exception as e:
    print(f"Error loading pipeline model: {e}")
    sys.exit(1)

# UDF for probability extraction
def extract_probability(probability_vector):
    try:
        if probability_vector is None:
            return 0.0
        if hasattr(probability_vector, 'toArray'):
            prob_array = probability_vector.toArray()
        else:
            prob_array = list(probability_vector)
        return float(prob_array[1]) if len(prob_array) > 1 else 0.0
    except Exception:
        return 0.0

extract_prob_udf = udf(extract_probability, DoubleType())

# Haversine distance UDF
def haversine_distance(lat1, lon1, lat2, lon2):
    try:
        if any(x is None for x in [lat1, lon1, lat2, lon2]):
            return 0.0
        
        R = 6371
        lat1_rad = radians(lat1)
        lon1_rad = radians(lon1)
        lat2_rad = radians(lat2)
        lon2_rad = radians(lon2)
        
        dlat = lat2_rad - lat1_rad
        dlon = lon2_rad - lon1_rad
        
        a = sin(dlat / 2) ** 2 + cos(lat1_rad) * cos(lat2_rad) * sin(dlon / 2) ** 2
        c = 2 * atan2(sqrt(a), sqrt(1 - a))
        
        return R * c
    except Exception:
        return 0.0

haversine_udf = udf(haversine_distance, DoubleType())

print(f"Connecting to Kafka topic: {TOPIC_NAME}")

# Read from Kafka
kafka_stream_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER_URL) \
    .option("subscribe", TOPIC_NAME) \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .option("maxOffsetsPerTrigger", SPARK_STREAMING_MAX_OFFSETS_PER_TRIGGER) \
    .load()

print("Connected to Kafka successfully")

# Parse JSON and select only required fields
parsed_df = kafka_stream_df \
    .selectExpr("CAST(value AS STRING) as json_data") \
    .withColumn("parsed_value", from_json(col("json_data"), transaction_schema)) \
    .select("parsed_value.*") \
    .filter(col("trans_num").isNotNull())

print("Parsing Kafka messages...")

# Apply transformations in batches to avoid deep query plans
def process_batch(batch_df, batch_id):
    try:
        print(f"Processing batch {batch_id} with {batch_df.count()} records")
        
        if batch_df.count() == 0:
            print("Empty batch, skipping...")
            return

        transformed_df = batch_df \
            .withColumn("trans_date_trans_time", 
                       to_timestamp(col("trans_date_trans_time"), "yyyy-MM-dd HH:mm:ss")) \
            .withColumn("hour", hour(col("trans_date_trans_time"))) \
            .withColumn("day_of_week", dayofweek(col("trans_date_trans_time"))) \
            .withColumn("distance", 
                       haversine_udf(col("lat"), col("long"), col("merch_lat"), col("merch_long"))) \
            .withColumn("dob_date", to_date(col("dob"))) \
            .withColumn("transaction_date", to_date(col("trans_date_trans_time"))) \
            .withColumn("age", floor(datediff(col("transaction_date"), col("dob_date")) / 365.25)) \
            .withColumn("daily_spending", col("amt")) \
            .withColumn("daily_transactions", lit(1.0)) \
            .withColumn("class_weight", lit(1.0)) \
            .withColumn("indexedLabel", col("is_fraud").cast(DoubleType())) \
            .drop("dob_date", "transaction_date")

        print("Applying ML model...")

        predictions_df = loaded_pipeline_model.transform(transformed_df)
        
        # Extract probability and make final predictions
        final_df = predictions_df \
            .withColumn("fraud_probability", extract_prob_udf(col("probability"))) \
            .withColumn("is_fraud_predicted", 
                       when(col("fraud_probability") >= CONSUMER_FRAUD_THRESHOLD, 1.0).otherwise(0.0))
        
        # Select final columns and show results
        result_df = final_df.select(
            col("trans_num"),
            col("amt"),
            col("category"),
            col("is_fraud").alias("actual_fraud"),
            col("fraud_probability"),
            col("is_fraud_predicted").alias("predicted_label")
        )
        
        print("=== Fraud Detection Results ===")
        result_df.show(20, truncate=False)
        
        # Saving log to database will be added later
        
    except Exception as e:
        print(f"Error processing batch {batch_id}: {e}")
        import traceback
        traceback.print_exc()

# Use foreachBatch for better control
print("Starting streaming query...")

query = parsed_df.writeStream \
    .foreachBatch(process_batch) \
    .option("checkpointLocation", CHECKPOINT_PATH) \
    .trigger(processingTime=SPARK_STREAMING_TRIGGER_TIME) \
    .start()

print("Streaming query started. Waiting for data...")

try:
    query.awaitTermination()
except KeyboardInterrupt:
    print("Stopping the streaming query...")
    query.stop()
    spark.stop()
except Exception as e:
    print(f"Error in streaming query: {e}")
    import traceback
    traceback.print_exc()
    query.stop()
    spark.stop()
    sys.exit(1)