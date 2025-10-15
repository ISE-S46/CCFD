from pyspark.sql import SparkSession, Column
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
import traceback
import psycopg2
from psycopg2 import extras

KAFKA_BROKER_URL = os.getenv('KAFKA_BROKER_URL', 'kafka:29092')
TOPIC_NAME = os.getenv('KAFKA_TOPIC_NAME', 'raw_transactions')
MODEL_PATH = '/app/Pipeline'
CHECKPOINT_PATH = os.getenv('CONSUMER_CHECKPOINT_PATH', '/app/spark_checkpoints')

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

PG_DB = os.getenv('POSTGRES_DB')
PG_USER = os.getenv('POSTGRES_USER')
PG_PASSWORD = os.getenv('POSTGRES_PASSWORD')
PG_HOST = os.getenv('POSTGRES_HOST')
PG_PORT = os.getenv('POSTGRES_PORT')

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
        .config("spark.sql.streaming.metricsEnabled", "true") \
        .config("spark.sql.adaptive.enabled", "false") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "false") \
        .config("spark.serializer", SPARK_SERIALIZER) \
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
        .config("spark.sql.streaming.checkpointLocation.deletedFileRetention", "100") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "false") \
        .getOrCreate()
    
    print("Spark Session created successfully")
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

def haversine_distance_spark(lat1: Column, lon1: Column, lat2: Column, lon2: Column) -> Column:
    R = 6371
    dlat = radians(lat2 - lat1)
    dlon = radians(lon2 - lon1)
    a = sin(dlat / 2) ** 2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon / 2) ** 2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))
    return when(lat1.isNotNull() & lon1.isNotNull() & lat2.isNotNull() & lon2.isNotNull(), R * c).otherwise(0.0)

print(f"Connecting to Kafka topic: {TOPIC_NAME}")

# Read from Kafka
kafka_stream_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER_URL) \
    .option("subscribe", TOPIC_NAME) \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .option("maxOffsetsPerTrigger", SPARK_STREAMING_MAX_OFFSETS_PER_TRIGGER) \
    .option("kafka.session.timeout.ms", "30000") \
    .option("kafka.request.timeout.ms", "40000") \
    .option("kafka.max.poll.records", "500") \
    .load()

print("Connected to Kafka successfully")

# Parse JSON and select only required fields
parsed_df = kafka_stream_df \
    .selectExpr("CAST(value AS STRING) as json_data") \
    .withColumn("parsed_value", from_json(col("json_data"), transaction_schema)) \
    .select("parsed_value.*") \
    .filter(col("trans_num").isNotNull())

print("Parsing Kafka messages...")

def process_batch(batch_df, batch_id):
    try:
        result_df, all_transactions_df, fraud_transactions_df = transform_and_predict(batch_df, batch_id)
        if result_df is not None:
            save_to_postgres(all_transactions_df, fraud_transactions_df)
    except Exception as e:
        print(f"Error processing batch {batch_id}: {e}")
        traceback.print_exc()

def get_db_connection():
    return psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        database=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD
    )

def transform_and_predict(batch_df, batch_id):
    record_count = batch_df.cache().count()
    print(f"Processing batch {batch_id} with {record_count} records")

    if record_count == 0:
        print("Empty batch, skipping...")
        batch_df.unpersist()
        return None, None, None

    all_transactions_df = batch_df.select(
        col("trans_num"),
        to_timestamp(col("trans_date_trans_time"), "yyyy-MM-dd HH:mm:ss").alias("trans_date_trans_time"),
        col("cc_num"),
        col("merchant"),
        col("category"),
        col("amt"),
        col("first").alias("first_name"),
        col("last").alias("last_name"),
        col("gender"),
        col("street"),
        col("city"),
        col("state"),
        col("zip"),
        col("lat"),
        col("long"),
        col("city_pop"),
        col("job"),
        to_date(col("dob")).alias("dob"),
        col("unix_time"),
        col("merch_lat"),
        col("merch_long"),
        col("is_fraud")
    )

    batch_df.unpersist()

    transformed_df = all_transactions_df \
        .withColumn("trans_date_trans_time", 
                    to_timestamp(col("trans_date_trans_time"), "yyyy-MM-dd HH:mm:ss")) \
        .withColumn("hour", hour(col("trans_date_trans_time"))) \
        .withColumn("day_of_week", dayofweek(col("trans_date_trans_time"))) \
        .withColumn("distance", haversine_distance_spark(col("lat"), col("long"), col("merch_lat"), col("merch_long"))) \
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

    final_df = predictions_df \
        .withColumn("fraud_probability", extract_prob_udf(col("probability"))) \
        .withColumn("is_fraud_predicted", 
                    when(col("fraud_probability") >= CONSUMER_FRAUD_THRESHOLD, 1.0).otherwise(0.0))

    result_df = final_df.select(
        col("trans_num"),
        col("amt"),
        col("category"),
        col("trans_date_trans_time"),
        col("is_fraud").alias("actual_fraud"),
        col("fraud_probability"),
        col("is_fraud_predicted").alias("predicted_label")
    )

    fraud_transactions_df = final_df \
        .filter(col("is_fraud_predicted") == 1.0) \
        .select(
            col("trans_num"),
            col("amt"),
            col("category"),
            col("trans_date_trans_time"),
            col("is_fraud").alias("actual_fraud"),
            col("fraud_probability"),
            col("is_fraud_predicted").alias("predicted_label")
        )

    print("=== Fraud Detection Results ===")
    result_df.show(20, truncate=False)

    return result_df, all_transactions_df, fraud_transactions_df

def save_to_postgres(all_transactions_df, fraud_transactions_df):
    conn = None
    cur = None

    try:
        all_transactions_pandas_df = all_transactions_df.toPandas() if all_transactions_df is not None else None
        fraud_transactions_pandas_df = fraud_transactions_df.toPandas() if fraud_transactions_df is not None else None

        if (all_transactions_pandas_df is None or all_transactions_pandas_df.empty) and \
           (fraud_transactions_pandas_df is None or fraud_transactions_pandas_df.empty):
            print("ℹ️ No data to insert into PostgreSQL.")
            return

        conn = get_db_connection()
        cur = conn.cursor()

        if all_transactions_pandas_df is not None and not all_transactions_pandas_df.empty:
            all_cols = [
                "trans_num", "trans_date_trans_time", "cc_num", "merchant", "category", "amt",
                "first_name", "last_name", "gender", "street", "city", "state", "zip",
                "lat", "long", "city_pop", "job", "dob", "unix_time", "merch_lat",
                "merch_long", "is_fraud"
            ]
            all_values = all_transactions_pandas_df[all_cols].values.tolist()

            insert_all_sql = f"""
                INSERT INTO all_transactions ({', '.join(all_cols)})
                VALUES %s
                ON CONFLICT (trans_num) DO NOTHING;
            """
            extras.execute_values(cur, insert_all_sql, all_values)
            print(f"Inserted {len(all_values)} records into all_transactions.")

        if fraud_transactions_pandas_df is not None and not fraud_transactions_pandas_df.empty:
            fraud_cols = [
                "trans_num", "amt", "category", "trans_date_trans_time",
                "actual_fraud", "fraud_probability", "predicted_label"
            ]
            fraud_values = fraud_transactions_pandas_df[fraud_cols].values.tolist()
            insert_fraud_sql = f"""
                INSERT INTO fraud_transactions ({', '.join(fraud_cols)})
                VALUES %s
                ON CONFLICT (trans_num) DO UPDATE SET
                    amt = EXCLUDED.amt,
                    category = EXCLUDED.category,
                    trans_date_trans_time = EXCLUDED.trans_date_trans_time,
                    actual_fraud = EXCLUDED.actual_fraud,
                    fraud_probability = EXCLUDED.fraud_probability,
                    predicted_label = EXCLUDED.predicted_label;
            """
            extras.execute_values(cur, insert_fraud_sql, fraud_values)
            print(f"Inserted/Updated {len(fraud_values)} fraud records into fraud_transactions.")
        
        conn.commit()

    except psycopg2.Error as db_err:
        print(f"Database error: {db_err}")
        if conn:
            conn.rollback()
        traceback.print_exc()
    except Exception as ex:
        print(f"An unexpected error occurred during database insert: {ex}")
        if conn:
            conn.rollback()
        traceback.print_exc()
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

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
    traceback.print_exc()
    query.stop()
    spark.stop()
    sys.exit(1)