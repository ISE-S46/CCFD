import csv
import json
from kafka import KafkaProducer
import time
import sys
from datetime import datetime
import os

KAFKA_BROKER_URL = os.getenv('KAFKA_BROKER_URL', 'kafka:29092')
TOPIC_NAME = os.getenv('KAFKA_TOPIC_NAME', 'raw_transactions')
CSV_FILE_PATH = os.getenv('PRODUCER_CSV_FILE', 'fraudTest.csv')

def create_kafka_producer():
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKER_URL,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            api_version=(0, 10, 1)
        )

        print(f"Successfully connected to Kafka at {KAFKA_BROKER_URL}")

        return producer

    except Exception as e:
        print(f"Error connecting to Kafka: {e}")
        sys.exit(1)

def produce_messages(producer):
    print(f"Starting to read from {CSV_FILE_PATH} and send to topic {TOPIC_NAME}")

    last_transaction_time = None
    first_transaction_wall_time = time.time()

    try:
        with open(CSV_FILE_PATH, mode='r', encoding='utf-8') as file:
            csv_reader = csv.DictReader(file)
            header = csv_reader.fieldnames
            if 'trans_date_trans_time' not in header:
                print("Error: 'trans_date_trans_time' column not found in CSV header.")
                sys.exit(1)

            for i, row in enumerate(csv_reader):
                try:
                    # Numeric conversions
                    row['amt'] = float(row.get('amt', 0.0))
                    row['zip'] = int(row.get('zip', 0))
                    row['lat'] = float(row.get('lat', 0.0))
                    row['long'] = float(row.get('long', 0.0))
                    row['city_pop'] = int(row.get('city_pop', 0))
                    row['unix_time'] = int(row.get('unix_time', 0))
                    row['merch_lat'] = float(row.get('merch_lat', 0.0))
                    row['merch_long'] = float(row.get('merch_long', 0.0))
                    row['is_fraud'] = int(row.get('is_fraud', 0))

                    # String conversions (ensure they exist, even if empty)
                    row['cc_num'] = row.get('cc_num', '')
                    row['merchant'] = row.get('merchant', '')
                    row['category'] = row.get('category', '')
                    row['trans_date_trans_time'] = row.get('trans_date_trans_time', '')
                    row['first'] = row.get('first', '')
                    row['last'] = row.get('last', '')
                    row['gender'] = row.get('gender', '')
                    row['street'] = row.get('street', '')
                    row['city'] = row.get('city', '')
                    row['state'] = row.get('state', '')
                    row['job'] = row.get('job', '')
                    row['dob'] = row.get('dob', '')
                    row['trans_num'] = row.get('trans_num', '')

                    current_trans_time_str = row['trans_date_trans_time']
                    current_trans_time_dt = datetime.strptime(current_trans_time_str, "%Y-%m-%d %H:%M:%S")

                    if last_transaction_time:
                        time_diff_seconds = (current_trans_time_dt - last_transaction_time).total_seconds()
                        if time_diff_seconds > 0:
                            time.sleep(time_diff_seconds)

                    last_transaction_time = current_trans_time_dt

                    producer.send(TOPIC_NAME, value=row)

                    if i % 10 == 0:
                        print(f"Sent {i+1} messages. Last message trans_num: {row['trans_num']} at {current_trans_time_str}")

                except ValueError as ve:
                    print(f"Skipping row {i+1} due to data type conversion error: {ve} - Row: {row}")
                except Exception as e:
                    print(f"Error sending message for row {i+1}: {e}")

    except FileNotFoundError:
        print(f"Error: CSV file not found at {CSV_FILE_PATH}")
        sys.exit(1)
    finally:
        producer.flush()
        total_wall_time = time.time() - first_transaction_wall_time # Calculate total time
        print(f"\nFinished sending messages. Total wall clock time: {total_wall_time:.2f} seconds.")


if __name__ == "__main__":
    print("Waiting for Kafka broker to be available...")
    time.sleep(30)

    producer = create_kafka_producer()
    produce_messages(producer)