import csv
import json
from kafka import KafkaProducer
import time
import sys
import signal
from datetime import datetime
import os

KAFKA_BROKER_URL = os.getenv('KAFKA_BROKER_URL', 'kafka:29092')
TOPIC_NAME = os.getenv('KAFKA_TOPIC_NAME', 'raw_transactions')
CSV_FILE_PATH = os.getenv('PRODUCER_CSV_FILE', 'fraudTest.csv')
OFFSET_FILE_PATH = '/app/data/producer_offset.txt'
SAVE_INTERVAL = int(os.getenv('PRODUCER_SAVE_INTERVAL', '50'))

# Global variables for signal handling
current_offset = -1
offset_lock = False

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

def get_last_offset():
    try:
        if os.path.exists(OFFSET_FILE_PATH):
            with open(OFFSET_FILE_PATH, 'r') as f:
                offset = int(f.read().strip())
                print(f"Resuming from row {offset + 1}")
                return offset
        else:
            print("No offset file found, starting from beginning")
            return -1
    except Exception as e:
        print(f"Error reading offset file: {e}, starting from beginning")
        return -1

def save_offset(row_number):
    global offset_lock
    if offset_lock:
        return
    
    try:
        offset_lock = True
        os.makedirs(os.path.dirname(OFFSET_FILE_PATH), exist_ok=True)

        temp_file = OFFSET_FILE_PATH + '.tmp'
        with open(temp_file, 'w') as f:
            f.write(str(row_number))
        os.rename(temp_file, OFFSET_FILE_PATH)
        
    except Exception as e:
        print(f"Error saving offset: {e}")
    finally:
        offset_lock = False

def signal_handler(signum, frame):
    global current_offset
    print(f"\nReceived signal {signum}. Flushing producer and saving current progress at offset {current_offset}...")

    try:
        producer.flush(timeout=30) # Wait up to 30 seconds for all messages to be sent
        print("Producer flushed successfully")
    except Exception as e:
        print(f"Error flushing producer: {e}")
    
    if current_offset >= 0:
        save_offset(current_offset)
    print("Progress saved. Exiting gracefully.")
    sys.exit(0)

def produce_messages(producer):
    global current_offset
    
    print(f"Starting to read from {CSV_FILE_PATH} and send to topic {TOPIC_NAME}")

    last_transaction_time = None
    first_transaction_wall_time = time.time()
    last_offset = get_last_offset()
    current_offset = last_offset
    
    try:
        with open(CSV_FILE_PATH, mode='r', encoding='utf-8') as file:
            csv_reader = csv.DictReader(file)
            header = csv_reader.fieldnames
            if 'trans_date_trans_time' not in header:
                print("Error: 'trans_date_trans_time' column not found in CSV header.")
                sys.exit(1)

            rows = list(csv_reader)
            total_rows = len(rows)
            
            print(f"Total rows in CSV: {total_rows}")
            print(f"Starting from row: {last_offset + 1}")

            for i, row in enumerate(rows):
                if i <= last_offset:
                    continue
                
                current_offset = i
                    
                try:
                    # Data type conversions
                    row['amt'] = float(row.get('amt', 0.0))
                    row['zip'] = int(row.get('zip', 0))
                    row['lat'] = float(row.get('lat', 0.0))
                    row['long'] = float(row.get('long', 0.0))
                    row['city_pop'] = int(row.get('city_pop', 0))
                    row['unix_time'] = int(row.get('unix_time', 0))
                    row['merch_lat'] = float(row.get('merch_lat', 0.0))
                    row['merch_long'] = float(row.get('merch_long', 0.0))
                    row['is_fraud'] = int(row.get('is_fraud', 0))

                    # String conversions
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

                    # Send message and wait for acknowledgment
                    future = producer.send(TOPIC_NAME, value=row)
                    try:
                        # Wait for acknowledgment with timeout
                        record_metadata = future.get(timeout=10)
                        print(f"Message sent successfully to {record_metadata.topic} partition {record_metadata.partition} offset {record_metadata.offset}")
                    except Exception as send_error:
                        print(f"Failed to send message for row {i+1}: {send_error}")
                        continue

                    # Save offset periodically and on important milestones
                    if (i - last_offset) % SAVE_INTERVAL == 0 or i == len(rows) - 1:
                        save_offset(i)
                        print(f"Sent {i+1} messages (total processed: {i - last_offset}). Last message trans_num: {row['trans_num']} at {current_trans_time_str}")

                except ValueError as ve:
                    print(f"Skipping row {i+1} due to data type conversion error: {ve}")
                    if (i - last_offset) % SAVE_INTERVAL == 0:
                        save_offset(i)
                except Exception as e:
                    print(f"Error sending message for row {i+1}: {e}")
                    if (i - last_offset) % SAVE_INTERVAL == 0:
                        save_offset(i)

            print(f"Finished processing all rows. Final offset saved: {len(rows) - 1}")

    except FileNotFoundError:
        print(f"Error: CSV file not found at {CSV_FILE_PATH}")
        sys.exit(1)
    except KeyboardInterrupt:
        print("\nReceived interrupt signal, saving current progress...")
        save_offset(current_offset)
        sys.exit(0)
    finally:
        producer.flush()
        total_wall_time = time.time() - first_transaction_wall_time
        print(f"\nFinished sending messages. Total wall clock time: {total_wall_time:.2f} seconds.")

if __name__ == "__main__":
    # Register signal handlers for graceful shutdown
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)
    
    print("Waiting for Kafka broker to be available...")
    time.sleep(30)

    producer = create_kafka_producer()
    produce_messages(producer)