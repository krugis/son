# ~/code/son/telemetry_collector/app.py

import os
import json
import time
import sys
import logging
from kafka import KafkaProducer
from telemetry_collector.config import KAFKA_BOOTSTRAP_SERVERS, RAW_TELEMETRY_TOPIC, TELEMETRY_INPUT_DIR # Added TELEMETRY_INPUT_DIR

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def create_kafka_producer(retries=5, delay=2):
    """
    Creates and returns a KafkaProducer instance, with retry logic.
    """
    producer = None
    for i in range(retries):
        try:
            logger.info(f"Attempt {i+1}/{retries}: Connecting to Kafka at {KAFKA_BOOTSTRAP_SERVERS}...")
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: v.encode('utf-8'), # Raw string data, so encode to bytes
                api_version=(2, 0, 0)
            )
            producer.bootstrap_connected()
            logger.info("✅ Kafka Producer connected successfully.")
            return producer
        except Exception as e:
            logger.warning(f"❌ Could not connect to Kafka: {e}. Retrying in {delay} seconds...")
            time.sleep(delay)
            delay *= 1.5
    logger.error("Failed to connect to Kafka after multiple retries. Is Kafka running?")
    sys.exit(1)

def send_raw_telemetry(producer, raw_data):
    """
    Sends a raw telemetry string to the Kafka topic.
    """
    try:
        future = producer.send(RAW_TELEMETRY_TOPIC, value=raw_data)
        record_metadata = future.get(timeout=10)
        logger.info(f"Published raw telemetry to topic '{record_metadata.topic}' partition {record_metadata.partition} offset {record_metadata.offset}")
        logger.info(f"   Raw Data Sent: '{raw_data}'")
        return True
    except Exception as e:
        logger.error(f"Failed to send raw telemetry to Kafka: {e}")
        return False

def process_telemetry_file(producer, file_path):
    """
    Reads a telemetry file, sends its content to Kafka, and moves it to a processed directory.
    """
    logger.info(f"Processing new telemetry file: {file_path}")
    try:
        with open(file_path, 'r') as f:
            # Assuming each file contains one JSON object or a list of JSON objects
            # For simplicity, let's assume it's a list of JSON objects similar to sample_enodeb_telemetry.json
            raw_data_dicts = json.load(f)
            telemetry_entries = [json.dumps(d) for d in raw_data_dicts]

        all_sent_successfully = True
        for entry in telemetry_entries:
            if not send_raw_telemetry(producer, entry):
                all_sent_successfully = False # Keep track if any message fails to send

        if all_sent_successfully:
            # Move the file to a 'processed' subdirectory
            processed_dir = os.path.join(TELEMETRY_INPUT_DIR, 'processed')
            os.makedirs(processed_dir, exist_ok=True)
            shutil.move(file_path, os.path.join(processed_dir, os.path.basename(file_path)))
            logger.info(f"Successfully processed and moved {os.path.basename(file_path)} to {processed_dir}")
        else:
            logger.warning(f"Not all telemetry entries from {os.path.basename(file_path)} were sent successfully. Leaving file in input directory.")

    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse JSON from {file_path}: {e}. Skipping file.")
        # Optionally, move to an 'error' directory
    except Exception as e:
        logger.error(f"An unexpected error occurred while processing file {file_path}: {e}")


if __name__ == "__main__":
    logger.info("🚀 Starting Telemetry Collector Service 🚀")
    logger.info(f"Publishing to Kafka topic: {RAW_TELEMETRY_TOPIC}")
    logger.info(f"Monitoring directory for new telemetry files: {TELEMETRY_INPUT_DIR}")
    time.sleep(1)

    # Ensure the input and processed directories exist
    os.makedirs(TELEMETRY_INPUT_DIR, exist_ok=True)
    os.makedirs(os.path.join(TELEMETRY_INPUT_DIR, 'processed'), exist_ok=True)


    producer = create_kafka_producer()

    if not producer:
        logger.error("Exiting as Kafka producer could not be initialized.")
        sys.exit(1)

    logger.info("--- Monitoring for new telemetry files ---")
    while True:
        try:
            # List files in the input directory, excluding subdirectories
            files_in_dir = [f for f in os.listdir(TELEMETRY_INPUT_DIR) if os.path.isfile(os.path.join(TELEMETRY_INPUT_DIR, f))]
            
            # Filter for JSON files and exclude files in 'processed' or 'error' subdirectories if they were accidentally listed
            # The os.path.isfile check should generally prevent processing directories
            json_files = [f for f in files_in_dir if f.endswith('.json')]

            for filename in json_files:
                file_path = os.path.join(TELEMETRY_INPUT_DIR, filename)
                process_telemetry_file(producer, file_path)

            producer.flush() # Ensure all messages are sent

        except Exception as e:
            logger.error(f"An error occurred during directory monitoring: {e}")
            
        time.sleep(5) # Poll every 5 seconds (adjust as needed)

    # The following lines are unreachable in an infinite loop, but good for understanding
    # producer.close()
    # logger.info("Telemetry Collector Producer closed.")
