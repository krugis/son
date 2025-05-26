# ~/code/son/telemetry_collector/app.py

import os
import json
import time
import sys
import logging
from kafka import KafkaProducer
from telemetry_collector.config import KAFKA_BOOTSTRAP_SERVERS, RAW_TELEMETRY_TOPIC, SIMULATED_RAW_TELEMETRY_DATA

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
    except Exception as e:
        logger.error(f"Failed to send raw telemetry to Kafka: {e}")

if __name__ == "__main__":
    logger.info("🚀 Starting Telemetry Collector Service 🚀")
    logger.info(f"Publishing to Kafka topic: {RAW_TELEMETRY_TOPIC}")
    time.sleep(1)

    producer = create_kafka_producer()

    if not producer:
        logger.error("Exiting as Kafka producer could not be initialized.")
        sys.exit(1)

    logger.info("--- Simulating Raw Telemetry Collection ---")
    for i, raw_entry in enumerate(SIMULATED_RAW_TELEMETRY_DATA):
        logger.info(f"\nCollecting raw data point {i+1}...")
        send_raw_telemetry(producer, raw_entry)
        time.sleep(2) # Simulate collection interval

    logger.info("\n--- Finished simulating initial telemetry collection. ---")
    logger.info("Telemetry Collector can now continue to run, waiting for new data or you can stop it.")
    # In a real scenario, this would be an infinite loop, perhaps polling a source.
    # For this simulation, we send a batch and then exit.
    # If you want it to run indefinitely, you can add a while True loop here.

    producer.flush()
    producer.close()
    logger.info("Telemetry Collector Producer closed.")
