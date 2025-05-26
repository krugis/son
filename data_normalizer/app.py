# ~/code/son/data_normalizer/app.py

import os
import json
import time
import sys
import logging
from kafka import KafkaConsumer, KafkaProducer
from datetime import datetime, timezone
# Import the updated config
from data_normalizer.config import KAFKA_BOOTSTRAP_SERVERS, RAW_TELEMETRY_TOPIC, STRUCTURED_TELEMETRY_TOPIC

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def create_kafka_consumer(topic, group_id, retries=5, delay=2):
    """
    Creates and returns a KafkaConsumer instance, with retry logic.
    Modified to deserialize JSON directly.
    """
    consumer = None
    for i in range(retries):
        try:
            logger.info(f"Attempt {i+1}/{retries}: Connecting Kafka Consumer to {KAFKA_BOOTSTRAP_SERVERS} for topic {topic}...")
            consumer = KafkaConsumer(
                topic,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                group_id=group_id,
                value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                api_version=(2, 0, 0)
            )
            consumer.topics() # Check connection
            logger.info(f"✅ Kafka Consumer for {topic} connected successfully.")
            return consumer
        except Exception as e: # Catch broader exceptions for connection issues
            logger.warning(f"❌ Could not connect Kafka Consumer: {e}. Retrying in {delay} seconds...")
            time.sleep(delay)
            delay *= 1.5
    logger.error("Failed to connect Kafka Consumer after multiple retries. Is Kafka running?")
    sys.exit(1)

def create_kafka_producer(retries=5, delay=2):
    """
    Creates and returns a KafkaProducer instance, with retry logic.
    """
    producer = None
    for i in range(retries):
        try:
            logger.info(f"Attempt {i+1}/{retries}: Connecting Kafka Producer to {KAFKA_BOOTSTRAP_SERVERS}...")
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'), # Output will be JSON
                api_version=(2, 0, 0)
            )
            producer.bootstrap_connected()
            logger.info("✅ Kafka Producer connected successfully.")
            return producer
        except Exception as e:
            logger.warning(f"❌ Could not connect Kafka Producer: {e}. Retrying in {delay} seconds...")
            time.sleep(delay)
            delay *= 1.5
    logger.error("Failed to connect Kafka Producer after multiple retries. Is Kafka running?")
    sys.exit(1)


def parse_and_validate_telemetry(raw_json_data):
    """
    Parses and validates the incoming JSON telemetry data.
    Returns the parsed dictionary if valid, None otherwise.
    This function will now be responsible for ensuring the data is properly structured
    before passing it to the ML engine.
    """
    if not isinstance(raw_json_data, dict):
        logger.error(f"Invalid telemetry format: Expected JSON object, got {type(raw_json_data)}")
        return None

    # Basic validation of essential fields for structured telemetry
    required_fields = ['enodeb_id', 'timestamp', 'kpis', 'alarms', 'status']
    if not all(field in raw_json_data for field in required_fields):
        logger.warning(f"Missing required fields in telemetry: {raw_json_data}")
        return None

    # You might want to add more sophisticated validation here,
    # e.g., checking if kpis is a dict, alarms is a list, etc.

    return raw_json_data

# Renamed and simplified: This function now just prepares a structured event,
# without deriving risk_score or final outage status. That's for ML.
def prepare_structured_telemetry(parsed_data):
    """
    Prepares the parsed telemetry data into a structured format suitable for
    the Outage Predictor (ML Engine).
    """
    if not parsed_data:
        return None

    # Create a standardized structured telemetry event.
    # It includes all relevant raw fields, possibly with some basic standardization
    # (e.g., ensuring timestamp format, converting types if needed).
    structured_event = {
        "event_id": f"structured-telemetry-{parsed_data.get('enodeb_id', 'UNKNOWN_ENODEB')}-{int(time.time() * 1000)}",
        "timestamp": parsed_data.get('timestamp', datetime.now(timezone.utc).isoformat()),
        "component_type": "RAN", # Assuming this is always RAN for eNodeB data
        "component_id": parsed_data.get('enodeb_id', 'UNKNOWN_ENODEB'),
        "cell_id": parsed_data.get('cell_id', 'UNKNOWN_CELL'),
        "vendor": parsed_data.get('vendor', 'UNKNOWN_VENDOR'),
        "location": parsed_data.get('location', {}),
        "kpis": parsed_data.get('kpis', {}),
        "alarms": parsed_data.get('alarms', []),
        "raw_status": parsed_data.get('status', 'OK') # Keep original status for ML to interpret
    }
    return structured_event


if __name__ == "__main__":
    logger.info("🚀 Starting Data Normalizer Service 🚀")
    logger.info(f"Consuming from topic: {RAW_TELEMETRY_TOPIC}")
    logger.info(f"Producing to topic: {STRUCTURED_TELEMETRY_TOPIC}")

    consumer = create_kafka_consumer(RAW_TELEMETRY_TOPIC, group_id='data-normalizer-group')
    producer = create_kafka_producer()

    if not consumer or not producer:
        logger.error("Exiting as Kafka client(s) could not be initialized.")
        sys.exit(1)

    try:
        for message in consumer:
            received_telemetry = message.value # This is now a Python dict from JSON
            logger.info(f"\n➡️ Received raw telemetry from topic {message.topic} offset {message.offset}")
            logger.info(f"   Received Data: {json.dumps(received_telemetry, indent=2)}")

            logger.info("   Parsing and validating telemetry...")
            parsed_data = parse_and_validate_telemetry(received_telemetry)

            if parsed_data:
                logger.info("   Preparing structured telemetry...")
                structured_telemetry_event = prepare_structured_telemetry(parsed_data)

                if structured_telemetry_event:
                    logger.info("   ✅ Successfully normalized and structured telemetry.")
                    logger.debug(f"   Structured Telemetry Event: {json.dumps(structured_telemetry_event, indent=2)}")

                    logger.info(f"   ➡️ Publishing Structured Telemetry to Kafka topic: {STRUCTURED_TELEMETRY_TOPIC}...")
                    send_future = producer.send(STRUCTURED_TELEMETRY_TOPIC, value=structured_telemetry_event)
                    record_metadata = send_future.get(timeout=10)
                    logger.info(f"   ✅ Published Structured Telemetry for component: {structured_telemetry_event['component_id']}")
                    logger.info(f"   Topic: '{record_metadata.topic}' Partition: {record_metadata.partition} Offset: {record_metadata.offset}")
                else:
                    logger.warning("   Skipping: Could not prepare structured telemetry after parsing.")
            else:
                logger.warning("   Skipping: Invalid or incomplete raw telemetry data received.")
            time.sleep(0.5) # Small delay for readability
    except KeyboardInterrupt:
        logger.info("Data Normalizer Service stopped by user.")
    except Exception as e:
        logger.error(f"An error occurred in Data Normalizer main loop: {e}", exc_info=True)
    finally:
        if consumer:
            consumer.close()
            logger.info("Kafka Consumer closed.")
        if producer:
            producer.flush()
            producer.close()
            logger.info("Kafka Producer closed.")
