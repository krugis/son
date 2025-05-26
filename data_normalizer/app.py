# ~/code/son/data_normalizer/app.py

import os
import json
import time
import sys
import logging
from kafka import KafkaConsumer, KafkaProducer
from datetime import datetime, timezone # Import timezone for datetime.now(timezone.utc)
from data_normalizer.config import KAFKA_BOOTSTRAP_SERVERS, RAW_TELEMETRY_TOPIC, OUTAGE_EVENT_TOPIC

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
                value_deserializer=lambda x: json.loads(x.decode('utf-8')), # <-- Deserialize JSON directly
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                api_version=(2, 0, 0)
            )
            consumer.topics() # Check connection
            logger.info(f"✅ Kafka Consumer for {topic} connected successfully.")
            return consumer
        except NoBrokersAvailable as e:
            logger.warning(f"❌ No Kafka brokers available: {e}. Retrying in {delay} seconds...")
            time.sleep(delay)
            delay *= 1.5
        except Exception as e:
            logger.error(f"❌ An unexpected error occurred during consumer connection: {e}. Retrying in {delay} seconds...")
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
    """
    if not isinstance(raw_json_data, dict):
        logger.error(f"Invalid telemetry format: Expected JSON object, got {type(raw_json_data)}")
        return None

    # Basic validation of essential fields
    required_fields = ['enodeb_id', 'timestamp', 'kpis', 'status']
    if not all(field in raw_json_data for field in required_fields):
        logger.warning(f"Missing required fields in telemetry: {raw_json_data}")
        return None

    # Further validation could go here (e.g., check types, ranges)
    return raw_json_data

def transform_to_outage_event(parsed_telemetry):
    """
    Transforms parsed eNodeB telemetry data into an OutageEvent structure.
    This includes deriving risk_score and appropriate status based on simple rules
    from KPIs and alarms.
    """
    if not parsed_telemetry:
        return None

    enodeb_id = parsed_telemetry.get('enodeb_id', 'UNKNOWN_ENODEB')
    cell_id = parsed_telemetry.get('cell_id', 'UNKNOWN_CELL')
    component_type = 'RAN' # Fixed as it's eNodeB telemetry
    timestamp = parsed_telemetry.get('timestamp', datetime.utcnow().isoformat()) # Use provided or current UTC time

    kpis = parsed_telemetry.get('kpis', {})
    alarms = parsed_telemetry.get('alarms', [])
    overall_status = parsed_telemetry.get('status', 'OK').upper() # Use provided status as base

    # --- Derive Status and Risk Score for OutageEvent based on complex rules ---
    outage_event_status = overall_status
    risk_score = 0.2 # Default low risk

    # Rule 1: Critical Alarms --> CRITICAL status, high risk
    critical_alarms_active = any(a.get('severity') == 'CRITICAL' and a.get('active') for a in alarms)
    if critical_alarms_active:
        outage_event_status = 'CRITICAL'
        risk_score = 0.95
        logger.debug(f"Critical alarm detected for {enodeb_id}, setting status to CRITICAL, risk to 0.95")

    # Rule 2: Major Alarms without Critical --> DEGRADED status, medium-high risk
    elif any(a.get('severity') == 'MAJOR' and a.get('active') for a in alarms):
        if overall_status == 'OK': # Only upgrade if not already worse
             outage_event_status = 'DEGRADED'
        risk_score = max(risk_score, 0.75) # Ensure risk is at least 0.75
        logger.debug(f"Major alarm detected for {enodeb_id}, setting risk to 0.75")


    # Rule 3: High ERAB Drop Rate --> DEGRADED/CRITICAL status, high risk
    erab_drop_rate = kpis.get('erab_drop_rate_percent', 0.0)
    if erab_drop_rate > 5.0:
        if overall_status == 'OK' or overall_status == 'DEGRADED':
            outage_event_status = 'DEGRADED' # Can be CRITICAL too depending on threshold
        risk_score = max(risk_score, 0.88) # Very high risk
        logger.debug(f"High ERAB Drop Rate ({erab_drop_rate}%) for {enodeb_id}, setting risk to 0.88")
    elif erab_drop_rate > 1.0:
        if overall_status == 'OK':
            outage_event_status = 'DEGRADED'
        risk_score = max(risk_score, 0.6) # Moderate risk
        logger.debug(f"Moderate ERAB Drop Rate ({erab_drop_rate}%) for {enodeb_id}, setting risk to 0.6")

    # Rule 4: Very Low SINR or RSRP --> DEGRADED status, high risk
    sinr_avg = kpis.get('sinr_avg', 999.0)
    rsrp_avg = kpis.get('rsrp_avg', 0.0)
    if sinr_avg < 5.0 or rsrp_avg < -100: # Thresholds for poor signal
        if overall_status == 'OK':
            outage_event_status = 'DEGRADED'
        risk_score = max(risk_score, 0.8)
        logger.debug(f"Poor SINR ({sinr_avg}) or RSRP ({rsrp_avg}) for {enodeb_id}, setting risk to 0.8")

    # Final decision for status based on highest derived severity
    # If the original status was already higher (e.g., FAULT), maintain it
    if overall_status == 'CRITICAL' or overall_status == 'FAULT':
        outage_event_status = overall_status
        risk_score = max(risk_score, 0.99) # Ensure highest risk if already critical/fault

    # --- Construct the OutageEvent ---
    outage_event = {
        "event_id": f"enodeb-telemetry-{enodeb_id}-{int(time.time() * 1000)}",
        "timestamp": timestamp,
        "component_type": component_type,
        "component_id": enodeb_id,
        "metric": "composite_telemetry_report", # Indicate this is a summary from full telemetry
        "value": overall_status, # The overall status from the raw data
        "status": outage_event_status, # The derived status for the OutageEvent
        "risk_score": round(risk_score, 2), # Round for cleaner output
        "description": f"Comprehensive telemetry report for eNodeB {enodeb_id}. Overall status: {overall_status}. Derived event status: {outage_event_status}. Active Alarms: {len(alarms)}. KPIs: {kpis}."
    }
    return outage_event

if __name__ == "__main__":
    logger.info("🚀 Starting Data Normalizer Service 🚀")
    logger.info(f"Consuming from topic: {RAW_TELEMETRY_TOPIC}")
    logger.info(f"Producing to topic: {OUTAGE_EVENT_TOPIC}")

    consumer = create_kafka_consumer(RAW_TELEMETRY_TOPIC, group_id='data-normalizer-group')
    producer = create_kafka_producer()

    if not consumer or not producer:
        logger.error("Exiting as Kafka client(s) could not be initialized.")
        sys.exit(1)

    try:
        for message in consumer:
            # message.value is now a Python dict directly due to value_deserializer
            received_telemetry = message.value
            logger.info(f"\n➡️ Received raw (JSON) telemetry from topic {message.topic} offset {message.offset}")
            logger.info(f"   Received Data: {json.dumps(received_telemetry, indent=2)}")

            logger.info("   Parsing and validating telemetry...")
            parsed_data = parse_and_validate_telemetry(received_telemetry)

            if parsed_data:
                logger.info("   Transforming to OutageEvent...")
                outage_event = transform_to_outage_event(parsed_data)

                if outage_event:
                    logger.info("   ✅ Successfully normalized and transformed to OutageEvent.")
                    logger.debug(f"   Transformed OutageEvent: {json.dumps(outage_event, indent=2)}")

                    logger.info(f"   ➡️ Publishing OutageEvent to Kafka topic: {OUTAGE_EVENT_TOPIC}...")
                    send_future = producer.send(OUTAGE_EVENT_TOPIC, value=outage_event)
                    record_metadata = send_future.get(timeout=10)
                    logger.info(f"   ✅ Published OutageEvent! Component: {outage_event['component_id']} (Derived Status: {outage_event['status']}, Risk: {outage_event['risk_score']})")
                    logger.info(f"   Topic: '{record_metadata.topic}' Partition: {record_metadata.partition} Offset: {record_metadata.offset}")
                else:
                    logger.warning("   Skipping: Could not transform to OutageEvent after parsing.")
            else:
                logger.warning("   Skipping: Invalid or incomplete telemetry data received.")
            time.sleep(0.5) # Small delay for readability
    except KeyboardInterrupt:
        logger.info("Data Normalizer Service stopped by user.")
    except Exception as e:
        logger.error(f"An error occurred in Data Normalizer main loop: {e}", exc_info=True) # exc_info to print traceback
    finally:
        if consumer:
            consumer.close()
            logger.info("Kafka Consumer closed.")
        if producer:
            producer.flush()
            producer.close()
            logger.info("Kafka Producer closed.")
