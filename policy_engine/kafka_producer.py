import json
import logging
from kafka import KafkaProducer
from kafka.errors import KafkaError
from config import KAFKA_BOOTSTRAP_SERVERS, SCALING_ACTION_TOPIC

producer = None

def get_kafka_producer():
    global producer
    if producer is None:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                acks='all',
                retries=5
            )
            logging.info(f"Kafka Producer initialized for {KAFKA_BOOTSTRAP_SERVERS}")
        except KafkaError as e:
            logging.error(f"Failed to initialize Kafka Producer: {e}")
            producer = None
    return producer

def send_scaling_action(action_data):
    kafka_producer = get_kafka_producer()
    if kafka_producer:
        try:
            future = kafka_producer.send(SCALING_ACTION_TOPIC, action_data)
            record_metadata = future.get(timeout=10)
            logging.info(f"Sent scaling action to {SCALING_ACTION_TOPIC}: {action_data} "
                         f"at topic {record_metadata.topic}, partition {record_metadata.partition}, offset {record_metadata.offset}")
            return True
        except KafkaError as e:
            logging.error(f"Failed to send scaling action to Kafka: {e}")
            return False
        except Exception as e:
            logging.error(f"An unexpected error occurred while sending scaling action: {e}")
            return False
    else:
        logging.error("Kafka Producer is not initialized.")
        return False

def close_producer():
    global producer
    if producer:
        producer.close()
        logging.info("Kafka Producer closed.")
