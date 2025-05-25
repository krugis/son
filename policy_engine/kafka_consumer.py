import json
import logging
import threading
from kafka import KafkaConsumer
from kafka.errors import KafkaError
from config import KAFKA_BOOTSTRAP_SERVERS, OUTAGE_EVENT_TOPIC, POLICY_ENGINE_GROUP_ID
from policies import evaluate_policies
from kafka_producer import send_scaling_action

consumer_thread = None
running_event = threading.Event()

def consume_outage_events():
    logging.info(f"Starting Kafka Consumer for topic: {OUTAGE_EVENT_TOPIC}")
    consumer = None
    try:
        consumer = KafkaConsumer(
            OUTAGE_EVENT_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            group_id=POLICY_ENGINE_GROUP_ID,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            auto_commit_interval_ms=1000
        )
        logging.info(f"Kafka Consumer connected to {KAFKA_BOOTSTRAP_SERVERS}")

        for message in consumer:
            if not running_event.is_set():
                break

            logging.info(f"Received OutageEvent: Topic={message.topic}, Partition={message.partition}, Offset={message.offset}")
            outage_event = message.value

            actions = evaluate_policies(outage_event)

            for action in actions:
                send_scaling_action(action)

    except KafkaError as e:
        logging.error(f"Kafka Consumer error: {e}")
    except Exception as e:
        logging.error(f"An unexpected error occurred in consumer thread: {e}")
    finally:
        if consumer:
            consumer.close()
            logging.info("Kafka Consumer closed.")

def start_consumer_thread():
    global consumer_thread
    if consumer_thread is None or not consumer_thread.is_alive():
        running_event.set()
        consumer_thread = threading.Thread(target=consume_outage_events)
        consumer_thread.daemon = True
        consumer_thread.start()
        logging.info("Kafka Consumer thread started.")

def stop_consumer_thread():
    global consumer_thread
    if consumer_thread and consumer_thread.is_alive():
        running_event.clear()
        consumer_thread.join(timeout=10)
        if consumer_thread.is_alive():
            logging.warning("Kafka Consumer thread did not terminate gracefully.")
        else:
            logging.info("Kafka Consumer thread stopped.")
