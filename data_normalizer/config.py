# ~/code/son/data_normalizer/config.py

import os

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
RAW_TELEMETRY_TOPIC = os.getenv("RAW_TELEMETRY_TOPIC", "RawTelemetry")
OUTAGE_EVENT_TOPIC = os.getenv("OUTAGE_EVENT_TOPIC", "OutageEvent")
