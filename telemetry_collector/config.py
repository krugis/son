# ~/code/son/telemetry_collector/config.py

import os
import json
import shutil # Import shutil for file operations

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092") #
RAW_TELEMETRY_TOPIC = os.getenv("RAW_TELEMETRY_TOPIC", "RawTelemetry") #

# New: Directory where telemetry files will be placed for collection
TELEMETRY_INPUT_DIR = os.getenv("TELEMETRY_INPUT_DIR", os.path.join(os.path.dirname(os.path.abspath(__file__)), 'input_telemetry'))
