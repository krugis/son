# ~/code/son/telemetry_collector/config.py

import os
import json

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
RAW_TELEMETRY_TOPIC = os.getenv("RAW_TELEMETRY_TOPIC", "RawTelemetry")

# Path to the sample telemetry data
SAMPLE_TELEMETRY_FILE = os.path.join(os.path.dirname(__file__), 'sample_enodeb_telemetry.json')

# Load simulated raw telemetry data from the JSON file
SIMULATED_RAW_TELEMETRY_DATA = []
try:
    with open(SAMPLE_TELEMETRY_FILE, 'r') as f:
        # Load the list of dictionaries and convert each dict to a JSON string
        raw_data_dicts = json.load(f)
        SIMULATED_RAW_TELEMETRY_DATA = [json.dumps(d) for d in raw_data_dicts]
except FileNotFoundError:
    print(f"WARNING: Sample telemetry file not found at {SAMPLE_TELEMETRY_FILE}. Using empty data.")
except json.JSONDecodeError as e:
    print(f"ERROR: Could not decode JSON from {SAMPLE_TELEMETRY_FILE}: {e}")
except Exception as e:
    print(f"ERROR: An unexpected error occurred loading telemetry data: {e}")

# If the file load fails, you might want a fallback or ensure the list is empty
if not SIMULATED_RAW_TELEMETRY_DATA:
    print("WARNING: No simulated raw telemetry data loaded. Collector will not send messages.")
