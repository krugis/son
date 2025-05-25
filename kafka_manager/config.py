import os

# Database Configuration
POLICY_DB_URL = os.getenv("POLICY_DB_URL", "postgresql://policy_user:password@localhost:5432/policy_db")

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
OUTAGE_EVENT_TOPIC = os.getenv("OUTAGE_EVENT_TOPIC", "OutageEvent")
SCALING_ACTION_TOPIC = os.getenv("SCALING_ACTION_TOPIC", "ScalingAction")
POLICY_ENGINE_GROUP_ID = os.getenv("POLICY_ENGINE_GROUP_ID", "policy-engine-group")

# Kafka Installation Paths (must match where kafka_manager.py installs Kafka)
KAFKA_VERSION = "3.9.1" # IMPORTANT: Update this to the exact Kafka version you installed via kafka_manager!
SCALA_VERSION = "2.13" # IMPORTANT: Update this to the exact Scala version you installed!
INSTALL_DIR = os.path.expanduser("~/kafka_install") # Must match kafka_manager's INSTALL_DIR
KAFKA_HOME = os.path.join(INSTALL_DIR, f"kafka_{SCALA_VERSION}-{KAFKA_VERSION}")
KAFKA_BIN_DIR = os.path.join(KAFKA_HOME, "bin")
KAFKA_TOPICS_SCRIPT = os.path.join(KAFKA_BIN_DIR, "kafka-topics.sh") # Path to the topics script

# Flask Configuration
FLASK_DEBUG = os.getenv("FLASK_DEBUG", "True").lower() == "true"
FLASK_PORT = int(os.getenv("FLASK_PORT", 5001))

# Default Policies (will be loaded if DB is empty)
DEFAULT_POLICIES = [
    {
        "name": "High_Risk_Scale_Up",
        "condition": "risk_score > 0.8 and status == 'OK' and component_type == 'RAN'",
        "action": {
            "type": "scale_up",
            "target_component": "Network_Function",
            "scale_factor": 1
        },
        "enabled": True
    },
    {
        "name": "Critical_Status_Backup",
        "condition": "status == 'CRITICAL'",
        "action": {
            "type": "enable_backup",
            "target_component": "Database"
        },
        "enabled": True
    }
]
