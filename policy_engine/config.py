import os

# Database Configuration
POLICY_DB_URL = os.getenv("DATABASE_URL", "postgresql://user:password@localhost:5432/policy_db")
# Make sure to replace 'user', 'password', 'localhost:5432', and 'policy_db'
# with your actual PostgreSQL credentials and database name.

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
OUTAGE_EVENT_TOPIC = os.getenv("OUTAGE_EVENT_TOPIC", "OutageEvent")
SCALING_ACTION_TOPIC = os.getenv("SCALING_ACTION_TOPIC", "ScalingAction")
POLICY_ENGINE_GROUP_ID = os.getenv("POLICY_ENGINE_GROUP_ID", "policy-engine-group")

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
