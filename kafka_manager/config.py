import os

# Kafka Version and Download
KAFKA_VERSION = "3.9.1"  # IMPORTANT: Use the exact version you installed
SCALA_VERSION = "2.13"
KAFKA_ARCHIVE = f"kafka_{SCALA_VERSION}-{KAFKA_VERSION}.tgz"
KAFKA_DOWNLOAD_URL = f"https://downloads.apache.org/kafka/{KAFKA_VERSION}/{KAFKA_ARCHIVE}"

# Installation Paths
INSTALL_DIR = os.path.expanduser("~/kafka_install")
KAFKA_HOME = os.path.join(INSTALL_DIR, f"kafka_{SCALA_VERSION}-{KAFKA_VERSION}")

# Zookeeper Configuration
ZOOKEEPER_PROPERTIES_PATH = os.path.join(KAFKA_HOME, "config", "zookeeper.properties")
ZOOKEEPER_PORT = 2181
ZOOKEEPER_DATA_DIR = os.path.join(INSTALL_DIR, "zookeeper-data")

# Kafka Broker Configuration
SERVER_PROPERTIES_PATH = os.path.join(KAFKA_HOME, "config", "server.properties")
KAFKA_BROKER_PORT = 9092
KAFKA_LOG_DIRS = os.path.join(INSTALL_DIR, "kafka-logs")
KAFKA_BOOTSTRAP_SERVERS = f"localhost:{KAFKA_BROKER_PORT}" # Common Kafka bootstrap server

# Kafka Topic Names (used by Policy Engine and other services)
OUTAGE_EVENT_TOPIC = "OutageEvent"
SCALING_ACTION_TOPIC = "ScalingAction"


# Command-line tools paths
KAFKA_BIN_DIR = os.path.join(KAFKA_HOME, "bin")
ZOOKEEPER_START_SCRIPT = os.path.join(KAFKA_BIN_DIR, "zookeeper-server-start.sh")
KAFKA_START_SCRIPT = os.path.join(KAFKA_BIN_DIR, "kafka-server-start.sh")
KAFKA_TOPICS_SCRIPT = os.path.join(KAFKA_BIN_DIR, "kafka-topics.sh")
