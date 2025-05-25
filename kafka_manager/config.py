import os

# Kafka Version and Download
KAFKA_VERSION = "3.9.1"  # Check Apache Kafka website for the latest stable version
SCALA_VERSION = "2.13"   # Typically matches Kafka version, check download page
KAFKA_ARCHIVE = f"kafka_{SCALA_VERSION}-{KAFKA_VERSION}.tgz"
KAFKA_DOWNLOAD_URL = f"https://downloads.apache.org/kafka/{KAFKA_VERSION}/{KAFKA_ARCHIVE}"

# Installation Paths
# Base directory where Kafka will be extracted
INSTALL_DIR = os.path.expanduser("~/kafka_install")
KAFKA_HOME = os.path.join(INSTALL_DIR, f"kafka_{SCALA_VERSION}-{KAFKA_VERSION}")

# Zookeeper Configuration
ZOOKEEPER_PROPERTIES_PATH = os.path.join(KAFKA_HOME, "config", "zookeeper.properties")
ZOOKEEPER_PORT = 2181 # Default Zookeeper client port
ZOOKEEPER_DATA_DIR = os.path.join(INSTALL_DIR, "zookeeper-data")

# Kafka Broker Configuration
SERVER_PROPERTIES_PATH = os.path.join(KAFKA_HOME, "config", "server.properties")
KAFKA_BROKER_PORT = 9092 # Default Kafka broker port
KAFKA_LOG_DIRS = os.path.join(INSTALL_DIR, "kafka-logs")

# Command-line tools paths
KAFKA_BIN_DIR = os.path.join(KAFKA_HOME, "bin")
ZOOKEEPER_START_SCRIPT = os.path.join(KAFKA_BIN_DIR, "zookeeper-server-start.sh")
KAFKA_START_SCRIPT = os.path.join(KAFKA_BIN_DIR, "kafka-server-start.sh")
KAFKA_TOPICS_SCRIPT = os.path.join(KAFKA_BIN_DIR, "kafka-topics.sh")
