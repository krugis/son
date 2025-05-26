import os
import subprocess
import shutil
import time
import logging
import argparse

from config import (
    KAFKA_DOWNLOAD_URL, KAFKA_ARCHIVE, INSTALL_DIR, KAFKA_HOME,
    ZOOKEEPER_PROPERTIES_PATH, ZOOKEEPER_DATA_DIR, ZOOKEEPER_START_SCRIPT,
    SERVER_PROPERTIES_PATH, KAFKA_LOG_DIRS, KAFKA_START_SCRIPT,
    KAFKA_TOPICS_SCRIPT, KAFKA_BROKER_PORT, ZOOKEEPER_PORT,
    KAFKA_BOOTSTRAP_SERVERS, OUTAGE_EVENT_TOPIC, SCALING_ACTION_TOPIC, RAW_TELEMETRY_TOPIC, STRUCTURED_TELEMETRY # New imports
)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def run_command(cmd, cwd=None, check_success=True, capture_output=False, timeout=30):
    """Helper to run shell commands."""
    logger.debug(f"Running command: {' '.join(cmd)}")
    try:
        result = subprocess.run(cmd, cwd=cwd, check=check_success, capture_output=capture_output, text=True, timeout=timeout)
        if capture_output:
            return result.stdout.strip()
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"Command failed: {' '.join(cmd)}")
        if capture_output:
            logger.error(f"Stderr: {e.stderr}")
        logger.error(f"Error output: {e.stdout}") # Also print stdout on error
        if check_success:
            raise
        return False
    except FileNotFoundError:
        logger.error(f"Command not found: {cmd[0]}. Make sure it's in your PATH or correctly specified.")
        if check_success:
            raise
        return False
    except subprocess.TimeoutExpired:
        logger.error(f"Command timed out after {timeout} seconds: {' '.join(cmd)}")
        if check_success:
            raise
        return False
    except Exception as e:
        logger.error(f"An unexpected error occurred while running command: {e}")
        if check_success:
            raise
        return False

def download_and_extract_kafka():
    """Downloads and extracts Kafka binaries."""
    if os.path.exists(KAFKA_HOME):
        logger.info(f"Kafka already extracted at {KAFKA_HOME}. Skipping download and extraction.")
        return

    logger.info(f"Creating installation directory: {INSTALL_DIR}")
    os.makedirs(INSTALL_DIR, exist_ok=True)

    archive_path = os.path.join(INSTALL_DIR, KAFKA_ARCHIVE)
    
    logger.info(f"Downloading Kafka from {KAFKA_DOWNLOAD_URL} to {archive_path}")
    try:
        run_command(["wget", "-O", archive_path, KAFKA_DOWNLOAD_URL])
    except Exception as e:
        logger.error(f"Failed to download Kafka: {e}")
        logger.error("Please ensure 'wget' is installed and the download URL is correct.")
        raise

    logger.info(f"Extracting Kafka to {INSTALL_DIR}")
    run_command(["tar", "-xzf", archive_path, "-C", INSTALL_DIR])
    logger.info("Kafka extraction complete.")

    # Clean up archive
    # os.remove(archive_path)
    # logger.info(f"Removed Kafka archive: {archive_path}")

def configure_kafka():
    """Configures Zookeeper and Kafka server properties."""
    logger.info("Configuring Zookeeper properties...")
    # Ensure Zookeeper data directory exists
    os.makedirs(ZOOKEEPER_DATA_DIR, exist_ok=True)
    
    # Safely modify zookeeper.properties
    with open(ZOOKEEPER_PROPERTIES_PATH, 'r+') as f:
        content = f.read()
        if "dataDir=" not in content:
            content += f"\ndataDir={ZOOKEEPER_DATA_DIR}"
        else:
            content = content.replace("dataDir=/tmp/zookeeper", f"dataDir={ZOOKEEPER_DATA_DIR}")
        
        # Add 4lw.commands.whitelist for ruok check
        if "4lw.commands.whitelist=" not in content:
            content += "\n4lw.commands.whitelist=ruok,stat,mntr,conf,isro"
        
        f.seek(0)
        f.write(content)
        f.truncate()
        logger.info(f"Set Zookeeper dataDir to {ZOOKEEPER_DATA_DIR} and whitelisted commands.")


    logger.info("Configuring Kafka server properties...")
    # Ensure Kafka log directory exists
    os.makedirs(KAFKA_LOG_DIRS, exist_ok=True)
    
    # Safely modify server.properties
    with open(SERVER_PROPERTIES_PATH, 'r+') as f:
        content = f.read()
        if "log.dirs=" not in content:
            content += f"\nlog.dirs={KAFKA_LOG_DIRS}"
        else:
            content = content.replace("log.dirs=/tmp/kafka-logs", f"log.dirs={KAFKA_LOG_DIRS}")
        
        # Enable topic deletion (important for 'delete-topic' command)
        if "delete.topic.enable=" not in content:
            content += "\ndelete.topic.enable=true"
        else:
            content = content.replace("delete.topic.enable=false", "delete.topic.enable=true")
        
        f.seek(0)
        f.write(content)
        f.truncate()
        logger.info(f"Set Kafka log.dirs to {KAFKA_LOG_DIRS} and enabled topic deletion.")

    logger.info("Kafka configuration complete.")

def install_kafka():
    """Performs a full Kafka installation."""
    logger.info("Starting Kafka installation process...")
    download_and_extract_kafka()
    configure_kafka()
    logger.info("Kafka installation finished.")

def start_zookeeper():
    """Starts the Zookeeper server."""
    logger.info("Starting Zookeeper server...")
    process = subprocess.Popen([ZOOKEEPER_START_SCRIPT, ZOOKEEPER_PROPERTIES_PATH],
                               stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    logger.info(f"Zookeeper started with PID: {process.pid}. Check logs for status.")
    return process

def start_kafka_broker():
    """Starts the Kafka broker."""
    logger.info("Starting Kafka broker...")
    process = subprocess.Popen([KAFKA_START_SCRIPT, SERVER_PROPERTIES_PATH],
                               stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    logger.info(f"Kafka broker started with PID: {process.pid}. Check logs for status.")
    return process

def stop_process(process, name):
    """Attempts to terminate a process."""
    if process and process.poll() is None: # Check if process is still running
        logger.info(f"Attempting to stop {name} (PID: {process.pid})...")
        process.terminate() # Send SIGTERM
        try:
            process.wait(timeout=10) # Wait for process to terminate
            logger.info(f"{name} stopped.")
        except subprocess.TimeoutExpired:
            logger.warning(f"{name} did not terminate gracefully, killing...")
            process.kill() # Send SIGKILL
            process.wait()
            logger.info(f"{name} killed.")
    else:
        logger.info(f"{name} is not running or already stopped.")

def create_topic(topic_name, partitions=1, replication_factor=1):
    """Creates a Kafka topic."""
    logger.info(f"Creating Kafka topic: {topic_name}")
    cmd = [
        KAFKA_TOPICS_SCRIPT, "--create",
        "--topic", topic_name,
        "--bootstrap-server", KAFKA_BOOTSTRAP_SERVERS,
        "--partitions", str(partitions),
        "--replication-factor", str(replication_factor)
    ]
    try:
        run_command(cmd)
        logger.info(f"Topic '{topic_name}' created successfully.")
    except subprocess.CalledProcessError as e:
        if "Topic already exists" in e.stderr:
            logger.info(f"Topic '{topic_name}' already exists. Skipping creation.")
        else:
            raise # Re-raise if it's another error

def list_topics():
    """Lists all Kafka topics."""
    logger.info("Listing Kafka topics...")
    cmd = [
        KAFKA_TOPICS_SCRIPT, "--list",
        "--bootstrap-server", KAFKA_BOOTSTRAP_SERVERS
    ]
    topics = run_command(cmd, capture_output=True)
    logger.info("Current Kafka topics:\n" + topics)
    return topics

def delete_topic(topic_name):
    """Deletes a Kafka topic."""
    logger.info(f"Deleting Kafka topic: {topic_name}")
    cmd = [
        KAFKA_TOPICS_SCRIPT, "--delete",
        "--topic", topic_name,
        "--bootstrap-server", KAFKA_BOOTSTRAP_SERVERS
    ]
    run_command(cmd)
    logger.info(f"Topic '{topic_name}' deletion requested. (Note: Topic deletion requires 'delete.topic.enable=true' in server.properties and broker restart)")

def check_kafka_broker_ready():
    """Checks if the Kafka broker is ready to accept connections."""
    logger.info(f"Checking if Kafka broker is ready at {KAFKA_BOOTSTRAP_SERVERS}...")
    try:
        # A simple check: try to list topics. If it works, broker is likely ready.
        run_command([KAFKA_TOPICS_SCRIPT, "--list", "--bootstrap-server", KAFKA_BOOTSTRAP_SERVERS],
                    check_success=True, capture_output=True, timeout=5)
        logger.info("Kafka broker is ready.")
        return True
    except Exception:
        logger.warning("Kafka broker not ready yet.")
        return False

def ensure_kafka_topics_exist_managed():
    """Ensures that required Kafka topics exist for the Policy Engine."""
    required_topics = [OUTAGE_EVENT_TOPIC, SCALING_ACTION_TOPIC, RAW_TELEMETRY_TOPIC, STRUCTURED_TELEMETRY]
    
    logger.info("Ensuring required Kafka topics exist...")
    
    # Retry loop for Kafka broker availability
    max_retries = 10
    retry_delay_sec = 5
    
    for attempt in range(max_retries):
        if check_kafka_broker_ready():
            for topic_name in required_topics:
                create_topic(topic_name) # Uses the create_topic function which handles 'already exists'
            logger.info("All required Kafka topics ensured.")
            return True # Success
        else:
            logger.warning(f"Kafka broker not ready (attempt {attempt + 1}/{max_retries}). Retrying in {retry_delay_sec}s...")
            time.sleep(retry_delay_sec)
            
    logger.error("Failed to ensure Kafka topics after multiple retries. Please ensure Kafka broker is running and accessible.")
    sys.exit(1) # Exit if Kafka topic setup fails

def main():
    parser = argparse.ArgumentParser(description="Kafka Installation and Management Tool.")
    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # Install command
    install_parser = subparsers.add_parser("install", help="Download, extract, and configure Kafka.")

    # Start command
    start_parser = subparsers.add_parser("start", help="Start Zookeeper and Kafka broker (in foreground).")

    # Stop command
    stop_parser = subparsers.add_parser("stop", help="Stop Zookeeper and Kafka broker (best-effort, requires manual PID management for robust shutdown).")

    # Topic management commands
    create_topic_parser = subparsers.add_parser("create-topic", help="Create a new Kafka topic.")
    create_topic_parser.add_argument("topic_name", help="Name of the topic to create.")
    create_topic_parser.add_argument("--partitions", type=int, default=1, help="Number of partitions.")
    create_topic_parser.add_argument("--replication-factor", type=int, default=1, help="Replication factor.")

    list_topics_parser = subparsers.add_parser("list-topics", help="List all Kafka topics.")

    delete_topic_parser = subparsers.add_parser("delete-topic", help="Delete a Kafka topic.")
    delete_topic_parser.add_argument("topic_name", help="Name of the topic to delete.")

    # New: Check topics command
    check_topics_parser = subparsers.add_parser("check-topics", help="Ensure all required Policy Engine topics exist.")

    args = parser.parse_args()

    zookeeper_process = None
    kafka_process = None

    if args.command == "install":
        install_kafka()
        # After installation, we can prompt or automatically create topics
        logger.info("Kafka installed. You can now run 'start' command.")
    elif args.command == "start":
        logger.warning("Starting Zookeeper and Kafka in foreground. For background, use systemd services.")
        try:
            zookeeper_process = start_zookeeper()
            time.sleep(5) # Give Zookeeper time to start
            kafka_process = start_kafka_broker()
            time.sleep(10) # Give Kafka broker time to fully initialize
            ensure_kafka_topics_exist_managed() # Ensure topics after starting Kafka
            logger.info("Zookeeper and Kafka started, and topics ensured. Press Ctrl+C to stop.")
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            logger.info("Ctrl+C detected. Attempting to stop processes.")
        finally:
            stop_process(kafka_process, "Kafka Broker")
            stop_process(zookeeper_process, "Zookeeper")
    elif args.command == "stop":
        logger.warning("Stopping processes via this script is best-effort and relies on PIDs. For robust shutdown, use systemd services or manual 'kill' based on PIDs.")
        logger.info("Please manually identify and kill Zookeeper and Kafka processes if they were started in the background without PID tracking.")
    elif args.command == "create-topic":
        ensure_kafka_topics_exist_managed() # Ensure broker is ready
        create_topic(args.topic_name, args.partitions, args.replication_factor)
    elif args.command == "list-topics":
        ensure_kafka_topics_exist_managed() # Ensure broker is ready
        list_topics()
    elif args.command == "delete-topic":
        ensure_kafka_topics_exist_managed() # Ensure broker is ready
        delete_topic(args.topic_name)
    elif args.command == "check-topics": # New command
        ensure_kafka_topics_exist_managed()
    else:
        parser.print_help()

if __name__ == "__main__":
    main()
