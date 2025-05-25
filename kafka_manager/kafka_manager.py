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
    KAFKA_TOPICS_SCRIPT, KAFKA_BROKER_PORT, ZOOKEEPER_PORT
)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def run_command(cmd, cwd=None, check_success=True, capture_output=False):
    """Helper to run shell commands."""
    logger.debug(f"Running command: {' '.join(cmd)}")
    try:
        result = subprocess.run(cmd, cwd=cwd, check=check_success, capture_output=capture_output, text=True)
        if capture_output:
            return result.stdout.strip()
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"Command failed: {' '.join(cmd)}")
        if capture_output:
            logger.error(f"Stderr: {e.stderr}")
        if check_success:
            raise
        return False
    except FileNotFoundError:
        logger.error(f"Command not found: {cmd[0]}. Make sure it's in your PATH or correctly specified.")
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
    with open(ZOOKEEPER_PROPERTIES_PATH, 'r+') as f:
        content = f.read()
        # Ensure dataDir is set
        if f"dataDir={ZOOKEEPER_DATA_DIR}" not in content:
            content = content.replace("dataDir=/tmp/zookeeper", f"dataDir={ZOOKEEPER_DATA_DIR}")
            f.seek(0)
            f.write(content)
            f.truncate()
            logger.info(f"Set Zookeeper dataDir to {ZOOKEEPER_DATA_DIR}")
    os.makedirs(ZOOKEEPER_DATA_DIR, exist_ok=True)
    logger.info(f"Ensured Zookeeper data directory exists: {ZOOKEEPER_DATA_DIR}")

    logger.info("Configuring Kafka server properties...")
    with open(SERVER_PROPERTIES_PATH, 'r+') as f:
        content = f.read()
        # Ensure log.dirs is set
        if f"log.dirs={KAFKA_LOG_DIRS}" not in content:
            content = content.replace("log.dirs=/tmp/kafka-logs", f"log.dirs={KAFKA_LOG_DIRS}")
            f.seek(0)
            f.write(content)
            f.truncate()
            logger.info(f"Set Kafka log.dirs to {KAFKA_LOG_DIRS}")
    os.makedirs(KAFKA_LOG_DIRS, exist_ok=True)
    logger.info(f"Ensured Kafka log directory exists: {KAFKA_LOG_DIRS}")

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
    # Using subprocess.Popen to run in the background
    # This is a simplified approach; for production, use systemd services.
    process = subprocess.Popen([ZOOKEEPER_START_SCRIPT, ZOOKEEPER_PROPERTIES_PATH],
                               stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    logger.info(f"Zookeeper started with PID: {process.pid}. Check logs for status.")
    # Store PID for later termination if needed (more complex for robust management)
    return process

def start_kafka_broker():
    """Starts the Kafka broker."""
    logger.info("Starting Kafka broker...")
    # Using subprocess.Popen to run in the background
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
        "--bootstrap-server", f"localhost:{KAFKA_BROKER_PORT}",
        "--partitions", str(partitions),
        "--replication-factor", str(replication_factor)
    ]
    run_command(cmd)
    logger.info(f"Topic '{topic_name}' created successfully.")

def list_topics():
    """Lists all Kafka topics."""
    logger.info("Listing Kafka topics...")
    cmd = [
        KAFKA_TOPICS_SCRIPT, "--list",
        "--bootstrap-server", f"localhost:{KAFKA_BROKER_PORT}"
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
        "--bootstrap-server", f"localhost:{KAFKA_BROKER_PORT}"
    ]
    run_command(cmd)
    logger.info(f"Topic '{topic_name}' deletion requested. (Note: Topic deletion might require 'delete.topic.enable=true' in server.properties and broker restart)")

def main():
    parser = argparse.ArgumentParser(description="Kafka Installation and Management Tool.")
    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # Install command
    install_parser = subparsers.add_parser("install", help="Download, extract, and configure Kafka.")

    # Start command
    start_parser = subparsers.add_parser("start", help="Start Zookeeper and Kafka broker.")

    # Stop command
    stop_parser = subparsers.add_parser("stop", help="Stop Zookeeper and Kafka broker (requires manual PID management for robust shutdown).")

    # Topic management commands
    create_topic_parser = subparsers.add_parser("create-topic", help="Create a new Kafka topic.")
    create_topic_parser.add_argument("topic_name", help="Name of the topic to create.")
    create_topic_parser.add_argument("--partitions", type=int, default=1, help="Number of partitions.")
    create_topic_parser.add_argument("--replication-factor", type=int, default=1, help="Replication factor.")

    list_topics_parser = subparsers.add_parser("list-topics", help="List all Kafka topics.")

    delete_topic_parser = subparsers.add_parser("delete-topic", help="Delete a Kafka topic.")
    delete_topic_parser.add_argument("topic_name", help="Name of the topic to delete.")

    args = parser.parse_args()

    # Process management (start/stop) is simplified for demonstration.
    # For robust background services, consider systemd or process managers.
    zookeeper_process = None
    kafka_process = None

    if args.command == "install":
        install_kafka()
    elif args.command == "start":
        logger.warning("Starting Zookeeper and Kafka in foreground. For background, use systemd services.")
        try:
            zookeeper_process = start_zookeeper()
            time.sleep(5) # Give Zookeeper time to start
            kafka_process = start_kafka_broker()
            logger.info("Zookeeper and Kafka started. Press Ctrl+C to stop (may not be graceful).")
            # Keep the main thread alive to keep subprocesses running
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
        # Example: pkill -f zookeeper-server-start.sh
        # Example: pkill -f kafka-server-start.sh
    elif args.command == "create-topic":
        create_topic(args.topic_name, args.partitions, args.replication_factor)
    elif args.command == "list-topics":
        list_topics()
    elif args.command == "delete-topic":
        delete_topic(args.topic_name)
    else:
        parser.print_help()

if __name__ == "__main__":
    main()
