import os
import sys
import logging
import subprocess # Still needed for subprocess.run in run_shell_command if still using it
import time # Still needed for time.sleep if still using it
from sqlalchemy import inspect
from sqlalchemy.exc import IntegrityError
import psycopg2 # Added import for psycopg2
from psycopg2 import sql # Added import for sql module

from db import engine, Base, SessionLocal
from models import Policy
from config import DEFAULT_POLICIES, POLICY_DB_URL # Only import DEFAULT_POLICIES and POLICY_DB_URL

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# This function might not be strictly needed anymore if Kafka topic creation is fully removed
# but keeping it for completeness in case it's used elsewhere for generic shell commands.
def run_shell_command(cmd, check_success=True, timeout=30):
    """Helper to run shell commands with logging and error handling."""
    logger.info(f"Executing: {' '.join(cmd)}")
    try:
        result = subprocess.run(cmd, check=check_success, capture_output=True, text=True, timeout=timeout)
        if result.stdout:
            logger.debug(f"Stdout: {result.stdout.strip()}")
        if result.stderr:
            logger.debug(f"Stderr: {result.stderr.strip()}")
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"Command failed with exit code {e.returncode}: {' '.join(cmd)}")
        logger.error(f"Stdout: {e.stdout.strip()}")
        logger.error(f"Stderr: {e.stderr.strip()}")
        raise
    except subprocess.TimeoutExpired:
        logger.error(f"Command timed out after {timeout} seconds: {' '.join(cmd)}")
        raise
    except FileNotFoundError:
        logger.error(f"Command not found: {cmd[0]}. Ensure all necessary binaries are in PATH.")
        raise
    except Exception as e:
        logger.error(f"An unexpected error occurred while running command: {e}")
        raise

def create_database_if_not_exists(db_url):
    """Creates the PostgreSQL database if it doesn't exist."""
    # Parse the DATABASE_URL to get admin connection details
    from urllib.parse import urlparse
    parsed_url = urlparse(db_url)
    
    db_user = parsed_url.username if parsed_url.username else os.getenv("DB_USER", "postgres")
    db_password = parsed_url.password if parsed_url.password else os.getenv("DATABASE_PWD", "1999!rTrT1999")
    db_host = parsed_url.hostname if parsed_url.hostname else os.getenv("DB_HOST", "localhost")
    db_port = parsed_url.port if parsed_url.port else int(os.getenv("DB_PORT", 5432))
    target_db_name = parsed_url.path.strip('/') # Extract db name from path

    if not target_db_name:
        logger.error("DATABASE_URL does not specify a database name.")
        sys.exit(1)

    admin_conn = None
    try:
        # Attempt to connect to the default 'postgres' database to create the target_db_name
        admin_conn = psycopg2.connect(
            dbname="postgres",
            user=db_user,
            password=db_password,
            host=db_host,
            port=db_port
        )
        admin_conn.autocommit = True
        with admin_conn.cursor() as cursor:
            cursor.execute(f"SELECT 1 FROM pg_database WHERE datname = %s", (target_db_name,))
            exists = cursor.fetchone()
            if not exists:
                logger.info(f"🔧 Creating database '{target_db_name}'...")
                cursor.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(target_db_name)))
                # Optional: Set owner for the new database if the connecting user is not the owner
                # cursor.execute(sql.SQL("ALTER DATABASE {} OWNER TO {}").format(sql.Identifier(target_db_name), sql.Identifier(db_user)))
            else:
                logger.info(f"✅ Database '{target_db_name}' already exists.")
    except Exception as e:
        logger.error(f"❌ Error connecting to PostgreSQL or creating database '{target_db_name}': {e}")
        logger.error("Please ensure PostgreSQL is running and the user has privileges to create databases.")
        sys.exit(1)
    finally:
        if admin_conn:
            admin_conn.close()

def check_and_create_tables():
    """Checks if tables exist and creates them if not. Loads default policies if table is empty."""
    inspector = inspect(engine)
    required_tables = [Policy.__tablename__]
    existing_tables = inspector.get_table_names()

    tables_missing = False
    for table_name in required_tables:
        if table_name not in existing_tables:
            logger.warning(f"Table '{table_name}' does not exist.")
            tables_missing = True

    if tables_missing:
        logger.info("Attempting to create missing tables...")
        try:
            Base.metadata.create_all(bind=engine)
            logger.info("All tables created successfully.")
            # After creating tables, load default policies
            load_default_policies_into_db(DEFAULT_POLICIES)
        except Exception as e:
            logger.error(f"Error creating tables: {e}")
            sys.exit(1) # Exit if table creation fails
    else:
        logger.info("All required tables already exist.")
        # Only load defaults if the table is empty and already exists
        load_default_policies_into_db(DEFAULT_POLICIES)


def load_default_policies_into_db(default_policies_data):
    """
    Loads default policies into the database if the 'policies' table is empty.
    """
    db = SessionLocal()
    try:
        if db.query(Policy).count() == 0:
            logger.info("No policies found in DB. Loading default policies.")
            for policy_data in default_policies_data:
                try:
                    policy = Policy(
                        name=policy_data["name"],
                        condition=policy_data["condition"],
                        action=policy_data["action"],
                        enabled=policy_data.get("enabled", True)
                    )
                    db.add(policy)
                    db.commit()
                    logger.info(f"Default policy '{policy_data['name']}' loaded.")
                except IntegrityError:
                    db.rollback()
                    logger.warning(f"Default policy '{policy_data['name']}' already exists (integrity error), skipping.")
                except Exception as e:
                    db.rollback()
                    logger.error(f"Error loading default policy '{policy_data['name']}': {e}")
        else:
            logger.info("Policies already exist in DB. Skipping default policy load.")
    finally:
        db.close()

# Removed ensure_kafka_topics_exist as it's now handled by kafka_manager.py

if __name__ == "__main__":
    logger.info("Starting Policy Engine database initialization...")
    # Ensure the database itself exists before trying to connect and create tables within it
    create_database_if_not_exists(POLICY_DB_URL)
    check_and_create_tables() # Handles PostgreSQL tables and default policies
    logger.info("Policy Engine database initialization complete.")
