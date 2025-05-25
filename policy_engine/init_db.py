import os
import sys
import logging
from sqlalchemy import inspect
from db import engine, Base, SessionLocal
from models import Policy  # Ensure Policy model is imported so Base knows about it
from config import DEFAULT_POLICIES # For loading initial data

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

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

if __name__ == "__main__":
    check_and_create_tables()
    logger.info("Database initialization complete.")
