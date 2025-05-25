import os
import psycopg2
from psycopg2 import sql
import requests
import time
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def create_database_if_not_exists(admin_conn, db_name):
    admin_conn.autocommit = True
    with admin_conn.cursor() as cursor:
        cursor.execute(f"SELECT 1 FROM pg_database WHERE datname = %s", (db_name,))
        exists = cursor.fetchone()
        if not exists:
            logger.info(f"🔧 Creating database '{db_name}'...")
            cursor.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(db_name)))
        else:
            logger.info(f"✅ Database '{db_name}' already exists.")

def create_tables(conn):
    create_routes_table = """
    CREATE TABLE IF NOT EXISTS routes (
        id SERIAL PRIMARY KEY,
        path TEXT NOT NULL,
        method TEXT NOT NULL,
        service_url TEXT NOT NULL,
        UNIQUE (path, method) -- Ensure unique paths for method
    );
    """

    create_logs_table = """
    CREATE TABLE IF NOT EXISTS request_logs (
        id SERIAL PRIMARY KEY,
        method TEXT NOT NULL,
        path TEXT NOT NULL,
        status_code INTEGER NOT NULL,
        user_identity TEXT,
        timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """

    with conn.cursor() as cursor:
        cursor.execute(create_routes_table)
        cursor.execute(create_logs_table)
        conn.commit()
        logger.info("✅ Tables created or ensured in 'son_gateway'.")

def get_api_gateway_token(api_gateway_url, username, password):
    """Logs into the API Gateway and returns an access token."""
    login_url = f"{api_gateway_url}/auth/login"
    payload = {"username": username, "password": password}
    
    logger.info(f"Attempting to get token from {login_url} for user '{username}'...")
    try:
        response = requests.post(login_url, json=payload, timeout=10)
        response.raise_for_status() # Raise an exception for HTTP errors
        token_data = response.json()
        access_token = token_data.get("access_token")
        if access_token:
            logger.info("Successfully obtained API Gateway access token.")
            return access_token
        else:
            logger.error(f"Login successful but no access_token in response: {token_data}")
            return None
    except requests.exceptions.ConnectionError:
        logger.error(f"API Gateway not reachable at {api_gateway_url}. Please ensure it's running.")
        return None
    except requests.exceptions.HTTPError as e:
        logger.error(f"HTTP error during token request: {e.response.status_code} - {e.response.text}")
        return None
    except Exception as e:
        logger.error(f"Error getting API Gateway token: {e}")
        return None

def add_route_to_api_gateway(api_gateway_url, token, path, method, service_url):
    """Adds a single route to the API Gateway if it doesn't already exist."""
    routes_url = f"{api_gateway_url}/routes"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {token}"
    }
    payload = {
        "path": path,
        "method": method,
        "service_url": service_url
    }

    logger.info(f"Attempting to add route: {method} {path} -> {service_url}")
    try:
        response = requests.post(routes_url, headers=headers, json=payload, timeout=10)
        if response.status_code == 409: # Conflict - route already exists
            logger.info(f"Route {method} {path} already exists. Skipping.")
            return True
        response.raise_for_status() # Raise an exception for other HTTP errors
        logger.info(f"Successfully added route {method} {path}.")
        return True
    except requests.exceptions.HTTPError as e:
        logger.error(f"Failed to add route {method} {path}. HTTP error: {e.response.status_code} - {e.response.text}")
        return False
    except Exception as e:
        logger.error(f"Error adding route {method} {path}: {e}")
        return False

def configure_policy_engine_routes(api_gateway_url, api_gateway_user, api_gateway_password, policy_engine_url):
    """Configures API Gateway routes for the Policy Engine microservice."""
    logger.info("Configuring API Gateway routes for Policy Engine...")

    # Retry getting token as API Gateway might be starting up
    token = None
    for i in range(5): # Try up to 5 times with delay
        token = get_api_gateway_token(api_gateway_url, api_gateway_user, api_gateway_password)
        if token:
            break
        logger.warning(f"Failed to get API Gateway token, retrying in {2**i} seconds...")
        time.sleep(2**i) # Exponential backoff
    
    if not token:
        logger.error("Could not obtain API Gateway token. Cannot configure Policy Engine routes.")
        return

    routes_to_add = [
        {"path": "/policies", "method": "GET", "service_url": f"{policy_engine_url}/policies"},
        {"path": "/policies", "method": "POST", "service_url": f"{policy_engine_url}/policies"},
        # For dynamic paths like /policies/<policy_name>, the API Gateway's /<path:path>
        # handler will correctly forward the full path.
        {"path": "/policies/", "method": "PUT", "service_url": f"{policy_engine_url}/policies/"},
        {"path": "/policies/", "method": "DELETE", "service_url": f"{policy_engine_url}/policies/"},
        {"path": "/policy-engine/health", "method": "GET", "service_url": f"{policy_engine_url}/health"}
    ]

    all_routes_added = True
    for route in routes_to_add:
        success = add_route_to_api_gateway(
            api_gateway_url, token, route["path"], route["method"], route["service_url"]
        )
        if not success:
            all_routes_added = False
            # Don't exit immediately, try to add other routes

    if all_routes_added:
        logger.info("All Policy Engine routes successfully configured in API Gateway.")
    else:
        logger.warning("Some Policy Engine routes could not be added to API Gateway.")


def init_postgres_db():
    db_user = "postgres"
    db_password = os.getenv("DATABASE_PWD", "1999!rTrT1999")  # fallback value
    db_host = "localhost"
    db_port = 5432
    target_db = "son_gateway"

    api_gateway_url = os.getenv("API_GATEWAY_URL", "http://localhost:5000")
    api_gateway_user = os.getenv("API_GATEWAY_USER", "admin") # Default admin user
    api_gateway_password = os.getenv("API_GATEWAY_PASSWORD", "adminpass") # Default admin password (for init only)
    policy_engine_url = os.getenv("POLICY_ENGINE_URL", "http://localhost:5001")


    try:
        # Connect to default 'postgres' DB to check/create target DB
        admin_conn = psycopg2.connect(
            dbname="postgres",
            user=db_user,
            password=db_password,
            host=db_host,
            port=db_port
        )
        create_database_if_not_exists(admin_conn, target_db)
        admin_conn.close()

        # Connect to target DB and create tables
        target_conn = psycopg2.connect(
            dbname=target_db,
            user=db_user,
            password=db_password,
            host=db_host,
            port=db_port
        )
        create_tables(target_conn)
        target_conn.close()

        # After API Gateway's tables are created, attempt to configure its routes
        logger.info("Waiting for API Gateway to be ready before configuring routes...")
        time.sleep(5) # Give API Gateway a moment to fully start up

        configure_policy_engine_routes(api_gateway_url, api_gateway_user, api_gateway_password, policy_engine_url)

    except Exception as e:
        logger.error(f"❌ Error initializing API Gateway database or configuring routes: {e}")
        sys.exit(1) # Exit if critical initialization fails

if __name__ == "__main__":
    init_postgres_db()
