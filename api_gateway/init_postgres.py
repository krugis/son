import os
import psycopg2
from psycopg2 import sql

def create_database_if_not_exists(admin_conn, db_name):
    admin_conn.autocommit = True
    with admin_conn.cursor() as cursor:
        cursor.execute(f"SELECT 1 FROM pg_database WHERE datname = %s", (db_name,))
        exists = cursor.fetchone()
        if not exists:
            print(f"🔧 Creating database '{db_name}'...")
            cursor.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(db_name)))
        else:
            print(f"✅ Database '{db_name}' already exists.")

def create_tables(conn):
    create_routes_table = """
    CREATE TABLE IF NOT EXISTS routes (
        id SERIAL PRIMARY KEY,
        path TEXT NOT NULL,
        method TEXT NOT NULL,
        service_url TEXT NOT NULL
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
        print("✅ Tables created in 'son_gateway'.")

def init_postgres_db():
    db_user = "postgres"
    db_password = os.getenv("DATABASE_PWD", "1999!rTrT1999")  # fallback value
    db_host = "localhost"
    db_port = 5432
    target_db = "son_gateway"

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

    except Exception as e:
        print("❌ Error initializing database:", e)

if __name__ == "__main__":
    init_postgres_db()
