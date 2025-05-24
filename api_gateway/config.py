import os

# PostgreSQL configuration
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///gateway.db")
SECRET_KEY = os.getenv("SECRET_KEY", "super-secret-jwt-key")
