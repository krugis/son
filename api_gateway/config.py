import os
import secrets
import logging

# Database configuration
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///gateway.db")

# JWT Security Configuration
def get_secret_key():
    """Get JWT secret key with proper validation"""
    secret = os.getenv("SECRET_KEY")
    
    if not secret:
        # In development, generate a random key
        if os.getenv("FLASK_ENV") == "development":
            secret = secrets.token_urlsafe(64)
            logging.warning("Using generated SECRET_KEY for development. Set SECRET_KEY environment variable for production.")
        else:
            raise ValueError("SECRET_KEY environment variable must be set in production")
    
    # Validate secret strength
    if len(secret) < 32:
        raise ValueError("SECRET_KEY must be at least 32 characters long")
    
    return secret

SECRET_KEY = get_secret_key()

# Token expiration settings
ACCESS_TOKEN_EXPIRE_MINUTES = int(os.getenv("ACCESS_TOKEN_EXPIRE_MINUTES", "15"))  # 15 minutes
REFRESH_TOKEN_EXPIRE_DAYS = int(os.getenv("REFRESH_TOKEN_EXPIRE_DAYS", "7"))      # 7 days

# API Security Settings
RATE_LIMIT_PER_MINUTE = int(os.getenv("RATE_LIMIT_PER_MINUTE", "60"))
MAX_CONTENT_LENGTH = int(os.getenv("MAX_CONTENT_LENGTH", "16777216"))  # 16MB

# CORS Settings
ALLOWED_ORIGINS = os.getenv("ALLOWED_ORIGINS", "*").split(",")
ALLOWED_METHODS = ["GET", "POST", "PATCH", "DELETE", "OPTIONS"]
ALLOWED_HEADERS = ["Content-Type", "Authorization"]

# User authentication settings (now driven by DB, removed VALID_USERS and REQUIRE_USER_AUTH)
# REQUIRE_USER_AUTH is implicitly True if users are expected in DB
# VALID_USERS is no longer needed as users are in the database

# Logging configuration
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL.upper()),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
