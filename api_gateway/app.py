import os
import logging
from flask import Flask, request, jsonify
from flask_cors import CORS
# Removed werkzeug.security imports, now using passlib for hashing
from db import SessionLocal, engine # Import SessionLocal and engine
from models import Base, Route, RequestLog, User # Import all models including User
from auth import token_required, generate_tokens, refresh_access_token, validate_token_format
from config import (
    ALLOWED_ORIGINS, ALLOWED_METHODS, ALLOWED_HEADERS,
    MAX_CONTENT_LENGTH # Removed REQUIRE_USER_AUTH, VALID_USERS as they are now DB-driven
)
from sqlalchemy.exc import NoResultFound
from passlib.hash import bcrypt # Import bcrypt for password hashing
import requests

app = Flask(__name__)
app.config['MAX_CONTENT_LENGTH'] = MAX_CONTENT_LENGTH

# Configure CORS
CORS(app,
     origins=ALLOWED_ORIGINS,
     methods=ALLOWED_METHODS,
     allow_headers=ALLOWED_HEADERS,
     supports_credentials=True)

# Database tables and default users are now handled by init_postgres.py
# Removed: if os.getenv("CREATE_TABLES", "false").lower() == "true": Base.metadata.create_all(bind=engine)

@app.errorhandler(413)
def request_entity_too_large(error):
    """Handler for request payload too large errors."""
    return jsonify({"error": "Request payload too large"}), 413

@app.errorhandler(429)
def ratelimit_handler(e):
    """Handler for rate limit exceeded errors."""
    return jsonify({"error": "Rate limit exceeded", "retry_after": str(e.retry_after)}), 429

@app.route("/auth/login", methods=["POST"])
def login():
    """Authenticate user and issue tokens based on database credentials."""
    db = SessionLocal() # Get a database session
    try:
        data = request.get_json()
        if not data:
            return jsonify({"error": "Request body required"}), 400

        username = data.get("username")
        password = data.get("password")

        if not username or not password:
            return jsonify({"error": "Username and password required"}), 400

        # Authenticate user against database
        user = db.query(User).filter_by(username=username).first()

        # Verify password using bcrypt
        if not user or not bcrypt.verify(password, user.password_hash):
            logging.warning(f"Failed login attempt for user: {username}")
            return jsonify({"error": "Invalid credentials"}), 401

        if not user.is_active:
            logging.warning(f"Inactive user attempted login: {username}")
            return jsonify({"error": "User account is inactive"}), 401

        # Generate tokens with roles fetched from the database
        tokens = generate_tokens(user.username, roles=user.roles)

        logging.info(f"User {username} successfully authenticated")

        return jsonify({
            "message": "Login successful",
            **tokens
        }), 200

    except Exception as e:
        logging.error(f"Login error: {str(e)}")
        return jsonify({"error": "Authentication failed"}), 500
    finally:
        db.close() # Ensure the database session is closed

@app.route("/auth/refresh", methods=["POST"])
def refresh_token():
    """Refresh access token using a valid refresh token."""
    try:
        data = request.get_json()
        if not data or "refresh_token" not in data:
            return jsonify({"error": "Refresh token required"}), 400

        refresh_token = data["refresh_token"]

        if not validate_token_format(refresh_token):
            return jsonify({"error": "Invalid token format"}), 400

        # Generate new tokens (logic handled in auth.py, which fetches roles from DB)
        new_tokens = refresh_access_token(refresh_token)

        return jsonify({
            "message": "Token refreshed successfully",
            **new_tokens
        }), 200

    except Exception as e:
        logging.error(f"Token refresh error: {str(e)}")
        return jsonify({"error": "Token refresh failed"}), 401

@app.route("/auth/token", methods=["POST"])
# This endpoint is NOT decorated with @token_required as it's for initial token issuance.
# It's marked as "Legacy" but still functional for basic user-based token generation.
def issue_token():
    """Legacy endpoint - issues token for a user without password verification.
    Should be used with caution or removed in production."""
    db = SessionLocal() # Get a database session
    try:
        data = request.get_json()
        if not data or "user" not in data:
            return jsonify({"error": "User parameter required"}), 400

        username = data["user"]

        # For this legacy endpoint, we just check if the user exists and is active
        user = db.query(User).filter_by(username=username).first()
        if not user or not user.is_active:
            return jsonify({"error": "User not found or inactive"}), 401

        # Generate tokens with roles fetched from the database
        tokens = generate_tokens(user.username, roles=user.roles)

        logging.info(f"Token issued for user: {username}")
        return jsonify(tokens)
    except Exception as e:
        logging.error(f"Legacy token issuance error: {str(e)}")
        return jsonify({"error": "Token issuance failed"}), 500
    finally:
        db.close() # Ensure the database session is closed

@app.route("/routes", methods=["POST"])
@token_required
def add_route():
    """Add new route configuration to the API Gateway's database."""
    db = SessionLocal()
    try:
        data = request.get_json()
        if not data:
            return jsonify({"error": "Request body required"}), 400

        required_fields = ["path", "service_url"]
        for field in required_fields:
            if field not in data:
                return jsonify({"error": f"Missing required field: {field}"}), 400

        # Validate path format
        path = data["path"]
        if not path.startswith("/"):
            path = "/" + path

        method = data.get("method", "GET").upper()
        service_url = data["service_url"]

        # Check for duplicate routes (database unique constraint will also catch this)
        existing = db.query(Route).filter_by(path=path, method=method).first()
        if existing:
            return jsonify({"error": "Route already exists"}), 409

        route = Route(path=path, method=method, service_url=service_url)
        db.add(route)
        db.commit() # Commit the new route to the database

        logging.info(f"Route added: {method} {path} -> {service_url}")

        return jsonify({
            "status": "Route added successfully",
            "route": {
                "id": route.id,
                "path": path,
                "method": method,
                "service_url": service_url
            }
        }), 201

    except Exception as e:
        db.rollback() # Rollback in case of error
        logging.error(f"Error adding route: {str(e)}")
        return jsonify({"error": "Failed to add route"}), 500
    finally:
        db.close()

@app.route("/routes", methods=["GET"])
@token_required
def list_routes():
    """List all configured routes from the API Gateway's database."""
    db = SessionLocal()
    try:
        routes = db.query(Route).all()
        return jsonify({
            "routes": [
                {
                    "id": route.id,
                    "path": route.path,
                    "method": route.method,
                    "service_url": route.service_url
                }
                for route in routes
            ]
        })
    finally:
        db.close()

@app.route("/<path:path>", methods=["GET", "POST", "PATCH", "DELETE"])
@token_required
def route_request(path):
    """Proxy requests to configured backend services based on routes in the database."""
    full_path = f"/{path}"
    db = SessionLocal()

    try:
        # Find matching route in the database
        route = db.query(Route).filter_by(path=full_path, method=request.method).one()

        # Prepare request parameters for forwarding
        headers = {"Content-Type": "application/json"}
        timeout = 30 # Request timeout in seconds

        # Forward request based on HTTP method
        if request.method in ["POST", "PATCH"]:
            response = requests.request(
                method=request.method,
                url=route.service_url,
                json=request.get_json(), # Forward JSON body
                headers=headers,
                timeout=timeout
            )
        elif request.method == "DELETE":
            response = requests.delete(
                url=route.service_url,
                headers=headers,
                timeout=timeout
            )
        else:  # GET
            response = requests.get(
                url=route.service_url,
                params=request.args, # Forward query parameters
                headers=headers,
                timeout=timeout
            )

        # Log the forwarded request
        log = RequestLog(
            path=full_path,
            method=request.method,
            user=request.user, # User from the JWT token
            status=response.status_code
        )
        db.add(log)
        db.commit() # Commit the log entry

        # Return response from the proxied service
        try:
            return jsonify(response.json()), response.status_code
        except ValueError:
            # Handle non-JSON responses (e.g., plain text, HTML)
            return response.text, response.status_code

    except NoResultFound:
        # If no matching route is found in the database
        return jsonify({"error": "Route not found"}), 404
    except requests.RequestException as e:
        # Handle errors during the request to the backend service
        logging.error(f"Service request failed for {full_path} to {route.service_url}: {str(e)}")
        return jsonify({"error": "Service unavailable"}), 503
    except Exception as e:
        # Catch any other unexpected errors
        logging.error(f"Route processing error for {full_path}: {str(e)}")
        return jsonify({"error": "Internal server error"}), 500
    finally:
        db.close() # Ensure the database session is closed

@app.route("/health", methods=["GET"])
def health_check():
    """Health check endpoint for the API Gateway."""
    return jsonify({
        "status": "healthy",
        "service": "API Gateway",
        "version": "1.0.0"
    })

if __name__ == "__main__":
    # Run the Flask application
    app.run(debug=os.getenv("FLASK_ENV") == "development", host="0.0.0.0", port=5000)
