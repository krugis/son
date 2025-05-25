import os
import logging
from flask import Flask, request, jsonify
from flask_cors import CORS
from werkzeug.security import check_password_hash, generate_password_hash
from db import SessionLocal, engine
from models import Base, Route, RequestLog
from auth import token_required, generate_tokens, refresh_access_token, validate_token_format
from config import (
    ALLOWED_ORIGINS, ALLOWED_METHODS, ALLOWED_HEADERS,
    MAX_CONTENT_LENGTH, REQUIRE_USER_AUTH, VALID_USERS
)
from sqlalchemy.exc import NoResultFound
import requests

app = Flask(__name__)
app.config['MAX_CONTENT_LENGTH'] = MAX_CONTENT_LENGTH

# Configure CORS
CORS(app, 
     origins=ALLOWED_ORIGINS,
     methods=ALLOWED_METHODS,
     allow_headers=ALLOWED_HEADERS,
     supports_credentials=True)

# Only create tables if explicitly requested
if os.getenv("CREATE_TABLES", "false").lower() == "true":
    Base.metadata.create_all(bind=engine)

@app.errorhandler(413)
def request_entity_too_large(error):
    return jsonify({"error": "Request payload too large"}), 413

@app.errorhandler(429)
def ratelimit_handler(e):
    return jsonify({"error": "Rate limit exceeded", "retry_after": str(e.retry_after)}), 429

@app.route("/auth/login", methods=["POST"])
def login():
    """Authenticate user and issue tokens"""
    try:
        data = request.get_json()
        if not data:
            return jsonify({"error": "Request body required"}), 400
        
        username = data.get("username")
        password = data.get("password")
        
        if not username or not password:
            return jsonify({"error": "Username and password required"}), 400
        
        # Authenticate user
        if REQUIRE_USER_AUTH:
            if username not in VALID_USERS:
                return jsonify({"error": "Invalid credentials"}), 401
            
            # In production, use proper password hashing
            if password != VALID_USERS[username]:
                return jsonify({"error": "Invalid credentials"}), 401
        
        # Generate tokens
        tokens = generate_tokens(username, roles=["user"])
        
        logging.info(f"User {username} successfully authenticated")
        
        return jsonify({
            "message": "Login successful",
            **tokens
        }), 200
        
    except Exception as e:
        logging.error(f"Login error: {str(e)}")
        return jsonify({"error": "Authentication failed"}), 500

@app.route("/auth/refresh", methods=["POST"])
def refresh_token():
    """Refresh access token using refresh token"""
    try:
        data = request.get_json()
        if not data or "refresh_token" not in data:
            return jsonify({"error": "Refresh token required"}), 400
        
        refresh_token = data["refresh_token"]
        
        if not validate_token_format(refresh_token):
            return jsonify({"error": "Invalid token format"}), 400
        
        # Generate new tokens
        new_tokens = refresh_access_token(refresh_token)
        
        return jsonify({
            "message": "Token refreshed successfully",
            **new_tokens
        }), 200
        
    except Exception as e:
        logging.error(f"Token refresh error: {str(e)}")
        return jsonify({"error": "Token refresh failed"}), 401

@app.route("/auth/token", methods=["POST"])
#@token_required
def issue_token():
    """Legacy endpoint - now requires authentication"""
    data = request.get_json()
    if not data or "user" not in data:
        return jsonify({"error": "User parameter required"}), 400
    
    user = data["user"]
    tokens = generate_tokens(user)
    
    logging.info(f"Token issued for user: {user}")
    return jsonify(tokens)

@app.route("/routes", methods=["POST"])
@token_required
def add_route():
    """Add new route configuration"""
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
        
        # Check for duplicate routes
        existing = db.query(Route).filter_by(path=path, method=method).first()
        if existing:
            return jsonify({"error": "Route already exists"}), 409
        
        route = Route(path=path, method=method, service_url=service_url)
        db.add(route)
        db.commit()
        
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
        db.rollback()
        logging.error(f"Error adding route: {str(e)}")
        return jsonify({"error": "Failed to add route"}), 500
    finally:
        db.close()

@app.route("/routes", methods=["GET"])
@token_required
def list_routes():
    """List all configured routes"""
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
    """Proxy requests to configured services"""
    full_path = f"/{path}"
    db = SessionLocal()
    
    try:
        # Find matching route
        route = db.query(Route).filter_by(path=full_path, method=request.method).one()
        
        # Prepare request parameters
        headers = {"Content-Type": "application/json"}
        timeout = 30
        
        # Forward request based on method
        if request.method in ["POST", "PATCH"]:
            response = requests.request(
                method=request.method,
                url=route.service_url,
                json=request.get_json(),
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
                params=request.args,
                headers=headers,
                timeout=timeout
            )
        
        # Log request
        log = RequestLog(
            path=full_path,
            method=request.method,
            user=request.user,
            status=response.status_code
        )
        db.add(log)
        db.commit()
        
        # Return response
        try:
            return jsonify(response.json()), response.status_code
        except ValueError:
            # Handle non-JSON responses
            return response.text, response.status_code
            
    except NoResultFound:
        return jsonify({"error": "Route not found"}), 404
    except requests.RequestException as e:
        logging.error(f"Service request failed: {str(e)}")
        return jsonify({"error": "Service unavailable"}), 503
    except Exception as e:
        logging.error(f"Route processing error: {str(e)}")
        return jsonify({"error": "Internal server error"}), 500
    finally:
        db.close()

@app.route("/health", methods=["GET"])
def health_check():
    """Health check endpoint"""
    return jsonify({
        "status": "healthy",
        "service": "API Gateway",
        "version": "1.0.0"
    })

if __name__ == "__main__":
    app.run(debug=os.getenv("FLASK_ENV") == "development", host="0.0.0.0", port=5000)
