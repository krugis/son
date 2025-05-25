import os
import logging
from flask import Flask, request, jsonify
from flask_cors import CORS # Assuming you'll use CORS for frontend interaction
from policies import get_all_policies, add_policy, update_policy, delete_policy, load_default_policies
from kafka_consumer import start_consumer_thread, stop_consumer_thread
from kafka_producer import close_producer
from config import ALLOWED_ORIGINS, ALLOWED_METHODS, ALLOWED_HEADERS, DEFAULT_POLICIES, FLASK_DEBUG, FLASK_PORT

app = Flask(__name__)
app.debug = FLASK_DEBUG

# Configure CORS
CORS(app,
     origins=ALLOWED_ORIGINS,
     methods=ALLOWED_METHODS,
     allow_headers=ALLOWED_HEADERS,
     supports_credentials=True)

# Load default policies on startup
load_default_policies(DEFAULT_POLICIES)

@app.route("/policies", methods=["GET"])
def list_policies():
    """List all configured policies."""
    return jsonify(get_all_policies()), 200

@app.route("/policies", methods=["POST"])
def create_policy():
    """Add a new policy."""
    data = request.get_json()
    if not data:
        return jsonify({"error": "Request body required"}), 400
    try:
        new_policy = add_policy(data)
        return jsonify(new_policy), 201
    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    except Exception as e:
        logging.error(f"Error creating policy: {e}")
        return jsonify({"error": "Failed to create policy"}), 500

@app.route("/policies/<string:policy_name>", methods=["PUT"])
def update_existing_policy(policy_name):
    """Update an existing policy."""
    data = request.get_json()
    if not data:
        return jsonify({"error": "Request body required"}), 400
    try:
        updated = update_policy(policy_name, data)
        return jsonify(updated), 200
    except ValueError as e:
        return jsonify({"error": str(e)}), 404
    except Exception as e:
        logging.error(f"Error updating policy '{policy_name}': {e}")
        return jsonify({"error": "Failed to update policy"}), 500

@app.route("/policies/<string:policy_name>", methods=["DELETE"])
def delete_existing_policy(policy_name):
    """Delete a policy."""
    try:
        delete_policy(policy_name)
        return jsonify({"message": f"Policy '{policy_name}' deleted"}), 204
    except ValueError as e:
        return jsonify({"error": str(e)}), 404
    except Exception as e:
        logging.error(f"Error deleting policy '{policy_name}': {e}")
        return jsonify({"error": "Failed to delete policy"}), 500

@app.route("/health", methods=["GET"])
def health_check():
    """Health check endpoint"""
    return jsonify({
        "status": "healthy",
        "service": "Policy Engine",
        "version": "1.0.0"
    }), 200

if __name__ == "__main__":
    logging.info("Starting Policy Engine application...")
    start_consumer_thread()
    try:
        app.run(host="0.0.0.0", port=FLASK_PORT, debug=FLASK_DEBUG, use_reloader=False)
    finally:
        logging.info("Shutting down Policy Engine...")
        stop_consumer_thread()
        close_producer()
