import os
from flask import Flask, request, jsonify
from db import SessionLocal, engine
from models import Base, Route, RequestLog
from auth import token_required, generate_token
from sqlalchemy.exc import NoResultFound
import requests

app = Flask(__name__)
# Only create tables if explicitly requested
if os.getenv("CREATE_TABLES", "false").lower() == "true":
    Base.metadata.create_all(bind=engine)

@app.route("/token", methods=["POST"])
def issue_token():
    user = request.json.get("user")
    token = generate_token(user)
    return jsonify({"token": token})

@app.route("/routes", methods=["POST"])
@token_required
def add_route():
    db = SessionLocal()
    data = request.json
    route = Route(path=data["path"], method=data.get("method", "GET"), service_url=data["service_url"])
    db.add(route)
    db.commit()
    db.close()
    return jsonify({"status": "route added"})

@app.route("/<path:path>", methods=["GET", "POST", "PATCH"])
@token_required
def route_request(path):
    full_path = f"/{path}"
    db = SessionLocal()
    try:
        route = db.query(Route).filter_by(path=full_path, method=request.method).one()
        if request.method == "POST":
            r = requests.post(route.service_url, json=request.json)
        elif request.method == "PATCH":
            r = requests.patch(route.service_url, json=request.json)
        else:
            r = requests.get(route.service_url, params=request.args)

        # Log request
        log = RequestLog(path=full_path, method=request.method, user=request.user, status=r.status_code)
        db.add(log)
        db.commit()
        return jsonify(r.json()), r.status_code
    except NoResultFound:
        return jsonify({"error": "Route not found"}), 404
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        db.close()
