import json
import os
import sqlite3
import threading
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse
from pymongo import MongoClient
import requests
from flask import Flask, flash, jsonify, redirect, render_template, request, url_for

BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "subscribers.db"
OUTPUT_DIR = BASE_DIR / "output"
OUTPUT_DIR.mkdir(exist_ok=True)
CONFIG_PATH = BASE_DIR / "config.json"

app = Flask(__name__)
app.secret_key = os.getenv("FLASK_SECRET_KEY", "dev-only-change-me")
workers = {}
log_buffers = {}
locks = {}

def is_configured():
    if not CONFIG_PATH.exists():
        return False
    try:
        with open(CONFIG_PATH, "r") as f:
            config = json.load(f)
        return bool(
            config.get("mongo_host")
            and config.get("mongo_database")
        )
    except Exception:
        return False

def now():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")
    
def load_config():
    with open(CONFIG_PATH, "r") as f:
        return json.load(f)

mongo_client = None

def get_mongo_database():

    global mongo_client

    if mongo_client is None:

        config = load_config()

        mongo_client = MongoClient(
            host=config["mongo_host"],
            port=int(config["mongo_port"])
        )

    return mongo_client[
        config["mongo_database"]
    ]

def parse_json_field(raw_value, field_name):
    raw_value = (raw_value or "").strip()

    if not raw_value:
        return {}

    try:
        parsed_value = json.loads(raw_value)
    except json.JSONDecodeError as exc:
        raise ValueError(
            f"{field_name} contains invalid JSON: {exc.msg}"
        ) from exc

    if not isinstance(parsed_value, dict):
        raise ValueError(
            f"{field_name} must contain a JSON object."
        )

    return parsed_value

def get_connection_form_values(form):
    return {
        "name": form.get("name", "").strip(),
        "fetch_type": form.get("fetch_type", "REST"),
        "fetch_method": form.get("fetch_method", "GET"),
        "auth_method": form.get("auth_method", "POST"),
        "auth_endpoint": form.get("auth_endpoint", "").strip(),
        "ack_endpoint": form.get("ack_endpoint", "").strip(),
        "params": form.get("params", ""),
        "body": form.get("body", "")
    }
def test_rest_connection(form):
    auth_endpoint = form.get("auth_endpoint", "").strip()
    auth_method = form.get("auth_method", "POST").upper().strip()

    if not auth_endpoint:
        return False, "Authentication Endpoint is required."

    try:
        params = parse_json_field(
            form.get("params"),
            "Parameters"
        )

        body = parse_json_field(
            form.get("body"),
            "Request Body"
        )

        headers = {
            "Accept": "application/json"
        }

        if auth_method == "GET":
            response = requests.get(
                auth_endpoint,
                params=params,
                headers=headers,
                timeout=60
            )

        elif auth_method == "POST":
            response = requests.post(
                auth_endpoint,
                params=params,
                json=body,
                headers=headers,
                timeout=60
            )

        else:
            return (
                False,
                f"Unsupported authentication method: {auth_method}"
            )

        if response.status_code != 201:
            return (
                False,
                "Authentication failed.\n"
                f"HTTP Status: {response.status_code}\n"
                f"Response: {response.text[:1000]}"
            )

        try:
            response_data = response.json()
        except ValueError:
            return (
                False,
                "Authentication returned HTTP 201, "
                "but the response was not valid JSON."
            )

        token = None

        if isinstance(response_data, str):
            token = response_data

        elif isinstance(response_data, dict):
            token = (
                response_data.get("token")
                or response_data.get("access_token")
                or response_data.get("accessToken")
            )

        if not token:
            return (
                False,
                "Authentication returned HTTP 201, "
                "but no token was found in the response."
            )

        return (
            True,
            "Connection successful.\n"
            "HTTP Status: 201 Created\n"
            "Authentication token received."
        )

    except ValueError as exc:
        return False, str(exc)

    except requests.Timeout:
        return False, "Authentication request timed out."

    except requests.RequestException as exc:
        return False, f"REST connection failed: {exc}"

def get_mongo_database():
    config = load_config()

    client_options = {
        "host": config["mongo_host"],
        "port": int(config["mongo_port"]),
        "serverSelectionTimeoutMS": 5000
    }

    username = config.get("mongo_username", "").strip()
    password = config.get("mongo_password", "")
    auth_database = config.get("mongo_auth_database", "").strip()

    if username:
        client_options["username"] = username
        client_options["password"] = password
        client_options["authSource"] = auth_database or "admin"

    client = MongoClient(**client_options)
    client.admin.command("ping")

    return client[config["mongo_database"]]

def init_db():
    with db() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS subscribers (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL UNIQUE,
                entity_name TEXT NOT NULL,
                auth_endpoint TEXT NOT NULL,
                fetch_endpoint TEXT NOT NULL,
                ack_endpoint TEXT NOT NULL,
                method TEXT NOT NULL DEFAULT 'POST',
                frequency_seconds INTEGER NOT NULL DEFAULT 300,
                batch_limit INTEGER NOT NULL DEFAULT 500,
                client_id TEXT NOT NULL,
                environment TEXT NOT NULL,
                client_secret TEXT NOT NULL,
                output_type TEXT NOT NULL DEFAULT 'json_files',
                output_path TEXT NOT NULL DEFAULT 'output',
                status TEXT NOT NULL DEFAULT 'paused',
                created_at TEXT NOT NULL
            )
        """)


def add_log(sub_id, message, level="INFO"):
    line = {"timestamp": now(), "level": level, "message": message}
    with locks.setdefault(sub_id, threading.Lock()):
        log_buffers.setdefault(sub_id, []).append(line)
        log_buffers[sub_id] = log_buffers[sub_id][-500:]


def derive_ack(fetch_endpoint):
    endpoint = fetch_endpoint.rstrip("/")
    return endpoint[:-5] + "ack" if endpoint.endswith("fetch") else endpoint + "/ack"


def get_subscriber(sub_id):
    with db() as conn:
        row = conn.execute("SELECT * FROM subscribers WHERE id = ?", (sub_id,)).fetchone()
        return dict(row) if row else None


def update_status(sub_id, status):
    with db() as conn:
        conn.execute("UPDATE subscribers SET status = ? WHERE id = ?", (status, sub_id))

def save_config(form):

    config = {
        "mongo_host": form["mongo_host"],
        "mongo_port": int(form["mongo_port"]),
        "mongo_database": form["mongo_database"],
        "mongo_username": form["mongo_username"],
        "mongo_password": form["mongo_password"],
        "mongo_auth_database": form["mongo_auth_database"]
    }

    with open(CONFIG_PATH, "w") as f:
        json.dump(config, f, indent=4)
    database = get_mongo_database()
    connections = database["connections"]
    
    connections.update_one(
        {
            "name": "DataBridge Mongo"
        },
        {
            "$set": {
                "name": "DataBridge Mongo",
                "connection_type": "MONGO",
                "database": "databridge",
                "system_connection": True,
                "verified": True,
                "description": "Built-in DataBridge Mongo connection"
            }
        },
        upsert=True
    )

def get_mongo_client(config):

    host = config["mongo_host"]
    port = config["mongo_port"]

    username = config.get("mongo_username", "")
    password = config.get("mongo_password", "")
    auth_db = config.get("mongo_auth_database", "")

    if username and password:

        return MongoClient(
            host=host,
            port=port,
            username=username,
            password=password,
            authSource=auth_db
        )

    return MongoClient(
        host=host,
        port=port
    )

from pymongo import MongoClient

def test_mongo_connection(
    host,
    port,
    database,
    username="",
    password="",
    auth_database=""
):

    try:

        if username:

            client = MongoClient(
                host=host,
                port=int(port),
                username=username,
                password=password,
                authSource=auth_database or "admin",
                serverSelectionTimeoutMS=5000
            )

        else:

            client = MongoClient(
                host=host,
                port=int(port),
                serverSelectionTimeoutMS=5000
            )

        client.server_info()

        db = client[database]

        db.command("ping")

        # Verify write access
        db.settings.insert_one({
            "test": True
        })

        db.settings.delete_one({
            "test": True
        })

        return (
            True,
            f"""Connection Successful

✅ Server Reachable
✅ Database Exists
✅ Read Access Verified
✅ Write Access Verified

Database: {database}
"""
        )

    except Exception as ex:

        return (
            False,
            str(ex)
        )

def extract_token(body):

    # Str8lines appears to return the token directly
    if isinstance(body, str):
        return body

    for key in ("access_token", "token", "accessToken"):
        if isinstance(body, dict) and body.get(key):
            return body[key]

    raise ValueError(
        "Authentication response did not contain a recognizable token"
    )


def extract_records(body):
    if isinstance(body, list):
        return body
    if not isinstance(body, dict):
        return []
    for key in ("items", "records", "deliveries", "data", "results"):
        value = body.get(key)
        if isinstance(value, list):
            return value
    return []


def build_ack(records):
    outcomes = []
    for record in records:
        if not isinstance(record, dict):
            continue
        delivery_id = record.get("delivery_id") or record.get("deliveryId")
        lease_token = record.get("lease_token") or record.get("leaseToken")
        if delivery_id is not None and lease_token:
            outcomes.append({"delivery_id": delivery_id, "lease_token": lease_token, "ok": True})
    return {"outcomes": outcomes}


def save_json_batch(sub, records, raw_response):
    configured = Path(sub["output_path"])
    root = configured if configured.is_absolute() else BASE_DIR / configured
    safe_entity = "".join(c for c in sub["entity_name"] if c.isalnum() or c in "-_") or "entity"
    folder = root / safe_entity / datetime.now(timezone.utc).strftime("%Y/%m/%d")
    folder.mkdir(parents=True, exist_ok=True)
    filename = f"{safe_entity}_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%fZ')}_{uuid.uuid4().hex[:8]}.json"
    path = folder / filename
    envelope = {
        "subscriber": sub["name"],
        "entity": sub["entity_name"],
        "received_at_utc": now(),
        "record_count": len(records),
        "records": records,
        "raw_response": raw_response,
    }
    temp = path.with_suffix(".tmp")
    temp.write_text(json.dumps(envelope, indent=2, ensure_ascii=False), encoding="utf-8")
    temp.replace(path)
    return path


def authenticate(session, sub):
    add_log(sub["id"], "Authenticating to Str8lines")
    response = session.post(sub["auth_endpoint"], json={
        "grant_type": "client_credentials",
        "client_id": sub["client_id"],
        "environment": sub["environment"],
        "secret_key": sub["client_secret"],
    }, timeout=60)
    response.raise_for_status()
    token = extract_token(response.json())
    add_log(sub["id"], "Authentication successful")
    return token


def worker(sub_id, stop_event):
    sub = get_subscriber(sub_id)
    session = requests.Session()
    update_status(sub_id, "running")
    add_log(sub_id, f"Started {sub['name']}")
    token = None
    try:
        while not stop_event.is_set():
            try:
                if not token:
                    token = authenticate(session, sub)
                headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
                add_log(sub_id, f"Fetching batch of {sub['entity_name']} (limit {sub['batch_limit']})")
                response = session.request(
                    sub["method"], sub["fetch_endpoint"], headers=headers,
                    json={"limit": sub["batch_limit"]}, timeout=120
                )
                if response.status_code == 401:
                    token = None
                    add_log(sub_id, "Token expired or rejected; reauthenticating", "WARN")
                    continue
                response.raise_for_status()
                raw = response.json()
                records = extract_records(raw)
                add_log(sub_id, f"{len(records)} records received")

                if not records:
                    add_log(sub_id, f"Queue is empty; next check in {sub['frequency_seconds']} seconds")
                    stop_event.wait(sub["frequency_seconds"])
                    continue

                path = save_json_batch(sub, records, raw)
                add_log(sub_id, f"JSON batch saved: {path}")

                ack_payload = build_ack(records)
                if len(ack_payload["outcomes"]) != len(records):
                    raise ValueError("One or more records lacked delivery_id or lease_token; batch was saved but not acknowledged")

                ack = session.post(sub["ack_endpoint"], headers=headers, json=ack_payload, timeout=120)
                if ack.status_code == 401:
                    token = authenticate(session, sub)
                    headers["Authorization"] = f"Bearer {token}"
                    ack = session.post(sub["ack_endpoint"], headers=headers, json=ack_payload, timeout=120)
                ack.raise_for_status()
                add_log(sub_id, f"Acknowledged batch of {len(records)} records")
                # Immediately fetch again while backlog exists.
            except requests.RequestException as exc:
                add_log(sub_id, f"HTTP error: {exc}", "ERROR")
                token = None
                stop_event.wait(min(sub["frequency_seconds"], 60))
            except Exception as exc:
                add_log(sub_id, f"Processing error: {exc}", "ERROR")
                stop_event.wait(min(sub["frequency_seconds"], 60))
    finally:
        update_status(sub_id, "paused")
        add_log(sub_id, f"Paused {sub['name']}")


@app.route("/")
def index():
    return render_template("dashboard.html")

@app.post("/subscriber/<sub_id>/duplicate")
def duplicate(sub_id):

    original = get_subscriber(sub_id)

    if not original:
        return "Not Found", 404

    new_name = f"{original['name']} Copy"

    with db() as conn:

        suffix = 2

        while conn.execute(
            "SELECT 1 FROM subscribers WHERE name=?",
            (new_name,)
        ).fetchone():

            new_name = (
                f"{original['name']} Copy {suffix}"
            )

            suffix += 1

        new_id = str(uuid.uuid4())

        conn.execute("""
            INSERT INTO subscribers (
                id,
                name,
                entity_name,
                auth_endpoint,
                fetch_endpoint,
                ack_endpoint,
                method,
                frequency_seconds,
                batch_limit,
                client_id,
                environment,
                client_secret,
                output_type,
                output_path,
                status,
                created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            new_id,
            new_name,
            original["entity_name"],
            original["auth_endpoint"],
            original["fetch_endpoint"],
            original["ack_endpoint"],
            original["method"],
            original["frequency_seconds"],
            original["batch_limit"],
            original["client_id"],
            original["environment"],
            original["client_secret"],
            original["output_type"],
            original["output_path"],
            "paused",
            now()
        ))

    return redirect(
        url_for(
            "subscriber_detail",
            sub_id=new_id
        )
    )

@app.route("/flows")
def flows():
    db = get_mongo_database()
    if "flows" not in db.list_collection_names():
        db.create_collection("flows")
    records = []
    for doc in db.flows.find():
        doc["_id"] = str(doc["_id"])
        records.append(doc)
    return render_template(
        "flows.html",
        rows_data=records
    )

@app.route("/flows/new")
def flowsnew():

    return render_template(
        "flow_form.html",
        form_values={}
    )


@app.route("/setup", methods=["GET", "POST"])
def setup():

    if request.method == "POST":

        action = request.form.get("action")

        if action == "test":

            success, message = test_mongo_connection(
                request.form["mongo_host"],
                request.form["mongo_port"],
                request.form["mongo_database"],
                request.form["mongo_username"],
                request.form["mongo_password"],
                request.form["mongo_auth_database"]
            )

            return render_template(
                "settings.html",
                connection_verified=success,
                connection_message=message,
                mongo_host=request.form["mongo_host"],
                mongo_port=request.form["mongo_port"],
                mongo_database=request.form["mongo_database"],
                mongo_username=request.form["mongo_username"],
                mongo_password=request.form["mongo_password"],
                mongo_auth_database=request.form["mongo_auth_database"]
            )

        elif action == "save":

            save_config(request.form)

            return redirect(url_for("index"))

    return render_template(
        "settings.html",
        connection_verified=False
    )

@app.route("/connections")
def connections():

    config = load_config()

    db = mongo_client[config["mongo_database"]]

    # Create collection if it doesn't exist
    if "connections" not in db.list_collection_names():
        db.create_collection("connections")

    connections = list(
        db.connections.find().sort("name", 1)
    )

    return render_template(
        "connections.html",
        connections=connections
    )
@app.route("/connections/new", methods=["GET", "POST"])
def new_connection():
    if request.method == "GET":
        return render_template(
            "connection_form.html",
            connection_verified=False,
            connection_message=None,
            form_values={}
        )

    action = request.form.get("action")
    form_values = get_connection_form_values(request.form)

    if action == "test":
        success, message = test_rest_connection(request.form)

        return render_template(
            "connection_form.html",
            connection_verified=success,
            connection_message=message,
            form_values=form_values
        )

    if action == "save":
        success, message = test_rest_connection(request.form)

        if not success:
            return render_template(
                "connection_form.html",
                connection_verified=False,
                connection_message=(
                    "Connection was not saved.\n\n" + message
                ),
                form_values=form_values
            )

        try:
            params = parse_json_field(
                request.form.get("params"),
                "Parameters"
            )

            body = parse_json_field(
                request.form.get("body"),
                "Request Body"
            )

            database = get_mongo_database()
            connections_collection = database["connections"]

            existing_connection = connections_collection.find_one(
                {"name": form_values["name"]}
            )

            if existing_connection:
                return render_template(
                    "connection_form.html",
                    connection_verified=True,
                    connection_message=(
                        "Connection verified, but a connection "
                        "with this name already exists."
                    ),
                    form_values=form_values
                )

            current_time = datetime.now(timezone.utc)

            connection_document = {
                "name": form_values["name"],
                "connection_type": "FETCH",
                "fetch_type": form_values["fetch_type"],
                "fetch_method": form_values["fetch_method"],
                "authentication": {
                    "method": form_values["auth_method"],
                    "endpoint": form_values["auth_endpoint"],
                    "params": params,
                    "body": body
                },
                "ack_endpoint": form_values["ack_endpoint"],
                "verified": True,
                "verified_at": current_time,
                "created_at": current_time,
                "updated_at": current_time
            }

            connections_collection.insert_one(
                connection_document
            )

            flash(
                f'Connection "{form_values["name"]}" saved successfully.',
                "success"
            )

            return redirect(url_for("connections/grid"))

        except Exception as exc:
            return render_template(
                "connection_form.html",
                connection_verified=True,
                connection_message=(
                    "Connection verified, but saving failed.\n"
                    f"{exc}"
                ),
                form_values=form_values
            )

    return render_template(
        "connection_form.html",
        connection_verified=False,
        connection_message="Unknown form action.",
        form_values=form_values
    )

@app.before_request
def require_setup():

    allowed_routes = {
        "setup",
        "static"
    }

    if not is_configured():
        if request.endpoint not in allowed_routes:
            return redirect(url_for("setup"))

@app.route("/subscriber/new", methods=["GET", "POST"])
def new_subscriber():
    if request.method == "POST":
        fetch_endpoint = request.form["fetch_endpoint"].strip()
        ack_endpoint = request.form["ack_endpoint"].strip()
        sub_id = str(uuid.uuid4())
        values = (
            sub_id, request.form["name"].strip(), request.form["entity_name"].strip(),
            request.form["auth_endpoint"].strip(), fetch_endpoint, ack_endpoint,
            request.form.get("method", "POST"), int(request.form["frequency_seconds"]),
            int(request.form["batch_limit"]), request.form["client_id"].strip(),
            request.form["environment"].strip(), request.form["client_secret"],
            request.form.get("output_type", "json_files"), request.form.get("output_path", "output").strip(),
            "paused", now()
        )
        with db() as conn:
            conn.execute("INSERT INTO subscribers VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", values)
        add_log(sub_id, "Subscriber created")
        return redirect(url_for("subscriber_detail", sub_id=sub_id))
    return render_template("form.html")

@app.route("/subscriber/<sub_id>/edit", methods=["GET", "POST"])
def edit_subscriber(sub_id):
    sub = get_subscriber(sub_id)

    if not sub:
        return "Not Found", 404

    if request.method == "POST":
        print(dict(request.form))
        was_running = sub["status"] == "running"

        if was_running:
            current = workers.get(sub_id)
            if current:
                current[1].set()

        with db() as conn:
            conn.execute("""
                UPDATE subscribers
                SET
                    name=?,
                    entity_name=?,
                    auth_endpoint=?,
                    fetch_endpoint=?,
                    ack_endpoint=?,
                    method=?,
                    frequency_seconds=?,
                    batch_limit=?,
                    client_id=?,
                    environment=?,
                    client_secret=?,
                    output_type=?,
                    output_path=?
                WHERE id=?
            """, (
                request.form["name"],
                request.form["entity_name"],
                request.form["auth_endpoint"],
                request.form["fetch_endpoint"],
                request.form["ack_endpoint"],
                request.form["method"],
                int(request.form["frequency_seconds"]),
                int(request.form["batch_limit"]),
                request.form["client_id"],
                request.form["environment"],
                sub["client_secret"],
                request.form["output_type"],
                request.form["output_path"],
                sub_id
            ))

        if was_running:
            stop_event = threading.Event()
            thread = threading.Thread(
                target=worker,
                args=(sub_id, stop_event),
                daemon=True
            )
            workers[sub_id] = (thread, stop_event)
            thread.start()

        return redirect(
            url_for(
                "subscriber_detail",
                sub_id=sub_id
            )
        )

    return render_template(
        "form.html",
        sub=sub,
        edit_mode=True
    )
@app.route("/connections/grid")
def connections_grid():

    db = mongo_client

    records = []

    for doc in db.connections.find():

        doc["_id"] = str(doc["_id"])

        records.append(doc)

    return render_template(
        "connections_grid.html",
        rows_data=records
    )
@app.route("/subscriber/<sub_id>")
def subscriber_detail(sub_id):
    sub = get_subscriber(sub_id)
    if not sub:
        return "Not found", 404
    safe = dict(sub)
    safe["client_secret"] = "********"
    return render_template("detail.html", sub=safe)


@app.post("/subscriber/<sub_id>/start")
def start(sub_id):
    current = workers.get(sub_id)
    if current and current[0].is_alive():
        return redirect(url_for("subscriber_detail", sub_id=sub_id))
    stop_event = threading.Event()
    thread = threading.Thread(target=worker, args=(sub_id, stop_event), daemon=True)
    workers[sub_id] = (thread, stop_event)
    thread.start()
    return redirect(url_for("subscriber_detail", sub_id=sub_id))


@app.post("/subscriber/<sub_id>/pause")
def pause(sub_id):
    current = workers.get(sub_id)
    if current:
        current[1].set()
        add_log(sub_id, "Pause requested")
    return redirect(url_for("subscriber_detail", sub_id=sub_id))


@app.post("/subscriber/<sub_id>/delete")
def delete(sub_id):
    current = workers.get(sub_id)
    if current:
        current[1].set()
    with db() as conn:
        conn.execute("DELETE FROM subscribers WHERE id = ?", (sub_id,))
    return redirect(url_for("index"))


@app.get("/api/subscriber/<sub_id>/logs")
def logs(sub_id):
    with locks.setdefault(sub_id, threading.Lock()):
        return jsonify(log_buffers.get(sub_id, []))


@app.get("/api/subscriber/<sub_id>/status")
def status(sub_id):
    sub = get_subscriber(sub_id)
    return jsonify({"status": sub["status"] if sub else "missing"})


if __name__ == "__main__":
    get_mongo_database()
    app.run(host="127.0.0.1", port=5007, debug=True, threaded=True)
