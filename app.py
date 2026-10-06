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
    return CONFIG_PATH.exists()

def now():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


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

def test_mongo_connection(
        host,
        port,
        db_name):

    try:
        client = MongoClient(
            host=host,
            port=int(port),
            serverSelectionTimeoutMS=5000
        )

        client.server_info()

        db = client[db_name]

        return True, "Connected"

    except Exception as e:
        return False, str(e)

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
    with db() as conn:
        subscribers = [dict(row) for row in conn.execute("SELECT * FROM subscribers ORDER BY created_at DESC")]
    return render_template("index.html", subscribers=subscribers)

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

@app.route("/setup", methods=["GET", "POST"])
def setup():
    connection_status = None
    if request.method == "POST":
        action = request.form.get("action")
        if action == "test":
            success, message = test_mongo_connection(...)
            return render_template(
                "settings.html",
                connection_status=message
            )
        elif action == "save":
            save_config(...)
            return redirect(url_for("index"))
    return render_template("settings.html")


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
    init_db()
    app.run(host="127.0.0.1", port=5007, debug=True, threaded=True)
