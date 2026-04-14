from flask import Flask, request, jsonify
import os
import psycopg2
from psycopg2.extras import RealDictCursor

app = Flask(__name__)

# ---------------------------------------------------------
# POSTGRES CONNECTION (Railway Environment Variables)
# ---------------------------------------------------------
def get_db_connection():
    return psycopg2.connect(
        host=os.environ.get("PGHOST"),
        database=os.environ.get("PGDATABASE"),
        user=os.environ.get("PGUSER"),
        password=os.environ.get("PGPASSWORD"),
        port=os.environ.get("PGPORT"),
        cursor_factory=RealDictCursor
    )

# Create table if not exists
def init_db():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS events (
            id SERIAL PRIMARY KEY,
            device_id INTEGER,
            state TEXT,
            timestamp TEXT
        );
    """)
    conn.commit()
    cur.close()
    conn.close()

init_db()

# ---------------------------------------------------------
# WEBHOOK ENDPOINT (iMonnit → Railway)
# ---------------------------------------------------------
@app.route('/imonnit-webhook', methods=['POST'])
def webhook():
    data = request.json

    if not data:
        return jsonify({"status": "error", "message": "No data"}), 400

    print(f"Data Received: {data}")

    device_id = data.get("deviceID")
    state = data.get("reading")  # "Open" or "Closed"
    timestamp = f"{data.get('date')}T{data.get('time')}"

    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute(
        "INSERT INTO events (device_id, state, timestamp) VALUES (%s, %s, %s)",
        (device_id, state, timestamp)
    )

    conn.commit()
    cur.close()
    conn.close()

    return jsonify({"status": "success"}), 200

# ---------------------------------------------------------
# DASHBOARD ENDPOINT (Dashboard → Railway)
# Returns the most recent 50 events
# ---------------------------------------------------------
@app.route('/latest', methods=['GET'])
def latest():
    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT device_id, state, timestamp
        FROM events
        ORDER BY id DESC
        LIMIT 50;
    """)

    rows = cur.fetchall()

    cur.close()
    conn.close()

    return jsonify({"events": rows})
