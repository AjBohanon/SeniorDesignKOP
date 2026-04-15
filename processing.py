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
            timestamp TEXT,
            message_guid TEXT,
            UNIQUE (device_id, timestamp)
        );
    """)
    conn.commit()
    print("Table 'events' verified/created successfully", flush=True)
    cur.close()
    conn.close()

init_db()

# Dry contact sensor name prefixes to accept
DRY_CONTACT_NAMES = (
    "Dry Contact - Wrap",
    "Dry Contact - Air Cooled",
    "Dry Contact - Water Cooled",
)

# ---------------------------------------------------------
# WEBHOOK ENDPOINT (iMonnit → Railway)
# ---------------------------------------------------------
@app.route('/imonnit-webhook', methods=['POST'])
def webhook():
    print("WEBHOOK FUNCTION CALLED", flush=True)
    data = request.json

    if not data:
        return jsonify({"status": "error", "message": "No data"}), 400

    print(f"Data Received: {data}")

    # Validate top-level structure
    gateway_message = data.get("gatewayMessage")
    if not gateway_message:
        return jsonify({"status": "error", "message": "Missing required field: gatewayMessage"}), 400

    sensor_messages = gateway_message.get("sensorMessages")
    if not sensor_messages or not isinstance(sensor_messages, list):
        return jsonify({"status": "error", "message": "Missing or invalid field: sensorMessages"}), 400

    try:
        conn = get_db_connection()
        cur = conn.cursor()
        print("DB connection established successfully", flush=True)
    except Exception as e:
        print(f"ERROR: Failed to connect to database: {e}", flush=True)
        return jsonify({"status": "error", "message": "Database connection failed", "detail": str(e)}), 500

    inserted = 0
    failed = 0

    for sensor in sensor_messages:
        sensor_name = sensor.get("sensorName", "")

        # Only process dry contact sensors
        if not any(sensor_name.startswith(name) for name in DRY_CONTACT_NAMES):
            continue

        sensor_id = sensor.get("sensorID")
        state = sensor.get("state")
        message_date = sensor.get("messageDate")
        message_guid = sensor.get("dataMessageGUID")

        missing = [f for f, v in {"sensorID": sensor_id, "state": state, "messageDate": message_date, "dataMessageGUID": message_guid}.items() if v is None]
        if missing:
            print(f"Skipping sensor '{sensor_name}': missing fields {', '.join(missing)}", flush=True)
            continue

        print(f"Attempting insert — device_id={sensor_id}, state={state}, message_date={message_date}, message_guid={message_guid}", flush=True)
        try:
            cur.execute(
    """INSERT INTO events (device_id, state, timestamp, message_guid) 
       VALUES (%s, %s, %s, %s)
       ON CONFLICT (device_id, timestamp) DO NOTHING""",
    (sensor_id, state, message_date, message_guid)
)
            print(f"Insert succeeded for device_id={sensor_id}, message_guid={message_guid}", flush=True)
            inserted += 1
        except Exception as e:
            print(f"ERROR: Insert failed for device_id={sensor_id}: {type(e).__name__}: {e}", flush=True)
            failed += 1

    try:
        conn.commit()
        print(f"Commit succeeded — {inserted} row(s) inserted, {failed} failed", flush=True)
    except Exception as e:
        print(f"ERROR: Commit failed: {e}", flush=True)
        cur.close()
        conn.close()
        return jsonify({"status": "error", "message": "Database commit failed", "detail": str(e)}), 500

    cur.close()
    conn.close()

    return jsonify({"status": "success", "inserted": inserted, "failed": failed}), 200

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
