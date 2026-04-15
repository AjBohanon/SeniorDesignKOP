from flask import Flask, request, jsonify
import os
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
import psycopg2
from psycopg2.extras import RealDictCursor

app = Flask(__name__)
LOCAL_TIMEZONE = ZoneInfo("America/New_York")


DRY_CONTACT_NAMES = (
    "Dry Contact - Wrap",
    "Dry Contact - Air Cooled",
    "Dry Contact - Water Cooled",
)

REQUIRED_DRY_CONTACT_FIELDS = (
    "sensorID",
    "sensorName",
    "messageDate",
    "dataMessageGUID",
    "dataType",
    "dataValue",
)


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
        cursor_factory=RealDictCursor,
    )


def is_blank(value):
    return value is None or (isinstance(value, str) and value.strip() == "")


def is_dry_contact_sensor(sensor_name):
    return any(sensor_name == name for name in DRY_CONTACT_NAMES)


def parse_dry_contact_state(raw_state):
    if raw_state == "True":
        return "Closed"
    if raw_state == "False":
        return "Open"
    return None


def classify_sensor(sensor):
    sensor_name = sensor.get("sensorName", "")
    data_type = sensor.get("dataType")

    if not is_dry_contact_sensor(sensor_name):
        return {
            "status": "skipped",
            "reason": f"sensor '{sensor_name}' is not one of the configured dry contact sensors",
        }

    missing = [field for field in REQUIRED_DRY_CONTACT_FIELDS if is_blank(sensor.get(field))]
    if missing:
        return {
            "status": "invalid",
            "reason": f"missing required fields: {', '.join(missing)}",
        }

    if data_type != "DryContact":
        return {
            "status": "invalid",
            "reason": f"unexpected dataType '{data_type}'",
        }

    state = parse_dry_contact_state(sensor.get("dataValue"))
    if state is None:
        return {
            "status": "invalid",
            "reason": f"unexpected dataValue '{sensor.get('dataValue')}'",
        }

    return {"status": "processed", "state": state}


def parse_message_timestamp(raw_timestamp):
    if is_blank(raw_timestamp):
        return None

    try:
        parsed = datetime.strptime(raw_timestamp, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        return None

    # iMonnit timestamps are treated as UTC, then converted to local Eastern time.
    return parsed.replace(tzinfo=timezone.utc).astimezone(LOCAL_TIMEZONE)


# Create table if not exists
def init_db():
    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS events (
            id           SERIAL PRIMARY KEY,
            device_id    INTEGER,
            sensor_name  TEXT,
            state        TEXT,
            timestamp    TIMESTAMP,
            message_guid TEXT
        );
    """
    )

    cur.execute("ALTER TABLE events ADD COLUMN IF NOT EXISTS message_guid TEXT;")
    cur.execute("ALTER TABLE events ADD COLUMN IF NOT EXISTS sensor_name TEXT;")
    cur.execute(
        """
        ALTER TABLE events
        ALTER COLUMN timestamp TYPE TIMESTAMP
        USING CASE
            WHEN timestamp IS NULL THEN NULL
            ELSE timezone('America/New_York', timestamp::timestamptz)
        END;
    """
    )

    cur.execute(
        """
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM pg_constraint
                WHERE conname = 'events_device_id_sensor_name_timestamp_key'
            ) THEN
                ALTER TABLE events
                ADD CONSTRAINT events_device_id_sensor_name_timestamp_key
                UNIQUE (device_id, sensor_name, timestamp);
            END IF;
        END$$;
    """
    )

    # Replace the older partial index so ON CONFLICT can match the message_guid key.
    cur.execute("DROP INDEX IF EXISTS events_message_guid_key;")
    cur.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS events_message_guid_key
        ON events (message_guid);
    """
    )

    conn.commit()
    print("Table 'events' verified/created successfully", flush=True)
    cur.close()
    conn.close()


init_db()


# ---------------------------------------------------------
# WEBHOOK ENDPOINT (iMonnit -> Railway)
# ---------------------------------------------------------
@app.route("/imonnit-webhook", methods=["POST"])
def webhook():
    print("WEBHOOK FUNCTION CALLED", flush=True)
    data = request.get_json(silent=True)

    if not data:
        return jsonify({"status": "error", "message": "No data"}), 400

    print(f"Data Received: {data}", flush=True)

    gateway_message = data.get("gatewayMessage")
    if not isinstance(gateway_message, dict):
        print("PAYLOAD INVALID: missing or invalid gatewayMessage", flush=True)
        return jsonify({"status": "error", "message": "Missing required field: gatewayMessage"}), 400

    sensor_messages = data.get("sensorMessages")
    if not isinstance(sensor_messages, list):
        print("PAYLOAD INVALID: missing or invalid top-level sensorMessages", flush=True)
        return jsonify({"status": "error", "message": "Missing or invalid field: sensorMessages"}), 400

    print(
        f"Payload validated: gatewayID={gateway_message.get('gatewayID')}, sensor_count={len(sensor_messages)}",
        flush=True,
    )
    print("About to establish database connection", flush=True)

    try:
        conn = get_db_connection()
        cur = conn.cursor()
        print("DB connection established successfully", flush=True)
    except Exception as e:
        print(f"ERROR: Failed to connect to database: {e}", flush=True)
        return jsonify(
            {"status": "error", "message": "Database connection failed", "detail": str(e)}
        ), 500

    counts = {
        "processed": 0,
        "skipped": 0,
        "invalid": 0,
        "inserted": 0,
        "duplicate": 0,
        "failed": 0,
    }

    for index, sensor in enumerate(sensor_messages, start=1):
        sensor_name = sensor.get("sensorName", "")
        sensor_id = sensor.get("sensorID")
        print(
            f"Processing sensor {index}: sensorName='{sensor_name}', sensorID={sensor_id}",
            flush=True,
        )

        classification = classify_sensor(sensor)
        status = classification["status"]
        reason = classification.get("reason")

        if status == "skipped":
            counts["skipped"] += 1
            print(f"SKIPPED: sensorName='{sensor_name}' reason={reason}", flush=True)
            continue

        if status == "invalid":
            counts["invalid"] += 1
            print(f"INVALID: sensorName='{sensor_name}' reason={reason}", flush=True)
            continue

        counts["processed"] += 1

        message_date = sensor.get("messageDate")
        message_guid = sensor.get("dataMessageGUID")
        state = classification["state"]
        parsed_timestamp = parse_message_timestamp(message_date)

        if parsed_timestamp is None:
            counts["invalid"] += 1
            counts["processed"] -= 1
            print(
                f"INVALID: sensorName='{sensor_name}' reason=unexpected messageDate '{message_date}'",
                flush=True,
            )
            continue

        print(
            "PROCESSED: "
            f"sensorName='{sensor_name}', message_guid='{message_guid}', "
            f"state='{state}', timestamp='{parsed_timestamp.isoformat()}'",
            flush=True,
        )

        try:
            cur.execute(
                """
                INSERT INTO events (device_id, sensor_name, state, timestamp, message_guid)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT DO NOTHING
                """,
                (sensor_id, sensor_name, state, parsed_timestamp.replace(tzinfo=None), message_guid),
            )

            if cur.rowcount == 1:
                counts["inserted"] += 1
                print(
                    f"INSERTED: sensorName='{sensor_name}', message_guid='{message_guid}'",
                    flush=True,
                )
            else:
                counts["duplicate"] += 1
                print(
                    "DUPLICATE: "
                    f"sensorName='{sensor_name}', message_guid='{message_guid}', timestamp='{parsed_timestamp.isoformat()}'",
                    flush=True,
                )
        except Exception as insert_error:
            counts["failed"] += 1
            print(
                f"FAILED: sensorName='{sensor_name}', error={type(insert_error).__name__}: {insert_error}",
                flush=True,
            )
            conn.rollback()
            cur = conn.cursor()

    print(f"Loop complete: {counts}", flush=True)

    try:
        conn.commit()
        print(f"Commit succeeded: {counts}", flush=True)
    except Exception as e:
        print(f"ERROR: Commit failed: {e}", flush=True)
        cur.close()
        conn.close()
        return jsonify(
            {"status": "error", "message": "Database commit failed", "detail": str(e)}
        ), 500

    cur.close()
    conn.close()

    return (
        jsonify(
            {
                "status": "success",
                "message": "Webhook processed",
                "counts": counts,
            }
        ),
        200,
    )


# ---------------------------------------------------------
# DASHBOARD ENDPOINT (Dashboard -> Railway)
# Returns the most recent 50 events
# ---------------------------------------------------------
@app.route("/latest", methods=["GET"])
def latest():
    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute(
        """
        SELECT device_id, sensor_name, state, timestamp, message_guid
        FROM events
        ORDER BY id DESC
        LIMIT 50;
    """
    )

    rows = cur.fetchall()
    for row in rows:
        if row.get("timestamp") is not None:
            row["timestamp"] = row["timestamp"].isoformat()

    cur.close()
    conn.close()

    return jsonify({"events": rows})
