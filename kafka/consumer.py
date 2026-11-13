"""
Kafka Consumer with real-time inference (Steps 6 and 8)

For every message of the `happiness-events` topic (group `happiness-ml-team`):
  1. If its event_id was already predicted, it goes to the duplicate_events table and stops there.
  2. Otherwise it stores the ORIGINAL message in raw_happiness_events (before anything else).
  3. Validates the event against the contract.
  4. Builds the features with the same order used during training.
  5. Predicts with the serialized model (models/model.pkl).
  6. Stores the prediction in fact_predictions (with dim_country and dim_date).

Invalid events are stored in the raw table with their status, skipped from the
prediction and do NOT stop the pipeline.

Statuses in raw_happiness_events.processing_status:
  RECEIVED          just stored, not processed yet
  VALID             passed validation and its prediction was stored
  INVALID_SCHEMA    broken JSON, missing fields or wrong data types
  INVALID_VALUES    out-of-range values, empty text, NaN, malformed date...
  PREDICTION_ERROR  the model failed while predicting

Repeated events (same event_id as an already predicted one) are NOT a status of the raw
table: they are stored in duplicate_events, so the fact table never gets duplicates.

Usage (from the project root):
    python kafka/consumer.py                    # runs until Ctrl+C
    python kafka/consumer.py --idle-timeout 30  # stops after 30 s without messages
"""
import argparse
import json
import math
from datetime import datetime, timezone

import joblib
import pandas as pd
import mysql.connector
from kafka import KafkaConsumer

from settings import (
    COUNTRY_REGION_FILE,
    CREATE_TABLES_SQL,
    DB_CONFIG,
    FEATURE_RANGE,
    FEATURES,
    GROUP_ID,
    KAFKA_BOOTSTRAP_SERVERS,
    MAX_COUNTRY_LENGTH,
    MAX_EVENT_ID_LENGTH,
    MODEL_PATH,
    REQUIRED_FIELDS,
    SCORE_RANGE,
    TARGET_FIELD,
    TOPIC,
    YEAR_RANGE,
)

VALID = "VALID"
INVALID_SCHEMA = "INVALID_SCHEMA"
INVALID_VALUES = "INVALID_VALUES"
PREDICTION_ERROR = "PREDICTION_ERROR"
DUPLICATE_EVENT = "DUPLICATE_EVENT"   # outcome only: it is stored in duplicate_events, not in raw


# ======================================================================
# 1. Event contract validation
# ======================================================================
def is_number(value):
    """Real number (int or float). In Python bool is a subclass of int, so it is excluded."""
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def is_integer(value):
    return isinstance(value, int) and not isinstance(value, bool)


def validate_event(key, raw_text):
    """
    Validate a message against the contract.
    Returns (status, event, detail). The event is None if the JSON could not be read.
    """
    # --- The message must be a JSON object
    try:
        event = json.loads(raw_text)
    except (ValueError, RecursionError) as error:
        return INVALID_SCHEMA, None, f"invalid JSON: {error}"
    if not isinstance(event, dict):
        return INVALID_SCHEMA, None, "the message is not a JSON object"

    # --- Required fields (this includes every feature the model needs)
    missing = [field for field in REQUIRED_FIELDS if event.get(field) is None]
    if missing:
        return INVALID_SCHEMA, event, f"missing fields: {missing}"

    # --- Data types
    problems = []
    if not isinstance(event["event_id"], str):
        problems.append("event_id must be text")
    if not isinstance(event["country"], str):
        problems.append("country must be text")
    if not is_integer(event["year"]):
        problems.append("year must be an integer")
    for field in [*FEATURES, TARGET_FIELD]:
        if not is_number(event[field]):
            problems.append(f"{field} must be numeric")
    if not isinstance(event["event_time"], str):
        problems.append("event_time must be an ISO-8601 text")
    if problems:
        return INVALID_SCHEMA, event, "; ".join(problems)

    # --- Values
    problems = []
    if not event["event_id"].strip():
        problems.append("empty event_id")
    elif len(event["event_id"]) > MAX_EVENT_ID_LENGTH:
        problems.append(f"event_id longer than {MAX_EVENT_ID_LENGTH} characters")
    if not event["country"].strip():
        problems.append("empty country")
    elif len(event["country"]) > MAX_COUNTRY_LENGTH:
        problems.append(f"country longer than {MAX_COUNTRY_LENGTH} characters")
    elif key != event["country"]:
        problems.append(f"the Kafka key ({key!r}) does not match country ({event['country']!r})")
    if not YEAR_RANGE[0] <= event["year"] <= YEAR_RANGE[1]:
        problems.append(f"year out of range {YEAR_RANGE}: {event['year']}")
    for field in FEATURES:
        value = event[field]
        if not math.isfinite(value) or not FEATURE_RANGE[0] <= value <= FEATURE_RANGE[1]:
            problems.append(f"invalid {field} (range {FEATURE_RANGE}): {value}")
    score = event[TARGET_FIELD]
    if not math.isfinite(score) or not SCORE_RANGE[0] <= score <= SCORE_RANGE[1]:
        problems.append(f"invalid {TARGET_FIELD} (range {SCORE_RANGE}): {score}")
    try:
        datetime.fromisoformat(event["event_time"])
    except ValueError:
        problems.append(f"event_time is not ISO-8601: {event['event_time']!r}")
    if problems:
        return INVALID_VALUES, event, "; ".join(problems)

    return VALID, event, None


# ======================================================================
# 2. Prediction
# ======================================================================
def load_model():
    """Load the serialized Pipeline and check that it matches the contract."""
    model = joblib.load(MODEL_PATH)
    if list(model.feature_names_in_) != FEATURES:
        raise SystemExit(
            f"The model expects {list(model.feature_names_in_)} but the contract defines {FEATURES}. "
            "Retrain with notebooks/model_training.ipynb"
        )
    return model


def predict(model, event):
    """Same column order as in training; the Pipeline applies the scaling."""
    X = pd.DataFrame([[event[f] for f in FEATURES]], columns=FEATURES)
    return float(model.predict(X)[0])


# ======================================================================
# 3. Database (MySQL)
# ======================================================================
def connect():
    return mysql.connector.connect(**DB_CONFIG, charset="utf8mb4")


def init_db(conn):
    """Create the tables if they do not exist (sql/create_tables.sql is idempotent)."""
    cur = conn.cursor(buffered=True)
    # The file is split on ";" and the comment-only lines are removed
    for statement in CREATE_TABLES_SQL.read_text(encoding="utf-8").split(";"):
        lines = [line for line in statement.splitlines() if not line.strip().startswith("--")]
        statement = "\n".join(lines).strip()
        if statement:
            cur.execute(statement)
    conn.commit()
    cur.close()


def load_regions():
    """Country -> region catalog generated by the EDA notebook (optional)."""
    if COUNTRY_REGION_FILE.exists():
        df = pd.read_csv(COUNTRY_REGION_FILE)
        return dict(zip(df["country"], df["region"]))
    return {}


def peek_event_id(raw_text):
    """Read the event_id of a message (only to detect repeated events). None if it cannot be read."""
    try:
        event = json.loads(raw_text)
    except (ValueError, RecursionError):
        return None
    if isinstance(event, dict) and isinstance(event.get("event_id"), str) and event["event_id"].strip():
        return event["event_id"]
    return None


def find_raw_message(conn, topic, partition, offset):
    """(raw_event_id, status) if this exact Kafka message is already in the raw table."""
    cur = conn.cursor(buffered=True)
    cur.execute(
        """
        SELECT raw_event_id, processing_status FROM raw_happiness_events
        WHERE kafka_topic = %s AND kafka_partition = %s AND kafka_offset = %s
        """,
        (topic, partition, offset),
    )
    row = cur.fetchone()
    cur.close()
    return row


def is_stored_duplicate(conn, topic, partition, offset):
    """True if this exact Kafka message is already in the duplicate_events table."""
    cur = conn.cursor(buffered=True)
    cur.execute(
        """
        SELECT 1 FROM duplicate_events
        WHERE kafka_topic = %s AND kafka_partition = %s AND kafka_offset = %s
        """,
        (topic, partition, offset),
    )
    found = cur.fetchone() is not None
    cur.close()
    return found


def find_original(conn, event_id):
    """raw_event_id of the event that already has a prediction with this event_id (or None)."""
    cur = conn.cursor(buffered=True)
    cur.execute("SELECT raw_event_id FROM fact_predictions WHERE event_id = %s", (event_id,))
    row = cur.fetchone()
    cur.close()
    return row[0] if row else None


def store_duplicate(conn, original_raw_event_id, event_id, topic, partition, offset, key, raw_text):
    cur = conn.cursor(buffered=True)
    cur.execute(
        """
        INSERT INTO duplicate_events
            (event_id, original_raw_event_id, kafka_topic, kafka_partition, kafka_offset,
             kafka_key, raw_payload, received_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (event_id, original_raw_event_id, topic, partition, offset, key, raw_text, datetime.now()),
    )
    conn.commit()
    cur.close()


def store_raw(conn, topic, partition, offset, key, raw_text):
    """Store the original message with status RECEIVED and return its raw_event_id."""
    cur = conn.cursor(buffered=True)
    cur.execute(
        """
        INSERT INTO raw_happiness_events
            (kafka_topic, kafka_partition, kafka_offset, kafka_key, raw_payload, received_at)
        VALUES (%s, %s, %s, %s, %s, %s)
        """,
        (topic, partition, offset, key, raw_text, datetime.now()),
    )
    conn.commit()
    raw_event_id = cur.lastrowid
    cur.close()
    return raw_event_id


def set_status(cur, raw_event_id, status, detail, event):
    """Update the status of the raw event (and its event_id if it could be read)."""
    event_id = None
    if isinstance(event, dict) and isinstance(event.get("event_id"), str):
        event_id = event["event_id"][:MAX_EVENT_ID_LENGTH]
    cur.execute(
        """
        UPDATE raw_happiness_events
        SET processing_status = %s, status_detail = %s, event_id = %s, processed_at = %s
        WHERE raw_event_id = %s
        """,
        (status, detail[:2000] if detail else None, event_id, datetime.now(), raw_event_id),
    )


def get_country_id(cur, country, regions):
    cur.execute(
        "INSERT INTO dim_country (country_name, region) VALUES (%s, %s) "
        "ON DUPLICATE KEY UPDATE country_name = country_name",
        (country, regions.get(country)),
    )
    cur.execute("SELECT country_id FROM dim_country WHERE country_name = %s", (country,))
    return cur.fetchone()[0]


def get_date_id(cur, year):
    """dim_date has one row per observation year, and the year itself is the key."""
    cur.execute(
        "INSERT INTO dim_date (date_id, year, decade) VALUES (%s, %s, %s) "
        "ON DUPLICATE KEY UPDATE date_id = date_id",
        (year, year, f"{year // 10 * 10}s"),
    )
    return year


def store_prediction(cur, raw_event_id, event, predicted, regions):
    """Insert the prediction into fact_predictions, linked to the raw event."""
    event_time = datetime.fromisoformat(event["event_time"])
    if event_time.tzinfo is not None:  # always stored in UTC without time zone
        event_time = event_time.astimezone(timezone.utc).replace(tzinfo=None)

    actual = float(event[TARGET_FIELD])
    cur.execute(
        """
        INSERT INTO fact_predictions
            (event_id, raw_event_id, country_id, date_id,
             gdp, family, health, freedom, generosity, corruption,
             actual_score, predicted_score, prediction_error, event_time, prediction_timestamp)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (event["event_id"], raw_event_id,
         get_country_id(cur, event["country"], regions), get_date_id(cur, event["year"]),
         *[float(event[f]) for f in FEATURES],
         actual, predicted, predicted - actual, event_time, datetime.now()),
    )


# ======================================================================
# 4. Processing of a single message
# ======================================================================
def process_message(conn, model, regions, topic, partition, offset, key, value):
    """
    Process one Kafka message. Returns the final outcome (or None if it was already handled).
    It never raises exceptions because of the message content.
    """
    # Message exactly as it arrived (invalid bytes are replaced; NUL is replaced too)
    raw_text = (value or b"").decode("utf-8", errors="replace").replace("\x00", "\ufffd")
    key_text = key.decode("utf-8", errors="replace")[:255] if key is not None else None

    # 0) Was this exact Kafka message already handled? (Kafka can redeliver messages)
    already = find_raw_message(conn, topic, partition, offset)
    if already is not None:
        if already[1] != "RECEIVED":
            return None
        raw_event_id = already[0]               # it was left unfinished: process it again
    elif is_stored_duplicate(conn, topic, partition, offset):
        return None
    else:
        # 1) Same event_id as an event that already has a prediction? -> duplicate_events
        event_id = peek_event_id(raw_text)
        original = find_original(conn, event_id) if event_id else None
        if original is not None:
            store_duplicate(conn, original, event_id, topic, partition, offset, key_text, raw_text)
            return DUPLICATE_EVENT
        # 2) RAW first, before validating or predicting
        raw_event_id = store_raw(conn, topic, partition, offset, key_text, raw_text)

    # 3) Validate against the contract
    status, event, detail = validate_event(key_text, raw_text)
    if status != VALID:
        cur = conn.cursor(buffered=True)
        set_status(cur, raw_event_id, status, detail, event)
        conn.commit()
        cur.close()
        return status

    # 4) Predict (a model failure must not bring the pipeline down)
    try:
        predicted = predict(model, event)
        if not math.isfinite(predicted):
            raise ValueError(f"non-finite prediction: {predicted}")
    except Exception as error:  # noqa: BLE001
        cur = conn.cursor(buffered=True)
        set_status(cur, raw_event_id, PREDICTION_ERROR, f"{type(error).__name__}: {error}", event)
        conn.commit()
        cur.close()
        return PREDICTION_ERROR

    # 5) Store the prediction + mark the raw event as VALID in a single transaction
    cur = conn.cursor(buffered=True)
    try:
        store_prediction(cur, raw_event_id, event, predicted, regions)
        set_status(cur, raw_event_id, VALID, None, event)
        conn.commit()
    except Exception:
        conn.rollback()      # nothing is half-saved; the error is raised so it is not hidden
        raise
    finally:
        cur.close()
    return VALID


# ======================================================================
# 5. Main loop
# ======================================================================
def main():
    parser = argparse.ArgumentParser(description="Happiness events consumer")
    parser.add_argument("--idle-timeout", type=int, default=0,
                        help="stop after N seconds without messages (0 = run until Ctrl+C)")
    args = parser.parse_args()

    model = load_model()
    regions = load_regions()
    conn = connect()
    init_db(conn)

    settings = {}
    if args.idle_timeout > 0:           # without this option the consumer waits for messages forever
        settings["consumer_timeout_ms"] = args.idle_timeout * 1000

    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=GROUP_ID,
        auto_offset_reset="earliest",   # if the group is new, start from the first event
        enable_auto_commit=False,       # the offset is committed ONLY after saving to the database
        **settings,
    )

    print(f"Consumer '{GROUP_ID}' listening to topic '{TOPIC}' (Ctrl+C to stop)...")
    counts = {}
    try:
        for message in consumer:
            status = process_message(conn, model, regions, message.topic, message.partition,
                                     message.offset, message.key, message.value)
            consumer.commit()
            if status is not None:
                counts[status] = counts.get(status, 0) + 1
            key_text = message.key.decode("utf-8", errors="replace") if message.key else "-"
            print(f"partition {message.partition} offset {message.offset:>4} key={key_text:<28} -> {status}")
    except KeyboardInterrupt:
        print("\nStopped by the user.")
    finally:
        consumer.close()
        conn.close()
        print("Summary of this run:", counts or "no new messages")


if __name__ == "__main__":
    main()
