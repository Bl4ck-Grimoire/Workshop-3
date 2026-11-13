"""
Kafka Producer (Step 5)

Reads the prepared historical dataset (data/streaming/streaming_events.csv) record by
record and publishes each one as a JSON event to the `happiness-events` topic, using the
country as the event key and a configurable pause between records to simulate data
arriving over time.

Usage (from the project root):
    python kafka/producer.py                  # 0.5 s between events
    python kafka/producer.py --delay 0.1      # faster
    python kafka/producer.py --limit 20       # only 20 events (testing)
"""
import argparse
import json
import os
import time
from datetime import datetime

import pandas as pd
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

from settings import (
    FEATURES,
    KAFKA_BOOTSTRAP_SERVERS,
    NUM_PARTITIONS,
    REPLICATION_FACTOR,
    STREAMING_FILE,
    TARGET_FIELD,
    TOPIC,
)

# Columns the CSV must contain to build the event
CSV_COLUMNS = ["country", "year", *FEATURES, TARGET_FIELD]


def ensure_topic():
    """Create the topic (3 partitions, replication factor 1) if it does not exist."""
    admin = KafkaAdminClient(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
    try:
        admin.create_topics([NewTopic(TOPIC, NUM_PARTITIONS, REPLICATION_FACTOR)])
        print(f"Topic '{TOPIC}' created ({NUM_PARTITIONS} partitions, replication {REPLICATION_FACTOR})")
    except TopicAlreadyExistsError:
        print(f"Topic '{TOPIC}' already exists")
    finally:
        admin.close()


def build_event(number, row):
    """Build the event using the JSON format required by the workshop."""
    return {
        "event_id": f"E{number:03d}",
        "country": str(row["country"]),
        "year": int(row["year"]),
        "gdp": float(row["gdp"]),
        "family": float(row["family"]),
        "health": float(row["health"]),
        "freedom": float(row["freedom"]),
        "generosity": float(row["generosity"]),
        "corruption": float(row["corruption"]),
        "actual_happiness_score": float(row[TARGET_FIELD]),
        "event_time": datetime.now().isoformat(timespec="seconds"),  # emission moment
    }


def main():
    parser = argparse.ArgumentParser(description="Happiness events producer")
    parser.add_argument("--file", default=str(STREAMING_FILE), help="CSV with the data to send")
    parser.add_argument("--delay", type=float, default=float(os.getenv("PRODUCER_DELAY", "0.5")),
                        help="seconds between events (default 0.5)")
    parser.add_argument("--limit", type=int, default=None, help="send only the first N records")
    args = parser.parse_args()

    df = pd.read_csv(args.file)
    missing = [c for c in CSV_COLUMNS if c not in df.columns]
    if missing:
        raise SystemExit(f"The CSV is missing columns: {missing}")
    if args.limit:
        df = df.head(args.limit)

    ensure_topic()

    producer = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS, acks="all")

    print(f"Sending {len(df)} events to '{TOPIC}' (pause of {args.delay}s)...")
    try:
        for number, row in enumerate(df.to_dict("records"), start=1):
            event = build_event(number, row)
            key = event["country"].encode("utf-8")                              # key = country
            value = json.dumps(event, ensure_ascii=False).encode("utf-8")       # event as JSON
            # With the same key, all events of a country land in the same partition
            metadata = producer.send(TOPIC, key=key, value=value).get(timeout=10)
            print(f"[{event['event_id']}] {event['country']:<28} {event['year']} "
                  f"-> partition {metadata.partition}, offset {metadata.offset}")
            time.sleep(args.delay)
        producer.flush()
        print("Done: all events were sent.")
    finally:
        producer.close()


if __name__ == "__main__":
    main()
