"""
Shared project configuration (single source of truth).

It is used by the training notebook, the producer, the consumer and the dashboard layer.
This way the topic, the consumer group, the event contract and the feature order
are always the same in every part of the pipeline.
"""
import os
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent

# ---------------------------------------------------------------- Kafka
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = "happiness-events"
GROUP_ID = "happiness-ml-team"
NUM_PARTITIONS = 3
REPLICATION_FACTOR = 1

# ------------------------------------------------------ Event contract
# Model features (THIS ORDER is used both when training and when predicting)
FEATURES = ["gdp", "family", "health", "freedom", "generosity", "corruption"]
# Used only for evaluation: NEVER a model input
TARGET_FIELD = "actual_happiness_score"
# Required fields of every event
REQUIRED_FIELDS = ["event_id", "country", "year", *FEATURES, TARGET_FIELD, "event_time"]

# Valid ranges (numeric value validation)
FEATURE_RANGE = (0.0, 3.0)   # real data goes from 0 to ~2.1
SCORE_RANGE = (0.0, 10.0)    # the happiness score goes from 0 to 10
YEAR_RANGE = (2000, 2100)

# Maximum text lengths (they must fit the database columns)
MAX_EVENT_ID_LENGTH = 100    # raw_happiness_events.event_id / fact_predictions.event_id
MAX_COUNTRY_LENGTH = 150     # dim_country.country_name

# --------------------------------------------------------------- Files
MODEL_PATH = BASE_DIR / "models" / "model.pkl"
STREAMING_FILE = BASE_DIR / "data" / "streaming" / "streaming_events.csv"
COUNTRY_REGION_FILE = BASE_DIR / "data" / "processed" / "country_region.csv"
CREATE_TABLES_SQL = BASE_DIR / "sql" / "create_tables.sql"

# ------------------------------------------------------------ Database
# MySQL (local container defined in docker-compose.yml)
DB_CONFIG = {
    "host": os.getenv("MYSQL_HOST", "localhost"),
    "port": int(os.getenv("MYSQL_PORT", "3307")),
    "database": os.getenv("MYSQL_DATABASE", "happiness"),
    "user": os.getenv("MYSQL_USER", "happiness"),
    "password": os.getenv("MYSQL_PASSWORD", "happiness"),
}
