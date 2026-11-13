-- =====================================================================
-- Analytical model for predictions (MySQL 8)
--
-- STAR SCHEMA
--   GRAIN of fact_predictions: ONE ROW PER UNIQUE EVENT (event_id) that was
--   validated and predicted, i.e. one prediction of the happiness score of
--   one country in one observation year.
--
--   dim_country      one row per country
--   dim_date         one row per observation year (the source reports are annual)
--   fact_predictions measures: actual, predicted, error, model inputs
--
-- STAGING / AUDIT TABLES (they are not part of the star)
--   raw_happiness_events  one row per Kafka message stored exactly as it arrived
--                         (valid and invalid ones, with their processing status)
--   duplicate_events      one row per Kafka message whose event_id was already predicted
--
--   vw_predictions_detail flat view (fact + dimensions) for Power BI
--
-- It is idempotent (IF NOT EXISTS): it can be executed several times.
-- Note: do not write semicolons inside comments, the consumer splits this file on them.
-- =====================================================================

-- ---------------------------------------------------------------------
-- RAW: Kafka message exactly as it arrived, BEFORE validating or predicting
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw_happiness_events (
    raw_event_id      INT AUTO_INCREMENT PRIMARY KEY,
    event_id          VARCHAR(100)  NULL,               -- extracted from the JSON when possible
    kafka_topic       VARCHAR(200)  NOT NULL,
    kafka_partition   INT           NOT NULL,
    kafka_offset      BIGINT        NOT NULL,
    kafka_key         VARCHAR(255)  NULL,
    raw_payload       MEDIUMTEXT    NOT NULL,           -- original message (text)
    received_at       DATETIME(6)   NOT NULL,
    processing_status VARCHAR(20)   NOT NULL DEFAULT 'RECEIVED',
    status_detail     TEXT          NULL,               -- reason when the event was not VALID
    processed_at      DATETIME(6)   NULL,
    CONSTRAINT chk_raw_status CHECK (processing_status IN
        ('RECEIVED', 'VALID', 'INVALID_SCHEMA', 'INVALID_VALUES', 'PREDICTION_ERROR')),
    -- A Kafka message is identified by (topic, partition, offset).
    -- It prevents duplicates if Kafka redelivers a message.
    UNIQUE KEY uq_raw_kafka_message (kafka_topic, kafka_partition, kafka_offset),
    KEY idx_raw_event_id (event_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ---------------------------------------------------------------------
-- DIMENSIONS
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_country (
    country_id   INT AUTO_INCREMENT PRIMARY KEY,
    country_name VARCHAR(150) NOT NULL,
    region       VARCHAR(100) NULL,                     -- NULL if unknown
    UNIQUE KEY uq_country_name (country_name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS dim_date (
    date_id INT         NOT NULL PRIMARY KEY,           -- the observation year (e.g. 2019)
    year    INT         NOT NULL,
    decade  VARCHAR(10) NOT NULL                        -- e.g. 2010s
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ---------------------------------------------------------------------
-- FACT: one row per predicted event
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fact_predictions (
    prediction_id        BIGINT AUTO_INCREMENT PRIMARY KEY,
    event_id             VARCHAR(100) NOT NULL,         -- degenerate dimension, guarantees no duplicates
    raw_event_id         INT          NOT NULL,         -- link to the original message
    country_id           INT          NOT NULL,
    date_id              INT          NOT NULL,
    -- model inputs (same names and order as the event contract)
    gdp                  DOUBLE       NOT NULL,
    family               DOUBLE       NOT NULL,
    health               DOUBLE       NOT NULL,
    freedom              DOUBLE       NOT NULL,
    generosity           DOUBLE       NOT NULL,
    corruption           DOUBLE       NOT NULL,
    -- measures
    actual_score         DOUBLE       NOT NULL,
    predicted_score      DOUBLE       NOT NULL,
    prediction_error     DOUBLE       NOT NULL,         -- predicted - actual (signed)
    -- timestamps
    event_time           DATETIME(6)  NOT NULL,         -- when the producer emitted the event
    prediction_timestamp DATETIME(6)  NOT NULL,         -- when the consumer generated the prediction
    UNIQUE KEY uq_fact_event_id (event_id),
    UNIQUE KEY uq_fact_raw_event (raw_event_id),
    KEY idx_fact_country (country_id),
    KEY idx_fact_date (date_id),
    CONSTRAINT fk_fact_raw     FOREIGN KEY (raw_event_id) REFERENCES raw_happiness_events (raw_event_id),
    CONSTRAINT fk_fact_country FOREIGN KEY (country_id)   REFERENCES dim_country (country_id),
    CONSTRAINT fk_fact_date    FOREIGN KEY (date_id)      REFERENCES dim_date (date_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ---------------------------------------------------------------------
-- DUPLICATES: messages whose event_id was already predicted
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS duplicate_events (
    duplicate_event_id    INT AUTO_INCREMENT PRIMARY KEY,
    event_id              VARCHAR(100) NOT NULL,
    original_raw_event_id INT          NOT NULL,        -- the first (accepted) message with that event_id
    kafka_topic           VARCHAR(200) NOT NULL,
    kafka_partition       INT          NOT NULL,
    kafka_offset          BIGINT       NOT NULL,
    kafka_key             VARCHAR(255) NULL,
    raw_payload           MEDIUMTEXT   NOT NULL,        -- duplicated message exactly as it arrived
    received_at           DATETIME(6)  NOT NULL,
    UNIQUE KEY uq_dup_kafka_message (kafka_topic, kafka_partition, kafka_offset),
    KEY idx_dup_event_id (event_id),
    CONSTRAINT fk_dup_original FOREIGN KEY (original_raw_event_id)
        REFERENCES raw_happiness_events (raw_event_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ---------------------------------------------------------------------
-- Flat VIEW (useful for Power BI / Looker Studio / Tableau)
-- ---------------------------------------------------------------------
CREATE OR REPLACE VIEW vw_predictions_detail AS
SELECT
    f.prediction_id,
    f.event_id,
    f.raw_event_id,
    c.country_name,
    c.region,
    d.year AS observation_year,
    f.gdp,
    f.family,
    f.health,
    f.freedom,
    f.generosity,
    f.corruption,
    f.actual_score,
    f.predicted_score,
    f.prediction_error,
    ABS(f.prediction_error) AS abs_error,
    f.event_time,
    f.prediction_timestamp
FROM fact_predictions f
JOIN dim_country c ON c.country_id = f.country_id
JOIN dim_date    d ON d.date_id    = f.date_id;
