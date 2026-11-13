-- =====================================================================
-- Dashboard KPIs (MySQL 8).
-- They can be pasted as they are into Power BI (native SQL query) / Looker Studio / Tableau.
-- =====================================================================

-- KPI 1. Average prediction error (MAE, RMSE and bias)
SELECT
    COUNT(*)                                       AS total_predictions,
    ROUND(AVG(ABS(prediction_error)), 4)           AS mae,
    ROUND(SQRT(AVG(POWER(prediction_error, 2))), 4) AS rmse,
    ROUND(AVG(prediction_error), 4)                AS mean_bias
FROM fact_predictions;

-- KPI 2. Predictions by country (and their average error)
SELECT
    c.country_name,
    COALESCE(c.region, 'Unknown')                  AS region,
    COUNT(*)                                       AS total_predictions,
    ROUND(AVG(ABS(f.prediction_error)), 4)         AS avg_abs_error
FROM fact_predictions f
JOIN dim_country c ON c.country_id = f.country_id
GROUP BY c.country_name, c.region
ORDER BY total_predictions DESC, c.country_name;

-- KPI 3. Predicted vs actual score (one row per prediction)
SELECT
    c.country_name,
    d.year AS observation_year,
    f.actual_score,
    f.predicted_score
FROM fact_predictions f
JOIN dim_country c ON c.country_id = f.country_id
JOIN dim_date    d ON d.date_id    = f.date_id
ORDER BY f.prediction_id;

-- KPI 4a. Trend by observation year (2015-2019)
SELECT
    d.year AS observation_year,
    COUNT(*)                                       AS total_predictions,
    ROUND(AVG(f.actual_score), 4)                  AS avg_actual,
    ROUND(AVG(f.predicted_score), 4)               AS avg_predicted,
    ROUND(AVG(ABS(f.prediction_error)), 4)         AS mae
FROM fact_predictions f
JOIN dim_date d ON d.date_id = f.date_id
GROUP BY d.year
ORDER BY d.year;

-- KPI 4b. Trend over processing time: running average error
SELECT
    prediction_timestamp,
    ABS(prediction_error) AS abs_error,
    AVG(ABS(prediction_error)) OVER (
        ORDER BY prediction_timestamp, prediction_id
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS running_mae
FROM fact_predictions
ORDER BY prediction_timestamp, prediction_id;

-- Extra. Pipeline quality: received messages by outcome (duplicates included)
SELECT processing_status, COUNT(*) AS total_events
FROM raw_happiness_events
GROUP BY processing_status
UNION ALL
SELECT 'DUPLICATE_EVENT', COUNT(*)
FROM duplicate_events
ORDER BY total_events DESC;
