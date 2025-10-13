-- Enable TimescaleDB extension
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- Raw data table (as received from API)
CREATE TABLE IF NOT EXISTS neo_raw (
    id TEXT,
    neo_reference_id TEXT,
    name TEXT,
    nasa_jpl_url TEXT,
    absolute_magnitude_h FLOAT,
    is_potentially_hazardous BOOLEAN,
    is_sentry_object BOOLEAN,
    close_approach_date TIMESTAMP NOT NULL,
    close_approach_date_full TEXT,
    epoch_date_close_approach BIGINT,
    relative_velocity_km_s FLOAT,
    relative_velocity_km_h FLOAT,
    relative_velocity_mph FLOAT,
    miss_distance_astronomical FLOAT,
    miss_distance_lunar FLOAT,
    miss_distance_km FLOAT,
    miss_distance_miles FLOAT,
    orbiting_body TEXT,
    estimated_diameter_km_min FLOAT,
    estimated_diameter_km_max FLOAT,
    estimated_diameter_m_min FLOAT,
    estimated_diameter_m_max FLOAT,
    estimated_diameter_miles_min FLOAT,
    estimated_diameter_miles_max FLOAT,
    estimated_diameter_feet_min FLOAT,
    estimated_diameter_feet_max FLOAT,
    ingestion_time TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (id, close_approach_date)
);

-- Create hypertable for time-series optimization
SELECT create_hypertable('neo_raw', 'close_approach_date', if_not_exists => TRUE);

-- Processed data table (after Spark processing)
CREATE TABLE IF NOT EXISTS neo_processed (
    id TEXT,
    name TEXT,
    close_approach_date TIMESTAMP NOT NULL,
    is_potentially_hazardous BOOLEAN,
    diameter_mean_km FLOAT,
    miss_distance_km FLOAT,
    relative_velocity_km_s FLOAT,
    risk_score FLOAT,
    risk_level TEXT,
    diameter_zscore FLOAT,
    velocity_zscore FLOAT,
    miss_distance_zscore FLOAT,
    is_anomaly BOOLEAN,
    processed_time TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (id, close_approach_date)
);

-- Create hypertable for processed data
SELECT create_hypertable('neo_processed', 'close_approach_date', if_not_exists => TRUE);

-- Daily aggregated metrics table
CREATE TABLE IF NOT EXISTS neo_daily_metrics (
    metric_date DATE NOT NULL PRIMARY KEY,
    asteroid_count INT,
    hazardous_count INT,
    avg_diameter_km FLOAT,
    max_diameter_km FLOAT,
    avg_velocity_km_s FLOAT,
    max_velocity_km_s FLOAT,
    min_miss_distance_km FLOAT,
    avg_miss_distance_km FLOAT,
    avg_risk_score FLOAT,
    high_risk_count INT,
    anomaly_count INT,
    calculated_time TIMESTAMP DEFAULT NOW()
);

-- Create indices for better query performance
CREATE INDEX IF NOT EXISTS idx_neo_raw_date ON neo_raw(close_approach_date DESC);
CREATE INDEX IF NOT EXISTS idx_neo_raw_hazardous ON neo_raw(is_potentially_hazardous);
CREATE INDEX IF NOT EXISTS idx_neo_processed_date ON neo_processed(close_approach_date DESC);
CREATE INDEX IF NOT EXISTS idx_neo_processed_risk ON neo_processed(risk_score DESC);
CREATE INDEX IF NOT EXISTS idx_neo_processed_anomaly ON neo_processed(is_anomaly);

-- Create continuous aggregate for real-time analytics
CREATE MATERIALIZED VIEW IF NOT EXISTS neo_hourly_stats
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 hour', close_approach_date) AS hour,
    COUNT(*) as count,
    AVG(miss_distance_km) as avg_miss_distance,
    MIN(miss_distance_km) as min_miss_distance,
    AVG(relative_velocity_km_s) as avg_velocity,
    MAX(relative_velocity_km_s) as max_velocity
FROM neo_raw
GROUP BY hour
WITH NO DATA;

-- Refresh policy for continuous aggregate
SELECT add_continuous_aggregate_policy('neo_hourly_stats',
    start_offset => INTERVAL '3 hours',
    end_offset => INTERVAL '1 hour',
    schedule_interval => INTERVAL '1 hour',
    if_not_exists => TRUE
);