-- Main Table Schema provided by User

CREATE EXTENSION IF NOT EXISTS pg_trgm;

CREATE TABLE IF NOT EXISTS neo_close_approaches (
    close_approach_date DATE NOT NULL,
    asteroid_id TEXT NOT NULL,
    neo_reference_id TEXT,
    name TEXT,
    absolute_magnitude_h DOUBLE PRECISION,
    
    -- Estimated diameters are floats in JSON
    estimated_diameter_km_min DOUBLE PRECISION,
    estimated_diameter_km_max DOUBLE PRECISION,
    estimated_diameter_m_min DOUBLE PRECISION,
    estimated_diameter_m_max DOUBLE PRECISION,
    estimated_diameter_miles_min DOUBLE PRECISION,
    estimated_diameter_miles_max DOUBLE PRECISION,
    estimated_diameter_feet_min DOUBLE PRECISION,
    estimated_diameter_feet_max DOUBLE PRECISION,
    
    is_potentially_hazardous BOOLEAN,
    is_sentry_object BOOLEAN,
    close_approach_date_full TEXT,
    epoch_date_close_approach BIGINT,
    orbiting_body TEXT,
    
    -- Velocity & Distance: Using NUMERIC for exact precision matching JSON strings
    relative_velocity_km_s NUMERIC,
    relative_velocity_km_h NUMERIC,
    relative_velocity_mph NUMERIC,
    
    miss_distance_astronomical NUMERIC,
    miss_distance_lunar NUMERIC,
    miss_distance_km NUMERIC,
    miss_distance_miles NUMERIC,
    
    nasa_jpl_url TEXT,
    row_hash TEXT,
    ingestion_time TIMESTAMPTZ DEFAULT now(),
    
    -- Constraint: Same asteroid, different dates allowed. Same asteroid, same date NOT allowed.
    CONSTRAINT neo_close_approaches_pk 
        PRIMARY KEY (close_approach_date, asteroid_id)
);

-- Hypertable
SELECT create_hypertable(
    'neo_close_approaches',
    'close_approach_date',
    chunk_time_interval => INTERVAL '1 year',
    if_not_exists => TRUE
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_neo_asteroid_id ON neo_close_approaches (asteroid_id, close_approach_date DESC);
CREATE INDEX IF NOT EXISTS idx_neo_hazardous ON neo_close_approaches (close_approach_date, is_potentially_hazardous) WHERE is_potentially_hazardous = TRUE;

-- Compression
ALTER TABLE neo_close_approaches SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'asteroid_id'
);
SELECT add_compression_policy('neo_close_approaches', INTERVAL '6 months', if_not_exists => TRUE);

-- Index for fast text search on asteroid name/id
CREATE INDEX IF NOT EXISTS idx_neo_name_trgm ON neo_close_approaches USING gin (name gin_trgm_ops);

-- Materialized view for analytics dashboard
CREATE MATERIALIZED VIEW IF NOT EXISTS neo_analytics_summary AS
SELECT
  date_trunc('month', close_approach_date) as month,
  COUNT(*) as approach_count,
  SUM(CASE WHEN is_potentially_hazardous THEN 1 ELSE 0 END) as hazardous_count,
  AVG(relative_velocity_km_s::float) as avg_velocity,
  MIN(miss_distance_km::float) as min_distance,
  AVG(estimated_diameter_km_max) as avg_diameter
FROM neo_close_approaches
GROUP BY 1;

-- ==========================================
-- MULTI-AGENCY RELATIONAL TABLES
-- ==========================================

-- A. JPL SBDB (Physical & Orbital Params)
CREATE TABLE IF NOT EXISTS neo_agency_sbdb (
    asteroid_id TEXT PRIMARY KEY,
    orbit_class TEXT,
    spkid TEXT,
    epoch_tdb DOUBLE PRECISION,
    data_arc_days INTEGER,
    n_obs_used INTEGER,
    condition_code TEXT,
    moid_au DOUBLE PRECISION,
    first_obs_date DATE,
    last_obs_date DATE,
    absolute_magnitude_h DOUBLE PRECISION,
    diameter_km DOUBLE PRECISION,
    albedo DOUBLE PRECISION,
    discovery_date DATE,
    discovery_site TEXT,
    ingestion_time TIMESTAMPTZ DEFAULT now(),
    row_hash TEXT
);

-- B. JPL Sentry (Impact Risk)
CREATE TABLE IF NOT EXISTS neo_agency_sentry (
    asteroid_id TEXT PRIMARY KEY,
    status TEXT,
    impact_probability DOUBLE PRECISION,
    torino_scale INTEGER,
    palermo_scale DOUBLE PRECISION,
    n_impacts INTEGER,
    v_infinity_km_s DOUBLE PRECISION,
    energy_mt DOUBLE PRECISION,
    ingestion_time TIMESTAMPTZ DEFAULT now(),
    row_hash TEXT
);

-- C. ESA NEOCC (Risk List)
CREATE TABLE IF NOT EXISTS neo_agency_esa (
    asteroid_id TEXT PRIMARY KEY,
    on_risk_list BOOLEAN,
    priority_list INTEGER,
    ingestion_time TIMESTAMPTZ DEFAULT now(),
    row_hash TEXT
);

-- D. IAU MPC (Official Orbit Status)
CREATE TABLE IF NOT EXISTS neo_agency_mpc (
    asteroid_id TEXT PRIMARY KEY,
    status TEXT,
    mpc_designation TEXT,
    epoch DOUBLE PRECISION,
    eccentricity DOUBLE PRECISION,
    inclination DOUBLE PRECISION,
    ingestion_time TIMESTAMPTZ DEFAULT now(),
    row_hash TEXT
);

-- E. JPL CAD (Close Approaches)
-- 1-to-Many relation with asteroid
CREATE TABLE IF NOT EXISTS neo_agency_cad (
    asteroid_id TEXT NOT NULL,
    approach_date DATE NOT NULL,
    distance_au DOUBLE PRECISION,
    v_rel_km_s DOUBLE PRECISION,
    ingestion_time TIMESTAMPTZ DEFAULT now(),
    row_hash TEXT,
    CONSTRAINT neo_agency_cad_pk PRIMARY KEY (asteroid_id, approach_date)
);

-- Add GIN index for fuzzy search on asteroid names/IDs
CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE INDEX IF NOT EXISTS idx_neo_asteroid_id_trgm ON neo_close_approaches USING gin (asteroid_id gin_trgm_ops);

-- Track external-source misses so old or sparse designations do not get
-- re-requested every ingestion cycle after a confirmed 404.
CREATE TABLE IF NOT EXISTS neo_agency_fetch_status (
    asteroid_id TEXT NOT NULL,
    source TEXT NOT NULL,
    status_code INTEGER,
    last_failure_type TEXT,
    last_checked_at TIMESTAMPTZ DEFAULT now(),
    not_found_until TIMESTAMPTZ,
    PRIMARY KEY (asteroid_id, source)
);

CREATE INDEX IF NOT EXISTS idx_neo_agency_fetch_status_hold
    ON neo_agency_fetch_status (source, not_found_until)
    WHERE status_code = 404;

-- Support for Grafana Dashboards
CREATE OR REPLACE VIEW neo_daily_metrics AS
WITH processed AS (
    SELECT
        close_approach_date,
        COALESCE(estimated_diameter_km_max, estimated_diameter_km_min) AS diameter_mean_km,
        relative_velocity_km_s::float AS relative_velocity_km_s,
        miss_distance_km::float AS miss_distance_km,
        is_potentially_hazardous,
        CASE
            WHEN miss_distance_km IS NULL THEN 0
            ELSE LEAST(1.0, GREATEST(
                0.0,
                (COALESCE(relative_velocity_km_s::float, 0) / 40.0 * 0.35) +
                (COALESCE(estimated_diameter_km_max, 0) / 5.0 * 0.35) +
                ((1.0 / GREATEST(miss_distance_km::float, 1.0)) * 1000000.0 * 0.30) +
                (CASE WHEN is_potentially_hazardous THEN 0.15 ELSE 0 END)
            ))
        END AS risk_score
    FROM neo_close_approaches
),
scored AS (
    SELECT
        *,
        CASE
            WHEN STDDEV_POP(diameter_mean_km) OVER () = 0 THEN 0
            ELSE (diameter_mean_km - AVG(diameter_mean_km) OVER ()) / NULLIF(STDDEV_POP(diameter_mean_km) OVER (), 0)
        END AS diameter_zscore,
        CASE
            WHEN STDDEV_POP(relative_velocity_km_s) OVER () = 0 THEN 0
            ELSE (relative_velocity_km_s - AVG(relative_velocity_km_s) OVER ()) / NULLIF(STDDEV_POP(relative_velocity_km_s) OVER (), 0)
        END AS velocity_zscore,
        CASE
            WHEN STDDEV_POP(miss_distance_km) OVER () = 0 THEN 0
            ELSE (miss_distance_km - AVG(miss_distance_km) OVER ()) / NULLIF(STDDEV_POP(miss_distance_km) OVER (), 0)
        END AS miss_distance_zscore
    FROM processed
)
SELECT
    date_trunc('day', close_approach_date)::date AS metric_date,
    COUNT(*) AS asteroid_count,
    COUNT(*) FILTER (WHERE is_potentially_hazardous = TRUE) AS hazardous_count,
    AVG(relative_velocity_km_s) AS avg_velocity_km_s,
    AVG(diameter_mean_km) AS avg_diameter_km,
    COUNT(*) FILTER (WHERE risk_score >= 0.6) AS high_risk_count,
    AVG(risk_score) AS avg_risk_score,
    MAX(relative_velocity_km_s) AS max_velocity_km_s,
    MAX(diameter_mean_km) AS max_diameter_km,
    MIN(miss_distance_km) AS min_miss_distance_km,
    AVG(miss_distance_km) AS avg_miss_distance_km,
    COUNT(*) FILTER (
        WHERE GREATEST(
            ABS(COALESCE(diameter_zscore, 0)),
            ABS(COALESCE(velocity_zscore, 0)),
            ABS(COALESCE(miss_distance_zscore, 0))
        ) > 2
    ) AS anomaly_count
FROM scored
GROUP BY 1
ORDER BY 1;

CREATE OR REPLACE VIEW neo_processed AS
WITH base AS (
    SELECT
        close_approach_date,
        asteroid_id,
        name,
        is_potentially_hazardous,
        COALESCE(estimated_diameter_km_max, estimated_diameter_km_min) AS diameter_mean_km,
        relative_velocity_km_s::float AS relative_velocity_km_s,
        miss_distance_km::float AS miss_distance_km,
        absolute_magnitude_h,
        nasa_jpl_url
    FROM neo_close_approaches
),
scored AS (
    SELECT
        *,
        LEAST(1.0, GREATEST(
            0.0,
            (COALESCE(relative_velocity_km_s, 0) / 40.0 * 0.35) +
            (COALESCE(diameter_mean_km, 0) / 5.0 * 0.35) +
            ((1.0 / GREATEST(COALESCE(miss_distance_km, 1.0), 1.0)) * 1000000.0 * 0.30) +
            (CASE WHEN is_potentially_hazardous THEN 0.15 ELSE 0 END)
        )) AS risk_score,
        CASE
            WHEN STDDEV_POP(diameter_mean_km) OVER () = 0 THEN 0
            ELSE (diameter_mean_km - AVG(diameter_mean_km) OVER ()) / NULLIF(STDDEV_POP(diameter_mean_km) OVER (), 0)
        END AS diameter_zscore,
        CASE
            WHEN STDDEV_POP(relative_velocity_km_s) OVER () = 0 THEN 0
            ELSE (relative_velocity_km_s - AVG(relative_velocity_km_s) OVER ()) / NULLIF(STDDEV_POP(relative_velocity_km_s) OVER (), 0)
        END AS velocity_zscore,
        CASE
            WHEN STDDEV_POP(miss_distance_km) OVER () = 0 THEN 0
            ELSE (miss_distance_km - AVG(miss_distance_km) OVER ()) / NULLIF(STDDEV_POP(miss_distance_km) OVER (), 0)
        END AS miss_distance_zscore
    FROM base
)
SELECT
    *,
    CASE
        WHEN risk_score >= 0.75 THEN 'critical'
        WHEN risk_score >= 0.50 THEN 'high'
        WHEN risk_score >= 0.25 THEN 'medium'
        ELSE 'low'
    END AS risk_level,
    GREATEST(
        ABS(COALESCE(diameter_zscore, 0)),
        ABS(COALESCE(velocity_zscore, 0)),
        ABS(COALESCE(miss_distance_zscore, 0))
    ) > 2 AS is_anomaly
FROM scored;

