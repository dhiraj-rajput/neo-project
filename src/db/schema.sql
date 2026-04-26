-- Main Table Schema provided by User

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
