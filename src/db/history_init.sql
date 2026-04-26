-- History table mirrors neo_close_approaches

CREATE TABLE IF NOT EXISTS neo_close_approaches_history (
    history_id SERIAL PRIMARY KEY,
    
    -- PK from Main
    close_approach_date DATE NOT NULL,
    asteroid_id TEXT NOT NULL,
    
    neo_reference_id TEXT,
    name TEXT,
    absolute_magnitude_h DOUBLE PRECISION,
    
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
    
    relative_velocity_km_s NUMERIC,
    relative_velocity_km_h NUMERIC,
    relative_velocity_mph NUMERIC,
    
    miss_distance_astronomical NUMERIC,
    miss_distance_lunar NUMERIC,
    miss_distance_km NUMERIC,
    miss_distance_miles NUMERIC,
    
    nasa_jpl_url TEXT,
    row_hash TEXT,
    ingestion_time TIMESTAMPTZ,
    
    -- History Meta
    archived_at TIMESTAMPTZ DEFAULT now(),
    change_type TEXT
);

CREATE INDEX IF NOT EXISTS idx_history_asteroid_id ON neo_close_approaches_history (asteroid_id);
CREATE INDEX IF NOT EXISTS idx_history_date ON neo_close_approaches_history (close_approach_date);
