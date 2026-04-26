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

-- ==========================================
-- MULTI-AGENCY HISTORY TABLES
-- ==========================================

-- A. JPL SBDB History
CREATE TABLE IF NOT EXISTS neo_agency_sbdb_history (
    history_id SERIAL PRIMARY KEY,
    asteroid_id TEXT NOT NULL,
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
    ingestion_time TIMESTAMPTZ,
    row_hash TEXT,
    archived_at TIMESTAMPTZ DEFAULT now(),
    change_type TEXT
);

-- B. JPL Sentry History
CREATE TABLE IF NOT EXISTS neo_agency_sentry_history (
    history_id SERIAL PRIMARY KEY,
    asteroid_id TEXT NOT NULL,
    status TEXT,
    impact_probability DOUBLE PRECISION,
    torino_scale INTEGER,
    palermo_scale DOUBLE PRECISION,
    n_impacts INTEGER,
    v_infinity_km_s DOUBLE PRECISION,
    energy_mt DOUBLE PRECISION,
    ingestion_time TIMESTAMPTZ,
    row_hash TEXT,
    archived_at TIMESTAMPTZ DEFAULT now(),
    change_type TEXT
);

-- C. ESA NEOCC History
CREATE TABLE IF NOT EXISTS neo_agency_esa_history (
    history_id SERIAL PRIMARY KEY,
    asteroid_id TEXT NOT NULL,
    on_risk_list BOOLEAN,
    priority_list INTEGER,
    ingestion_time TIMESTAMPTZ,
    row_hash TEXT,
    archived_at TIMESTAMPTZ DEFAULT now(),
    change_type TEXT
);

-- D. IAU MPC History
CREATE TABLE IF NOT EXISTS neo_agency_mpc_history (
    history_id SERIAL PRIMARY KEY,
    asteroid_id TEXT NOT NULL,
    status TEXT,
    mpc_designation TEXT,
    epoch DOUBLE PRECISION,
    eccentricity DOUBLE PRECISION,
    inclination DOUBLE PRECISION,
    ingestion_time TIMESTAMPTZ,
    row_hash TEXT,
    archived_at TIMESTAMPTZ DEFAULT now(),
    change_type TEXT
);

-- E. JPL CAD History
CREATE TABLE IF NOT EXISTS neo_agency_cad_history (
    history_id SERIAL PRIMARY KEY,
    asteroid_id TEXT NOT NULL,
    approach_date DATE NOT NULL,
    distance_au DOUBLE PRECISION,
    v_rel_km_s DOUBLE PRECISION,
    ingestion_time TIMESTAMPTZ,
    row_hash TEXT,
    archived_at TIMESTAMPTZ DEFAULT now(),
    change_type TEXT
);
