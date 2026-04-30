-- ============================================================
-- HISTORY TABLES — mirrors main v4 schema for CDC tracking
-- Schema built from LIVE API field discovery (2026-04-27)
-- Zero JSONB — all fields explicit
-- ============================================================

-- A. NeoWs Close Approaches History
CREATE TABLE IF NOT EXISTS neo_close_approaches_history (
    history_id SERIAL PRIMARY KEY,
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
    archived_at TIMESTAMPTZ DEFAULT now(),
    change_type TEXT
);
CREATE INDEX IF NOT EXISTS idx_history_asteroid_id ON neo_close_approaches_history (asteroid_id);
CREATE INDEX IF NOT EXISTS idx_history_date ON neo_close_approaches_history (close_approach_date);


-- B. JPL SBDB History — 63 columns from live API
-- Fields: object(13) + orbit_meta(12) + elements(11) + orbit_quality(11) + phys(7) + discovery(9)
CREATE TABLE IF NOT EXISTS neo_agency_sbdb_history (
    history_id SERIAL PRIMARY KEY,
    asteroid_id TEXT NOT NULL,
    -- object
    designation TEXT, fullname TEXT, shortname TEXT, spkid TEXT,
    object_kind TEXT, prefix TEXT, orbit_class TEXT, orbit_class_name TEXT,
    orbit_id TEXT, is_neo BOOLEAN, is_pha BOOLEAN, des_alt TEXT,
    -- orbit metadata
    epoch_tdb TEXT, cov_epoch TEXT, equinox TEXT, orbit_source TEXT,
    producer TEXT, soln_date TEXT, pe_used TEXT, sb_used TEXT,
    two_body TEXT, comment TEXT, not_valid_before TEXT, not_valid_after TEXT,
    -- Keplerian elements
    eccentricity DOUBLE PRECISION, semi_major_axis_au DOUBLE PRECISION,
    perihelion_dist_au DOUBLE PRECISION, aphelion_dist_au DOUBLE PRECISION,
    inclination_deg DOUBLE PRECISION, long_asc_node_deg DOUBLE PRECISION,
    arg_perihelion_deg DOUBLE PRECISION, mean_anomaly_deg DOUBLE PRECISION,
    time_perihelion_tdb TEXT, orbital_period_days DOUBLE PRECISION,
    mean_motion_deg_d DOUBLE PRECISION,
    -- orbit quality
    moid_au DOUBLE PRECISION, moid_jup DOUBLE PRECISION, t_jup DOUBLE PRECISION,
    condition_code TEXT, data_arc_days INTEGER, n_obs_used INTEGER,
    n_del_obs_used INTEGER, n_dop_obs_used INTEGER, rms DOUBLE PRECISION,
    first_obs_date TEXT, last_obs_date TEXT, model_pars TEXT,
    -- physical
    absolute_magnitude_h DOUBLE PRECISION, magnitude_slope_g DOUBLE PRECISION,
    diameter_km DOUBLE PRECISION, albedo DOUBLE PRECISION,
    rotation_period_h DOUBLE PRECISION, thermal_inertia TEXT, spectral_type TEXT,
    -- discovery
    discovery_date TEXT, discovery_site TEXT, discovery_location TEXT,
    discovery_who TEXT, discovery_name TEXT, discovery_ref TEXT,
    discovery_cref TEXT, discovery_text TEXT, discovery_citation TEXT,
    -- meta
    ingestion_time TIMESTAMPTZ, row_hash TEXT,
    archived_at TIMESTAMPTZ DEFAULT now(),
    change_type TEXT
);
CREATE INDEX IF NOT EXISTS idx_sbdb_hist_asteroid ON neo_agency_sbdb_history (asteroid_id);
CREATE INDEX IF NOT EXISTS idx_sbdb_hist_archived ON neo_agency_sbdb_history (archived_at);


-- C. JPL Sentry History — 26 columns from live API
-- summary: des, fullname, method, ip, ts_max, ps_cum, ps_max, n_imp,
--   v_inf, v_imp, energy, mass, diameter, h, first_obs, last_obs,
--   darc, nobs, ndel, ndop, nsat, pdate, cdate
-- watchlist extras: id (sentry_id), range (impact_date_range), last_obs_jd
CREATE TABLE IF NOT EXISTS neo_agency_sentry_history (
    history_id SERIAL PRIMARY KEY,
    asteroid_id TEXT NOT NULL,
    status TEXT, designation TEXT, fullname TEXT, sentry_id TEXT, method TEXT,
    impact_probability DOUBLE PRECISION, torino_scale INTEGER,
    palermo_scale_cum DOUBLE PRECISION, palermo_scale_max DOUBLE PRECISION,
    n_impacts INTEGER, impact_date_range TEXT,
    v_infinity_km_s DOUBLE PRECISION, v_impact_km_s DOUBLE PRECISION,
    energy_mt DOUBLE PRECISION, mass_kg DOUBLE PRECISION,
    diameter_km DOUBLE PRECISION, h_mag DOUBLE PRECISION,
    first_obs TEXT, last_obs TEXT, last_obs_jd TEXT, arc_days TEXT,
    n_obs INTEGER, n_del INTEGER, n_dop INTEGER, n_sat TEXT,
    pdate TEXT, cdate TEXT, removed_date TEXT,
    ingestion_time TIMESTAMPTZ, row_hash TEXT,
    archived_at TIMESTAMPTZ DEFAULT now(),
    change_type TEXT
);
CREATE INDEX IF NOT EXISTS idx_sentry_hist_asteroid ON neo_agency_sentry_history (asteroid_id);
CREATE INDEX IF NOT EXISTS idx_sentry_hist_archived ON neo_agency_sentry_history (archived_at);


-- D. ESA NEOCC History — 14 columns from pipe-delimited file
-- esa_designation, esa_name, diameter_m, diameter_certain,
-- vi_date, ip_max, ps_max, ts, vel_km_s, years, ip_cum, ps_cum
CREATE TABLE IF NOT EXISTS neo_agency_esa_history (
    history_id SERIAL PRIMARY KEY,
    asteroid_id TEXT NOT NULL,
    on_risk_list BOOLEAN, esa_designation TEXT, esa_name TEXT,
    diameter_m DOUBLE PRECISION, diameter_certain BOOLEAN,
    vi_date TEXT, ip_max DOUBLE PRECISION, ps_max DOUBLE PRECISION,
    ts INTEGER, vel_km_s DOUBLE PRECISION,
    years TEXT, ip_cum DOUBLE PRECISION, ps_cum DOUBLE PRECISION,
    ingestion_time TIMESTAMPTZ, row_hash TEXT,
    archived_at TIMESTAMPTZ DEFAULT now(),
    change_type TEXT
);
CREATE INDEX IF NOT EXISTS idx_esa_hist_asteroid ON neo_agency_esa_history (asteroid_id);
CREATE INDEX IF NOT EXISTS idx_esa_hist_archived ON neo_agency_esa_history (archived_at);


-- E. JPL CAD History — 16 columns (NO JSONB)
-- API: des, orbit_id, jd, cd, dist, dist_min, dist_max, v_rel,
--   v_inf, t_sigma_f, h, diameter, diameter_sigma, fullname
-- + approach_date (parsed), body (query param)
CREATE TABLE IF NOT EXISTS neo_agency_cad_history (
    history_id SERIAL PRIMARY KEY,
    asteroid_id TEXT NOT NULL,
    approach_date DATE NOT NULL,
    approach_datetime TEXT, orbit_id TEXT, jd DOUBLE PRECISION,
    distance_au DOUBLE PRECISION, distance_min_au DOUBLE PRECISION,
    distance_max_au DOUBLE PRECISION, v_rel_km_s DOUBLE PRECISION,
    v_inf_km_s DOUBLE PRECISION, t_sigma_f TEXT,
    h_mag DOUBLE PRECISION, diameter_km DOUBLE PRECISION,
    diameter_sigma DOUBLE PRECISION, fullname TEXT, body TEXT,
    ingestion_time TIMESTAMPTZ, row_hash TEXT,
    archived_at TIMESTAMPTZ DEFAULT now(),
    change_type TEXT
);
CREATE INDEX IF NOT EXISTS idx_cad_hist_asteroid ON neo_agency_cad_history (asteroid_id);
CREATE INDEX IF NOT EXISTS idx_cad_hist_archived ON neo_agency_cad_history (archived_at);


-- F. Fireball Events History — 9 columns (NO JSONB)
-- API fields: date, energy, impact-e, lat, lat-dir, lon, lon-dir, alt, vel
CREATE TABLE IF NOT EXISTS neo_fireball_events_history (
    history_id SERIAL PRIMARY KEY,
    event_date TIMESTAMPTZ NOT NULL,
    total_radiated_energy_j DOUBLE PRECISION,
    impact_energy_kt DOUBLE PRECISION,
    latitude DOUBLE PRECISION, latitude_dir TEXT,
    longitude DOUBLE PRECISION, longitude_dir TEXT,
    altitude_km DOUBLE PRECISION, velocity_km_s DOUBLE PRECISION,
    ingestion_time TIMESTAMPTZ, row_hash TEXT,
    archived_at TIMESTAMPTZ DEFAULT now(),
    change_type TEXT
);
CREATE INDEX IF NOT EXISTS idx_fireball_hist_date ON neo_fireball_events_history (event_date);
CREATE INDEX IF NOT EXISTS idx_fireball_hist_archived ON neo_fireball_events_history (archived_at);
