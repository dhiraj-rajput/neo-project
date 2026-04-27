-- ============================================================
-- NEO Multi-Agency Database Schema (v4 — Zero JSONB)
-- Schema built from LIVE API responses — every field captured
--
-- SBDB  : object(11) + orbit(24) + elements(11 Keplerian)
--         + phys_par(7) + discovery(9) = 62 columns
-- Sentry: summary(23 fields) + watchlist extras(3) = 26 cols
-- CAD   : 14 API fields + approach_date parsed = 15 cols
-- Fireball: 9 API fields
-- ESA   : 12 parsed fields from pipe-delimited file
-- ============================================================

CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- ==========================================================
-- 1. CORE NeoWs CLOSE-APPROACH TABLE
-- ==========================================================

CREATE TABLE IF NOT EXISTS neo_close_approaches (
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
    ingestion_time TIMESTAMPTZ DEFAULT now(),

    CONSTRAINT neo_close_approaches_pk
        PRIMARY KEY (close_approach_date, asteroid_id)
);

SELECT create_hypertable(
    'neo_close_approaches',
    'close_approach_date',
    chunk_time_interval => INTERVAL '1 year',
    if_not_exists => TRUE
);

CREATE INDEX IF NOT EXISTS idx_neo_asteroid_id    ON neo_close_approaches (asteroid_id, close_approach_date DESC);
CREATE INDEX IF NOT EXISTS idx_neo_hazardous       ON neo_close_approaches (close_approach_date, is_potentially_hazardous) WHERE is_potentially_hazardous = TRUE;
CREATE INDEX IF NOT EXISTS idx_neo_name_trgm       ON neo_close_approaches USING gin (name gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_neo_asteroid_id_trgm ON neo_close_approaches USING gin (asteroid_id gin_trgm_ops);

ALTER TABLE neo_close_approaches SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'asteroid_id'
);
SELECT add_compression_policy('neo_close_approaches', INTERVAL '6 months', if_not_exists => TRUE);


-- ==========================================================
-- 2. JPL SBDB — Physical & Orbital Parameters
--    Source: ssd-api.jpl.nasa.gov/sbdb.api
--    Live response keys (Apophis 99942):
--      object: kind, prefix, des_alt, fullname, neo, spkid,
--              orbit_class{code,name}, des, orbit_id, pha, shortname
--      orbit:  epoch, cov_epoch, orbit_id, equinox, n_obs_used,
--              n_dop_obs_used, rms, first_obs, producer, sb_used,
--              soln_date, moid_jup, not_valid_after, model_pars,
--              source, t_jup, data_arc, moid, last_obs,
--              not_valid_before, two_body, n_del_obs_used,
--              condition_code, pe_used, comment
--      elements: e, a, q, i, om, w, ma, tp, per, n, ad
--      phys_par: H, G, diameter, rot_per, albedo, I, spec_B
--      discovery: location, who, name, ref, site, date, citation,
--                 discovery(text), cref
-- ==========================================================

CREATE TABLE IF NOT EXISTS neo_agency_sbdb (
    asteroid_id TEXT PRIMARY KEY,

    -- object fields
    designation TEXT,                      -- object.des
    fullname TEXT,                         -- object.fullname
    shortname TEXT,                        -- object.shortname
    spkid TEXT,                            -- object.spkid
    object_kind TEXT,                      -- object.kind (an/au/cn/cu)
    prefix TEXT,                           -- object.prefix
    orbit_class TEXT,                      -- object.orbit_class.code
    orbit_class_name TEXT,                 -- object.orbit_class.name
    orbit_id TEXT,                         -- object.orbit_id
    is_neo BOOLEAN DEFAULT FALSE,          -- object.neo
    is_pha BOOLEAN DEFAULT FALSE,          -- object.pha
    des_alt TEXT,                          -- object.des_alt serialised as JSON string

    -- orbit metadata
    epoch_tdb TEXT,                        -- orbit.epoch (JD as string)
    cov_epoch TEXT,                        -- orbit.cov_epoch
    equinox TEXT,                          -- orbit.equinox
    orbit_source TEXT,                     -- orbit.source
    producer TEXT,                         -- orbit.producer
    soln_date TEXT,                        -- orbit.soln_date
    pe_used TEXT,                          -- orbit.pe_used
    sb_used TEXT,                          -- orbit.sb_used
    two_body TEXT,                         -- orbit.two_body
    comment TEXT,                          -- orbit.comment
    not_valid_before TEXT,                 -- orbit.not_valid_before
    not_valid_after TEXT,                  -- orbit.not_valid_after

    -- Keplerian elements (orbit.elements[])
    eccentricity DOUBLE PRECISION,         -- e
    semi_major_axis_au DOUBLE PRECISION,   -- a
    perihelion_dist_au DOUBLE PRECISION,   -- q
    aphelion_dist_au DOUBLE PRECISION,     -- ad
    inclination_deg DOUBLE PRECISION,      -- i
    long_asc_node_deg DOUBLE PRECISION,    -- om
    arg_perihelion_deg DOUBLE PRECISION,   -- w
    mean_anomaly_deg DOUBLE PRECISION,     -- ma
    time_perihelion_tdb TEXT,              -- tp (JD string e.g. "2461042.918")
    orbital_period_days DOUBLE PRECISION,  -- per
    mean_motion_deg_d DOUBLE PRECISION,    -- n

    -- orbit quality
    moid_au DOUBLE PRECISION,              -- orbit.moid
    moid_jup DOUBLE PRECISION,             -- orbit.moid_jup
    t_jup DOUBLE PRECISION,               -- orbit.t_jup
    condition_code TEXT,                   -- orbit.condition_code
    data_arc_days INTEGER,                 -- orbit.data_arc
    n_obs_used INTEGER,                    -- orbit.n_obs_used
    n_del_obs_used INTEGER,                -- orbit.n_del_obs_used
    n_dop_obs_used INTEGER,                -- orbit.n_dop_obs_used
    rms DOUBLE PRECISION,                  -- orbit.rms

    first_obs_date TEXT,                   -- orbit.first_obs  "2004-03-15"
    last_obs_date TEXT,                    -- orbit.last_obs   "2022-04-09"
    model_pars TEXT,                       -- orbit.model_pars serialised JSON

    -- physical parameters (phys_par[])
    absolute_magnitude_h DOUBLE PRECISION, -- H
    magnitude_slope_g DOUBLE PRECISION,    -- G
    diameter_km DOUBLE PRECISION,          -- diameter (km)
    albedo DOUBLE PRECISION,               -- albedo
    rotation_period_h DOUBLE PRECISION,    -- rot_per (h)
    thermal_inertia TEXT,                  -- I (range string e.g. "50-500")
    spectral_type TEXT,                    -- spec_B

    -- discovery block
    discovery_date TEXT,                   -- discovery.date  "2004-Jun-19"
    discovery_site TEXT,                   -- discovery.site
    discovery_location TEXT,               -- discovery.location
    discovery_who TEXT,                    -- discovery.who
    discovery_name TEXT,                   -- discovery.name
    discovery_ref TEXT,                    -- discovery.ref
    discovery_cref TEXT,                   -- discovery.cref
    discovery_text TEXT,                   -- discovery.discovery (full text)
    discovery_citation TEXT,               -- discovery.citation

    ingestion_time TIMESTAMPTZ DEFAULT now(),
    row_hash TEXT
);

CREATE INDEX IF NOT EXISTS idx_sbdb_designation  ON neo_agency_sbdb (designation);
CREATE INDEX IF NOT EXISTS idx_sbdb_spkid         ON neo_agency_sbdb (spkid);
CREATE INDEX IF NOT EXISTS idx_sbdb_orbit_class   ON neo_agency_sbdb (orbit_class);
CREATE INDEX IF NOT EXISTS idx_sbdb_is_pha        ON neo_agency_sbdb (is_pha) WHERE is_pha = TRUE;
CREATE INDEX IF NOT EXISTS idx_sbdb_moid          ON neo_agency_sbdb (moid_au);


-- ==========================================================
-- 3. JPL SENTRY — Impact Risk Assessment
--    Source: ssd-api.jpl.nasa.gov/sentry.api
--    summary keys (live): darc, ndop, ps_cum, mass, energy,
--      ts_max, v_imp, method, des, ip, fullname, ps_max,
--      first_obs, nobs, last_obs, h, v_inf, nsat, pdate,
--      n_imp, diameter, ndel, cdate
--    watchlist data[] extra keys: last_obs_jd, range, id
-- ==========================================================

CREATE TABLE IF NOT EXISTS neo_agency_sentry (
    asteroid_id TEXT PRIMARY KEY,

    status TEXT NOT NULL DEFAULT 'unknown',   -- 'active', 'removed', 'not_found'
    designation TEXT,                          -- summary.des
    fullname TEXT,                             -- summary.fullname
    sentry_id TEXT,                            -- watchlist data[].id
    method TEXT,                               -- summary.method (IOBS/LOV/MC)

    -- impact risk
    impact_probability DOUBLE PRECISION,       -- summary.ip
    torino_scale INTEGER,                      -- summary.ts_max
    palermo_scale_cum DOUBLE PRECISION,        -- summary.ps_cum
    palermo_scale_max DOUBLE PRECISION,        -- summary.ps_max
    n_impacts INTEGER,                         -- summary.n_imp
    impact_date_range TEXT,                    -- watchlist data[].range e.g. "2056-2113"

    -- physical
    v_infinity_km_s DOUBLE PRECISION,          -- summary.v_inf
    v_impact_km_s DOUBLE PRECISION,            -- summary.v_imp
    energy_mt DOUBLE PRECISION,                -- summary.energy
    mass_kg DOUBLE PRECISION,                  -- summary.mass
    diameter_km DOUBLE PRECISION,              -- summary.diameter
    h_mag DOUBLE PRECISION,                    -- summary.h

    -- observations
    first_obs TEXT,                            -- summary.first_obs
    last_obs TEXT,                             -- summary.last_obs
    last_obs_jd TEXT,                          -- watchlist data[].last_obs_jd
    arc_days TEXT,                             -- summary.darc
    n_obs INTEGER,                             -- summary.nobs
    n_del INTEGER,                             -- summary.ndel
    n_dop INTEGER,                             -- summary.ndop
    n_sat TEXT,                                -- summary.nsat

    -- timestamps
    pdate TEXT,                                -- summary.pdate
    cdate TEXT,                                -- summary.cdate
    removed_date TEXT,                         -- set when status='removed'

    ingestion_time TIMESTAMPTZ DEFAULT now(),
    row_hash TEXT
);

CREATE INDEX IF NOT EXISTS idx_sentry_palermo ON neo_agency_sentry (palermo_scale_cum);
CREATE INDEX IF NOT EXISTS idx_sentry_status  ON neo_agency_sentry (status);
CREATE INDEX IF NOT EXISTS idx_sentry_torino  ON neo_agency_sentry (torino_scale) WHERE torino_scale > 0;


-- ==========================================================
-- 4. ESA NEOCC — Risk List (pipe-delimited file)
--    Source: neo.ssa.esa.int/PSDB-portlet/download?file=esa_risk_list
--    Parsed columns: Num/des, Name, Diameter_m, diameter_starred,
--      VI_date, ip_max, ps_max, ts, vel_km_s, years, ip_cum, ps_cum
-- ==========================================================

CREATE TABLE IF NOT EXISTS neo_agency_esa (
    asteroid_id TEXT PRIMARY KEY,            -- Num/des (normalised)

    on_risk_list BOOLEAN DEFAULT TRUE,
    esa_designation TEXT,                    -- Num/des raw
    esa_name TEXT,                           -- Name field

    diameter_m DOUBLE PRECISION,             -- Diameter m
    diameter_certain BOOLEAN,               -- starred (*=Y) means diameter certain

    vi_date TEXT,                            -- Date/Time of max VI e.g. "2034-11-08 17:08"
    ip_max DOUBLE PRECISION,                 -- IP max
    ps_max DOUBLE PRECISION,                 -- PS max
    ts INTEGER,                              -- Torino scale
    vel_km_s DOUBLE PRECISION,               -- Vel km/s

    years TEXT,                              -- Impact years range e.g. "2034-2039"
    ip_cum DOUBLE PRECISION,                 -- IP cumulative
    ps_cum DOUBLE PRECISION,                 -- PS cumulative

    ingestion_time TIMESTAMPTZ DEFAULT now(),
    row_hash TEXT
);

CREATE INDEX IF NOT EXISTS idx_esa_ps_max ON neo_agency_esa (ps_max);
CREATE INDEX IF NOT EXISTS idx_esa_ts     ON neo_agency_esa (ts) WHERE ts > 0;


-- ==========================================================
-- 6. JPL CAD — Close-Approach Records (NO JSONB)
--    Source: ssd-api.jpl.nasa.gov/cad.api
--    API fields (live): des, orbit_id, jd, cd, dist, dist_min,
--      dist_max, v_rel, v_inf, t_sigma_f, h, diameter,
--      diameter_sigma, fullname
--    + body from query param, + approach_date parsed from cd
-- ==========================================================

CREATE TABLE IF NOT EXISTS neo_agency_cad (
    asteroid_id TEXT NOT NULL,             -- des
    approach_date DATE NOT NULL,           -- parsed from cd

    approach_datetime TEXT,               -- cd raw e.g. "1905-Dec-26 05:03"
    orbit_id TEXT,                         -- orbit_id
    jd DOUBLE PRECISION,                   -- jd (Julian Date)
    distance_au DOUBLE PRECISION,          -- dist
    distance_min_au DOUBLE PRECISION,      -- dist_min
    distance_max_au DOUBLE PRECISION,      -- dist_max
    v_rel_km_s DOUBLE PRECISION,           -- v_rel
    v_inf_km_s DOUBLE PRECISION,           -- v_inf
    t_sigma_f TEXT,                        -- t_sigma_f (e.g. "< 00:01")
    h_mag DOUBLE PRECISION,               -- h
    diameter_km DOUBLE PRECISION,          -- diameter (km)
    diameter_sigma DOUBLE PRECISION,       -- diameter_sigma
    fullname TEXT,                         -- fullname
    body TEXT DEFAULT 'Earth',             -- query param body

    ingestion_time TIMESTAMPTZ DEFAULT now(),
    row_hash TEXT,

    CONSTRAINT neo_agency_cad_pk PRIMARY KEY (asteroid_id, approach_date)
);

CREATE INDEX IF NOT EXISTS idx_cad_date     ON neo_agency_cad (approach_date DESC);
CREATE INDEX IF NOT EXISTS idx_cad_distance ON neo_agency_cad (distance_au);
CREATE INDEX IF NOT EXISTS idx_cad_future   ON neo_agency_cad (approach_date) WHERE approach_date >= CURRENT_DATE;


-- ==========================================================
-- 7. JPL FIREBALL — Bolide Detection Events (NO JSONB)
--    Source: ssd-api.jpl.nasa.gov/fireball.api
--    API fields (live): date, energy, impact-e, lat, lat-dir,
--      lon, lon-dir, alt, vel
-- ==========================================================

CREATE TABLE IF NOT EXISTS neo_fireball_events (
    event_date TIMESTAMPTZ NOT NULL,       -- date

    total_radiated_energy_j DOUBLE PRECISION,  -- energy (converted from TJ)
    impact_energy_kt DOUBLE PRECISION,         -- impact-e (kt)
    latitude DOUBLE PRECISION,                 -- lat
    latitude_dir TEXT,                         -- lat-dir ('N' or 'S')
    longitude DOUBLE PRECISION,                -- lon
    longitude_dir TEXT,                        -- lon-dir ('E' or 'W')
    altitude_km DOUBLE PRECISION,              -- alt
    velocity_km_s DOUBLE PRECISION,            -- vel

    ingestion_time TIMESTAMPTZ DEFAULT now(),
    row_hash TEXT,

    CONSTRAINT neo_fireball_pk PRIMARY KEY (event_date)
);

CREATE INDEX IF NOT EXISTS idx_fireball_date   ON neo_fireball_events (event_date DESC);
CREATE INDEX IF NOT EXISTS idx_fireball_energy ON neo_fireball_events (impact_energy_kt DESC);


-- ==========================================================
-- 8. FETCH STATUS TRACKING
-- ==========================================================

CREATE TABLE IF NOT EXISTS neo_agency_fetch_status (
    asteroid_id TEXT NOT NULL,
    source TEXT NOT NULL,
    status_code INTEGER,
    last_failure_type TEXT,
    last_checked_at TIMESTAMPTZ DEFAULT now(),
    not_found_until TIMESTAMPTZ,
    lookup_value TEXT,
    PRIMARY KEY (asteroid_id, source)
);


-- ==========================================================
-- 9. VIEWS
-- ==========================================================

-- Daily NeoWs metrics with risk scoring
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

    -- Full asteroid profile joining all 4 agency tables
CREATE OR REPLACE VIEW neo_asteroid_profile AS
SELECT
    n.asteroid_id,
    n.name,
    n.is_potentially_hazardous,
    n.is_sentry_object,
    n.close_approach_date AS last_close_approach,

    -- SBDB physical + orbital
    s.designation AS sbdb_designation,
    s.fullname AS sbdb_fullname,
    s.spkid,
    s.orbit_class,
    s.orbit_class_name,
    s.eccentricity,
    s.semi_major_axis_au,
    s.perihelion_dist_au,
    s.aphelion_dist_au,
    s.inclination_deg,
    s.moid_au,
    s.t_jup,
    s.condition_code,
    s.absolute_magnitude_h AS sbdb_h,
    s.diameter_km AS sbdb_diameter_km,
    s.albedo,
    s.spectral_type,
    s.is_neo,
    s.is_pha,
    s.discovery_date,
    s.discovery_location,

    -- Sentry risk
    sen.status AS sentry_status,
    sen.impact_probability,
    sen.torino_scale,
    sen.palermo_scale_cum,
    sen.palermo_scale_max,
    sen.n_impacts,
    sen.energy_mt,
    sen.v_impact_km_s,
    sen.impact_date_range,

    -- ESA risk
    esa.on_risk_list AS esa_on_risk_list,
    esa.ps_cum AS esa_ps_cum,
    esa.ts AS esa_torino_scale,
    esa.diameter_m AS esa_diameter_m

FROM (
    SELECT DISTINCT ON (asteroid_id)
        asteroid_id, name, is_potentially_hazardous, is_sentry_object, close_approach_date
    FROM neo_close_approaches
    ORDER BY asteroid_id, close_approach_date DESC
) n
LEFT JOIN neo_agency_sbdb    s   ON s.asteroid_id   = n.asteroid_id
LEFT JOIN neo_agency_sentry  sen ON sen.asteroid_id = n.asteroid_id
LEFT JOIN neo_agency_esa     esa ON esa.asteroid_id = n.asteroid_id;

-- Active Sentry watchlist for dashboard
CREATE OR REPLACE VIEW neo_sentry_watchlist AS
SELECT
    s.asteroid_id,
    s.fullname,
    s.designation,
    s.sentry_id,
    s.impact_probability,
    s.torino_scale,
    s.palermo_scale_cum,
    s.palermo_scale_max,
    s.n_impacts,
    s.impact_date_range,
    s.energy_mt,
    s.diameter_km,
    s.v_impact_km_s,
    s.h_mag,
    s.last_obs,
    s.cdate
FROM neo_agency_sentry s
WHERE s.status = 'active'
ORDER BY s.palermo_scale_cum DESC NULLS LAST;

-- Upcoming CAD close approaches (future dates)
CREATE OR REPLACE VIEW neo_cad_upcoming AS
SELECT
    c.asteroid_id,
    c.fullname,
    c.approach_date,
    c.approach_datetime,
    c.distance_au,
    c.distance_min_au,
    c.v_rel_km_s,
    c.h_mag,
    c.diameter_km,
    c.body,
    (sen.asteroid_id IS NOT NULL AND sen.status = 'active') AS on_sentry_watchlist,
    sen.palermo_scale_cum,
    sen.torino_scale,
    esa.on_risk_list AS esa_risk
FROM neo_agency_cad c
LEFT JOIN neo_agency_sentry sen ON sen.asteroid_id = c.asteroid_id
LEFT JOIN neo_agency_esa esa    ON esa.asteroid_id = c.asteroid_id
WHERE c.approach_date >= CURRENT_DATE
ORDER BY c.approach_date ASC;

-- Orbit class breakdown
CREATE OR REPLACE VIEW neo_orbit_class_dist AS
SELECT
    orbit_class,
    orbit_class_name,
    COUNT(*) AS total,
    COUNT(*) FILTER (WHERE is_pha) AS pha_count,
    AVG(diameter_km) AS avg_diameter_km,
    AVG(moid_au) AS avg_moid_au,
    AVG(eccentricity) AS avg_eccentricity,
    AVG(t_jup) AS avg_t_jup
FROM neo_agency_sbdb
WHERE orbit_class IS NOT NULL
GROUP BY orbit_class, orbit_class_name
ORDER BY total DESC;

-- Fireball annual summary
CREATE OR REPLACE VIEW neo_fireball_annual AS
SELECT
    EXTRACT(YEAR FROM event_date)::int AS year,
    COUNT(*) AS event_count,
    SUM(impact_energy_kt) AS total_energy_kt,
    MAX(impact_energy_kt) AS max_energy_kt,
    AVG(velocity_km_s) AS avg_velocity_km_s,
    AVG(altitude_km) AS avg_altitude_km
FROM neo_fireball_events
GROUP BY 1
ORDER BY 1;
