"""
Agency Kafka Consumer / Processor

Consumes messages from the 'agency_ingest' Kafka topic and upserts
into the respective neo_agency_* tables.

Column lists are derived from LIVE API field discovery (2026-04-27)
and exactly match schema.sql v4. Zero JSONB — every field stored.
"""

import json
import hashlib
import os

import psycopg2
import psycopg2.extras
from contextlib import contextmanager
from kafka import KafkaConsumer

from src.config import Config
from src.logger import logger


AGENCY_TOPIC = os.getenv("AGENCY_TOPIC", "agency_ingest")
CONSUMER_GROUP = "agency-processor-v4"


@contextmanager
def db_connection():
    conn = None
    try:
        conn = psycopg2.connect(
            host=Config.DB_HOST, port=Config.DB_PORT,
            dbname=Config.DB_NAME, user=Config.DB_USER, password=Config.DB_PASSWORD,
        )
        yield conn
    except Exception as e:
        logger.error(f"DB connection error: {e}")
        raise
    finally:
        if conn:
            conn.close()


def compute_hash(record: dict) -> str:
    """Deterministic MD5 hash of all values for CDC."""
    raw = "||".join(str(v) for v in sorted(record.items()))
    return hashlib.md5(raw.encode()).hexdigest()


# ──────────────────────────────────────────────────────────────
# SBDB Upsert — 63 columns (object + orbit + elements + phys + discovery)
# Matches: neo_agency_sbdb schema v4
# ──────────────────────────────────────────────────────────────

SBDB_COLUMNS = [
    # object
    "asteroid_id", "designation", "fullname", "shortname", "spkid",
    "object_kind", "prefix", "orbit_class", "orbit_class_name", "orbit_id",
    "is_neo", "is_pha", "des_alt",
    # orbit metadata
    "epoch_tdb", "cov_epoch", "equinox", "orbit_source", "producer",
    "soln_date", "pe_used", "sb_used", "two_body", "comment",
    "not_valid_before", "not_valid_after",
    # Keplerian elements
    "eccentricity", "semi_major_axis_au", "perihelion_dist_au", "aphelion_dist_au",
    "inclination_deg", "long_asc_node_deg", "arg_perihelion_deg",
    "mean_anomaly_deg", "time_perihelion_tdb", "orbital_period_days", "mean_motion_deg_d",
    # orbit quality
    "moid_au", "moid_jup", "t_jup", "condition_code",
    "data_arc_days", "n_obs_used", "n_del_obs_used", "n_dop_obs_used", "rms",
    "first_obs_date", "last_obs_date", "model_pars",
    # physical parameters
    "absolute_magnitude_h", "magnitude_slope_g", "diameter_km", "albedo",
    "rotation_period_h", "thermal_inertia", "spectral_type",
    # discovery
    "discovery_date", "discovery_site", "discovery_location",
    "discovery_who", "discovery_name", "discovery_ref",
    "discovery_cref", "discovery_text", "discovery_citation",
    # meta
    "row_hash",
]


def upsert_sbdb(cur, asteroid_id: str, data: dict):
    data["asteroid_id"] = asteroid_id
    data["row_hash"] = compute_hash(data)
    vals = [data.get(c) for c in SBDB_COLUMNS]
    placeholders = ",".join(["%s"] * len(SBDB_COLUMNS))
    updates = ",".join(f"{c}=EXCLUDED.{c}" for c in SBDB_COLUMNS if c != "asteroid_id")
    sql = f"""
        INSERT INTO neo_agency_sbdb ({','.join(SBDB_COLUMNS)})
        VALUES ({placeholders})
        ON CONFLICT (asteroid_id) DO UPDATE SET {updates}
    """
    cur.execute(sql, vals)


# ──────────────────────────────────────────────────────────────
# Sentry Upsert — 26 columns
# summary: des, fullname, method, ip, ts_max, ps_cum, ps_max, n_imp,
#          v_inf, v_imp, energy, mass, diameter, h, first_obs, last_obs,
#          darc, nobs, ndel, ndop, nsat, pdate, cdate
# watchlist extras: id (→sentry_id), range(→impact_date_range), last_obs_jd
# ──────────────────────────────────────────────────────────────

SENTRY_COLUMNS = [
    "asteroid_id", "status", "designation", "fullname", "sentry_id", "method",
    "impact_probability", "torino_scale", "palermo_scale_cum", "palermo_scale_max",
    "n_impacts", "impact_date_range",
    "v_infinity_km_s", "v_impact_km_s", "energy_mt", "mass_kg",
    "diameter_km", "h_mag",
    "first_obs", "last_obs", "last_obs_jd", "arc_days",
    "n_obs", "n_del", "n_dop", "n_sat",
    "pdate", "cdate", "removed_date",
    "row_hash",
]


def upsert_sentry(cur, asteroid_id: str, data: dict):
    data["asteroid_id"] = asteroid_id
    data["row_hash"] = compute_hash(data)
    vals = [data.get(c) for c in SENTRY_COLUMNS]
    placeholders = ",".join(["%s"] * len(SENTRY_COLUMNS))
    updates = ",".join(f"{c}=EXCLUDED.{c}" for c in SENTRY_COLUMNS if c != "asteroid_id")
    sql = f"""
        INSERT INTO neo_agency_sentry ({','.join(SENTRY_COLUMNS)})
        VALUES ({placeholders})
        ON CONFLICT (asteroid_id) DO UPDATE SET {updates}
    """
    cur.execute(sql, vals)


# ──────────────────────────────────────────────────────────────
# CAD Upsert — 16 columns (14 API fields + approach_date + body)
# API fields: des, orbit_id, jd, cd(→approach_datetime+approach_date),
#   dist, dist_min, dist_max, v_rel, v_inf, t_sigma_f, h,
#   diameter, diameter_sigma, fullname
# ──────────────────────────────────────────────────────────────

CAD_COLUMNS = [
    "asteroid_id", "approach_date",
    "approach_datetime", "orbit_id", "jd",
    "distance_au", "distance_min_au", "distance_max_au",
    "v_rel_km_s", "v_inf_km_s", "t_sigma_f",
    "h_mag", "diameter_km", "diameter_sigma",
    "fullname", "body",
    "row_hash",
]


def upsert_cad(cur, asteroid_id: str, records: list[dict]):
    if not records:
        return
    for rec in records:
        rec["asteroid_id"] = asteroid_id
        rec["row_hash"] = compute_hash(rec)

    placeholders = ",".join(["%s"] * len(CAD_COLUMNS))
    updates = ",".join(f"{c}=EXCLUDED.{c}" for c in CAD_COLUMNS if c not in ("asteroid_id", "approach_date"))
    sql = f"""
        INSERT INTO neo_agency_cad ({','.join(CAD_COLUMNS)})
        VALUES %s
        ON CONFLICT (asteroid_id, approach_date) DO UPDATE SET {updates}
    """
    template = f"({placeholders})"
    values = [tuple(rec.get(c) for c in CAD_COLUMNS) for rec in records]
    psycopg2.extras.execute_values(cur, sql, values, template=template)


# ──────────────────────────────────────────────────────────────
# ESA Upsert — 14 columns matching v4 schema
# Parsed from pipe-delimited file
# ──────────────────────────────────────────────────────────────

ESA_COLUMNS = [
    "asteroid_id", "on_risk_list", "esa_designation", "esa_name",
    "diameter_m", "diameter_certain",
    "vi_date", "ip_max", "ps_max", "ts", "vel_km_s",
    "years", "ip_cum", "ps_cum",
    "row_hash",
]


def upsert_esa(cur, asteroid_id: str, data: dict):
    data["asteroid_id"] = asteroid_id
    data["row_hash"] = compute_hash(data)
    vals = [data.get(c) for c in ESA_COLUMNS]
    placeholders = ",".join(["%s"] * len(ESA_COLUMNS))
    updates = ",".join(f"{c}=EXCLUDED.{c}" for c in ESA_COLUMNS if c != "asteroid_id")
    sql = f"""
        INSERT INTO neo_agency_esa ({','.join(ESA_COLUMNS)})
        VALUES ({placeholders})
        ON CONFLICT (asteroid_id) DO UPDATE SET {updates}
    """
    cur.execute(sql, vals)


# ──────────────────────────────────────────────────────────────
# Fireball Upsert — 9 columns (all API fields)
# API fields: date, energy, impact-e, lat, lat-dir, lon, lon-dir, alt, vel
# ──────────────────────────────────────────────────────────────

FIREBALL_COLUMNS = [
    "event_date",
    "total_radiated_energy_j", "impact_energy_kt",
    "latitude", "latitude_dir", "longitude", "longitude_dir",
    "altitude_km", "velocity_km_s",
    "row_hash",
]


def upsert_fireballs(cur, records: list[dict]):
    if not records:
        return
    for rec in records:
        rec["row_hash"] = compute_hash(rec)

    placeholders = ",".join(["%s"] * len(FIREBALL_COLUMNS))
    updates = ",".join(f"{c}=EXCLUDED.{c}" for c in FIREBALL_COLUMNS if c != "event_date")
    sql = f"""
        INSERT INTO neo_fireball_events ({','.join(FIREBALL_COLUMNS)})
        VALUES %s
        ON CONFLICT (event_date) DO UPDATE SET {updates}
    """
    template = f"({placeholders})"
    values = [tuple(rec.get(c) for c in FIREBALL_COLUMNS) for rec in records]
    psycopg2.extras.execute_values(cur, sql, values, template=template)


# ──────────────────────────────────────────────────────────────
# Main Consumer Loop
# ──────────────────────────────────────────────────────────────

def process_message(msg_bytes: bytes):
    """Process a single Kafka message containing multi-agency data."""
    try:
        payload = json.loads(msg_bytes.decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError) as e:
        logger.warning(f"Skipping malformed message: {e}")
        return

    asteroid_id = payload.get("asteroid_id")
    if not asteroid_id:
        return

    agencies = payload.get("agencies", {})

    with db_connection() as conn:
        with conn.cursor() as cur:
            sbdb = agencies.get("sbdb")
            if sbdb:
                upsert_sbdb(cur, asteroid_id, sbdb)

            sentry = agencies.get("sentry")
            if sentry:
                upsert_sentry(cur, asteroid_id, sentry)

            cad = agencies.get("cad")
            if isinstance(cad, list) and cad:
                upsert_cad(cur, asteroid_id, cad)

            esa = agencies.get("esa")
            if esa:
                upsert_esa(cur, asteroid_id, esa)

            # Fireball events are global (not per asteroid)
            fireballs = agencies.get("fireball")
            if isinstance(fireballs, list) and fireballs:
                upsert_fireballs(cur, fireballs)

        conn.commit()

    logger.debug(f"Processed agency data for {asteroid_id}")


def main():
    logger.info(f"Starting agency processor (topic={AGENCY_TOPIC}, group={CONSUMER_GROUP})")

    consumer = KafkaConsumer(
        AGENCY_TOPIC,
        bootstrap_servers=Config.KAFKA_BOOTSTRAP_SERVERS,
        group_id=CONSUMER_GROUP,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=None,
    )

    logger.info("Agency processor connected. Waiting for messages...")

    try:
        for message in consumer:
            process_message(message.value)
    except KeyboardInterrupt:
        logger.info("Agency processor shutting down.")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
