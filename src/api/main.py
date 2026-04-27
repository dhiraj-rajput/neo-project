"""
FastAPI backend for the NEO Orbital Tracker.
Multi-agency API aggregation: NASA NeoWs, JPL SBDB/Sentry/CAD, ESA NEOCC, IAU/MPC.
All external calls proxied server-side to avoid CORS issues.
"""
from contextlib import contextmanager
from datetime import datetime, date
from decimal import Decimal
from typing import Optional
import time

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
import psycopg2
import psycopg2.extras
import psycopg2.pool
import httpx
import os
import json

from src.logger import get_logger

logger = get_logger("neo-api", "api.log")

app = FastAPI(
    title="NEO Orbital Tracker API",
    description="Multi-agency comparative analysis engine for Near-Earth Objects",
    version="2.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Database Connection Pool ────────────────────────────
DB_CONFIG = {
    "host": os.getenv("DB_HOST", "timescaledb"),
    "port": int(os.getenv("DB_PORT", "5432")),
    "dbname": os.getenv("DB_NAME", "neo_db"),
    "user": os.getenv("DB_USER", "neo_user"),
    "password": os.getenv("DB_PASSWORD", "neo_password"),
}

# Lazy-initialized connection pool (created on first request)
_pool: Optional[psycopg2.pool.SimpleConnectionPool] = None


def get_pool() -> psycopg2.pool.SimpleConnectionPool:
    global _pool
    if _pool is None or _pool.closed:
        _pool = psycopg2.pool.SimpleConnectionPool(
            minconn=2,
            maxconn=10,
            **DB_CONFIG,
        )
    return _pool


@contextmanager
def get_db():
    """Context manager for database connections with automatic return to pool."""
    pool = get_pool()
    conn = pool.getconn()
    try:
        yield conn
    finally:
        pool.putconn(conn)


def serialize_row(row: dict) -> dict:
    """Convert Decimal/date types to JSON-safe types."""
    out = {}
    for k, v in row.items():
        if isinstance(v, Decimal):
            out[k] = float(v)
        elif isinstance(v, (date, datetime)):
            out[k] = v.isoformat()
        else:
            out[k] = v
    return out


# ── External API Client (shared, connection-pooled) ─────
_http_client: Optional[httpx.AsyncClient] = None

JPL_SBDB_URL = "https://ssd-api.jpl.nasa.gov/sbdb.api"
JPL_SENTRY_URL = "https://ssd-api.jpl.nasa.gov/sentry.api"
JPL_CAD_URL = "https://ssd-api.jpl.nasa.gov/cad.api"
JPL_FIREBALL_URL = "https://ssd-api.jpl.nasa.gov/fireball.api"
ESA_NEOCC_BASE = "https://neo.ssa.esa.int/PSDB-portlet/download"
MPC_ORB_URL = "https://data.minorplanetcenter.net/api/get-orb"

# In-memory cache: key -> (data, timestamp)
_cache: dict[str, tuple[dict, float]] = {}
CACHE_TTL = 3600  # 1 hour default
SENTRY_CACHE_TTL = 7200  # 2 hours for sentry watchlist (rarely changes)
NEGATIVE_CACHE_TTL = 24 * 3600  # 24 hours for confirmed external 404s


def get_cached(key: str, ttl: int = CACHE_TTL) -> Optional[dict]:
    if key in _cache:
        data, ts = _cache[key]
        if time.time() - ts < ttl:
            return data
        del _cache[key]
    return None


def set_cache(key: str, data: dict):
    _cache[key] = (data, time.time())


@app.on_event("startup")
async def startup():
    global _http_client
    _http_client = httpx.AsyncClient(
        timeout=httpx.Timeout(15.0, connect=5.0),
        limits=httpx.Limits(max_connections=20, max_keepalive_connections=10),
        headers={"User-Agent": "NEO-Orbital-Tracker/2.0 (Research)"},
    )


@app.on_event("shutdown")
async def shutdown():
    global _pool, _http_client
    if _pool and not _pool.closed:
        _pool.closeall()
    if _http_client:
        await _http_client.aclose()


async def _fetch_json(url: str, params: dict = None) -> Optional[dict]:
    """Safe external API fetch with error handling."""
    cache_key = f"external:{url}:{json.dumps(params or {}, sort_keys=True)}"
    cached = get_cached(cache_key, NEGATIVE_CACHE_TTL)
    if cached and cached.get("_not_found"):
        return None

    try:
        resp = await _http_client.get(url, params=params)
        if resp.status_code == 200:
            return resp.json()
        if resp.status_code == 404:
            set_cache(cache_key, {"_not_found": True})
            logger.info(f"API {url} returned 404; cached negative result for params={params}")
            return None
        logger.warning(f"API {url} returned {resp.status_code}")
        return None
    except Exception as e:
        logger.error(f"API fetch error {url}: {e}")
        return None


async def _fetch_text(url: str, params: dict = None) -> Optional[str]:
    """Fetch raw text (for ESA .lst files)."""
    try:
        resp = await _http_client.get(url, params=params)
        if resp.status_code == 200:
            return resp.text
        return None
    except Exception as e:
        logger.error(f"Text fetch error {url}: {e}")
        return None


# ── Existing Endpoints ──────────────────────────────────

@app.get("/api/asteroids")
def get_asteroids(
    date: str = Query(default=None, description="Start date in YYYY-MM-DD format"),
    days: int = Query(default=7, ge=1, le=30, description="Number of days to look ahead"),
):
    """
    Fetch asteroids for a date window.
    Defaults to today + 7 days if no date is provided.
    """
    if not date:
        date = datetime.utcnow().strftime("%Y-%m-%d")

    try:
        with get_db() as conn:
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            query = """
                SELECT
                    asteroid_id,
                    name,
                    close_approach_date,
                    is_potentially_hazardous,
                    estimated_diameter_km_max,
                    relative_velocity_km_s,
                    miss_distance_km,
                    absolute_magnitude_h,
                    nasa_jpl_url
                FROM neo_close_approaches
                WHERE close_approach_date BETWEEN %s::date AND %s::date + (%s || ' days')::interval
                ORDER BY close_approach_date ASC, miss_distance_km ASC
            """
            cur.execute(query, (date, date, str(days)))
            records = [serialize_row(dict(r)) for r in cur.fetchall()]
            cur.close()

        return {
            "date": date,
            "days": days,
            "count": len(records),
            "hazardous_count": sum(1 for r in records if r.get("is_potentially_hazardous")),
            "asteroids": records,
        }

    except psycopg2.OperationalError as e:
        raise HTTPException(status_code=503, detail=f"Database unavailable: {e}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/stats")
def get_stats():
    """Return high-level database statistics for the dashboard."""
    try:
        with get_db() as conn:
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cur.execute("""
                SELECT
                    COUNT(*) as total_records,
                    COUNT(DISTINCT asteroid_id) as unique_asteroids,
                    MIN(close_approach_date) as earliest_date,
                    MAX(close_approach_date) as latest_date,
                    SUM(CASE WHEN is_potentially_hazardous THEN 1 ELSE 0 END) as hazardous_approaches
                FROM neo_close_approaches
            """)
            stats = serialize_row(dict(cur.fetchone()))
            cur.close()
        return stats
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ── NEW: Multi-Agency Endpoints ─────────────────────────

@app.get("/api/search")
async def search_asteroids(q: str = Query(..., min_length=1, description="Search query")):
    """
    Unified search: local DB first, then JPL SBDB fallback.
    Returns top-10 matches.
    """
    results = []

    # 1. Search local DB
    try:
        with get_db() as conn:
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cur.execute("""
                SELECT DISTINCT ON (asteroid_id)
                    asteroid_id, name, is_potentially_hazardous,
                    estimated_diameter_km_max, close_approach_date
                FROM neo_close_approaches
                WHERE name ILIKE %s OR asteroid_id ILIKE %s
                ORDER BY asteroid_id, close_approach_date DESC
                LIMIT 10
            """, (f"%{q}%", f"%{q}%"))
            for row in cur.fetchall():
                r = serialize_row(dict(row))
                r["source"] = "local_db"
                results.append(r)
            cur.close()
    except Exception:
        pass  # DB might be down; continue with external

    # 2. If few local results, query JPL SBDB
    if len(results) < 5:
        sbdb = await _fetch_json(JPL_SBDB_URL, {"sstr": q})
        if sbdb and "object" in sbdb:
            obj = sbdb["object"]
            results.append({
                "asteroid_id": obj.get("des", ""),
                "name": obj.get("fullname", obj.get("shortname", q)),
                "is_potentially_hazardous": obj.get("pha", False),
                "estimated_diameter_km_max": None,
                "source": "jpl_sbdb",
            })

    return {"query": q, "count": len(results), "results": results[:10]}


@app.get("/api/asteroid/{designation}")
async def get_asteroid_profile(designation: str):
    """
    Unified Multi-Agency Profile.
    
    Supports lookup by: asteroid_id, name, SBDB designation, SBDB spkid,
    or Sentry designation. This fixes the Sentry watchlist → profile
    navigation bug.
    """
    try:
        with get_db() as conn:
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

            # ── Multi-identifier resolution ──
            # Try asteroid_id first (most common), then SBDB designation/spkid,
            # then name, then Sentry designation.
            asteroid_id = None

            cur.execute("SELECT asteroid_id FROM neo_agency_sbdb WHERE asteroid_id = %s", (designation,))
            row = cur.fetchone()
            if row:
                asteroid_id = row["asteroid_id"]

            if not asteroid_id:
                cur.execute("SELECT asteroid_id FROM neo_agency_sbdb WHERE designation = %s OR spkid = %s", (designation, designation))
                row = cur.fetchone()
                if row:
                    asteroid_id = row["asteroid_id"]

            if not asteroid_id:
                cur.execute("SELECT asteroid_id FROM neo_agency_sentry WHERE designation = %s OR fullname ILIKE %s", (designation, f"%{designation}%"))
                row = cur.fetchone()
                if row:
                    asteroid_id = row["asteroid_id"]

            if not asteroid_id:
                cur.execute("""
                    SELECT DISTINCT asteroid_id FROM neo_close_approaches
                    WHERE asteroid_id = %s OR name ILIKE %s
                    LIMIT 1
                """, (designation, f"%{designation}%"))
                row = cur.fetchone()
                if row:
                    asteroid_id = row["asteroid_id"]

            if not asteroid_id:
                # Try external SBDB as last resort
                sbdb_ext = await _fetch_json(JPL_SBDB_URL, {"sstr": designation})
                if sbdb_ext and "object" in sbdb_ext:
                    return {
                        "designation": designation,
                        "message": "Found in SBDB but not yet ingested locally.",
                        "external_sbdb": {
                            "fullname": sbdb_ext["object"].get("fullname"),
                            "des": sbdb_ext["object"].get("des"),
                            "neo": sbdb_ext["object"].get("neo"),
                            "pha": sbdb_ext["object"].get("pha"),
                        },
                        "agencies": {},
                    }
                return {"designation": designation, "message": "Profile pending ingestion.", "agencies": {}}

            # ── Fetch all agency data ──
            cur.execute("SELECT * FROM neo_agency_sbdb WHERE asteroid_id = %s", (asteroid_id,))
            sbdb = cur.fetchone()
            cur.execute("SELECT * FROM neo_agency_sentry WHERE asteroid_id = %s", (asteroid_id,))
            sentry = cur.fetchone()
            cur.execute("SELECT * FROM neo_agency_esa WHERE asteroid_id = %s", (asteroid_id,))
            esa = cur.fetchone()
            cur.execute("SELECT * FROM neo_agency_mpc WHERE asteroid_id = %s", (asteroid_id,))
            mpc = cur.fetchone()
            cur.execute("SELECT * FROM neo_agency_cad WHERE asteroid_id = %s ORDER BY approach_date DESC LIMIT 50", (asteroid_id,))
            cad_list = cur.fetchall()

            # ── Build response ──
            profile = {
                "designation": designation,
                "asteroid_id": asteroid_id,
                "name": (sbdb.get("fullname") if sbdb else None) or designation,
                "is_neo": sbdb.get("is_neo", False) if sbdb else None,
                "is_pha": sbdb.get("is_pha", False) if sbdb else None,
                "agencies": {},
            }

            # SBDB
            if sbdb:
                sbdb = serialize_row(dict(sbdb))
                profile["agencies"]["jpl_sbdb"] = {
                    "designation": sbdb.get("designation"),
                    "fullname": sbdb.get("fullname"),
                    "spkid": sbdb.get("spkid"),
                    "orbit_class": sbdb.get("orbit_class"),
                    "orbit_class_name": sbdb.get("orbit_class_name"),
                    "orbital_elements": {
                        "epoch": {"title": "Epoch", "value": sbdb.get("epoch_tdb"), "units": "TDB"},
                        "e": {"title": "Eccentricity", "value": sbdb.get("eccentricity"), "units": ""},
                        "a": {"title": "Semi-major Axis", "value": sbdb.get("semi_major_axis_au"), "units": "AU"},
                        "q": {"title": "Perihelion Dist", "value": sbdb.get("perihelion_dist_au"), "units": "AU"},
                        "ad": {"title": "Aphelion Dist", "value": sbdb.get("aphelion_dist_au"), "units": "AU"},
                        "i": {"title": "Inclination", "value": sbdb.get("inclination_deg"), "units": "deg"},
                        "om": {"title": "Long. Asc. Node", "value": sbdb.get("long_asc_node_deg"), "units": "deg"},
                        "w": {"title": "Arg. Perihelion", "value": sbdb.get("arg_perihelion_deg"), "units": "deg"},
                        "ma": {"title": "Mean Anomaly", "value": sbdb.get("mean_anomaly_deg"), "units": "deg"},
                        "per": {"title": "Orbital Period", "value": sbdb.get("orbital_period_days"), "units": "days"},
                        "n": {"title": "Mean Motion", "value": sbdb.get("mean_motion_deg_d"), "units": "deg/d"},
                        "moid": {"title": "MOID", "value": sbdb.get("moid_au"), "units": "AU"},
                        "condition": {"title": "Condition Code", "value": sbdb.get("condition_code"), "units": ""},
                        "arc": {"title": "Data Arc", "value": sbdb.get("data_arc_days"), "units": "days"},
                        "n_obs": {"title": "Observations Used", "value": sbdb.get("n_obs_used"), "units": ""},
                    },
                    "physical_params": {
                        "h": {"title": "Absolute Mag (H)", "value": sbdb.get("absolute_magnitude_h"), "units": "mag"},
                        "diameter": {"title": "Diameter", "value": sbdb.get("diameter_km"), "units": "km"},
                        "albedo": {"title": "Albedo", "value": sbdb.get("albedo"), "units": ""},
                        "rot_per": {"title": "Rotation Period", "value": sbdb.get("rotation_period_h"), "units": "h"},
                        "spec_type": {"title": "Spectral Type", "value": sbdb.get("spectral_type"), "units": ""},
                    },
                    "discovery": {
                        "date": sbdb.get("discovery_date"),
                        "site": sbdb.get("discovery_site"),
                    },
                }

            # Sentry
            if sentry:
                sentry = serialize_row(dict(sentry))
                profile["agencies"]["jpl_sentry"] = {
                    "status": sentry.get("status"),
                    "method": sentry.get("method"),
                    "impact_probability": sentry.get("impact_probability"),
                    "torino_scale": sentry.get("torino_scale"),
                    "palermo_scale_cum": sentry.get("palermo_scale_cum"),
                    "palermo_scale_max": sentry.get("palermo_scale_max"),
                    "n_impacts": sentry.get("n_impacts"),
                    "v_infinity_km_s": sentry.get("v_infinity_km_s"),
                    "energy_mt": sentry.get("energy_mt"),
                    "diameter_km": sentry.get("diameter_km"),
                    "h_mag": sentry.get("h_mag"),
                    "removed_date": sentry.get("removed_date"),
                }

            # CAD
            if cad_list:
                profile["agencies"]["jpl_cad"] = {
                    "count": len(cad_list),
                    "approaches": [
                        serialize_row({
                            "date": c["approach_date"],
                            "datetime": c.get("approach_datetime"),
                            "dist_au": c["distance_au"],
                            "dist_min_au": c.get("distance_min_au"),
                            "dist_max_au": c.get("distance_max_au"),
                            "v_rel_km_s": c["v_rel_km_s"],
                            "v_inf_km_s": c.get("v_inf_km_s"),
                            "h_mag": c.get("h_mag"),
                            "diameter_km": c.get("diameter_km"),
                            "body": c.get("body", "Earth"),
                        })
                        for c in cad_list
                    ],
                }

            # ESA
            if esa:
                esa = serialize_row(dict(esa))
                profile["agencies"]["esa_neocc"] = {
                    "on_risk_list": esa.get("on_risk_list", False),
                    "designation": esa.get("esa_designation"),
                    "moid": esa.get("esa_moid"),
                    "h": esa.get("esa_h"),
                    "diameter_km": esa.get("esa_diameter_km"),
                }

            # MPC
            if mpc:
                mpc = serialize_row(dict(mpc))
                profile["agencies"]["iau_mpc"] = {
                    "status": mpc.get("status", "found"),
                    "designation": mpc.get("mpc_designation"),
                    "name": mpc.get("mpc_name"),
                    "orbital_elements": {
                        "a": mpc.get("semi_major_axis_au"),
                        "e": mpc.get("eccentricity"),
                        "i": mpc.get("inclination_deg"),
                        "om": mpc.get("long_asc_node_deg"),
                        "w": mpc.get("arg_perihelion_deg"),
                        "ma": mpc.get("mean_anomaly_deg"),
                    },
                }

            cur.close()
            return profile

    except Exception as e:
        logger.error(f"Error fetching unified profile: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/sentry/watchlist")
async def get_sentry_watchlist():
    """
    JPL Sentry impact risk watchlist — objects with non-zero impact probability.
    Cached for 2 hours (data rarely changes).
    """
    cache_key = "sentry:watchlist"
    cached = get_cached(cache_key, SENTRY_CACHE_TTL)
    if cached:
        return cached

    data = await _fetch_json(JPL_SENTRY_URL, {"all": "true"})
    if not data or "data" not in data:
        raise HTTPException(status_code=502, detail="Failed to fetch Sentry data")

    # Group by object designation, keep highest-probability entry
    objects = {}
    for entry in data["data"]:
        des = entry.get("des", "")
        if des not in objects or float(entry.get("ip", 0)) > float(objects[des].get("ip", 0)):
            objects[des] = entry

    watchlist = sorted(objects.values(), key=lambda x: float(x.get("ip", 0)), reverse=True)

    result = {"count": len(watchlist), "watchlist": watchlist[:100]}  # top 100
    set_cache(cache_key, result)
    return result


@app.get("/api/esa/close-approaches")
async def get_esa_close_approaches():
    """ESA NEOCC upcoming close approaches."""
    cache_key = "esa:close_approaches"
    cached = get_cached(cache_key)
    if cached:
        return cached

    text = await _fetch_text(ESA_NEOCC_BASE, {"file": "esa_ca.lst"})
    if not text:
        raise HTTPException(status_code=502, detail="Failed to fetch ESA data")

    result = {"source": "ESA NEOCC", "raw_available": True, "length": len(text)}
    set_cache(cache_key, result)
    return result


@app.get("/api/fireball")
async def get_fireballs(limit: int = Query(default=20, ge=1, le=100)):
    """NASA/JPL Fireball API — observed bolide events."""
    cache_key = f"fireball:{limit}"
    cached = get_cached(cache_key)
    if cached:
        return cached

    data = await _fetch_json(JPL_FIREBALL_URL, {"limit": str(limit)})
    if not data:
        raise HTTPException(status_code=502, detail="Failed to fetch fireball data")

    fields = data.get("fields", [])
    events = []
    for row in data.get("data", []):
        events.append(dict(zip(fields, row)))

    result = {"count": len(events), "events": events}
    set_cache(cache_key, result)
    return result


@app.get("/api/analytics/overview")
def get_analytics():
    """Aggregated analytics from local DB."""
    try:
        with get_db() as conn:
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

            # Velocity distribution
            cur.execute("""
                SELECT
                    width_bucket(relative_velocity_km_s::float, 0, 40, 8) as bucket,
                    COUNT(*) as count,
                    ROUND(AVG(relative_velocity_km_s::numeric), 2) as avg_vel
                FROM neo_close_approaches
                WHERE relative_velocity_km_s IS NOT NULL
                GROUP BY bucket ORDER BY bucket
            """)
            vel_dist = [serialize_row(dict(r)) for r in cur.fetchall()]

            # Size distribution
            cur.execute("""
                SELECT
                    CASE
                        WHEN estimated_diameter_km_max < 0.01 THEN '<10m'
                        WHEN estimated_diameter_km_max < 0.1 THEN '10-100m'
                        WHEN estimated_diameter_km_max < 0.5 THEN '100-500m'
                        WHEN estimated_diameter_km_max < 1.0 THEN '500m-1km'
                        ELSE '>1km'
                    END as size_class,
                    COUNT(*) as count
                FROM neo_close_approaches
                WHERE estimated_diameter_km_max IS NOT NULL
                GROUP BY size_class ORDER BY MIN(estimated_diameter_km_max)
            """)
            size_dist = [serialize_row(dict(r)) for r in cur.fetchall()]

            # Hazardous ratio
            cur.execute("""
                SELECT
                    COUNT(*) as total,
                    SUM(CASE WHEN is_potentially_hazardous THEN 1 ELSE 0 END) as hazardous,
                    MIN(miss_distance_km::float) as closest_approach_km,
                    MAX(relative_velocity_km_s::float) as fastest_km_s
                FROM neo_close_approaches
            """)
            summary = serialize_row(dict(cur.fetchone()))
            cur.close()

        return {
            "velocity_distribution": vel_dist,
            "size_distribution": size_dist,
            "summary": summary,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/health")
def health_check():
    """Health check endpoint for Docker and monitoring."""
    try:
        with get_db() as conn:
            cur = conn.cursor()
            cur.execute("SELECT 1")
            cur.close()
        return {"status": "ok", "database": "connected"}
    except Exception:
        return {"status": "degraded", "database": "disconnected"}
