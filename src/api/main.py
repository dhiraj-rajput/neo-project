"""
FastAPI backend for the NEO Orbital Tracker.
Serves asteroid data from TimescaleDB to the Next.js 3D frontend.
"""
from contextlib import contextmanager
from datetime import datetime, date
from decimal import Decimal
from typing import Optional

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
import psycopg2
import psycopg2.extras
import psycopg2.pool
import os
import json

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


# ── Endpoints ───────────────────────────────────────────

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


@app.on_event("shutdown")
def shutdown():
    global _pool
    if _pool and not _pool.closed:
        _pool.closeall()
