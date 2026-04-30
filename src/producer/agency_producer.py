"""
Multi-Agency NEO Data Producer

Fetches enrichment data for asteroids from 5 external sources:
  1. JPL SBDB    — orbital + physical parameters
  2. JPL Sentry  — impact risk assessment
  3. JPL CAD     — close-approach records
  4. JPL Fireball — bolide events (batch, not per-asteroid)
  5. ESA NEOCC   — risk list

Publishes enriched profiles to the Kafka 'agency_ingest' topic.
"""

import asyncio
import json
import os
import re
import signal
import sys
import time
from datetime import datetime, timezone

import httpx
import psycopg2
from contextlib import contextmanager
from kafka import KafkaProducer

from src.config import Config
from src.logger import logger, console
from src.agencies import (
    SBDBClient, SentryClient, CADClient, FireballClient,
    ESAClient
)

# ── Constants ────────────────────────────────────────────────

AGENCY_TOPIC = os.getenv("AGENCY_TOPIC", "agency_ingest")
BATCH_SIZE = 500
CYCLE_SLEEP = 300
MAX_CONCURRENT = 50

_shutdown = False


def _handle_signal(sig, frame):
    global _shutdown
    _shutdown = True
    logger.info("Shutdown signal received, finishing current batch...")


signal.signal(signal.SIGINT, _handle_signal)
signal.signal(signal.SIGTERM, _handle_signal)


# ── Designation Utilities ────────────────────────────────────

def extract_designation_candidates(asteroid_id: str, name: str | None = None) -> list[str]:
    """Build a prioritised list of identifiers to try across APIs."""
    candidates = []

    if name:
        name = name.strip()
        num_match = re.match(r"^(\d+)\s", name)
        if num_match:
            candidates.append(num_match.group(1))

        prov_match = re.search(r"\((\d{4}\s[A-Z]{2}\d*)\)", name)
        if prov_match:
            candidates.append(prov_match.group(1))

        clean_name = re.sub(r"\(.*?\)", "", name).strip()
        if clean_name and clean_name not in candidates:
            candidates.append(clean_name)

    if asteroid_id and asteroid_id not in candidates:
        candidates.append(asteroid_id)

    return candidates


# ── Database Helpers ─────────────────────────────────────────

@contextmanager
def _db():
    """Context-managed DB connection — prevents leaks on exception paths."""
    conn = psycopg2.connect(
        host=Config.DB_HOST, port=Config.DB_PORT,
        database=Config.DB_NAME, user=Config.DB_USER, password=Config.DB_PASSWORD,
    )
    try:
        yield conn
    finally:
        conn.close()


def load_asteroids_to_profile(limit: int = BATCH_SIZE) -> list[dict]:
    """Load asteroids needing agency profiling (never-profiled or stale)."""
    with _db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT n.asteroid_id, n.name
                FROM (
                    SELECT DISTINCT asteroid_id, name FROM neo_close_approaches
                ) n
                LEFT JOIN neo_agency_sbdb s ON s.asteroid_id = n.asteroid_id
                WHERE s.asteroid_id IS NULL
                   OR s.ingestion_time < now() - INTERVAL '7 days'
                ORDER BY s.ingestion_time NULLS FIRST
                LIMIT %s
            """, (limit,))
            return [{"asteroid_id": r[0], "name": r[1]} for r in cur.fetchall()]


def should_skip_source(asteroid_id: str, source: str) -> bool:
    """Check if a source returned 404 recently."""
    with _db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT not_found_until FROM neo_agency_fetch_status
                WHERE asteroid_id = %s AND source = %s AND status_code = 404
            """, (asteroid_id, source))
            row = cur.fetchone()
            return bool(row and row[0] and row[0] > datetime.now(timezone.utc))


def save_fetch_status(asteroid_id: str, status):
    """Persist fetch status for 404-hold tracking."""
    with _db() as conn:
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO neo_agency_fetch_status
                        (asteroid_id, source, status_code, last_failure_type,
                         last_checked_at, not_found_until, lookup_value)
                    VALUES (%s, %s, %s, %s, now(), %s, %s)
                    ON CONFLICT (asteroid_id, source) DO UPDATE SET
                        status_code = EXCLUDED.status_code,
                        last_failure_type = EXCLUDED.last_failure_type,
                        last_checked_at = now(),
                        not_found_until = EXCLUDED.not_found_until,
                        lookup_value = EXCLUDED.lookup_value
                """, (
                    asteroid_id, status.source, status.status_code,
                    status.failure_type, status.not_found_until, status.lookup_value,
                ))
                conn.commit()
        except Exception as e:
            logger.warning(f"Failed to save fetch status: {e}")
            conn.rollback()


# ── Profile Builder ──────────────────────────────────────────

async def fetch_full_profile(
    asteroid_id: str,
    name: str | None,
    http_client: httpx.AsyncClient,
    jpl_sem: asyncio.Semaphore,
    esa_client: ESAClient,
) -> dict:
    """Fetch data from all agencies for a single asteroid."""
    candidates = extract_designation_candidates(asteroid_id, name)
    profile = {
        "asteroid_id": asteroid_id,
        "name": name,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "agencies": {},
    }

    # ── 1. JPL SBDB ──
    sbdb = SBDBClient(http_client=http_client, semaphore=jpl_sem)
    if not should_skip_source(asteroid_id, "JPL_SBDB"):
        for cand in candidates:
            sbdb_data, sbdb_status = await sbdb.fetch_with_status(cand)
            save_fetch_status(asteroid_id, sbdb_status)
            if sbdb_data:
                profile["agencies"]["sbdb"] = sbdb_data
                # Use SBDB's resolved designation for downstream lookups
                if sbdb_data.get("designation"):
                    des = sbdb_data["designation"]
                    candidates = [des] + [c for c in candidates if c != des]
                if sbdb_data.get("spkid"):
                    candidates.append(sbdb_data["spkid"])
                break

    # ── 2. JPL Sentry ──
    sentry = SentryClient(http_client=http_client, semaphore=jpl_sem)
    if not should_skip_source(asteroid_id, "JPL_Sentry"):
        for cand in candidates[:3]:
            sen_data, sen_status = await sentry.fetch_with_status(cand)
            save_fetch_status(asteroid_id, sen_status)
            if sen_data:
                profile["agencies"]["sentry"] = sen_data
                break
        # Try SPK-ID if available
        if "sentry" not in profile["agencies"]:
            sbdb_data = profile["agencies"].get("sbdb", {})
            spkid = sbdb_data.get("spkid")
            if spkid:
                sen_data, sen_status = await sentry.fetch_with_status(spkid, use_spk=True)
                save_fetch_status(asteroid_id, sen_status)
                if sen_data:
                    profile["agencies"]["sentry"] = sen_data

    # ── 3. JPL CAD ──
    cad = CADClient(http_client=http_client, semaphore=jpl_sem)
    if not should_skip_source(asteroid_id, "JPL_CAD"):
        best_des = candidates[0] if candidates else asteroid_id
        cad_data = await cad.fetch(best_des)
        if cad_data:
            profile["agencies"]["cad"] = cad_data

    # ── 4. ESA NEOCC ──
    if not should_skip_source(asteroid_id, "ESA_NEOCC"):
        for cand in candidates[:2]:
            esa_data, esa_status = await esa_client.check_risk(cand)
            save_fetch_status(asteroid_id, esa_status)
            if esa_data and esa_data.get("on_risk_list"):
                profile["agencies"]["esa"] = esa_data
                break

    return profile


# ── Kafka Producer ───────────────────────────────────────────

def create_kafka_producer() -> KafkaProducer:
    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", Config.KAFKA_BOOTSTRAP_SERVERS)
    for attempt in range(5):
        try:
            producer = KafkaProducer(
                bootstrap_servers=bootstrap,
                value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                acks="all",
                retries=3,
                max_in_flight_requests_per_connection=1,
            )
            logger.info(f"Kafka producer connected to {bootstrap}")
            return producer
        except Exception as e:
            logger.warning(f"Kafka connect attempt {attempt + 1}/5 failed: {e}")
            time.sleep(5)
    raise RuntimeError("Failed to connect to Kafka after 5 attempts")


# ── Fireball Batch Ingest ────────────────────────────────────

async def ingest_fireballs(http_client: httpx.AsyncClient, jpl_sem: asyncio.Semaphore, producer: KafkaProducer):
    """Fetch and publish recent fireball events."""
    fb = FireballClient(http_client=http_client, semaphore=jpl_sem)
    events = await fb.fetch(limit=200)
    if events:
        # Publish as a single batch message
        producer.send(AGENCY_TOPIC, key="fireball", value={
            "asteroid_id": "__fireball_batch__",
            "agencies": {"fireball": events},
            "timestamp": datetime.now(timezone.utc).isoformat(),
        })
        logger.info(f"Published {len(events)} fireball events to Kafka")


# ── Main Ingestion Loop ─────────────────────────────────────

async def run_ingestion_cycle():
    """Run full cycle: load asteroids, fetch profiles, publish to Kafka."""
    producer = create_kafka_producer()

    jpl_sem = asyncio.Semaphore(3)

    async with httpx.AsyncClient(
        timeout=httpx.Timeout(30.0, connect=10.0),
        limits=httpx.Limits(max_connections=100, max_keepalive_connections=50),
        follow_redirects=True,
        headers={"User-Agent": "NEO-Orbital-Tracker/3.0 (Research)"},
    ) as http_client:

        # Pre-load ESA risk list (shared cache)
        esa_client = ESAClient(http_client=http_client)
        await esa_client.refresh_risk_list()

        cycle = 0
        while not _shutdown:
            cycle += 1
            console.rule(f"[pipeline]Agency Ingestion Cycle {cycle}")

            asteroids = load_asteroids_to_profile(BATCH_SIZE)
            if not asteroids:
                logger.info("No asteroids need profiling. Sleeping...")
                await asyncio.sleep(CYCLE_SLEEP)
                continue

            logger.info(f"Processing {len(asteroids)} asteroids")

            sem = asyncio.Semaphore(MAX_CONCURRENT)

            async def process_one(ast):
                async with sem:
                    try:
                        profile = await fetch_full_profile(
                            ast["asteroid_id"], ast["name"],
                            http_client, jpl_sem, esa_client,
                        )
                        producer.send(AGENCY_TOPIC, key=ast["asteroid_id"], value=profile)
                        n = len(profile.get("agencies", {}))
                        logger.info(f"  {ast['asteroid_id']} -- {n} agencies")
                    except Exception as e:
                        logger.error(f"  {ast['asteroid_id']}: {e}")

            tasks = [process_one(ast) for ast in asteroids]
            await asyncio.gather(*tasks)
            producer.flush()

            # Fireball batch
            try:
                await ingest_fireballs(http_client, jpl_sem, producer)
            except Exception as e:
                logger.warning(f"Fireball ingest failed: {e}")

            logger.info(f"Cycle {cycle} complete. Sleeping {CYCLE_SLEEP}s...")
            await asyncio.sleep(CYCLE_SLEEP)

    producer.close()
    logger.info("Agency producer shut down cleanly.")


def main():
    console.rule("[pipeline]NEO Multi-Agency Data Producer")
    logger.info("Starting agency producer...")
    try:
        asyncio.run(run_ingestion_cycle())
    except KeyboardInterrupt:
        logger.info("Interrupted by user.")
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
