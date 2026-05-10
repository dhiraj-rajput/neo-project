"""
Multi-Agency NEO Data Producer — Fully Bulk Architecture

Fetches enrichment data for asteroids from 5 external sources:
  1. JPL SBDB    — orbital + physical (BULK via sbdb_query.api for numbered NEOs)
  2. JPL Sentry  — impact risk assessment (BULK: 1 call for ALL objects)
  3. JPL CAD     — close-approach records (BULK: 1 call for ALL NEOs)
  4. JPL Fireball — bolide events (BULK: 1 call, not per-asteroid)
  5. ESA NEOCC   — risk list (BULK: 1 file download)

Performance Architecture:
  Phase 1 — BULK FETCH (4 API calls total):
    • SBDB:   fetch_bulk_orbital()  → all numbered NEO orbits/phys (1 call)
    • Sentry: fetch_all_active()    → all active impact-risk objects (1 call)
    • CAD:    fetch_bulk()           → all NEO close-approaches (1 call)
    • ESA:    refresh_risk_list()    → all ESA risk objects (1 file download)

  Phase 2 — PER-ASTEROID GAP-FILL (rate-limited):
    • SBDB per-asteroid: only for asteroids NOT in the bulk cache
      (unnumbered, provisional designations, etc.)

  Phase 3 — ASSEMBLE & PUBLISH:
    • Merge bulk data with any per-asteroid gap-fill
    • Publish to Kafka in batch

  Result: ~80K API calls → 4 bulk + N gap-fill (N ≈ only unnumbered asteroids)
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
import psycopg2.extras
import psycopg2.pool
from contextlib import contextmanager
from kafka import KafkaProducer

from src.config import Config
from src.logger import logger, console
from src.agencies import (
    SBDBClient, SentryClient, CADClient, FireballClient,
    ESAClient, TokenBucketRateLimiter
)

# ── Constants (all from env) ─────────────────────────────────

AGENCY_TOPIC = os.getenv("AGENCY_TOPIC", "agency_ingest")
BATCH_SIZE = int(os.getenv("AGENCY_BATCH_SIZE", "500"))
CYCLE_SLEEP = int(os.getenv("AGENCY_CYCLE_SLEEP", "300"))
MAX_CONCURRENT = int(os.getenv("AGENCY_MAX_CONCURRENT", "50"))

# Rate limiter config — JPL SSD fair-use
JPL_RATE = float(os.getenv("JPL_RATE_LIMIT_RPS", "10"))
JPL_BURST = int(os.getenv("JPL_RATE_LIMIT_BURST", "5"))

# Bulk caching — refresh interval (seconds)
BULK_CACHE_TTL = int(os.getenv("BULK_CACHE_TTL", "3600"))  # 1 hour

# Date range for CAD bulk fetch (from .env)
CAD_DATE_MIN = Config.AGENCY_START_DATE
CAD_DATE_MAX = Config.AGENCY_END_DATE
CAD_DIST_MAX = Config.AGENCY_CAD_DIST_MAX

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

_pool: psycopg2.pool.ThreadedConnectionPool | None = None


def _get_pool() -> psycopg2.pool.ThreadedConnectionPool:
    """Lazy-init a shared connection pool (thread-safe, max 10 conns)."""
    global _pool
    if _pool is None or _pool.closed:
        _pool = psycopg2.pool.ThreadedConnectionPool(
            minconn=2,
            maxconn=10,
            host=Config.DB_HOST,
            port=Config.DB_PORT,
            database=Config.DB_NAME,
            user=Config.DB_USER,
            password=Config.DB_PASSWORD,
            keepalives=1, keepalives_idle=30, keepalives_interval=10, keepalives_count=5,
        )
    return _pool


@contextmanager
def _db():
    """Context-managed DB connection — recycles via pool instead of open/close."""
    pool = _get_pool()
    conn = pool.getconn()
    try:
        yield conn
    finally:
        pool.putconn(conn)


def load_asteroids_to_profile(limit: int = BATCH_SIZE) -> list[dict]:
    """Load asteroids needing agency profiling (never-profiled or stale)."""
    with _db() as conn:
        with conn.cursor() as cur:
            # 1. Fetch stale records first (fast index scan on small table)
            cur.execute("""
                SELECT s.asteroid_id, (
                    SELECT name FROM neo_close_approaches
                    WHERE asteroid_id = s.asteroid_id LIMIT 1
                ) as name
                FROM neo_agency_sbdb s
                WHERE s.ingestion_time < now() - INTERVAL '7 days'
                ORDER BY s.ingestion_time ASC
                LIMIT %s
            """, (limit,))
            stale = [{"asteroid_id": r[0], "name": r[1]} for r in cur.fetchall()]

            if len(stale) >= limit:
                return stale

            # 2. Fetch never-profiled records using Recursive CTE (Skip Scan)
            remaining = limit - len(stale)
            cur.execute("""
                WITH RECURSIVE t AS (
                    SELECT MIN(asteroid_id) AS asteroid_id FROM neo_close_approaches
                    UNION ALL
                    SELECT (SELECT MIN(asteroid_id) FROM neo_close_approaches WHERE asteroid_id > t.asteroid_id)
                    FROM t WHERE t.asteroid_id IS NOT NULL
                )
                SELECT t.asteroid_id,
                       (SELECT name FROM neo_close_approaches WHERE asteroid_id = t.asteroid_id LIMIT 1) as name
                FROM t
                WHERE t.asteroid_id IS NOT NULL
                  AND NOT EXISTS (SELECT 1 FROM neo_agency_sbdb s WHERE s.asteroid_id = t.asteroid_id)
                LIMIT %s
            """, (remaining,))

            new_records = [{"asteroid_id": r[0], "name": r[1]} for r in cur.fetchall()]
            return stale + new_records


# ── Batch Skip Status (replaces per-asteroid DB queries) ─────

def batch_load_skip_statuses(asteroid_ids: list[str]) -> dict[tuple[str, str], datetime]:
    """
    Preload all skip statuses for a batch in ONE query.
    Returns: {(asteroid_id, source): not_found_until}
    """
    if not asteroid_ids:
        return {}

    with _db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT asteroid_id, source, not_found_until
                FROM neo_agency_fetch_status
                WHERE asteroid_id = ANY(%s)
                  AND status_code = 404
                  AND not_found_until > now()
            """, (asteroid_ids,))
            return {(r[0], r[1]): r[2] for r in cur.fetchall()}


def batch_save_fetch_statuses(statuses: list[tuple]):
    """
    Batch-write accumulated fetch statuses in one execute_values call.
    Each tuple: (asteroid_id, source, status_code, failure_type, not_found_until, lookup_value)
    """
    if not statuses:
        return

    with _db() as conn:
        try:
            with conn.cursor() as cur:
                psycopg2.extras.execute_values(
                    cur,
                    """
                    INSERT INTO neo_agency_fetch_status
                        (asteroid_id, source, status_code, last_failure_type,
                         last_checked_at, not_found_until, lookup_value)
                    VALUES %s
                    ON CONFLICT (asteroid_id, source) DO UPDATE SET
                        status_code = EXCLUDED.status_code,
                        last_failure_type = EXCLUDED.last_failure_type,
                        last_checked_at = EXCLUDED.last_checked_at,
                        not_found_until = EXCLUDED.not_found_until,
                        lookup_value = EXCLUDED.lookup_value
                    """,
                    [(a_id, src, code, ftype, datetime.now(timezone.utc), nfu, lv)
                     for a_id, src, code, ftype, nfu, lv in statuses],
                    template="(%s, %s, %s, %s, %s, %s, %s)",
                    page_size=200,
                )
            conn.commit()
        except Exception as e:
            logger.warning(f"Failed to batch-save fetch statuses: {e}")
            conn.rollback()


# ── Bulk Data Cache ──────────────────────────────────────────

class BulkCache:
    """
    Holds pre-fetched bulk data from ALL agencies.
    Refreshed once per TTL (configurable via BULK_CACHE_TTL env var).

    After refresh, per-asteroid processing needs ZERO API calls for
    asteroids found in the bulk cache.
    """

    def __init__(self):
        self.sbdb_data: dict[str, dict] = {}     # designation → orbital/physical data
        self.sentry_data: dict[str, dict] = {}   # designation → impact risk data
        self.cad_data: dict[str, list[dict]] = {} # designation → close-approach records
        self.esa_data: dict[str, dict] = {}       # designation → ESA risk data
        self._last_refresh: float = 0

    @property
    def is_stale(self) -> bool:
        return (time.monotonic() - self._last_refresh) > BULK_CACHE_TTL

    async def refresh(self, http_client: httpx.AsyncClient, jpl_limiter: TokenBucketRateLimiter):
        """
        Refresh all bulk caches. Only 4 API calls total:
          1. SBDB query (all numbered NEO orbits/phys)
          2. Sentry mode S (all active impact-risk objects)
          3. CAD bulk (all NEO close-approaches in date range)
          4. ESA risk list (pipe-delimited file download)

        All four run in parallel via asyncio.gather().
        """
        t0 = time.monotonic()
        console.rule("[pipeline]Refreshing Bulk Caches (4 calls)")

        sbdb_client = SBDBClient(http_client=http_client, rate_limiter=jpl_limiter)
        sentry_client = SentryClient(http_client=http_client, rate_limiter=jpl_limiter)
        cad_client = CADClient(http_client=http_client, rate_limiter=jpl_limiter)
        esa_client = ESAClient(http_client=http_client)

        # All four in parallel — different endpoints, no conflict
        results = await asyncio.gather(
            sbdb_client.fetch_bulk_orbital(),
            sentry_client.fetch_all_active(),
            cad_client.fetch_bulk(
                date_min=CAD_DATE_MIN,
                date_max=CAD_DATE_MAX,
                dist_max=CAD_DIST_MAX,
            ),
            esa_client.refresh_risk_list(),
            return_exceptions=True,
        )

        # SBDB
        if isinstance(results[0], dict):
            self.sbdb_data = results[0]
        elif isinstance(results[0], Exception):
            logger.warning(f"[BulkCache] SBDB bulk failed: {results[0]}")

        # Sentry
        if isinstance(results[1], dict):
            self.sentry_data = results[1]
        elif isinstance(results[1], Exception):
            logger.warning(f"[BulkCache] Sentry fetch failed: {results[1]}")

        # CAD
        if isinstance(results[2], dict):
            self.cad_data = results[2]
        elif isinstance(results[2], Exception):
            logger.warning(f"[BulkCache] CAD fetch failed: {results[2]}")

        # ESA
        if isinstance(results[3], tuple):
            esa_entries, _ = results[3]
            if esa_entries:
                self.esa_data = esa_entries
        elif isinstance(results[3], Exception):
            logger.warning(f"[BulkCache] ESA fetch failed: {results[3]}")

        self._last_refresh = time.monotonic()
        elapsed = time.monotonic() - t0

        logger.info(
            f"[BulkCache] Refreshed in {elapsed:.1f}s — "
            f"SBDB: {len(self.sbdb_data)} entries, "
            f"Sentry: {len(self.sentry_data)} objects, "
            f"CAD: {len(self.cad_data)} objects, "
            f"ESA: {len(self.esa_data)} objects"
        )

    def lookup_sbdb(self, designation: str, candidates: list[str]) -> dict | None:
        """Look up SBDB data from bulk cache."""
        for cand in candidates:
            entry = self.sbdb_data.get(cand)
            if entry:
                return entry
            # Try lowercase name lookup
            entry = self.sbdb_data.get(cand.lower())
            if entry:
                return entry
        return None

    def lookup_sentry(self, designation: str, candidates: list[str]) -> dict | None:
        """Look up Sentry data from bulk cache."""
        for cand in candidates:
            norm = cand.strip().lower()
            norm = re.sub(r"[()']", "", norm)
            entry = self.sentry_data.get(norm)
            if entry:
                return entry
        return None

    def lookup_cad(self, designation: str, candidates: list[str]) -> list[dict] | None:
        """Look up CAD data from bulk cache."""
        for cand in candidates:
            records = self.cad_data.get(cand)
            if records:
                return records
            stripped = cand.strip()
            records = self.cad_data.get(stripped)
            if records:
                return records
        return None

    def lookup_esa(self, candidates: list[str]) -> dict | None:
        """Look up ESA data from bulk cache."""
        for cand in candidates:
            norm = cand.strip().lower()
            norm = re.sub(r"[()']", "", norm)
            norm = re.sub(r"\s+", " ", norm)
            entry = self.esa_data.get(norm)
            if entry:
                return {"on_risk_list": True, **entry}
            # Fuzzy match
            for key, val in self.esa_data.items():
                if norm in key or key in norm:
                    return {"on_risk_list": True, **val}
        return {"on_risk_list": False}


# ── Profile Builder ──────────────────────────────────────────

async def fetch_full_profile(
    asteroid_id: str,
    name: str | None,
    http_client: httpx.AsyncClient,
    jpl_limiter: TokenBucketRateLimiter,
    bulk_cache: BulkCache,
    skip_map: dict[tuple[str, str], datetime],
) -> tuple[dict, list[tuple]]:
    """
    Fetch data from all agencies for a single asteroid.

    SBDB, Sentry, CAD, and ESA all come from the pre-fetched bulk cache.
    Only asteroids NOT found in the SBDB bulk cache (unnumbered/provisional)
    require a per-asteroid SBDB API call.

    Returns (profile_dict, list_of_fetch_status_tuples).
    """
    candidates = extract_designation_candidates(asteroid_id, name)
    profile = {
        "asteroid_id": asteroid_id,
        "name": name,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "agencies": {},
    }
    accumulated_statuses: list[tuple] = []

    def _should_skip(source: str) -> bool:
        return (asteroid_id, source) in skip_map

    def _record_status(status):
        accumulated_statuses.append((
            asteroid_id, status.source, status.status_code,
            status.failure_type, status.not_found_until, status.lookup_value,
        ))

    # ── 1. JPL SBDB — try bulk cache first, fallback to per-asteroid ──
    if not _should_skip("JPL_SBDB"):
        sbdb_data = bulk_cache.lookup_sbdb(asteroid_id, candidates)
        if sbdb_data:
            profile["agencies"]["sbdb"] = sbdb_data
            # Use resolved designation for downstream lookups
            if sbdb_data.get("designation"):
                des = sbdb_data["designation"]
                candidates = [des] + [c for c in candidates if c != des]
            if sbdb_data.get("spkid"):
                spkid = sbdb_data["spkid"]
                if spkid not in candidates:
                    candidates.append(spkid)
        else:
            # Not in bulk cache → per-asteroid API call (unnumbered/provisional)
            sbdb = SBDBClient(http_client=http_client, rate_limiter=jpl_limiter)
            for cand in candidates:
                sbdb_result, sbdb_status = await sbdb.fetch_with_status(cand)
                _record_status(sbdb_status)
                if sbdb_result:
                    profile["agencies"]["sbdb"] = sbdb_result
                    if sbdb_result.get("designation"):
                        des = sbdb_result["designation"]
                        candidates = [des] + [c for c in candidates if c != des]
                    if sbdb_result.get("spkid"):
                        spkid = sbdb_result["spkid"]
                        if spkid not in candidates:
                            candidates.append(spkid)
                    break

    # ── 2. JPL Sentry (from bulk cache — 0 API calls) ──
    if not _should_skip("JPL_Sentry"):
        sentry_data = bulk_cache.lookup_sentry(asteroid_id, candidates)
        if sentry_data:
            profile["agencies"]["sentry"] = sentry_data

    # ── 3. JPL CAD (from bulk cache — 0 API calls) ──
    if not _should_skip("JPL_CAD"):
        cad_data = bulk_cache.lookup_cad(asteroid_id, candidates)
        if cad_data:
            profile["agencies"]["cad"] = cad_data

    # ── 4. ESA NEOCC (from bulk cache — 0 API calls) ──
    if not _should_skip("ESA_NEOCC"):
        esa_data = bulk_cache.lookup_esa(candidates)
        if esa_data is not None:
            profile["agencies"]["esa"] = esa_data

    return profile, accumulated_statuses


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
                max_in_flight_requests_per_connection=5,
                linger_ms=100,          # Batch messages for 100ms before sending
                batch_size=65536,       # 64KB batch size
                compression_type="lz4", # Compress batches
            )
            logger.info(f"Kafka producer connected to {bootstrap}")
            return producer
        except Exception as e:
            logger.warning(f"Kafka connect attempt {attempt + 1}/5 failed: {e}")
            time.sleep(5)
    raise RuntimeError("Failed to connect to Kafka after 5 attempts")


# ── Fireball Batch Ingest ────────────────────────────────────

async def ingest_fireballs(http_client: httpx.AsyncClient, jpl_limiter: TokenBucketRateLimiter, producer: KafkaProducer):
    """Fetch and publish recent fireball events."""
    fb = FireballClient(http_client=http_client, rate_limiter=jpl_limiter)
    events = await fb.fetch(limit=200)
    if events:
        producer.send(AGENCY_TOPIC, key="fireball", value={
            "asteroid_id": "__fireball_batch__",
            "agencies": {"fireball": events},
            "timestamp": datetime.now(timezone.utc).isoformat(),
        })
        logger.info(f"Published {len(events)} fireball events to Kafka")


# ── Main Ingestion Loop ─────────────────────────────────────

async def run_ingestion_cycle():
    """
    Run full cycle: load asteroids, fetch profiles, publish to Kafka.

    Architecture:
      1. Refresh bulk caches (SBDB, Sentry, CAD, ESA) — 4 API calls
      2. Load batch of asteroids needing profiling
      3. For each asteroid: bulk cache lookups + optional per-asteroid SBDB fallback
      4. Publish all profiles to Kafka
      5. Batch-write fetch statuses to DB
    """
    producer = create_kafka_producer()

    # Token-bucket rate limiter (only used for per-asteroid SBDB fallback)
    jpl_limiter = TokenBucketRateLimiter(rate=JPL_RATE, burst=JPL_BURST)

    # Bulk cache — refreshed once per BULK_CACHE_TTL
    bulk_cache = BulkCache()

    async with httpx.AsyncClient(
        timeout=httpx.Timeout(120.0, connect=15.0),  # Longer timeout for bulk queries
        limits=httpx.Limits(max_connections=100, max_keepalive_connections=50),
        follow_redirects=True,
        headers={"User-Agent": "NEO-Orbital-Tracker/3.0 (Research)"},
    ) as http_client:

        cycle = 0
        while not _shutdown:
            cycle += 1
            console.rule(f"[pipeline]Agency Ingestion Cycle {cycle}")

            # ── Phase 1: Refresh bulk caches if stale ──
            if bulk_cache.is_stale:
                await bulk_cache.refresh(http_client, jpl_limiter)

            # ── Phase 2: Load asteroids ──
            asteroids = await asyncio.to_thread(load_asteroids_to_profile, BATCH_SIZE)
            if not asteroids:
                logger.info("No asteroids need profiling. Sleeping...")
                await asyncio.sleep(CYCLE_SLEEP)
                continue

            # Pre-check how many are in the SBDB bulk cache
            bulk_hits = sum(
                1 for a in asteroids
                if bulk_cache.lookup_sbdb(
                    a["asteroid_id"],
                    extract_designation_candidates(a["asteroid_id"], a["name"])
                ) is not None
            )
            logger.info(
                f"Processing {len(asteroids)} asteroids — "
                f"SBDB bulk hits: {bulk_hits}/{len(asteroids)} "
                f"({len(asteroids) - bulk_hits} need per-asteroid API calls)"
            )

            # Batch-preload skip statuses (1 query instead of 4*500)
            asteroid_ids = [a["asteroid_id"] for a in asteroids]
            skip_map = await asyncio.to_thread(batch_load_skip_statuses, asteroid_ids)
            if skip_map:
                logger.info(f"Skip map: {len(skip_map)} active 404-holds")

            # ── Phase 3: Fetch profiles ──
            sem = asyncio.Semaphore(MAX_CONCURRENT)
            all_statuses: list[tuple] = []
            status_lock = asyncio.Lock()
            completed = 0
            t_start = time.monotonic()

            async def process_one(ast):
                nonlocal completed
                async with sem:
                    try:
                        profile, statuses = await fetch_full_profile(
                            ast["asteroid_id"], ast["name"],
                            http_client, jpl_limiter, bulk_cache,
                            skip_map,
                        )
                        producer.send(AGENCY_TOPIC, key=ast["asteroid_id"], value=profile)

                        if statuses:
                            async with status_lock:
                                all_statuses.extend(statuses)

                        completed += 1
                        if completed % 200 == 0:
                            elapsed = time.monotonic() - t_start
                            rate = completed / elapsed if elapsed > 0 else 0
                            logger.info(f"  Progress: {completed}/{len(asteroids)} "
                                        f"({rate:.1f} ast/s)")
                    except Exception as e:
                        logger.error(f"  {ast['asteroid_id']}: {e}")

            tasks = [process_one(ast) for ast in asteroids]
            await asyncio.gather(*tasks)
            producer.flush()

            elapsed = time.monotonic() - t_start
            rate = len(asteroids) / elapsed if elapsed > 0 else 0
            logger.info(f"Batch complete: {len(asteroids)} asteroids in {elapsed:.1f}s "
                        f"({rate:.1f} ast/s)")

            # ── Phase 4: Batch-write all fetch statuses ──
            if all_statuses:
                await asyncio.to_thread(batch_save_fetch_statuses, all_statuses)
                logger.info(f"Saved {len(all_statuses)} fetch statuses in batch")

            # Fireball batch (once per cycle)
            try:
                await ingest_fireballs(http_client, jpl_limiter, producer)
            except Exception as e:
                logger.warning(f"Fireball ingest failed: {e}")

            logger.info(f"Cycle {cycle} complete. Sleeping {CYCLE_SLEEP}s...")
            await asyncio.sleep(CYCLE_SLEEP)

    producer.close()
    global _pool
    if _pool and not _pool.closed:
        _pool.closeall()
        _pool = None
    logger.info("Agency producer shut down cleanly.")


def main():
    console.rule("[pipeline]NEO Multi-Agency Data Producer (FULL BULK)")
    logger.info("Starting agency producer...")
    logger.info(f"CAD date range: {CAD_DATE_MIN} → {CAD_DATE_MAX} (dist<{CAD_DIST_MAX} au)")
    logger.info(f"SBDB rate limiter (fallback): {JPL_RATE} req/s, burst={JPL_BURST}")
    logger.info(f"Bulk cache TTL: {BULK_CACHE_TTL}s")
    logger.info(f"ALL 4 agencies use bulk fetch — per-asteroid only for SBDB cache misses")
    try:
        asyncio.run(run_ingestion_cycle())
    except KeyboardInterrupt:
        logger.info("Interrupted by user.")
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
