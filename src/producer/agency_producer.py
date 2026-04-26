import asyncio
import httpx
import json
import time
import os
import psycopg2
from datetime import datetime, timedelta
from kafka import KafkaProducer
from src.config import Config
from src.logger import logger, console
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TimeRemainingColumn

# Endpoints
JPL_SBDB_URL = "https://ssd-api.jpl.nasa.gov/sbdb.api"
JPL_SENTRY_URL = "https://ssd-api.jpl.nasa.gov/sentry.api"
JPL_CAD_URL = "https://ssd-api.jpl.nasa.gov/cad.api"
ESA_NEOCC_BASE = "https://neo.ssa.esa.int/PSDB-portlet/download"
MPC_ORB_URL = "https://data.minorplanetcenter.net/api/get-orb"

AGENCY_TOPIC = "agency_ingest"
POLL_INTERVAL = 24 * 3600  # 24 hours
NOT_FOUND_HOLD_DAYS = int(os.getenv("AGENCY_NOT_FOUND_HOLD_DAYS", "30"))
AGENCY_BATCH_LIMIT = int(os.getenv("AGENCY_BATCH_LIMIT", "2000"))
AGENCY_CONCURRENCY = int(os.getenv("AGENCY_CONCURRENCY", "25"))
AGENCY_SENTRY_SEED_LIMIT = int(os.getenv("AGENCY_SENTRY_SEED_LIMIT", "500"))
AGENCY_SSD_CONCURRENCY = int(os.getenv("AGENCY_SSD_CONCURRENCY", "1"))
AGENCY_SSD_REQUEST_DELAY = float(os.getenv("AGENCY_SSD_REQUEST_DELAY", "0.25"))
JPL_SSD_HOST = "ssd-api.jpl.nasa.gov"

def get_db_connection():
    return psycopg2.connect(
        host=Config.DB_HOST, port=Config.DB_PORT,
        dbname=Config.DB_NAME, user=Config.DB_USER, password=Config.DB_PASSWORD
    )

async def fetch_esa_risk_list(client):
    try:
        resp = await client.get(ESA_NEOCC_BASE, params={"file": "risk.lst"})
        if resp.status_code == 200:
            return resp.text
    except Exception as e:
        logger.error(f"Failed to fetch ESA risk list: {e}")
    return ""


async def fetch_sentry_seed_objects(client, request_limiter=None):
    if AGENCY_SENTRY_SEED_LIMIT <= 0:
        return []
    data, _status = await fetch_json_endpoint(
        client,
        "jpl_sentry_seed",
        JPL_SENTRY_URL,
        {"all": "true"},
        "sentry_seed",
        max_retries=2,
        request_limiter=request_limiter,
    )
    if not data or "data" not in data:
        return []

    seeds = []
    seen = set()
    for row in data.get("data", []):
        des = row.get("des")
        if not des or des in seen:
            continue
        seen.add(des)
        seeds.append({"asteroid_id": des, "neo_reference_id": des, "name": des})
        if len(seeds) >= AGENCY_SENTRY_SEED_LIMIT:
            break
    return seeds


def _safe_json(resp):
    try:
        return resp.json()
    except ValueError:
        return None


def normalize_designation(value):
    if not value:
        return None
    text = str(value).strip()
    if not text:
        return None

    # NeoWs names commonly look like "(2024 AB)" or "12345 (1998 AA)".
    if "(" in text and ")" in text:
        inside = text[text.find("(") + 1:text.find(")")].strip()
        if inside:
            return inside

    for prefix in ("asteroid ", "comet "):
        if text.lower().startswith(prefix):
            text = text[len(prefix):].strip()

    return text


def build_identifier_candidates(record):
    candidates = []
    for value in (
        normalize_designation(record.get("name")),
        record.get("neo_reference_id"),
        record.get("asteroid_id"),
        record.get("name"),
    ):
        if value and value not in candidates:
            candidates.append(str(value))
    return candidates


def build_source_status(source, status_code, failure_type=None, not_found_until=None, lookup_value=None):
    status = {
        "source": source,
        "status_code": status_code,
        "last_failure_type": failure_type,
        "not_found_until": not_found_until,
    }
    if lookup_value:
        status["lookup_value"] = lookup_value
    return status


def is_request_format_status(source, status_code):
    if status_code == 415:
        return True
    return source.startswith("jpl_") and status_code in {400, 405}


async def send_checked_request(client, method, url, *, source, request_limiter=None, **kwargs):
    if request_limiter and JPL_SSD_HOST in url:
        async with request_limiter:
            resp = await client.request(method, url, **kwargs)
            await asyncio.sleep(AGENCY_SSD_REQUEST_DELAY)
            return resp
    return await client.request(method, url, **kwargs)


async def fetch_json_endpoint(client, source, url, params, asteroid_id, max_retries=3, request_limiter=None):
    """Fetch one agency endpoint, retrying transient failures but never retrying 404s."""
    for attempt in range(max_retries):
        try:
            resp = await send_checked_request(
                client,
                "GET",
                url,
                source=source,
                request_limiter=request_limiter,
                params=params,
            )
            status = resp.status_code

            if status == 200:
                data = _safe_json(resp)
                if data is None:
                    logger.warning(f"Agency source {source} returned HTTP 200 but invalid JSON for {asteroid_id}.")
                    return None, build_source_status(source, status, "invalid_json")
                if isinstance(data, dict) and data.get("code") in {"400", "405"}:
                    logger.warning(f"Agency source {source} rejected request parameters for {asteroid_id}: {data.get('message')}")
                    return None, build_source_status(source, status, "request_format_error")
                return data, build_source_status(source, status)

            if status == 404:
                hold_until = datetime.utcnow() + timedelta(days=NOT_FOUND_HOLD_DAYS)
                logger.info(
                    f"Agency source {source} has no record for {asteroid_id}; "
                    f"holding retries until {hold_until.date()}"
                )
                return None, build_source_status(source, status, "not_found", hold_until)

            if is_request_format_status(source, status):
                logger.warning(
                    f"Agency source {source} rejected the request shape with HTTP {status} "
                    f"for {asteroid_id}; this is not an asteroid validity problem."
                )
                return None, build_source_status(source, status, "request_format_error")

            if 400 <= status < 500:
                logger.warning(f"Agency source {source} returned HTTP {status} for {asteroid_id}; not retrying client error.")
                return None, build_source_status(source, status, "client_error")

            if attempt == max_retries - 1:
                logger.error(f"Agency source {source} returned HTTP {status} for {asteroid_id} after retries.")
                return None, build_source_status(source, status, "server_error")

        except (httpx.ConnectTimeout, httpx.ReadTimeout, httpx.RemoteProtocolError) as e:
            if attempt == max_retries - 1:
                logger.error(f"Final failure fetching {source} for {asteroid_id}: {e}")
                return None, build_source_status(source, None, type(e).__name__)
        except Exception as e:
            logger.error(f"Unexpected error fetching {source} for {asteroid_id}: {e}")
            return None, build_source_status(source, None, type(e).__name__)

        wait = (attempt + 1) * 2
        logger.warning(f"Retry {attempt + 1}/{max_retries} for {source}:{asteroid_id}. Waiting {wait}s...")
        await asyncio.sleep(wait)

    return None, {
        "source": source,
        "status_code": None,
        "last_failure_type": "unknown",
        "not_found_until": None,
    }


async def fetch_first_available(client, source, url, param_name, candidates, base_params=None, request_limiter=None):
    statuses = []
    saw_not_found = False

    for candidate in candidates:
        params = dict(base_params or {})
        params[param_name] = candidate
        data, status = await fetch_json_endpoint(client, source, url, params, candidate, request_limiter=request_limiter)
        statuses.append(status)
        if data:
            status["lookup_value"] = candidate
            return data, statuses, candidate
        if status["status_code"] == 404:
            saw_not_found = True

    if statuses and saw_not_found and all(s["status_code"] == 404 for s in statuses):
        return None, [statuses[-1]], candidates[-1] if candidates else None
    return None, statuses, None


async def fetch_mpc_orbit(client, candidates, request_limiter=None):
    """
    MPC get-orb rejects query-string lookups with HTTP 415.
    It expects a JSON request body, so keep this path separate from the
    query-param based JPL endpoints.
    """
    statuses = []

    for candidate in candidates:
        for body in ({"desig": candidate}, {"desigs": [candidate]}):
            try:
                resp = await send_checked_request(
                    client,
                    "GET",
                    MPC_ORB_URL,
                    source="iau_mpc",
                    request_limiter=request_limiter,
                    json=body,
                    headers={"Content-Type": "application/json"},
                )
                status_code = resp.status_code

                if status_code == 200:
                    data = _safe_json(resp)
                    if data:
                        status = build_source_status("iau_mpc", status_code, lookup_value=candidate)
                        statuses.append(status)
                        return data, statuses, candidate

                if status_code == 404:
                    hold_until = datetime.utcnow() + timedelta(days=NOT_FOUND_HOLD_DAYS)
                    status = build_source_status("iau_mpc", status_code, "not_found", hold_until)
                    statuses.append(status)
                    break

                if status_code == 415:
                    logger.warning(
                        "MPC rejected the request format with HTTP 415; "
                        "check JSON body/content-type handling."
                    )
                    statuses.append(build_source_status("iau_mpc", status_code, "request_format_error"))
                    continue

                if 400 <= status_code < 500:
                    logger.warning(f"Agency source iau_mpc returned HTTP {status_code} for {candidate}; not retrying client error.")
                    statuses.append(build_source_status("iau_mpc", status_code, "client_error"))
                    break

                logger.warning(f"Agency source iau_mpc returned HTTP {status_code} for {candidate}; trying next request shape.")
                statuses.append(build_source_status("iau_mpc", status_code, "server_error"))

            except (httpx.ConnectTimeout, httpx.ReadTimeout, httpx.RemoteProtocolError) as e:
                statuses.append(build_source_status("iau_mpc", None, type(e).__name__))
            except Exception as e:
                logger.error(f"Unexpected error fetching iau_mpc for {candidate}: {e}")
                statuses.append(build_source_status("iau_mpc", None, type(e).__name__))
                break

    if statuses and all(s["status_code"] == 404 for s in statuses):
        return None, [statuses[-1]], candidates[-1] if candidates else None
    return None, statuses, None


def update_fetch_statuses(asteroid_id, statuses):
    if not statuses:
        return

    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                for status in statuses:
                    cur.execute("""
                        INSERT INTO neo_agency_fetch_status (
                            asteroid_id, source, status_code, last_failure_type,
                            last_checked_at, not_found_until
                        )
                        VALUES (%s, %s, %s, %s, now(), %s)
                        ON CONFLICT (asteroid_id, source) DO UPDATE SET
                            status_code = EXCLUDED.status_code,
                            last_failure_type = EXCLUDED.last_failure_type,
                            last_checked_at = now(),
                            not_found_until = EXCLUDED.not_found_until
                    """, (
                        asteroid_id,
                        status["source"],
                        status["status_code"],
                        status["last_failure_type"],
                        status["not_found_until"],
                    ))
                conn.commit()
    except Exception as e:
        logger.warning(f"Could not update agency fetch status for {asteroid_id}: {e}")


def get_held_sources(asteroid_id):
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT source
                    FROM neo_agency_fetch_status
                    WHERE asteroid_id = %s
                      AND status_code = 404
                      AND not_found_until IS NOT NULL
                      AND not_found_until > now()
                """, (asteroid_id,))
                return {row[0] for row in cur.fetchall()}
    except Exception as e:
        logger.warning(f"Could not read agency fetch holds for {asteroid_id}: {e}")
        return set()


async def fetch_profile(client, asteroid_record, esa_risk_text, held_sources=None, request_limiter=None):
    asteroid_id = asteroid_record["asteroid_id"]
    candidates = build_identifier_candidates(asteroid_record)
    profile = {
        "asteroid_id": asteroid_id,
        "identifiers": {
            "name": asteroid_record.get("name"),
            "neo_reference_id": asteroid_record.get("neo_reference_id"),
            "lookup_candidates": candidates,
        },
        "agencies": {},
        "_fetch_status": [],
    }
    held_sources = held_sources or set()

    if "jpl_sbdb" not in held_sources:
        sbdb, statuses, matched = await fetch_first_available(
            client,
            "jpl_sbdb",
            JPL_SBDB_URL,
            "sstr",
            candidates,
            {"phys-par": "true", "discovery": "true", "ca-data": "false"},
            request_limiter=request_limiter,
        )
        profile["agencies"]["sbdb"] = sbdb
        profile["identifiers"]["sbdb_lookup"] = matched
        profile["_fetch_status"].extend(statuses)
    else:
        profile["agencies"]["sbdb"] = None

    if "jpl_sentry" not in held_sources:
        sentry, statuses, matched = await fetch_first_available(
            client,
            "jpl_sentry",
            JPL_SENTRY_URL,
            "des",
            candidates,
            request_limiter=request_limiter,
        )
        profile["agencies"]["sentry"] = sentry
        profile["identifiers"]["sentry_lookup"] = matched
        profile["_fetch_status"].extend(statuses)
    else:
        profile["agencies"]["sentry"] = None

    if "jpl_cad" not in held_sources:
        cad, statuses, matched = await fetch_first_available(
            client,
            "jpl_cad",
            JPL_CAD_URL,
            "des",
            candidates,
            {"date-min": "1900-01-01", "date-max": "2100-01-01", "dist-max": "10"},
            request_limiter=request_limiter,
        )
        profile["agencies"]["cad"] = cad
        profile["identifiers"]["cad_lookup"] = matched
        profile["_fetch_status"].extend(statuses)
    else:
        profile["agencies"]["cad"] = None

    if "iau_mpc" not in held_sources:
        mpc, statuses, matched = await fetch_mpc_orbit(client, candidates, request_limiter=request_limiter)
        profile["agencies"]["mpc"] = mpc
        profile["identifiers"]["mpc_lookup"] = matched
        profile["_fetch_status"].extend(statuses)
    else:
        profile["agencies"]["mpc"] = None

    profile["agencies"]["esa"] = {
        "on_risk_list": asteroid_id in esa_risk_text if esa_risk_text else False
    }

    return profile

async def run_ingestion_cycle(producer):
    logger.info("🚀 Starting Multi-Agency Profile Ingestion...")
    
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    WITH latest AS (
                        SELECT DISTINCT ON (a.asteroid_id)
                            a.asteroid_id,
                            a.neo_reference_id,
                            a.name,
                            a.is_potentially_hazardous,
                            a.close_approach_date
                        FROM neo_close_approaches a
                        ORDER BY a.asteroid_id, a.close_approach_date DESC
                    ),
                    coverage AS (
                        SELECT
                            l.*,
                            sb.asteroid_id IS NOT NULL AS has_sbdb,
                            se.asteroid_id IS NOT NULL AS has_sentry,
                            es.asteroid_id IS NOT NULL AS has_esa,
                            mp.asteroid_id IS NOT NULL AS has_mpc,
                            ca.asteroid_id IS NOT NULL AS has_cad,
                            COUNT(fs.source) FILTER (
                                WHERE fs.status_code = 404
                                  AND fs.not_found_until IS NOT NULL
                                  AND fs.not_found_until > now()
                            ) AS held_source_count
                        FROM latest l
                        LEFT JOIN neo_agency_sbdb sb ON l.asteroid_id = sb.asteroid_id
                        LEFT JOIN neo_agency_sentry se ON l.asteroid_id = se.asteroid_id
                        LEFT JOIN neo_agency_esa es ON l.asteroid_id = es.asteroid_id
                        LEFT JOIN neo_agency_mpc mp ON l.asteroid_id = mp.asteroid_id
                        LEFT JOIN (SELECT DISTINCT asteroid_id FROM neo_agency_cad) ca ON l.asteroid_id = ca.asteroid_id
                        LEFT JOIN neo_agency_fetch_status fs ON l.asteroid_id = fs.asteroid_id
                        GROUP BY
                            l.asteroid_id, l.neo_reference_id, l.name, l.is_potentially_hazardous,
                            l.close_approach_date, sb.asteroid_id, se.asteroid_id,
                            es.asteroid_id, mp.asteroid_id, ca.asteroid_id
                    )
                    SELECT asteroid_id, neo_reference_id, name
                    FROM coverage
                    WHERE (
                        NOT has_sbdb OR NOT has_sentry OR NOT has_esa OR NOT has_mpc OR NOT has_cad
                        OR is_potentially_hazardous = TRUE
                    )
                      AND (held_source_count < 4 OR NOT has_esa)
                    ORDER BY
                        is_potentially_hazardous DESC,
                        (has_sbdb::int + has_sentry::int + has_esa::int + has_mpc::int + has_cad::int) ASC,
                        close_approach_date DESC,
                        asteroid_id
                    LIMIT %s
                """, (AGENCY_BATCH_LIMIT,))
                rows = cur.fetchall()
                asteroids = [
                    {"asteroid_id": r[0], "neo_reference_id": r[1], "name": r[2]}
                    for r in rows
                ]
    except Exception as e:
        logger.error(f"Database error: {e}")
        return

    async with httpx.AsyncClient(timeout=20.0) as client:
        ssd_request_limiter = asyncio.Semaphore(max(1, AGENCY_SSD_CONCURRENCY))
        esa_risk_text = await fetch_esa_risk_list(client)
        sentry_seeds = await fetch_sentry_seed_objects(client, request_limiter=ssd_request_limiter)
        known_ids = {a["asteroid_id"] for a in asteroids}
        asteroids.extend([s for s in sentry_seeds if s["asteroid_id"] not in known_ids])

        logger.info(f"📅 Found {len(asteroids)} agency candidates to profile ({len(sentry_seeds)} from Sentry seed list).")
        
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(bar_width=40),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            TimeRemainingColumn(),
            console=console
        ) as progress:
            task = progress.add_task("[cyan]Fetching Agency Profiles...", total=len(asteroids))
            
            # Use a semaphore to limit concurrency (respect API limits)
            sem = asyncio.Semaphore(AGENCY_CONCURRENCY)

            async def process_with_sem(ast):
                async with sem:
                    ast_id = ast["asteroid_id"]
                    held_sources = get_held_sources(ast_id)
                    profile = await fetch_profile(
                        client,
                        ast,
                        esa_risk_text,
                        held_sources,
                        request_limiter=ssd_request_limiter,
                    )
                    update_fetch_statuses(ast_id, profile.pop("_fetch_status", []))
                    # Push to Kafka
                    producer.send(
                        AGENCY_TOPIC,
                        key=str(ast_id).encode('utf-8'),
                        value=profile
                    )
                    progress.advance(task)
                    # Small stagger to avoid burst limits
                    await asyncio.sleep(0.5)

            # Create tasks for all asteroids
            tasks = [process_with_sem(ast_id) for ast_id in asteroids]
            await asyncio.gather(*tasks)
                
    producer.flush()
    logger.info("✅ Agency Ingestion Cycle Complete.")

def main():
    logger.info("🔄 Starting Agency Producer Service")
    producer = KafkaProducer(
        bootstrap_servers=Config.KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        acks='all',
        retries=5
    )
    
    # Ensure topic exists
    from kafka.admin import KafkaAdminClient, NewTopic
    try:
        admin = KafkaAdminClient(bootstrap_servers=Config.KAFKA_BOOTSTRAP_SERVERS)
        if AGENCY_TOPIC not in admin.list_topics():
            admin.create_topics([NewTopic(name=AGENCY_TOPIC, num_partitions=6, replication_factor=1)])
            logger.info(f"✅ Created Kafka topic: {AGENCY_TOPIC}")
        admin.close()
    except Exception as e:
        logger.warning(f"⚠️ Could not verify/create topic {AGENCY_TOPIC}: {e}")
    
    while True:
        try:
            # Check how many were processed
            with get_db_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT COUNT(*) FROM neo_close_approaches")
                    total_available = cur.fetchone()[0]
            
            if total_available == 0:
                logger.info("⏳ Waiting for NeoWs data to arrive in database...")
                wait_time = 300 # 5 minutes
            else:
                asyncio.run(run_ingestion_cycle(producer))
                wait_time = POLL_INTERVAL
                
        except Exception as e:
            logger.error(f"🔥 Cycle failed: {e}")
            wait_time = 60
            
        logger.info(f"💤 Next cycle in {wait_time // 60} minutes...")
        time.sleep(wait_time)

if __name__ == "__main__":
    main()
