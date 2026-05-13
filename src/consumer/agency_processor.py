"""
Agency Kafka Consumer / Processor

Consumes messages from the 'agency_ingest' Kafka topic and upserts
into the respective neo_agency_* tables.

Column lists are derived from LIVE API field discovery (2026-04-27)
and exactly match schema.sql v4. Zero JSONB — every field stored.

Performance:
  - ALL upserts use psycopg2.extras.execute_values for batch efficiency
  - SBDB/Sentry/ESA are grouped and batch-upserted per poll cycle
  - Manual offset commit for backpressure control
  - Full progress tracking: lag, throughput, ETA, per-source timing
"""

import json
import hashlib
import os
import time
from collections import defaultdict

import psycopg2
import psycopg2.extras
import psycopg2.pool
from contextlib import contextmanager
from kafka import KafkaConsumer, TopicPartition
from kafka.admin import KafkaAdminClient, NewTopic

from src.config import Config
from src.logger import logger, console

from rich.table import Table


AGENCY_TOPIC = os.getenv("AGENCY_TOPIC", "agency_ingest")
CONSUMER_GROUP = "agency-processor"

# Tuning knobs
POLL_MAX_RECORDS = int(os.getenv("AGENCY_POLL_MAX_RECORDS", "2000"))
POLL_TIMEOUT_MS = int(os.getenv("AGENCY_POLL_TIMEOUT_MS", "3000"))
PROGRESS_INTERVAL = int(os.getenv("AGENCY_PROGRESS_INTERVAL", "30"))  # seconds
LOG_EVERY_N_BATCHES = int(os.getenv("AGENCY_LOG_EVERY_N", "10"))      # reduce log spam


# ── Connection Pool ──────────────────────────────────────────

_pool: psycopg2.pool.ThreadedConnectionPool | None = None

def _get_pool() -> psycopg2.pool.ThreadedConnectionPool:
    global _pool
    if _pool is None or _pool.closed:
        _pool = psycopg2.pool.ThreadedConnectionPool(
            minconn=2,
            maxconn=10,
            host=Config.DB_HOST, port=Config.DB_PORT,
            dbname=Config.DB_NAME, user=Config.DB_USER, password=Config.DB_PASSWORD,
            keepalives=1, keepalives_idle=30, keepalives_interval=10, keepalives_count=5,
        )
    return _pool

@contextmanager
def db_connection():
    pool = _get_pool()
    conn = pool.getconn()
    try:
        yield conn
    except Exception as e:
        logger.error(f"DB connection error: {e}")
        raise
    finally:
        pool.putconn(conn)


def compute_hash(record: dict) -> str:
    """Deterministic MD5 hash of all values for CDC.
    Excludes meta-fields so retries produce identical hashes."""
    exclude = {"row_hash", "ingestion_time"}
    raw = "||".join(str(v) for v in sorted(
        (k, v) for k, v in record.items() if k not in exclude
    ))
    return hashlib.md5(raw.encode()).hexdigest()


def _dedupe_rows_for_upsert(
    rows: list[tuple],
    key_indices: tuple[int, ...],
    label: str = "",
) -> list[tuple]:
    """
    execute_values + ON CONFLICT fails if two proposed rows share the same conflict key.
    Kafka polls can repeat the same asteroid in one batch — keep last row per key.
    """
    if not rows or not key_indices:
        return rows
    n_before = len(rows)
    by_key: dict[tuple, tuple] = {}
    for row in rows:
        key = tuple(row[i] for i in key_indices)
        by_key[key] = row
    out = list(by_key.values())
    if len(out) < n_before and label:
        logger.debug(
            f"{label}: deduped batch {n_before} -> {len(out)} rows (duplicate conflict keys in one INSERT)"
        )
    return out


# ══════════════════════════════════════════════════════════════
# Progress Tracker
# ══════════════════════════════════════════════════════════════

class ProgressTracker:
    """Tracks throughput, lag, and per-source metrics."""

    def __init__(self):
        self.start_time = time.monotonic()
        self.last_report_time = self.start_time
        self.total_messages = 0
        self.total_batches = 0
        self.initial_lag: int | None = None
        self.current_lag: int | None = None

        # Per-source counters
        self.source_counts = defaultdict(int)    # e.g. sbdb=500, cad=12000
        self.source_time_ms = defaultdict(float) # cumulative ms per source
        self.batch_times: list[float] = []       # last 50 batch times

    def record_batch(self, n_messages: int, batch_time_s: float,
                     source_breakdown: dict[str, int],
                     source_timings: dict[str, float]):
        """Record metrics for one poll cycle."""
        self.total_messages += n_messages
        self.total_batches += 1
        self.batch_times.append(batch_time_s)
        if len(self.batch_times) > 50:
            self.batch_times.pop(0)

        for src, count in source_breakdown.items():
            self.source_counts[src] += count
        for src, ms in source_timings.items():
            self.source_time_ms[src] += ms

    def update_lag(self, lag: int):
        if self.initial_lag is None:
            self.initial_lag = lag
        self.current_lag = lag

    @property
    def elapsed_s(self) -> float:
        return time.monotonic() - self.start_time

    @property
    def throughput(self) -> float:
        """Messages per second (overall)."""
        elapsed = self.elapsed_s
        return self.total_messages / elapsed if elapsed > 0 else 0

    @property
    def avg_batch_time(self) -> float:
        """Average batch time in seconds (recent 50)."""
        if not self.batch_times:
            return 0
        return sum(self.batch_times) / len(self.batch_times)

    @property
    def eta_seconds(self) -> float | None:
        """Estimated seconds to drain remaining lag."""
        if self.current_lag is None or self.throughput <= 0:
            return None
        return self.current_lag / self.throughput

    def should_report(self) -> bool:
        return (time.monotonic() - self.last_report_time) >= PROGRESS_INTERVAL

    def report(self):
        """Print a Rich progress summary table."""
        self.last_report_time = time.monotonic()
        elapsed = self.elapsed_s

        table = Table(
            title="[bold magenta]Agency Processor Progress[/]",
            show_header=True, header_style="bold cyan",
            border_style="dim", pad_edge=False,
        )
        table.add_column("Metric", style="white", min_width=22)
        table.add_column("Value", style="green", justify="right", min_width=16)

        # Overall
        table.add_row("Elapsed", _fmt_duration(elapsed))
        table.add_row("Total Messages", f"{self.total_messages:,}")
        table.add_row("Total Batches", f"{self.total_batches:,}")
        table.add_row("Throughput", f"{self.throughput:.1f} msg/s")
        table.add_row("Avg Batch Time", f"{self.avg_batch_time*1000:.0f} ms")

        # Lag
        if self.current_lag is not None:
            table.add_row("Remaining (lag)", f"{self.current_lag:,}")
            eta = self.eta_seconds
            if eta is not None and eta > 0:
                table.add_row("ETA", _fmt_duration(eta))
            processed_pct = 0
            if self.initial_lag and self.initial_lag > 0:
                processed_pct = (1 - self.current_lag / (self.initial_lag + self.total_messages)) * 100
            table.add_row("Progress", f"{processed_pct:.1f}%")

        # Per-source breakdown
        table.add_section()
        table.add_row("[bold]Source", "[bold]Rows / Time")
        for src in sorted(self.source_counts):
            count = self.source_counts[src]
            ms = self.source_time_ms.get(src, 0)
            table.add_row(f"  {src}", f"{count:,} / {ms:.0f}ms")

        console.print(table)


def _fmt_duration(seconds: float) -> str:
    if seconds < 60:
        return f"{seconds:.0f}s"
    m, s = divmod(int(seconds), 60)
    if m < 60:
        return f"{m}m {s}s"
    h, m = divmod(m, 60)
    return f"{h}h {m}m {s}s"


# ══════════════════════════════════════════════════════════════
# Column Definitions (unchanged from v4 schema)
# ══════════════════════════════════════════════════════════════

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

_SBDB_PLACEHOLDERS = ",".join(["%s"] * len(SBDB_COLUMNS))
_SBDB_UPDATES = ",".join(f"{c}=EXCLUDED.{c}" for c in SBDB_COLUMNS if c != "asteroid_id")
_SBDB_SQL = f"""
    INSERT INTO neo_agency_sbdb ({','.join(SBDB_COLUMNS)})
    VALUES %s
    ON CONFLICT (asteroid_id) DO UPDATE SET {_SBDB_UPDATES}
"""
_SBDB_TEMPLATE = f"({_SBDB_PLACEHOLDERS})"


# ──────────────────────────────────────────────────────────────
# Sentry Upsert — 26 columns
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

_SENTRY_PLACEHOLDERS = ",".join(["%s"] * len(SENTRY_COLUMNS))
_SENTRY_UPDATES = ",".join(f"{c}=EXCLUDED.{c}" for c in SENTRY_COLUMNS if c != "asteroid_id")
_SENTRY_SQL = f"""
    INSERT INTO neo_agency_sentry ({','.join(SENTRY_COLUMNS)})
    VALUES %s
    ON CONFLICT (asteroid_id) DO UPDATE SET {_SENTRY_UPDATES}
"""
_SENTRY_TEMPLATE = f"({_SENTRY_PLACEHOLDERS})"


# ──────────────────────────────────────────────────────────────
# CAD Upsert — 16 columns (14 API fields + approach_date + body)
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

_CAD_PLACEHOLDERS = ",".join(["%s"] * len(CAD_COLUMNS))
_CAD_UPDATES = ",".join(f"{c}=EXCLUDED.{c}" for c in CAD_COLUMNS if c not in ("asteroid_id", "approach_date"))
_CAD_SQL = f"""
    INSERT INTO neo_agency_cad ({','.join(CAD_COLUMNS)})
    VALUES %s
    ON CONFLICT (asteroid_id, approach_date) DO UPDATE SET {_CAD_UPDATES}
"""
_CAD_TEMPLATE = f"({_CAD_PLACEHOLDERS})"


# ──────────────────────────────────────────────────────────────
# ESA Upsert — 14 columns matching v4 schema
# ──────────────────────────────────────────────────────────────

ESA_COLUMNS = [
    "asteroid_id", "on_risk_list", "esa_designation", "esa_name",
    "diameter_m", "diameter_certain",
    "vi_date", "ip_max", "ps_max", "ts", "vel_km_s",
    "years", "ip_cum", "ps_cum",
    "row_hash",
]

_ESA_PLACEHOLDERS = ",".join(["%s"] * len(ESA_COLUMNS))
_ESA_UPDATES = ",".join(f"{c}=EXCLUDED.{c}" for c in ESA_COLUMNS if c != "asteroid_id")
_ESA_SQL = f"""
    INSERT INTO neo_agency_esa ({','.join(ESA_COLUMNS)})
    VALUES %s
    ON CONFLICT (asteroid_id) DO UPDATE SET {_ESA_UPDATES}
"""
_ESA_TEMPLATE = f"({_ESA_PLACEHOLDERS})"


# ──────────────────────────────────────────────────────────────
# Fireball Upsert — 9 columns (all API fields)
# ──────────────────────────────────────────────────────────────

FIREBALL_COLUMNS = [
    "event_date",
    "total_radiated_energy_j", "impact_energy_kt",
    "latitude", "latitude_dir", "longitude", "longitude_dir",
    "altitude_km", "velocity_km_s",
    "row_hash",
]

_FIREBALL_PLACEHOLDERS = ",".join(["%s"] * len(FIREBALL_COLUMNS))
_FIREBALL_UPDATES = ",".join(f"{c}=EXCLUDED.{c}" for c in FIREBALL_COLUMNS if c != "event_date")
_FIREBALL_SQL = f"""
    INSERT INTO neo_fireball_events ({','.join(FIREBALL_COLUMNS)})
    VALUES %s
    ON CONFLICT (event_date) DO UPDATE SET {_FIREBALL_UPDATES}
"""
_FIREBALL_TEMPLATE = f"({_FIREBALL_PLACEHOLDERS})"


# ══════════════════════════════════════════════════════════════
# Batch Processing — ALL sources use execute_values
# ══════════════════════════════════════════════════════════════

def process_batch(payloads: list[dict]) -> tuple[dict[str, int], dict[str, float]]:
    """
    Process a batch of parsed JSON payloads using a single DB connection.

    Groups all records by source and batch-upserts each table once,
    instead of per-message individual inserts.

    Returns:
        (source_counts, source_timings_ms) for progress tracking.
    """
    if not payloads:
        return {}, {}

    # ── Collect all rows per source across the entire batch ──
    sbdb_rows: list[tuple] = []
    sentry_rows: list[tuple] = []
    cad_rows: list[tuple] = []
    esa_rows: list[tuple] = []
    fireball_rows: list[tuple] = []

    for payload in payloads:
        asteroid_id = payload.get("asteroid_id")
        if not asteroid_id:
            continue

        agencies = payload.get("agencies", {})

        # SBDB
        sbdb = agencies.get("sbdb")
        if sbdb:
            sbdb["asteroid_id"] = asteroid_id
            sbdb["row_hash"] = compute_hash(sbdb)
            sbdb_rows.append(tuple(sbdb.get(c) for c in SBDB_COLUMNS))

        # Sentry
        sentry = agencies.get("sentry")
        if sentry:
            sentry["asteroid_id"] = asteroid_id
            sentry["row_hash"] = compute_hash(sentry)
            sentry_rows.append(tuple(sentry.get(c) for c in SENTRY_COLUMNS))

        # CAD (list of records)
        cad = agencies.get("cad")
        if isinstance(cad, list) and cad:
            for rec in cad:
                rec["asteroid_id"] = asteroid_id
                rec["row_hash"] = compute_hash(rec)
                cad_rows.append(tuple(rec.get(c) for c in CAD_COLUMNS))

        # ESA
        esa = agencies.get("esa")
        if esa:
            esa["asteroid_id"] = asteroid_id
            esa["row_hash"] = compute_hash(esa)
            esa_rows.append(tuple(esa.get(c) for c in ESA_COLUMNS))

        # Fireball (list of records)
        fireballs = agencies.get("fireball")
        if isinstance(fireballs, list) and fireballs:
            for rec in fireballs:
                rec["row_hash"] = compute_hash(rec)
                fireball_rows.append(tuple(rec.get(c) for c in FIREBALL_COLUMNS))

    # One INSERT … ON CONFLICT must not propose duplicate conflict keys
    sbdb_rows = _dedupe_rows_for_upsert(sbdb_rows, (0,), "sbdb")
    sentry_rows = _dedupe_rows_for_upsert(sentry_rows, (0,), "sentry")
    cad_rows = _dedupe_rows_for_upsert(cad_rows, (0, 1), "cad")
    esa_rows = _dedupe_rows_for_upsert(esa_rows, (0,), "esa")
    fireball_rows = _dedupe_rows_for_upsert(fireball_rows, (0,), "fireball")

    # ── Execute all batch upserts in one connection ──
    source_counts: dict[str, int] = {}
    source_timings: dict[str, float] = {}

    with db_connection() as conn:
        with conn.cursor() as cur:
            if sbdb_rows:
                t0 = time.monotonic()
                psycopg2.extras.execute_values(cur, _SBDB_SQL, sbdb_rows, template=_SBDB_TEMPLATE, page_size=200)
                source_timings["sbdb"] = (time.monotonic() - t0) * 1000
                source_counts["sbdb"] = len(sbdb_rows)

            if sentry_rows:
                t0 = time.monotonic()
                psycopg2.extras.execute_values(cur, _SENTRY_SQL, sentry_rows, template=_SENTRY_TEMPLATE, page_size=200)
                source_timings["sentry"] = (time.monotonic() - t0) * 1000
                source_counts["sentry"] = len(sentry_rows)

            if cad_rows:
                t0 = time.monotonic()
                # CAD can be huge — chunk into pages of 500
                psycopg2.extras.execute_values(cur, _CAD_SQL, cad_rows, template=_CAD_TEMPLATE, page_size=500)
                source_timings["cad"] = (time.monotonic() - t0) * 1000
                source_counts["cad"] = len(cad_rows)

            if esa_rows:
                t0 = time.monotonic()
                psycopg2.extras.execute_values(cur, _ESA_SQL, esa_rows, template=_ESA_TEMPLATE, page_size=200)
                source_timings["esa"] = (time.monotonic() - t0) * 1000
                source_counts["esa"] = len(esa_rows)

            if fireball_rows:
                t0 = time.monotonic()
                psycopg2.extras.execute_values(cur, _FIREBALL_SQL, fireball_rows, template=_FIREBALL_TEMPLATE, page_size=200)
                source_timings["fireball"] = (time.monotonic() - t0) * 1000
                source_counts["fireball"] = len(fireball_rows)

        conn.commit()

    return source_counts, source_timings


# ══════════════════════════════════════════════════════════════
# Kafka Lag Helpers
# ══════════════════════════════════════════════════════════════

def get_consumer_lag(consumer: KafkaConsumer) -> int | None:
    """Calculate total consumer lag across all assigned partitions."""
    try:
        partitions = consumer.assignment()
        if not partitions:
            return None

        end_offsets = consumer.end_offsets(partitions)
        total_lag = 0
        for tp in partitions:
            end = end_offsets.get(tp, 0)
            current = consumer.position(tp)
            lag = max(0, end - current)
            total_lag += lag
        return total_lag
    except Exception as e:
        logger.debug(f"Lag check failed: {e}")
        return None


def wait_for_partition_assignment(
    consumer: KafkaConsumer,
    max_wait_s: float = 30.0,
    poll_ms: int = 500,
) -> bool:
    """
    Drive the consumer until the group coordinator assigns partitions.
    A single poll() is often not enough right after connect — rebalance is async.
    """
    deadline = time.monotonic() + max_wait_s
    while time.monotonic() < deadline:
        consumer.poll(timeout_ms=poll_ms)
        if consumer.assignment():
            return True
    return False


def ensure_topic(topic_name: str, num_partitions: int = 6, replication_factor: int = 1):
    """Create a Kafka topic if it doesn't already exist."""
    try:
        admin = KafkaAdminClient(bootstrap_servers=Config.KAFKA_BOOTSTRAP_SERVERS)
        if topic_name not in admin.list_topics():
            logger.info(f"Creating missing Kafka topic: {topic_name}")
            admin.create_topics([
                NewTopic(name=topic_name, num_partitions=num_partitions, replication_factor=replication_factor)
            ])
        admin.close()
    except Exception as e:
        logger.warning(f"Topic verification failed for {topic_name}: {e}")


# ══════════════════════════════════════════════════════════════
# Main Consumer Loop
# ══════════════════════════════════════════════════════════════

def main():
    console.rule("[pipeline]Agency Processor[/]")
    logger.info(f"Starting agency processor (topic={AGENCY_TOPIC}, group={CONSUMER_GROUP})")
    logger.info(f"Config: poll_max={POLL_MAX_RECORDS}, poll_timeout={POLL_TIMEOUT_MS}ms, progress_interval={PROGRESS_INTERVAL}s")

    ensure_topic(AGENCY_TOPIC)

    consumer = KafkaConsumer(
        AGENCY_TOPIC,
        bootstrap_servers=Config.KAFKA_BOOTSTRAP_SERVERS,
        group_id=CONSUMER_GROUP,
        auto_offset_reset="earliest",
        enable_auto_commit=False,        # Manual commit for backpressure control
        value_deserializer=None,
        fetch_max_bytes=5242880,          # Max 5MB per poll to prevent Python OOM
        max_poll_interval_ms=600000,      # 10 minutes — allow slow batches
        fetch_min_bytes=32768,            # Wait for 32KB before responding (batch accumulation)
        fetch_max_wait_ms=3000,           # Max wait 3s for batch accumulation
        max_partition_fetch_bytes=1048576, # 1MB per partition
    )

    logger.info("Agency processor connected. Waiting for partition assignment...")

    tracker = ProgressTracker()

    if wait_for_partition_assignment(consumer, max_wait_s=30.0):
        initial_lag = get_consumer_lag(consumer)
        if initial_lag is not None:
            tracker.update_lag(initial_lag)
            logger.info(f"[bold cyan]Initial Kafka lag: {initial_lag:,} messages[/]")
        else:
            logger.info("Partitions assigned; lag not available (empty topic or broker quirk)")
    else:
        logger.warning(
            "No partition assignment after 30s — group rebalance still pending or broker issue. "
            "Lag will be reported after the first successful poll with data."
        )

    logger.info("Waiting for messages...")

    try:
        while True:
            msg_pack = consumer.poll(timeout_ms=POLL_TIMEOUT_MS, max_records=POLL_MAX_RECORDS)
            if not msg_pack:
                # Idle — check lag and report if interval elapsed
                if tracker.should_report() and tracker.total_messages > 0:
                    lag = get_consumer_lag(consumer)
                    if lag is not None:
                        tracker.update_lag(lag)
                    tracker.report()
                continue

            # ── Deserialize messages ──
            payloads = []
            for tp, messages in msg_pack.items():
                for message in messages:
                    try:
                        payload = json.loads(message.value.decode("utf-8"))
                        payloads.append(payload)
                    except (json.JSONDecodeError, UnicodeDecodeError) as e:
                        logger.warning(f"Skipping malformed message: {e}")

            if not payloads:
                consumer.commit()
                continue

            # ── Process batch with timing ──
            batch_start = time.monotonic()
            source_counts, source_timings = process_batch(payloads)
            batch_time = time.monotonic() - batch_start

            # ── Commit offsets after successful processing ──
            consumer.commit()

            # ── Record metrics ──
            tracker.record_batch(len(payloads), batch_time, source_counts, source_timings)

            # Per-batch log line — only log every Nth batch to reduce spam
            if tracker.total_batches % LOG_EVERY_N_BATCHES == 0 or len(payloads) > 50:
                src_summary = " | ".join(f"{k}={v}" for k, v in sorted(source_counts.items()))
                logger.info(
                    f"Batch #{tracker.total_batches}: {len(payloads)} msgs in {batch_time*1000:.0f}ms "
                    f"({tracker.throughput:.1f} msg/s) [{src_summary}]"
                )

            # ── Periodic detailed report ──
            if tracker.should_report():
                lag = get_consumer_lag(consumer)
                if lag is not None:
                    tracker.update_lag(lag)
                tracker.report()

    except KeyboardInterrupt:
        logger.info("Agency processor shutting down.")
    finally:
        # Final report
        if tracker.total_messages > 0:
            console.rule("[pipeline]Final Summary[/]")
            tracker.report()

        consumer.close()
        global _pool
        if _pool and not _pool.closed:
            _pool.closeall()


if __name__ == "__main__":
    main()
