import json
import time
import requests
import threading
import sys
import os
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from kafka import KafkaProducer
from kafka.errors import KafkaError
from rich.table import Table
from rich.panel import Panel
from src.config import Config
from src.logger import logger, update_log_file, force_flush, console

NEOWS_NOT_FOUND_HOLD_DAYS = int(os.getenv("NEOWS_NOT_FOUND_HOLD_DAYS", "30"))


def _not_found_windows_path():
    return os.path.join(Config.PRODUCER_CHECKPOINT_DIR, "neows_not_found_windows.json")


def load_not_found_windows():
    path = _not_found_windows_path()
    if not os.path.exists(path):
        return {}
    try:
        with open(path, "r") as f:
            return json.load(f)
    except Exception as e:
        logger.warning(f"Could not load NeoWs 404 hold file: {e}")
        return {}


def save_not_found_windows(windows):
    try:
        os.makedirs(Config.PRODUCER_CHECKPOINT_DIR, exist_ok=True)
        with open(_not_found_windows_path(), "w") as f:
            json.dump(windows, f, indent=2)
    except Exception as e:
        logger.warning(f"Could not save NeoWs 404 hold file: {e}")


def is_window_on_not_found_hold(start, end, windows=None):
    windows = windows if windows is not None else load_not_found_windows()
    hold = windows.get(f"{start}:{end}")
    if not hold:
        return False
    try:
        return datetime.fromisoformat(hold["not_found_until"]) > datetime.now()
    except Exception:
        return False


def mark_window_not_found(start, end):
    windows = load_not_found_windows()
    hold_until = datetime.now() + timedelta(days=NEOWS_NOT_FOUND_HOLD_DAYS)
    windows[f"{start}:{end}"] = {
        "window_start": start,
        "window_end": end,
        "last_status_code": 404,
        "last_checked_at": datetime.now().isoformat(),
        "not_found_until": hold_until.isoformat(),
    }
    save_not_found_windows(windows)
    return hold_until


# =========================================================
# CHECKPOINT MANAGER
# =========================================================
class CheckpointManager:
    def __init__(self, run_id):
        self.run_id = run_id
        self.windows = {}  # key = window_start, value = dict
        self.run_start_time = datetime.now()
        
        # Paths
        self.run_dir = Config.PRODUCER_CHECKPOINT_DIR
        self.filepath = os.path.join(self.run_dir, f"{self.run_id}.json")
        
        # Ensure dir exists
        os.makedirs(self.run_dir, exist_ok=True)
        
        # Load existing run data if it exists
        if os.path.exists(self.filepath):
            try:
                with open(self.filepath, 'r') as f:
                    data = json.load(f)
                    # Convert list back to dict for easy lookup
                    for w in data.get("windows_completed", []):
                        self.windows[w["window_start"]] = w
            except Exception as e:
                logger.error(f"Failed to load checkpoint: {e}")

    def register_window(self, start, end, status="PENDING"):
        # Explicitly tracking every window this run
        self.windows[start] = {
            "window_start": start,
            "window_end": end,
            "status": status,
            "records": 0,
            "error": None
        }

    def mark_success(self, start, end, records):
        self.windows[start] = {
            "window_start": start,
            "window_end": end,
            "status": "SUCCESS",
            "records": records,
            "error": None
        }
        self.save()

    def mark_failed(self, start, end, error):
        self.windows[start] = {
            "window_start": start,
            "window_end": end,
            "status": "FAILED",
            "records": 0,
            "error": str(error)
        }
        self.save()

    def mark_skipped(self, start, end, reason):
        self.windows[start] = {
            "window_start": start,
            "window_end": end,
            "status": "SKIPPED",
            "records": 0,
            "reason": reason
        }
        self.save()

    def finalize(self, termination_reason="COMPLETED", total_records=0):
        # Calculate stats
        total = len(self.windows)
        success = sum(1 for w in self.windows.values() if w["status"] == "SUCCESS")
        failed = sum(1 for w in self.windows.values() if w["status"] == "FAILED")
        skipped = sum(1 for w in self.windows.values() if w["status"] == "SKIPPED")
        pending = sum(1 for w in self.windows.values() if w["status"] == "PENDING")
        
        stats = {
            "total_windows": total,
            "successful_windows": success,
            "failed_windows": failed,
            "skipped_windows": skipped,
            "pending_windows": pending,
            "total_records_sent": total_records
        }
        
        self.save(final=True, termination_reason=termination_reason, stats=stats)
        return stats

    def save(self, final=False, termination_reason=None, stats=None):
        try:
            # Sort windows by start date
            ordered_windows = sorted(
                self.windows.values(),
                key=lambda x: x["window_start"]
            )
            
            payload = {
                "run_id": self.run_id,
                "run_started_at": self.run_start_time.isoformat(),
                "run_finished_at": datetime.now().isoformat() if final else None,
                "overall_range": {
                    "start_date": ordered_windows[0]["window_start"] if ordered_windows else Config.START_DATE,
                    "end_date": ordered_windows[-1]["window_end"] if ordered_windows else datetime.now().strftime("%Y-%m-%d")
                },
                "termination_reason": termination_reason,
                "stats": stats,
                "windows_completed": [w for w in ordered_windows if w["status"] == "SUCCESS"],
                "windows_failed": [w for w in ordered_windows if w["status"] != "SUCCESS"]
            }
            
            # Save specific run file only
            with open(self.filepath, 'w') as f:
                json.dump(payload, f, indent=2)
                 
        except Exception as e:
            logger.error(f"Failed to save checkpoint: {e}")

# =========================================================
# PIPELINE STATUS MANAGER (SYNC)
# =========================================================
class PipelineStatus:
    @staticmethod
    def update(status, run_id=None, wake_at=None, reason=None):
        """
        Updates the shared status file for Spark to read.
        Status: RUNNING | SLEEPING | COMPLETED
        """
        try:
            payload = {
                "status": status,
                "run_id": run_id or Config.RUN_ID,
                "last_updated": time.time(),
                "wake_at": wake_at, # Timestamp
                "reason": reason
            }
            # Ensure directory exists
            os.makedirs(os.path.dirname(Config.PIPELINE_STATUS_FILE), exist_ok=True)
            with open(Config.PIPELINE_STATUS_FILE, 'w') as f:
                json.dump(payload, f)
                f.flush()
                os.fsync(f.fileno())
        except Exception as e:
            logger.error(f"Failed to update pipeline status: {e}")


class MultiKeyRateLimiter:
    def __init__(self, keys):
        self.keys = keys if keys else ["DEMO_KEY"]
        self.lock = threading.Lock()

        self.key_cooldowns = {}      # key -> ready_at
        self.failed_this_round = set()

        self.global_sleep_until = 0

        # Configurable timing
        self.base_cooldown = 60 * 60     # 1 hour
        self.buffer = 5 * 60             # 5 min buffer

    def get_key(self):
        """
        Blocking call. Tries ALL keys before declaring global exhaustion.
        """
        while True:
            now = time.time()

            with self.lock:
                # Global sleep active
                if now < self.global_sleep_until:
                    wait = self.global_sleep_until - now
                else:
                    # Reset global sleep ONLY if passed
                    if self.global_sleep_until > 0 and now >= self.global_sleep_until:
                         self.global_sleep_until = 0
                    
                    # Try to find a ready key
                    for k in self.keys:
                        if (
                            k not in self.failed_this_round and
                            now >= self.key_cooldowns.get(k, 0)
                        ):
                            return k

                    # All keys have failed this round? -> global sleep
                    if len(self.failed_this_round) == len(self.keys):
                        self.failed_this_round.clear()
                        self.global_sleep_until = (
                            now + self.base_cooldown + self.buffer
                        )
                        wait = self.global_sleep_until - now
                        logger.warning(
                            f"⏸ ALL keys exhausted. Global sleep {wait/60:.1f} minutes"
                        )
                        # Notify Spark we are sleeping
                        PipelineStatus.update("SLEEPING", wake_at=self.global_sleep_until, reason="RATE_LIMIT")

                        # Ensure we flush logs so user sees this before we sleep
                        force_flush()

                    else:
                        # Keys still cooling from previous rounds or individual backoffs
                        # Wait for earliest cooldown
                        earliest = min(self.key_cooldowns.values(), default=now + 5)
                        wait = earliest - now

            # Sleep outside lock
            if wait > 0:
                time.sleep(max(wait, 5)) # Sleep at least 5s to avoid tight loops if wait is tiny
            # (Simple heuristic: if wait was long, we probably just woke up)
            # Actually better: just update to RUNNING if we found a key successfully
            

    def report_429(self, key):
        """
        Mark failure for this round. don't sleep yet.
        """
        with self.lock:
            if key in self.failed_this_round:
                return # Already handled, avoid log spam

            self.failed_this_round.add(key)
            # Set individual key cooldown (can be same as base or shorter, but safe to set to base)
            self.key_cooldowns[key] = time.time() + self.base_cooldown
            logger.warning(
                f"⚠️ Key {key[-4:]} hit 429. Marked failed for this round."
            )

    def report_success(self, key):
        """
        Clear key from failed_this_round if it succeeds.
        """
        with self.lock:
            self.failed_this_round.discard(key)

# =========================================================
# WORKER FUNCTION
# =========================================================
def fetch_and_send(start_date, end_date, limiter, producer, session):
    # key selection (BLOCKING if rate limits hit)
    # If we are blocked inside get_key for 1 hour, status is SLEEPING.
    # When we return, we are effectively RUNNING.
    api_key = limiter.get_key()
    
    # We got a key, so we are running. 
    # Update status to RUNNING (idempotent, lightweight json dump)
    # Optional: Cache last status to avoid IO spam? 
    # For simplicity, let's just do it in the main loop start.
    
    params = {'start_date': start_date, 'end_date': end_date, 'api_key': api_key}
    
    # Event to track if ANY send in this window fails
    send_failed = threading.Event()

    def on_send_error(excp):
        logger.error(f"❌ Kafka send failed: {excp}")
        send_failed.set()

    try:
        # Use passed session for connection reuse
        resp = session.get(Config.NASA_URL, params=params, timeout=30)
        
        if resp.status_code == 200:
            # Critical: Clear failure status on success
            limiter.report_success(api_key)

            data = resp.json()
            count = data.get('element_count', 0)
            neos = data.get('near_earth_objects', {})

            send_futures = []

            for date_str, objects in neos.items():
                for obj in objects:
                    if Config.PRODUCER_PARTITION_BY_DATE:
                        key_bytes = date_str.encode('utf-8')
                    else:
                        key_bytes = str(obj['id']).encode('utf-8')

                    message = {
                        "date": date_str,
                        "asteroid": obj
                    }

                    fut = producer.send(
                        Config.KAFKA_TOPIC, 
                        key=key_bytes, 
                        value=message
                    )
                    fut.add_errback(on_send_error)
                    send_futures.append(fut)

            # Wait for local confirmation to ensure data integrity
            # (Optional: can just check send_failed if you want non-blocking, 
            # but user requested treating send failure as window failure)
            # We'll check the flag after dispatch.
            
            # If any callback fired error immediately
            if send_failed.is_set():
                return False
                
            return count

        elif resp.status_code == 429:
            # Report the key and ask to RETRY
            limiter.report_429(api_key)
            return "RETRY"

        elif resp.status_code == 404:
            logger.warning(f"NeoWs returned 404 for {start_date} -> {end_date}; marking window as not found.")
            return "NOT_FOUND"
        
        else:
            logger.error(f"❌ API Error {resp.status_code}: {resp.text[:200]}")
            return False

    except Exception as e:
        logger.error(f"❌ Request failed ({start_date}): {e}")
        return False

# =========================================================
# MAIN LOGIC
# =========================================================
from concurrent.futures import wait, FIRST_COMPLETED
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, MofNCompleteColumn, TimeElapsedColumn, TimeRemainingColumn

def run_cycle(target_run_id=None):
    # 1. Setup Run ID
    if target_run_id:
        run_id = target_run_id
    else:
        run_id = f"run_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    Config.RUN_ID = run_id
    update_log_file(run_id)
    
    logger.info(f"🔄 STARTING INGESTION RUN: {datetime.now()} (ID: {run_id})")
    PipelineStatus.update("RUNNING", run_id=run_id)

    # High reliability + High Throughput Producer
    producer = KafkaProducer(
        bootstrap_servers=Config.KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda x: json.dumps(x).encode('utf-8'),
        acks='all',
        retries=10,
        max_in_flight_requests_per_connection=5,
        linger_ms=1000,
        batch_size=131072,
        compression_type="lz4"
    )

    checkpoint = CheckpointManager(run_id)
    limiter = MultiKeyRateLimiter(Config.NASA_API_KEYS)
    
    # Global Session for connection pooling
    session = requests.Session()
    # Optional: mount adapter for retries/pooling options
    adapter = requests.adapters.HTTPAdapter(pool_connections=10, pool_maxsize=10)
    session.mount('https://', adapter)

    try:
        start = datetime.strptime(Config.START_DATE, "%Y-%m-%d")
        end = datetime.now()

        # 2. Generate Windows (First-class objects)
        windows = []
        curr = start
        while curr <= end:
            window_end = min(curr + timedelta(days=6), end)
            windows.append({
                "window_start": curr.strftime("%Y-%m-%d"),
                "window_end": window_end.strftime("%Y-%m-%d")
            })
            curr = window_end + timedelta(days=1)

        # 3. Determine work (Tasks)
        tasks = []
        not_found_windows = load_not_found_windows()
        for w in windows:
            s_str = w["window_start"]
            e_str = w["window_end"]
            
            # Skip if already marked success in checkpoint
            if s_str in checkpoint.windows and checkpoint.windows[s_str]["status"] == "SUCCESS":
                continue

            if is_window_on_not_found_hold(s_str, e_str, not_found_windows):
                checkpoint.mark_skipped(s_str, e_str, "NeoWs returned 404 recently; retry hold is active")
                continue
                
            tasks.append((s_str, e_str))
            checkpoint.register_window(s_str, e_str, status="PENDING")

        if not tasks:
            logger.info("✨ No pending windows to process for this run.")
            return

        logger.info(f"📅 Remaining Windows to Process: {len(tasks)}")

        # 4. Process with Dynamic Retry Loop
        total_sent = sum(w["records"] for w in checkpoint.windows.values() if w["status"] == "SUCCESS")
        last_flush_count = 0 
        max_workers = len(limiter.keys) * 2
        logger.info(f"🚀 Launching with {max_workers} parallel workers")
        
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(bar_width=40),
            MofNCompleteColumn(),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            TimeElapsedColumn(),
            TimeRemainingColumn(),
            console=console
        ) as progress:
            
            main_task = progress.add_task("[cyan]Ingesting NASA Data...", total=len(tasks))
            
            with ThreadPoolExecutor(max_workers=max_workers) as pool:
                # Initial submission
                future_to_date = {
                    pool.submit(fetch_and_send, s, e, limiter, producer, session): (s, e) 
                    for s, e in tasks
                }
                
                while future_to_date:
                    # Wait for at least one future to complete
                    done, _ = wait(future_to_date.keys(), return_when=FIRST_COMPLETED)
                    
                    # Check status
                    if limiter.global_sleep_until <= time.time():
                        PipelineStatus.update("RUNNING")

                    for future in done:
                        start_key, end_key = future_to_date.pop(future)
                        
                        try:
                            result = future.result()

                            if result == "RETRY":
                                # Re-submit logic
                                new_future = pool.submit(fetch_and_send, start_key, end_key, limiter, producer, session)
                                future_to_date[new_future] = (start_key, end_key)

                            elif result == "NOT_FOUND":
                                hold_until = mark_window_not_found(start_key, end_key)
                                checkpoint.mark_skipped(
                                    start_key,
                                    end_key,
                                    f"NeoWs 404; retry held until {hold_until.date()}"
                                )
                                progress.advance(main_task)

                            elif result is not False:
                                # Success
                                count = int(result)
                                checkpoint.mark_success(start_key, end_key, count)
                                total_sent += count
                                progress.advance(main_task)
                                
                                # Flush Strategy
                                if (total_sent - last_flush_count) >= 5000:
                                    producer.flush(timeout=10)
                                    last_flush_count = total_sent
                                    
                            else:
                                # Hard failure
                                checkpoint.mark_failed(start_key, end_key, "Fetch or Send Failed")
                                progress.advance(main_task)

                        except Exception as e:
                            logger.error(f"❌ Worker Exception: {e}")
                            checkpoint.mark_failed(start_key, end_key, str(e))
                            progress.advance(main_task)

        sys.stdout.write("\n")
        logger.info(f"✅ Run Execution Finished. Flushing Kafka...")
        
    finally:
        # Graceful shutdown
        try:
            producer.flush()
            producer.close()
            session.close() # Close HTTP session
            PipelineStatus.update("COMPLETED") # Signal completion
        except Exception as e:
             logger.error(f"Error during shutdown: {e}")
    
    # 5. Finalize Stats & Report
    stats = checkpoint.finalize(
        termination_reason="COMPLETED",
        total_records=total_sent
    )

    # Rich summary table
    table = Table(title="🚀 Run Summary", show_header=True, header_style="bold cyan")
    table.add_column("Metric", style="dim")
    table.add_column("Value", justify="right", style="bold")
    table.add_row("Run ID", str(Config.RUN_ID))
    table.add_row("Total Windows", str(stats['total_windows']))
    table.add_row("✅ Successful", f"[green]{stats['successful_windows']}[/green]")
    table.add_row("❌ Failed", f"[red]{stats['failed_windows']}[/red]")
    table.add_row("⏳ Pending", f"[yellow]{stats['pending_windows']}[/yellow]")
    table.add_row("📦 Total Records", f"[bold white]{stats['total_records_sent']:,}[/bold white]")
    console.print(table)

def run_delta_cycle():
    """
    Lightweight cycle that only fetches the CURRENT week + 7 days ahead.
    Used for near-real-time updates between full scans.
    NASA updates close-approach data continuously, so we poll frequently.
    """
    run_id = f"delta_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    Config.RUN_ID = run_id
    update_log_file(run_id)

    logger.info(f"🔄 DELTA SCAN: Fetching current week data (ID: {run_id})")
    PipelineStatus.update("RUNNING", run_id=run_id)

    producer = KafkaProducer(
        bootstrap_servers=Config.KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda x: json.dumps(x).encode('utf-8'),
        acks='all',
        retries=10,
        max_in_flight_requests_per_connection=5,
        linger_ms=500,
        batch_size=65536,
        compression_type="lz4"
    )

    limiter = MultiKeyRateLimiter(Config.NASA_API_KEYS)
    session = requests.Session()
    adapter = requests.adapters.HTTPAdapter(pool_connections=5, pool_maxsize=5)
    session.mount('https://', adapter)

    total_sent = 0
    try:
        # Fetch today - 1 day through today + 14 days (covers upcoming approaches)
        start = datetime.now() - timedelta(days=1)
        end = datetime.now() + timedelta(days=14)

        curr = start
        while curr <= end:
            window_end = min(curr + timedelta(days=6), end)
            s_str = curr.strftime("%Y-%m-%d")
            e_str = window_end.strftime("%Y-%m-%d")

            result = fetch_and_send(s_str, e_str, limiter, producer, session)
            if result == "RETRY":
                # One retry attempt
                time.sleep(5)
                result = fetch_and_send(s_str, e_str, limiter, producer, session)

            if result == "NOT_FOUND":
                hold_until = mark_window_not_found(s_str, e_str)
                logger.warning(f"⏭ Delta window [{s_str} -> {e_str}] held after 404 until {hold_until.date()}")
                curr = window_end + timedelta(days=1)
                continue

            if result is not False and result != "RETRY":
                count = int(result)
                total_sent += count
                logger.info(f"✅ Delta [{s_str} -> {e_str}]: {count} records")

            curr = window_end + timedelta(days=1)

    finally:
        producer.flush()
        producer.close()
        session.close()
        PipelineStatus.update("COMPLETED")

    console.print(Panel(
        f"[bold green]✅ Delta Scan Complete[/bold green]\n"
        f"Records refreshed: [bold]{total_sent:,}[/bold]",
        title="Delta Cycle",
        border_style="green",
    ))
    force_flush()
    return total_sent


def get_latest_run_id():
    """Find the most recent run_*.json in the checkpoint directory."""
    try:
        path = Config.PRODUCER_CHECKPOINT_DIR
        if not os.path.exists(path):
            return None
        files = [f for f in os.listdir(path) if f.startswith("run_") and f.endswith(".json")]
        if not files:
            return None
        # Sort by filename (which contains timestamp)
        return sorted(files)[-1].replace(".json", "")
    except Exception:
        return None

def main():
    """
    Continuous pipeline loop:
    1. Initial full historical backfill (run_cycle)
    2. Sleep for 24 hours.
    3. Full re-scan every 24 hours.
    """
    FULL_SCAN_INTERVAL = 24 * 3600 # 24 hours between full scans

    # Phase 1: Full historical backfill
    latest_run = get_latest_run_id()
    if latest_run:
        logger.info(f"🚀 Found previous run: {latest_run}. Resuming...")
    else:
        logger.info("🚀 Phase 1: Full historical backfill starting...")
    
    try:
        run_cycle(target_run_id=latest_run)
    except Exception as e:
        logger.critical(f"🔥 Critical Failure in initial backfill: {e}")

    last_full_scan = time.time()

    # Phase 2: Continuous monitoring loop
    logger.info("🔁 Phase 2: Entering continuous monitoring mode (24h cycle)")
    while True:
        try:
            elapsed_since_full = time.time() - last_full_scan

            if elapsed_since_full >= FULL_SCAN_INTERVAL:
                # Time for a full re-scan
                logger.info("🔄 Periodic full re-scan triggered")
                run_cycle()
                last_full_scan = time.time()

        except Exception as e:
            logger.critical(f"🔥 Critical Failure in pipeline loop: {e}")

        wait_time = FULL_SCAN_INTERVAL - (time.time() - last_full_scan)
        if wait_time <= 0:
            wait_time = 60 # failsafe
        
        logger.info(f"💤 Next FULL scan in {int(wait_time // 3600)} hours...")
        PipelineStatus.update("SLEEPING", wake_at=time.time() + wait_time, reason="SCHEDULED")
        force_flush()
        time.sleep(wait_time)


if __name__ == "__main__":
    main()
