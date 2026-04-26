"""
Centralized logging with Rich console output.
Provides a beautiful terminal experience while maintaining file-based log rotation.
"""
import logging
import sys
import os
from logging.handlers import TimedRotatingFileHandler

from rich.console import Console
from rich.logging import RichHandler
from rich.theme import Theme

from src.config import Config

# ── Rich Console (shared instance) ─────────────────────
neo_theme = Theme({
    "info": "cyan",
    "warning": "yellow",
    "error": "bold red",
    "success": "bold green",
    "pipeline": "bold magenta",
})

console = Console(theme=neo_theme, force_terminal=True)


def setup_logger(name: str, log_filename: str) -> logging.Logger:
    """
    Creates a logger with:
      1. Rich console handler (beautiful terminal output)
      2. TimedRotatingFileHandler (daily rotation, 7-day retention)
    """
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    if not logger.handlers:
        # 1. Rich Console Handler
        rich_handler = RichHandler(
            console=console,
            show_time=True,
            show_path=False,
            markup=True,
            rich_tracebacks=True,
            tracebacks_show_locals=False,
        )
        rich_handler.setFormatter(logging.Formatter("%(message)s"))
        logger.addHandler(rich_handler)

        # 2. File Handler (plain text, rotating daily)
        try:
            os.makedirs(Config.LOG_DIR, exist_ok=True)
            log_path = os.path.join(Config.LOG_DIR, log_filename)

            file_formatter = logging.Formatter(
                "%(asctime)s [%(levelname)s] [%(name)s] %(message)s"
            )
            fh = TimedRotatingFileHandler(
                log_path, when="midnight", interval=1, backupCount=7
            )
            fh.suffix = "%Y-%m-%d"
            fh.setFormatter(file_formatter)
            logger.addHandler(fh)
        except Exception as e:
            sys.stderr.write(f"Failed to setup file logging: {e}\n")

    return logger


def force_flush():
    """Manually flushes all handlers for the shared logger."""
    log = logging.getLogger("NeoWsPipeline")
    for h in log.handlers:
        h.flush()
    sys.stdout.flush()


# ── Singleton Logger ────────────────────────────────────
logger = setup_logger("NeoWsPipeline", "pipeline.log")


def update_log_file(run_id: str):
    """Switches the file logger to a run-specific directory."""
    log = logging.getLogger("NeoWsPipeline")

    # Remove old file handlers
    handlers_to_remove = [
        h for h in log.handlers if isinstance(h, logging.FileHandler)
    ]
    for h in handlers_to_remove:
        log.removeHandler(h)
        h.close()

    # Add new handler for this run
    try:
        run_log_dir = os.path.join(Config.LOG_DIR, run_id)
        os.makedirs(run_log_dir, exist_ok=True)
        log_path = os.path.join(run_log_dir, "producer.log")

        fh = logging.FileHandler(log_path, encoding="utf-8")
        fh.setFormatter(
            logging.Formatter("%(asctime)s [%(levelname)s] [%(name)s] %(message)s")
        )
        log.addHandler(fh)

        log.info(f"📁 Logging switched to: {log_path}")
    except Exception as e:
        log.error(f"Failed to switch log file: {e}")