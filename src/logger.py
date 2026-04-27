"""Centralized application logging."""
import logging
import sys
import os
from logging.handlers import TimedRotatingFileHandler

from rich.console import Console
from rich.logging import RichHandler
from rich.theme import Theme

from src.config import Config

neo_theme = Theme({
    "info": "cyan",
    "warning": "yellow",
    "error": "bold red",
    "success": "bold green",
    "pipeline": "bold magenta",
})

console = Console(theme=neo_theme, force_terminal=True)


def _log_level() -> int:
    level_name = os.getenv("LOG_LEVEL", "INFO").upper()
    return getattr(logging, level_name, logging.INFO)


def _file_formatter() -> logging.Formatter:
    return logging.Formatter(
        "%(asctime)s [%(levelname)s] [%(name)s] %(message)s"
    )


def setup_logger(name: str, log_filename: str) -> logging.Logger:
    """
    Create a logger with clean console output and daily rotating file logs.
    """
    logger = logging.getLogger(name)
    logger.setLevel(_log_level())
    logger.propagate = False

    if not logger.handlers:
        rich_handler = RichHandler(
            console=console,
            show_time=True,
            show_path=False,
            markup=True,
            rich_tracebacks=True,
            tracebacks_show_locals=False,
        )
        rich_handler.setFormatter(logging.Formatter("%(message)s"))
        rich_handler.setLevel(_log_level())
        logger.addHandler(rich_handler)

        try:
            if os.path.exists(Config.LOG_DIR):
                if not os.path.isdir(Config.LOG_DIR):
                    sys.stderr.write(f"Removing file at {Config.LOG_DIR} to create log directory.\n")
                    os.remove(Config.LOG_DIR)
                    os.makedirs(Config.LOG_DIR, exist_ok=True)
            else:
                os.makedirs(Config.LOG_DIR, exist_ok=True)
                
            log_path = os.path.join(Config.LOG_DIR, log_filename)

            fh = TimedRotatingFileHandler(
                log_path,
                when="midnight",
                interval=1,
                backupCount=int(os.getenv("LOG_RETENTION_DAYS", "14")),
                encoding="utf-8",
            )
            fh.suffix = "%Y-%m-%d"
            fh.setLevel(_log_level())
            fh.setFormatter(_file_formatter())
            logger.addHandler(fh)
        except Exception as e:
            sys.stderr.write(f"Failed to setup file logging: {e}\n")

    return logger


def get_logger(name: str, log_filename: str | None = None) -> logging.Logger:
    safe_name = name.replace(".", "_").replace("-", "_")
    return setup_logger(name, log_filename or f"{safe_name}.log")


def force_flush():
    """Flush handlers for all project loggers."""
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
        # Ensure base log dir exists first
        os.makedirs(Config.LOG_DIR, exist_ok=True)
        
        run_log_dir = os.path.join(Config.LOG_DIR, run_id)
        os.makedirs(run_log_dir, exist_ok=True)
        log_path = os.path.join(run_log_dir, "producer.log")

        fh = logging.FileHandler(log_path, encoding="utf-8")
        fh.setLevel(_log_level())
        fh.setFormatter(_file_formatter())
        log.addHandler(fh)

        log.info(f"Logging switched to: {log_path}")
    except Exception as e:
        log.error(f"Failed to switch log file: {e}")


for noisy_logger_name in ("httpx", "httpcore", "urllib3", "kafka", "py4j"):
    logging.getLogger(noisy_logger_name).setLevel(logging.WARNING)
