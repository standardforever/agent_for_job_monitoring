import logging
from logging.handlers import BaseRotatingHandler
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


# ── Log folder: <project_root>/logs/ ──────────────────────────────────────────
LOG_DIR = Path(__file__).resolve().parents[2] / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

MAX_BYTES = 2 * 1024 * 1024  # 2 MB per file


# ── JSON formatter ─────────────────────────────────────────────────────────────
class JsonFormatter(logging.Formatter):
    def formatTime(self, record, datefmt=None):
        dt = datetime.fromtimestamp(record.created, tz=timezone.utc)
        return dt.isoformat()

    def format(self, record):
        exclude_attrs = {
            "args", "asctime", "created", "exc_info", "exc_text", "filename",
            "id", "levelno", "lineno", "message", "module", "msecs", "funcName",
            "msg", "pathname", "process", "processName", "relativeCreated",
            "stack_info", "thread", "threadName", "levelname",
        }

        log_record = {
            "timestamp": self.formatTime(record, self.datefmt),
            "function":  record.funcName,
            "message":   record.getMessage(),
            "module":    record.module,
            "level":     record.levelname,
            "domain":    getattr(record, "domain", "unknown"),
        }

        for attr, value in record.__dict__.items():
            if attr not in exclude_attrs:
                log_record[attr] = value

        return json.dumps(log_record)


# ── Rotating handler that never deletes — creates timestamped files ────────────
class NewFileRotatingHandler(BaseRotatingHandler):
    """
    Writes to  logs/job_process_<start_timestamp>.log
    On startup, resumes the most recent log file if it is still under MAX_BYTES
    so that application restarts do not fragment logs into many small files.
    When the current file reaches MAX_BYTES a new timestamped file is opened.
    Old files are never touched.
    """

    def __init__(self, log_dir: Path, max_bytes: int = MAX_BYTES, encoding: str = "utf-8"):
        self.log_dir   = log_dir
        self.max_bytes = max_bytes
        first_path = self._resume_or_new_path()
        super().__init__(str(first_path), mode="a", encoding=encoding, delay=False)

    # ── internal helpers ───────────────────────────────────────────────────────
    def _new_path(self) -> Path:
        ts = datetime.now(tz=timezone.utc).strftime("%Y%m%dT%H%M%S")
        return self.log_dir / f"job_process_{ts}.log"

    def _resume_or_new_path(self) -> Path:
        """
        Return the most recently modified log file in log_dir if it is still
        under max_bytes; otherwise create a fresh timestamped path.
        This lets the handler survive application restarts without fragmenting
        logs into tiny files.
        """
        existing = sorted(
            self.log_dir.glob("job_process_*.log"),
            key=lambda p: p.stat().st_mtime,
            reverse=True,
        )
        if existing and existing[0].stat().st_size < self.max_bytes:
            return existing[0]
        return self._new_path()

    def _open_new_file(self) -> None:
        """Close current stream and open a brand-new timestamped file."""
        if self.stream:
            self.stream.flush()
            self.stream.close()
        new_path = self._new_path()
        self.baseFilename = str(new_path)
        self.stream = self._open()

    # ── BaseRotatingHandler interface ──────────────────────────────────────────
    def shouldRollover(self, record: logging.LogRecord) -> bool:
        if self.stream is None:
            return False
        try:
            return os.fstat(self.stream.fileno()).st_size >= self.max_bytes
        except OSError:
            return False

    def doRollover(self) -> None:
        self._open_new_file()


# ── Public API ─────────────────────────────────────────────────────────────────
def configure_logging() -> None:
    root_logger = logging.getLogger("job_pipeline")
    if root_logger.handlers:
        return

    root_logger.setLevel(logging.INFO)
    root_logger.propagate = False

    formatter = JsonFormatter()

    # Console
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    root_logger.addHandler(stream_handler)

    # Rotating file — resumes latest file on restart, creates new only when full
    file_handler = NewFileRotatingHandler(LOG_DIR, max_bytes=MAX_BYTES)
    file_handler.setFormatter(formatter)
    root_logger.addHandler(file_handler)


def get_logger(name: str) -> logging.Logger:
    configure_logging()
    return logging.getLogger(f"job_pipeline.{name}")


def setup_logger(name: str) -> logging.Logger:
    return get_logger(name)


def log_event(
    logger: logging.Logger,
    level: str,
    message: str,
    *args: Any,
    domain: str | None = None,
    **fields: Any,
) -> None:
    extra = {"domain": str(domain or "unknown")}
    extra.update(fields)
    getattr(logger, level.lower())(message, *args, extra=extra)