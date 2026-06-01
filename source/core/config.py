from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv
from utils.logging import get_logger, log_event

logger = get_logger("config")


def load_environment() -> None:
    """Load env files from both repo root and source/ for local API runs."""
    source_env = Path(__file__).resolve().parents[1] / ".env"
    repo_env = Path(__file__).resolve().parents[2] / ".env"
    for env_path in (repo_env, source_env):
        if env_path.exists():
            load_dotenv(env_path, override=False)
            log_event(
                logger,
                "info",
                "environment_loaded env_path=%s",
                str(env_path),
                domain="config",
                env_path=str(env_path),
            )


load_environment()


@dataclass(slots=True)
class Settings:
    selenium_remote_url: str = os.getenv("SELENIUM_REMOTE_URL", "http://127.0.0.1:4445/wd/hub")
    selenium_grid_urls: str = os.getenv("SELENIUM_GRID_URLS", "")
    celery_broker_url: str = os.getenv("CELERY_BROKER_URL", "redis://127.0.0.1:6379/0")
    celery_result_backend: str = os.getenv("CELERY_RESULT_BACKEND", "redis://127.0.0.1:6379/1")
    heartbeat_interval_seconds: int = int(os.getenv("HEARTBEAT_INTERVAL_SECONDS", "30"))
    watchdog_interval_seconds: int = int(os.getenv("WATCHDOG_INTERVAL_SECONDS", "60"))
    stale_task_seconds: int = int(os.getenv("STALE_TASK_SECONDS", "300"))
    task_max_attempts: int = int(os.getenv("TASK_MAX_ATTEMPTS", "3"))
    max_sessions_per_selenium: int = int(os.getenv("MAX_SESSIONS_PER_SELENIUM", "10"))
    client_registration_password: str = os.getenv("CLIENT_REGISTRATION_PASSWORD", "")
    default_agent_count: int = int(os.getenv("DEFAULT_AGENT_COUNT", "1"))
    post_navigation_delay_ms: int = int(os.getenv("POST_NAVIGATION_DELAY_MS", "5000"))

    mongodb_uri: str = os.getenv("MONGODB_URI", "mongodb://admin:secret@127.0.0.1:27017")
    mongodb_database: str = os.getenv("MONGODB_DATABASE", "job_monitoring_agent")
    mongodb_clients_collection: str = os.getenv("MONGODB_CLIENTS_COLLECTION", "clients")
    mongodb_process_uploads_collection: str = os.getenv("MONGODB_PROCESS_UPLOADS_COLLECTION", "process_uploads")
    mongodb_process_domain_tasks_collection: str = os.getenv(
        "MONGODB_PROCESS_DOMAIN_TASKS_COLLECTION",
        "process_domain_tasks",
    )
    mongodb_selenium_nodes_collection: str = os.getenv("MONGODB_SELENIUM_NODES_COLLECTION", "selenium_nodes")
    mongodb_selenium_session_slots_collection: str = os.getenv(
        "MONGODB_SELENIUM_SESSION_SLOTS_COLLECTION",
        "selenium_session_slots",
    )

    process_email_subject_prefix: str = os.getenv("PROCESS_EMAIL_SUBJECT_PREFIX", "")


def get_settings() -> Settings:
    settings = Settings()
    log_event(
        logger,
        "info",
        "settings_loaded mongodb_database=%s clients_collection=%s process_uploads_collection=%s",
        settings.mongodb_database,
        settings.mongodb_clients_collection,
        settings.mongodb_process_uploads_collection,
        domain="config",
        mongodb_database=settings.mongodb_database,
        mongodb_clients_collection=settings.mongodb_clients_collection,
        mongodb_process_uploads_collection=settings.mongodb_process_uploads_collection,
    )
    return settings
