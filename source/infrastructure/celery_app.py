from __future__ import annotations

from celery import Celery

from core.config import get_settings


settings = get_settings()

celery_app = Celery(
    "job_monitoring_agent",
    broker=settings.celery_broker_url,
    backend=settings.celery_result_backend,
    include=["infrastructure.tasks"],
)

celery_app.conf.update(
    task_acks_late=True,
    task_reject_on_worker_lost=True,
    worker_prefetch_multiplier=1,
    task_default_queue="processes",
    task_routes={"infrastructure.tasks.*": {"queue": "processes"}},
    timezone="UTC",
)
