from __future__ import annotations

from datetime import datetime, timezone
from functools import lru_cache
from typing import Any
from uuid import uuid4

from pymongo import ASCENDING, DESCENDING

from core.config import Settings, get_settings
from services.email_service import build_alert_jobs_csv_attachment, send_email
from services.sync_mongodb_service import SyncMongoDBService, get_sync_mongodb_service
from utils.logging import get_logger, log_event


logger = get_logger("client_job_alert_service")


def _now() -> datetime:
    return datetime.now(timezone.utc)


class ClientJobAlertService:
    def __init__(self, mongodb: SyncMongoDBService, settings: Settings) -> None:
        self._settings = settings
        self._processes = mongodb.collection(settings.mongodb_process_uploads_collection)
        self._process_refs = mongodb.collection(settings.mongodb_process_domain_refs_collection)
        self._jobs = mongodb.collection(settings.mongodb_domain_jobs_collection)
        self._reports = mongodb.collection(settings.mongodb_client_job_reports_collection)
        self._alerts = mongodb.collection(settings.mongodb_client_job_alerts_collection)
        self._indexes_ready = False

    def ensure_indexes(self) -> None:
        if self._indexes_ready:
            return
        self._alerts.create_index([("alert_id", ASCENDING)], unique=True)
        self._alerts.create_index([("pipeline_run_id", ASCENDING)], unique=True)
        self._alerts.create_index([("process_id", ASCENDING), ("created_at", DESCENDING)])
        self._alerts.create_index([("status", ASCENDING), ("delivery.status", ASCENDING), ("created_at", ASCENDING)])
        self._indexes_ready = True

    def build_for_pipeline_run(self, process_id: str, pipeline_run_id: str) -> dict[str, Any]:
        self.ensure_indexes()
        process = self._load_process(process_id)
        report = self._load_report(process_id, pipeline_run_id)
        report = self._refresh_report_jobs(process, report)
        alert = self._build_alert(process, report)
        self._alerts.update_one({"pipeline_run_id": pipeline_run_id}, {"$set": alert}, upsert=True)
        self._remember_alert(process_id, alert)
        log_event(
            logger,
            "info",
            "client_job_alert_built",
            domain="alerts",
            process_id=process_id,
            pipeline_run_id=pipeline_run_id,
            alert_id=alert["alert_id"],
            total_new_jobs=alert["total_new_jobs"],
            total_relevant_jobs=alert["total_relevant_jobs"],
        )
        if self._settings.alert_auto_send and alert["total_relevant_jobs"] > 0:
            return self.send_alert(alert["alert_id"])
        return self._summary(alert)

    def rebuild_latest_for_process(self, process_id: str) -> dict[str, Any]:
        self.ensure_indexes()
        report = self._reports.find_one({"process_id": process_id, "status": "ready"}, sort=[("generated_at", DESCENDING)])
        if not report:
            raise ValueError(f"No ready report exists for process '{process_id}'")
        return self.build_for_pipeline_run(process_id, str(report["pipeline_run_id"]))

    def send_alert(self, alert_id: str) -> dict[str, Any]:
        self.ensure_indexes()
        alert = self._load_alert(alert_id)
        if alert.get("total_relevant_jobs", 0) <= 0:
            return self._mark_delivery_skipped(alert, "No relevant jobs to send")
        recipients = [email for email in alert.get("recipients") or [] if email]
        if not recipients:
            return self._mark_delivery_skipped(alert, "Client has no result emails configured")
        readiness_error = self._email_readiness_error()
        if readiness_error:
            return self._mark_delivery_not_configured(alert, readiness_error)
        try:
            provider_response = self._send_email(alert, recipients)
        except Exception as exc:
            log_event(
                logger,
                "warning",
                "client_job_alert_send_failed",
                domain="alerts",
                alert_id=alert_id,
                process_id=alert.get("process_id"),
                error=str(exc),
            )
            return self._mark_delivery_failed(alert, str(exc))
        return self._mark_delivery_sent(alert, recipients, provider_response)

    def send_pending(self, limit: int = 25) -> dict[str, Any]:
        self.ensure_indexes()
        cursor = self._alerts.find(
            {
                "status": "ready",
                "total_relevant_jobs": {"$gt": 0},
                "delivery.status": {"$in": ["pending", "failed", "not_configured"]},
            }
        ).sort("created_at", ASCENDING).limit(limit)
        sent = []
        skipped = []
        for alert in cursor:
            result = self.send_alert(str(alert["alert_id"]))
            if result.get("delivery", {}).get("status") == "sent":
                sent.append(result)
            else:
                skipped.append(result)
        return {"sent": sent, "skipped": skipped, "count": len(sent)}

    def list_alerts(self, *, process_id: str | None = None, limit: int = 25) -> dict[str, Any]:
        self.ensure_indexes()
        query = {"process_id": process_id} if process_id else {}
        alerts = list(self._alerts.find(query, {"_id": 0}).sort("created_at", DESCENDING).limit(limit))
        return {"alerts": alerts, "count": len(alerts)}

    def _load_process(self, process_id: str) -> dict[str, Any]:
        process = self._processes.find_one({"process_id": process_id})
        if not process:
            raise ValueError(f"Process '{process_id}' was not found")
        return process

    def _load_report(self, process_id: str, pipeline_run_id: str) -> dict[str, Any]:
        report = self._reports.find_one({"process_id": process_id, "pipeline_run_id": pipeline_run_id})
        if not report:
            raise ValueError(f"Report for pipeline run '{pipeline_run_id}' was not found")
        return report

    def _load_alert(self, alert_id: str) -> dict[str, Any]:
        alert = self._alerts.find_one({"alert_id": alert_id})
        if not alert:
            raise ValueError(f"Alert '{alert_id}' was not found")
        return alert

    def _refresh_report_jobs(self, process: dict[str, Any], report: dict[str, Any]) -> dict[str, Any]:
        domains = self._report_domains(process, report)
        jobs = self._new_jobs(domains, report.get("since"))
        timestamp = _now()
        update = {
            "domains": domains,
            "jobs": jobs,
            "new_jobs_count": len(jobs),
            "generated_at": timestamp,
            "updated_at": timestamp,
        }
        self._reports.update_one({"pipeline_run_id": report["pipeline_run_id"]}, {"$set": update})
        refreshed = {**report, **update}
        log_event(
            logger,
            "info",
            "client_job_report_refreshed",
            domain="alerts",
            process_id=process.get("process_id"),
            pipeline_run_id=report.get("pipeline_run_id"),
            domain_count=len(domains),
            new_jobs_count=len(jobs),
        )
        return refreshed

    def _report_domains(self, process: dict[str, Any], report: dict[str, Any]) -> list[str]:
        existing_domains = [str(domain).strip() for domain in list(report.get("domains") or []) if str(domain).strip()]
        if existing_domains:
            return sorted(set(existing_domains))
        cursor = self._process_refs.find(
            {"process_id": process["process_id"], "status": "completed"},
            {"_id": 0, "registered_domain": 1},
        )
        return sorted({str(ref.get("registered_domain") or "").strip() for ref in cursor if ref.get("registered_domain")})

    def _new_jobs(self, domains: list[str], since: Any) -> list[dict[str, Any]]:
        if not domains:
            return []
        query: dict[str, Any] = {"registered_domain": {"$in": domains}, "status": "active"}
        if since:
            query["first_seen_at"] = {"$gt": since}
        cursor = self._jobs.find(
            query,
            {
                "_id": 0,
                "job_key": 1,
                "registered_domain": 1,
                "title": 1,
                "job_url": 1,
                "source_url": 1,
                "first_seen_at": 1,
            },
        ).sort("first_seen_at", DESCENDING)
        return list(cursor)

    def _build_alert(self, process: dict[str, Any], report: dict[str, Any]) -> dict[str, Any]:
        filters = self._alert_filters(process)
        jobs = list(report.get("jobs") or [])
        relevant_jobs = [job for job in jobs if self._job_matches(job, filters)]
        max_jobs = int(filters.get("max_jobs") or 0)
        if max_jobs > 0:
            relevant_jobs = relevant_jobs[:max_jobs]
        status = "ready" if relevant_jobs else "no_relevant_jobs"
        now = _now()
        existing = self._alerts.find_one(
            {"pipeline_run_id": report["pipeline_run_id"]},
            {"_id": 0, "alert_id": 1, "created_at": 1, "delivery": 1},
        )
        previous_delivery = (existing or {}).get("delivery") or {}
        delivery_status = previous_delivery.get("status")
        if delivery_status == "sent" and status == "ready":
            delivery = previous_delivery
        else:
            delivery = {
                "status": "pending" if status == "ready" else "skipped",
                "last_error": None,
                "sent_to": [],
                "sent_at": None,
            }
        return {
            "alert_id": str((existing or {}).get("alert_id") or uuid4().hex),
            "pipeline_run_id": report["pipeline_run_id"],
            "report_id": report.get("report_id"),
            "process_id": process["process_id"],
            "client": process.get("client") or {},
            "recipients": list((process.get("client") or {}).get("email") or []),
            "period_start": report.get("since"),
            "period_end": report.get("generated_at"),
            "period_type": "daily",
            "domains": list(report.get("domains") or []),
            "filters_applied": filters,
            "total_new_jobs": len(jobs),
            "total_relevant_jobs": len(relevant_jobs),
            "jobs_before_filter": jobs,
            "jobs_after_filter": relevant_jobs,
            "status": status,
            "delivery": delivery,
            "created_at": (existing or {}).get("created_at") or now,
            "updated_at": now,
        }

    def _alert_filters(self, process: dict[str, Any]) -> dict[str, Any]:
        filters = process.get("alert_filters") or (process.get("process_config") or {}).get("alert_filters") or {}
        if not isinstance(filters, dict):
            filters = {}
        return {
            "include_keywords": self._string_list(filters.get("include_keywords")),
            "exclude_keywords": self._string_list(filters.get("exclude_keywords")),
            "title_include_keywords": self._string_list(filters.get("title_include_keywords")),
            "title_exclude_keywords": self._string_list(filters.get("title_exclude_keywords")),
            "domains_include": self._string_list(filters.get("domains_include")),
            "domains_exclude": self._string_list(filters.get("domains_exclude")),
            "max_jobs": int(filters.get("max_jobs") or 0),
        }

    def _string_list(self, value: Any) -> list[str]:
        if value is None:
            return []
        if isinstance(value, str):
            items = value.split(",")
        elif isinstance(value, list):
            items = value
        else:
            items = []
        return [str(item).strip().lower() for item in items if str(item).strip()]

    def _job_matches(self, job: dict[str, Any], filters: dict[str, Any]) -> bool:
        domain = str(job.get("registered_domain") or "").lower()
        title = str(job.get("title") or "").lower()
        haystack = " ".join(
            str(job.get(key) or "").lower()
            for key in ("registered_domain", "title", "job_url", "source_url")
        )
        if filters["domains_include"] and domain not in filters["domains_include"]:
            return False
        if filters["domains_exclude"] and domain in filters["domains_exclude"]:
            return False
        if filters["include_keywords"] and not any(keyword in haystack for keyword in filters["include_keywords"]):
            return False
        if filters["exclude_keywords"] and any(keyword in haystack for keyword in filters["exclude_keywords"]):
            return False
        if filters["title_include_keywords"] and not any(keyword in title for keyword in filters["title_include_keywords"]):
            return False
        if filters["title_exclude_keywords"] and any(keyword in title for keyword in filters["title_exclude_keywords"]):
            return False
        return True

    def _remember_alert(self, process_id: str, alert: dict[str, Any]) -> None:
        self._processes.update_one(
            {"process_id": process_id},
            {
                "$set": {
                    "alert_last_built_at": alert["updated_at"],
                    "alert_last_summary": self._summary(alert),
                    "updated_at": alert["updated_at"],
                }
            },
        )

    def _email_readiness_error(self) -> str | None:
        if not self._settings.process_email_enabled:
            return "Process email is disabled"
        if not self._settings.resend_api_key:
            return "RESEND_API_KEY is not configured"
        if not self._settings.email_from_address:
            return "EMAIL_FROM_ADDRESS is not configured"
        return None

    def _send_email(self, alert: dict[str, Any], recipients: list[str]) -> dict[str, Any]:
        return send_email(
            from_email=self._settings.email_from_address,
            from_name=self._settings.email_from_name,
            reply_to=self._settings.email_reply_to or None,
            to=recipients,
            subject=self._subject(alert),
            body=self._text_body(alert),
            attachments=[build_alert_jobs_csv_attachment(alert)],
            api_key=self._settings.resend_api_key,
        )

    def _subject(self, alert: dict[str, Any]) -> str:
        prefix = str(self._settings.process_email_subject_prefix or "").strip()
        client_name = (alert.get("client") or {}).get("client_name") or alert.get("process_id")
        period_type = str(alert.get("period_type") or "daily").title()
        subject = f"{period_type} alert for {client_name}: {alert.get('total_relevant_jobs', 0)} new relevant jobs"
        return f"{prefix} {subject}".strip()

    def _text_body(self, alert: dict[str, Any]) -> str:
        domain_count = len(alert.get("domains") or [])
        lines = [
            f"Hello {(alert.get('client') or {}).get('client_name') or 'there'},",
            "",
            f"Your {alert.get('period_type') or 'daily'} job alert is ready.",
            "",
            f"New relevant jobs: {alert.get('total_relevant_jobs', 0)}",
            f"Total new jobs before filters: {alert.get('total_new_jobs', 0)}",
            f"Total domains monitored: {domain_count}",
            "",
            "The attached CSV contains the filtered job details for this alert.",
            "",
        ]
        return "\n".join(lines)

    def _mark_delivery_sent(
        self,
        alert: dict[str, Any],
        recipients: list[str],
        provider_response: dict[str, Any],
    ) -> dict[str, Any]:
        delivery = {
            "status": "sent",
            "sent_to": recipients,
            "sent_at": _now(),
            "last_error": None,
            "provider": "resend",
            "provider_response": provider_response,
        }
        return self._update_delivery(alert, delivery)

    def _mark_delivery_skipped(self, alert: dict[str, Any], reason: str) -> dict[str, Any]:
        delivery = {"status": "skipped", "sent_to": [], "sent_at": None, "last_error": reason}
        return self._update_delivery(alert, delivery)

    def _mark_delivery_not_configured(self, alert: dict[str, Any], reason: str) -> dict[str, Any]:
        delivery = {"status": "not_configured", "sent_to": [], "sent_at": None, "last_error": reason}
        return self._update_delivery(alert, delivery)

    def _mark_delivery_failed(self, alert: dict[str, Any], reason: str) -> dict[str, Any]:
        delivery = {
            "status": "failed",
            "sent_to": [],
            "sent_at": None,
            "last_error": reason,
            "provider": "resend",
        }
        return self._update_delivery(alert, delivery)

    def _update_delivery(self, alert: dict[str, Any], delivery: dict[str, Any]) -> dict[str, Any]:
        timestamp = _now()
        self._alerts.update_one(
            {"alert_id": alert["alert_id"]},
            {"$set": {"delivery": delivery, "updated_at": timestamp}},
        )
        updated = {**alert, "delivery": delivery, "updated_at": timestamp}
        self._remember_alert(str(alert["process_id"]), updated)
        return self._summary(updated)

    def _summary(self, alert: dict[str, Any]) -> dict[str, Any]:
        return {
            "alert_id": alert["alert_id"],
            "process_id": alert["process_id"],
            "pipeline_run_id": alert["pipeline_run_id"],
            "period_type": alert.get("period_type") or "daily",
            "status": alert["status"],
            "total_new_jobs": alert["total_new_jobs"],
            "total_relevant_jobs": alert["total_relevant_jobs"],
            "delivery": alert.get("delivery") or {},
        }


@lru_cache(maxsize=1)
def get_client_job_alert_service() -> ClientJobAlertService:
    return ClientJobAlertService(get_sync_mongodb_service(), get_settings())
