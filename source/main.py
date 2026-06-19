from __future__ import annotations

from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import FileResponse, RedirectResponse

from api.alerts import router as alerts_router
from api.admin import router as admin_router
from api.career_category_node import router as career_category_node_router
from api.domain_jobs import router as domain_jobs_router
from api.health import router as health_router
from api.job_extraction_node import router as job_extraction_node_router
from api.job_pagination_node import router as job_pagination_node_router
from api.job_pattern_node import router as job_pattern_node_router
from api.observability import router as observability_router
from api.pipelines import router as pipeline_router
from api.processes import router as process_router
from api.search_node import router as search_node_router
from services.mongodb_service import get_mongodb_service


STATIC_DIR = Path(__file__).resolve().parent / "static"

app = FastAPI(title="Job Monitoring Agent API")
app.include_router(health_router)
app.include_router(admin_router)
app.include_router(alerts_router)
app.include_router(process_router)
app.include_router(domain_jobs_router)
app.include_router(observability_router)
app.include_router(pipeline_router)
app.include_router(search_node_router)
app.include_router(career_category_node_router)
app.include_router(job_pattern_node_router)
app.include_router(job_pagination_node_router)
app.include_router(job_extraction_node_router)


@app.get("/", include_in_schema=False)
async def index() -> RedirectResponse:
    return RedirectResponse(url="/admin")


@app.get("/admin", include_in_schema=False)
async def admin_ui() -> FileResponse:
    return FileResponse(STATIC_DIR / "admin.html")


@app.on_event("shutdown")
async def shutdown() -> None:
    await get_mongodb_service().close()
