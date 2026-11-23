import os
import sqlite3

import psycopg
from contextlib import asynccontextmanager
from datetime import date, datetime, timezone
from pathlib import Path

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, Response
from fastapi.staticfiles import StaticFiles
from pydantic import AwareDatetime

from .baseline import forecast
from .collection_quality import collection_quality
from .config import configured_store
from .dashboard_access import enforce_dashboard_access, load_users, station_scope, visible_stations
from .metrics import metrics_text
from .postgres import PostgresStore
from .stream import billing_windows

store = configured_store()
api_dsn = os.getenv("CDP_API_POSTGRES_DSN")
if api_dsn:
    store = PostgresStore(store.directory, api_dsn, api_reader=True)


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.dashboard_users = load_users(os.getenv("CDP_DASHBOARD_USERS_JSON"))
    if api_dsn is None:
        store.initialize()
    elif api_dsn and isinstance(store, PostgresStore):
        store.verify_api_reader()
    else:
        raise ValueError("API PostgreSQL reader connection is empty")
    yield


app = FastAPI(title="CDP-1 开发沙箱", version="0.31.0", lifespan=lifespan)
app.middleware("http")(enforce_dashboard_access)


@app.get("/health/ready")
def readiness() -> dict:
    try:
        store.ready()
    except (sqlite3.Error, psycopg.Error) as error:
        raise HTTPException(503, "metrics store unavailable") from error
    return {"status": "ready"}


@app.get("/v1/demo/station-daily")
def station_daily(request: Request, day: date) -> dict:
    return {
        "business_day": day,
        "metric_version": "demo-0.1",
        "timezone": "Asia/Shanghai",
        "rows": visible_stations(request, store.daily(day)),
    }


@app.get("/v1/demo/billing-events/windows")
def event_windows(request: Request, day: date) -> dict:
    return {"day_utc": day, "window_minutes": 5, "rows": visible_stations(request, billing_windows(store, day))}


@app.get("/metrics", include_in_schema=False)
def prometheus_metrics() -> Response:
    try:
        content = metrics_text(store)
    except (sqlite3.Error, psycopg.Error) as error:
        raise HTTPException(503, "metrics store unavailable") from error
    return Response(content, media_type="text/plain; version=0.0.4; charset=utf-8")


@app.get("/v1/demo/collection-quality")
def collection_quality_report(request: Request) -> dict:
    if station_scope(request) is not None:
        raise HTTPException(403, "collection quality requires unrestricted station access")
    return {"sources": collection_quality(store)}


@app.get("/v1/demo/station-forecast")
def station_forecast(request: Request, as_of: AwareDatetime | None = None) -> dict:
    try:
        report = forecast(store, as_of or datetime.now(timezone.utc))
    except ValueError as error:
        raise HTTPException(422, str(error)) from error
    report["predictions"] = visible_stations(request, report["predictions"])
    return report


dashboard = Path(__file__).with_name("dashboard")
app.mount("/dashboard/assets", StaticFiles(directory=dashboard), name="dashboard-assets")


@app.get("/dashboard", include_in_schema=False)
def dashboard_page() -> FileResponse:
    return FileResponse(dashboard / "index.html")
