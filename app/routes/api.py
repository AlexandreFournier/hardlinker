import asyncio

import humanize
from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse
from sqlalchemy import func

from app.models import HardlinkORM, HardlinkSchema, RunORM, RunSchema, SettingsSchema

router = APIRouter(prefix="/api")


@router.get("/health")
async def health(request: Request):
    orchestrator = request.app.state.orchestrator
    return {
        "status": "healthy",
        "running": orchestrator.is_running,
        "phase": orchestrator.progress.phase,
    }


@router.post("/run")
async def force_run(request: Request):
    orchestrator = request.app.state.orchestrator
    if orchestrator.is_running:
        return JSONResponse(
            status_code=409,
            content={"error": "A run is already in progress"},
        )
    request.app.state.current_task = asyncio.create_task(asyncio.to_thread(orchestrator.run, "manual"))
    return {"status": "started", "message": "Scan started"}


@router.post("/cancel")
async def cancel_run(request: Request):
    orchestrator = request.app.state.orchestrator
    if not orchestrator.is_running:
        return JSONResponse(
            status_code=409,
            content={"error": "No run in progress"},
        )
    orchestrator.cancel()
    return {"status": "cancelling"}


@router.get("/history")
async def history(request: Request):
    session = request.app.state.db_session_factory()
    try:
        runs = session.query(RunORM).order_by(RunORM.started_at.desc()).limit(20).all()
        return [RunSchema.model_validate(r) for r in runs]
    finally:
        session.close()


@router.get("/history/{run_id}/links")
async def run_links(request: Request, run_id: int):
    session = request.app.state.db_session_factory()
    try:
        links = (
            session.query(HardlinkORM)
            .filter(HardlinkORM.run_id == run_id)
            .order_by(HardlinkORM.created_at.desc())
            .limit(100)
            .all()
        )
        return [HardlinkSchema.model_validate(link) for link in links]
    finally:
        session.close()


@router.get("/links/recent")
async def recent_links(request: Request):
    session = request.app.state.db_session_factory()
    try:
        links = (
            session.query(HardlinkORM)
            .filter(HardlinkORM.is_existing == 0)
            .order_by(HardlinkORM.created_at.desc())
            .limit(50)
            .all()
        )
        return [HardlinkSchema.model_validate(link) for link in links]
    finally:
        session.close()


@router.get("/settings")
async def settings(request: Request):
    s = request.app.state.settings
    return SettingsSchema(
        scan_dirs=s.scan_dirs_list,
        min_size=s.hardlinker_min_size,
        min_size_human=humanize.naturalsize(s.hardlinker_min_size, binary=True),
        schedule=s.hardlinker_schedule,
        base_url=s.hardlinker_baseurl,
        db_path=s.hardlinker_db_path,
    )


@router.get("/stats")
async def stats(request: Request):
    session = request.app.state.db_session_factory()
    try:
        total_saved = session.query(func.sum(RunORM.space_saved)).scalar() or 0
        total_links = session.query(func.sum(RunORM.links_created)).scalar() or 0
        total_runs = session.query(func.count(RunORM.id)).scalar() or 0
        last_run = session.query(RunORM).order_by(RunORM.started_at.desc()).first()
        total_existing_saved = last_run.existing_space_saved if last_run else 0

        scheduler = request.app.state.scheduler
        job = scheduler.get_job("hardlinker_scan")
        next_run = job.next_run_time if job else None

        return {
            "total_space_saved": total_saved,
            "total_space_saved_human": humanize.naturalsize(total_saved, binary=True),
            "total_existing_space_saved": total_existing_saved,
            "total_existing_space_saved_human": humanize.naturalsize(total_existing_saved, binary=True),
            "total_links_created": total_links,
            "total_runs": total_runs,
            "last_run": RunSchema.model_validate(last_run) if last_run else None,
            "next_run_at": next_run.isoformat() if next_run else None,
        }
    finally:
        session.close()
