import humanize
from fastapi import APIRouter, Request
from fastapi.templating import Jinja2Templates
from sqlalchemy import func

from app.models import HardlinkORM, RunORM

router = APIRouter()
templates = Jinja2Templates(directory="app/templates")


def _format_bytes(value: int) -> str:
    return humanize.naturalsize(value, binary=True)


def _time_ago(value) -> str:
    if value is None:
        return "never"
    return humanize.naturaltime(value)


# Register template filters
templates.env.filters["format_bytes"] = _format_bytes
templates.env.filters["time_ago"] = _time_ago


@router.get("/")
async def dashboard(request: Request):
    orchestrator = request.app.state.orchestrator
    session = request.app.state.db_session_factory()

    try:
        total_saved = session.query(func.sum(RunORM.space_saved)).scalar() or 0
        total_links = session.query(func.sum(RunORM.links_created)).scalar() or 0
        total_runs = session.query(func.count(RunORM.id)).scalar() or 0
        last_run = session.query(RunORM).order_by(RunORM.started_at.desc()).first()
        total_existing_saved = last_run.existing_space_saved if last_run else 0

        recent_links = (
            session.query(HardlinkORM)
            .filter(HardlinkORM.is_existing == 0)
            .order_by(HardlinkORM.created_at.desc())
            .limit(50)
            .all()
        )

        run_history = session.query(RunORM).order_by(RunORM.started_at.desc()).limit(10).all()

        scheduler = request.app.state.scheduler
        job = scheduler.get_job("hardlinker_scan")
        next_run = job.next_run_time if job else None

        return templates.TemplateResponse(
            request=request,
            name="dashboard.html",
            context={
                "total_saved": total_saved,
                "total_existing_saved": total_existing_saved,
                "total_links": total_links,
                "total_runs": total_runs,
                "last_run": last_run,
                "recent_links": recent_links,
                "run_history": run_history,
                "next_run": next_run,
                "is_running": orchestrator.is_running,
                "progress": orchestrator.progress,
                "settings": request.app.state.settings,
            },
        )
    finally:
        session.close()
