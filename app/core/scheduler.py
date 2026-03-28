import asyncio
import logging

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from app.core.orchestrator import Orchestrator

logger = logging.getLogger(__name__)


def parse_cron_expression(expr: str) -> dict:
    """Parse a standard 5-field cron expression into APScheduler CronTrigger kwargs."""
    parts = expr.strip().split()
    if len(parts) != 5:
        raise ValueError(f"Invalid cron expression '{expr}': expected 5 fields (minute hour day month day_of_week)")
    return {
        "minute": parts[0],
        "hour": parts[1],
        "day": parts[2],
        "month": parts[3],
        "day_of_week": parts[4],
    }


def create_scheduler(orchestrator: Orchestrator, cron_expression: str) -> AsyncIOScheduler:
    scheduler = AsyncIOScheduler()

    trigger_kwargs = parse_cron_expression(cron_expression)
    trigger = CronTrigger(**trigger_kwargs)

    async def scheduled_run():
        if orchestrator.is_running:
            logger.info("Skipping scheduled run: a run is already in progress")
            return
        logger.info("Starting scheduled run")
        await asyncio.to_thread(orchestrator.run, "schedule")

    scheduler.add_job(
        scheduled_run,
        trigger,
        id="hardlinker_scan",
        replace_existing=True,
    )

    logger.info("Scheduler configured with cron expression: %s", cron_expression)
    return scheduler
