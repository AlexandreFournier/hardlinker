import logging
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from app.config import Settings
from app.core.orchestrator import Orchestrator
from app.core.scheduler import create_scheduler
from app.database import init_db
from app.routes import api, pages, sse

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = Settings()
    logger.info("Starting Hardlinker")
    logger.info("Scan directories: %s", settings.scan_dirs_list)
    logger.info("Minimum file size: %d bytes", settings.hardlinker_min_size)
    logger.info("Schedule: %s", settings.hardlinker_schedule)

    # Ensure DB directory exists
    db_dir = os.path.dirname(settings.hardlinker_db_path)
    if db_dir:
        os.makedirs(db_dir, exist_ok=True)

    engine, session_factory = init_db(settings.hardlinker_db_path)
    orchestrator = Orchestrator(settings, session_factory)
    scheduler = create_scheduler(orchestrator, settings.hardlinker_schedule)

    app.state.settings = settings
    app.state.db_session_factory = session_factory
    app.state.orchestrator = orchestrator
    app.state.scheduler = scheduler

    scheduler.start()
    logger.info("Scheduler started")

    yield

    scheduler.shutdown(wait=False)
    if orchestrator.is_running:
        orchestrator.cancel()
    engine.dispose()
    logger.info("Hardlinker stopped")


app = FastAPI(title="Hardlinker", lifespan=lifespan)

# Static files
static_dir = os.path.join(os.path.dirname(__file__), "static")
if os.path.isdir(static_dir):
    app.mount("/static", StaticFiles(directory=static_dir), name="static")

# Routes
app.include_router(pages.router)
app.include_router(api.router)
app.include_router(sse.router)
