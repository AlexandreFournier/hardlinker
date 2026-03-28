import asyncio
import contextlib
import json
import logging
import os
import threading
import traceback
from dataclasses import asdict, dataclass
from datetime import datetime
from enum import StrEnum

from app.config import Settings
from app.core.linker import LinkResult, cleanup_stale_backups, hardlink_group
from app.core.scanner import ScanCancelledError, scan_for_duplicates
from app.models import FileHashORM, HardlinkORM, RunORM

logger = logging.getLogger(__name__)


class RunPhase(StrEnum):
    IDLE = "idle"
    SCANNING = "scanning"
    HASHING_PARTIAL = "hashing_partial"
    HASHING_FULL = "hashing_full"
    LINKING = "linking"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass
class ProgressState:
    phase: str = RunPhase.IDLE.value
    message: str = ""
    current: int = 0
    total: int = 0
    files_scanned: int = 0
    duplicates_found: int = 0
    links_created: int = 0
    space_saved: int = 0
    existing_links_found: int = 0
    existing_space_saved: int = 0
    current_file: str = ""
    current_file_progress: int = 0
    current_file_size: int = 0
    started_at: str | None = None
    finished_at: str | None = None

    def to_json(self) -> str:
        return json.dumps(asdict(self))


class Orchestrator:
    """Coordinates scan+link runs and publishes progress to SSE subscribers."""

    def __init__(self, settings: Settings, db_session_factory):
        self._settings = settings
        self._db_session_factory = db_session_factory
        self._progress = ProgressState()
        self._lock = threading.Lock()
        self._cancel_event = threading.Event()
        self._running = False
        self._subscribers: list[asyncio.Queue] = []

    @property
    def is_running(self) -> bool:
        return self._running

    @property
    def progress(self) -> ProgressState:
        return self._progress

    def subscribe(self) -> asyncio.Queue:
        q: asyncio.Queue = asyncio.Queue(maxsize=50)
        self._subscribers.append(q)
        return q

    def unsubscribe(self, q: asyncio.Queue):
        with contextlib.suppress(ValueError):
            self._subscribers.remove(q)

    def _notify(self):
        data = self._progress.to_json()
        for q in list(self._subscribers):
            with contextlib.suppress(asyncio.QueueFull):
                q.put_nowait(data)

    def _update_progress(self, **kwargs):
        for k, v in kwargs.items():
            if hasattr(self._progress, k):
                setattr(self._progress, k, v)
        self._notify()

    def cancel(self):
        self._cancel_event.set()

    def _load_hash_cache(self, session) -> dict[str, str]:
        cache = {}
        for row in session.query(FileHashORM).all():
            key = f"{row.path}:{row.mtime}:{row.size}"
            cache[key] = row.hash
        return cache

    def _save_hash_cache(self, session, hash_cache: dict[str, str]):
        for key, hash_val in hash_cache.items():
            parts = key.rsplit(":", 2)
            if len(parts) != 3:
                continue
            path, mtime_str, size_str = parts
            try:
                mtime = float(mtime_str)
                size = int(size_str)
            except ValueError:
                continue

            try:
                st = os.lstat(path)
                inode = st.st_ino
                device_id = st.st_dev
            except OSError:
                continue

            existing = session.query(FileHashORM).filter_by(path=path).first()
            if existing:
                existing.size = size
                existing.mtime = mtime
                existing.inode = inode
                existing.device_id = device_id
                existing.hash = hash_val
                existing.updated_at = datetime.utcnow()
            else:
                session.add(
                    FileHashORM(
                        path=path,
                        size=size,
                        mtime=mtime,
                        inode=inode,
                        device_id=device_id,
                        hash=hash_val,
                    )
                )
        session.commit()

    def run(self, trigger: str = "manual"):
        """Execute a full scan+link run. Runs in a background thread."""
        if self._running:
            raise RuntimeError("A run is already in progress")

        self._running = True
        self._cancel_event.clear()
        now = datetime.utcnow()
        self._progress = ProgressState(
            phase=RunPhase.SCANNING.value,
            message="Starting scan...",
            started_at=now.isoformat(),
        )
        self._notify()

        session = self._db_session_factory()
        run_record = RunORM(
            started_at=now,
            status="running",
            trigger=trigger,
        )
        session.add(run_record)
        session.commit()

        try:
            # Clean up stale backups from previous crashes
            cleanup_stale_backups(self._settings.scan_dirs_list)

            # Load hash cache
            hash_cache = self._load_hash_cache(session)

            # Scan for duplicates
            def on_scan_progress(message: str, current: int, total: int):
                phase = RunPhase.SCANNING.value
                if "Partial hashing" in message:
                    phase = RunPhase.HASHING_PARTIAL.value
                elif "Full hashing" in message:
                    phase = RunPhase.HASHING_FULL.value
                self._update_progress(
                    phase=phase,
                    message=message,
                    current=current,
                    total=total,
                    current_file="",
                    current_file_progress=0,
                    current_file_size=0,
                )

            def on_file_progress(path: str, bytes_read: int, file_size: int):
                self._update_progress(
                    current_file=path,
                    current_file_progress=bytes_read,
                    current_file_size=file_size,
                )

            scan_result = scan_for_duplicates(
                scan_dirs=self._settings.scan_dirs_list,
                min_size=self._settings.hardlinker_min_size,
                partial_hash_size=self._settings.hardlinker_partial_hash_size,
                hash_chunk_size=self._settings.hardlinker_hash_chunk_size,
                progress_callback=on_scan_progress,
                file_progress_callback=on_file_progress,
                cancel_event=self._cancel_event,
                hash_cache=hash_cache,
            )

            groups = scan_result.duplicate_groups

            # Record existing hardlinks
            existing_links_count = sum(len(g.paths) - 1 for g in scan_result.existing_hardlinks)
            existing_space = sum(g.space_saved for g in scan_result.existing_hardlinks)

            for eg in scan_result.existing_hardlinks:
                source = eg.paths[0]
                for linked in eg.paths[1:]:
                    session.add(
                        HardlinkORM(
                            run_id=run_record.id,
                            source_path=source,
                            linked_path=linked,
                            file_size=eg.size,
                            hash="",
                            device_id=eg.device,
                            is_existing=1,
                        )
                    )

            self._update_progress(
                files_scanned=scan_result.total_files_scanned,
                duplicates_found=len(groups),
                existing_links_found=existing_links_count,
                existing_space_saved=existing_space,
            )

            # Save hash cache
            self._save_hash_cache(session, hash_cache)

            # Link duplicates
            self._update_progress(
                phase=RunPhase.LINKING.value,
                message="Hardlinking duplicates...",
                current=0,
                total=len(groups),
            )

            total_links = 0
            total_saved = 0

            for i, group in enumerate(groups):
                if self._cancel_event.is_set():
                    raise ScanCancelledError("Cancelled by user")

                results: list[LinkResult] = hardlink_group(group)

                for result in results:
                    if result.success:
                        total_links += 1
                        total_saved += result.file_size
                        session.add(
                            HardlinkORM(
                                run_id=run_record.id,
                                source_path=result.source_path,
                                linked_path=result.linked_path,
                                file_size=result.file_size,
                                hash=result.hash,
                                device_id=result.device,
                                is_existing=0,
                            )
                        )

                self._update_progress(
                    current=i + 1,
                    total=len(groups),
                    links_created=total_links,
                    space_saved=total_saved,
                    message=f"Linking group {i + 1}/{len(groups)}",
                )

            session.commit()

            # Finalize run record
            finished = datetime.utcnow()
            run_record.finished_at = finished
            run_record.status = "completed"
            run_record.files_scanned = scan_result.total_files_scanned
            run_record.duplicates_found = len(groups)
            run_record.links_created = total_links
            run_record.space_saved = total_saved
            run_record.existing_links_found = existing_links_count
            run_record.existing_space_saved = existing_space
            session.commit()

            self._update_progress(
                phase=RunPhase.COMPLETED.value,
                message=f"Completed: {total_links} new links ({total_saved} bytes), {existing_links_count} existing links ({existing_space} bytes)",
                finished_at=finished.isoformat(),
            )
            logger.info(
                "Run completed: %d new links (%d bytes), %d existing links (%d bytes)",
                total_links,
                total_saved,
                existing_links_count,
                existing_space,
            )

        except ScanCancelledError:
            run_record.finished_at = datetime.utcnow()
            run_record.status = "cancelled"
            session.commit()
            self._update_progress(
                phase=RunPhase.CANCELLED.value,
                message="Run cancelled by user",
                finished_at=datetime.utcnow().isoformat(),
            )
            logger.info("Run cancelled by user")

        except Exception as e:
            logger.error("Run failed: %s\n%s", e, traceback.format_exc())
            run_record.finished_at = datetime.utcnow()
            run_record.status = "failed"
            run_record.error_message = str(e)
            session.commit()
            self._update_progress(
                phase=RunPhase.FAILED.value,
                message=f"Run failed: {e}",
                finished_at=datetime.utcnow().isoformat(),
            )

        finally:
            session.close()
            self._running = False
