from datetime import datetime

from pydantic import BaseModel
from sqlalchemy import Column, DateTime, Float, ForeignKey, Integer, String, Text
from sqlalchemy.orm import declarative_base, relationship

Base = declarative_base()


# --- ORM Models ---


class RunORM(Base):
    __tablename__ = "runs"

    id = Column(Integer, primary_key=True, autoincrement=True)
    started_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    finished_at = Column(DateTime, nullable=True)
    status = Column(String, nullable=False, default="running")
    trigger = Column(String, nullable=False, default="schedule")
    files_scanned = Column(Integer, nullable=False, default=0)
    duplicates_found = Column(Integer, nullable=False, default=0)
    links_created = Column(Integer, nullable=False, default=0)
    space_saved = Column(Integer, nullable=False, default=0)
    existing_links_found = Column(Integer, nullable=False, default=0)
    existing_space_saved = Column(Integer, nullable=False, default=0)
    error_message = Column(Text, nullable=True)

    hardlinks = relationship("HardlinkORM", back_populates="run")


class HardlinkORM(Base):
    __tablename__ = "hardlinks"

    id = Column(Integer, primary_key=True, autoincrement=True)
    run_id = Column(Integer, ForeignKey("runs.id"), nullable=False)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    source_path = Column(Text, nullable=False)
    linked_path = Column(Text, nullable=False)
    file_size = Column(Integer, nullable=False)
    hash = Column(String, nullable=False)
    device_id = Column(Integer, nullable=False)
    is_existing = Column(Integer, nullable=False, default=0)  # 1 if found already hardlinked

    run = relationship("RunORM", back_populates="hardlinks")


class FileHashORM(Base):
    __tablename__ = "file_hashes"

    id = Column(Integer, primary_key=True, autoincrement=True)
    path = Column(Text, nullable=False, unique=True)
    size = Column(Integer, nullable=False)
    mtime = Column(Float, nullable=False)
    inode = Column(Integer, nullable=False)
    device_id = Column(Integer, nullable=False)
    hash = Column(String, nullable=False)
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow)


# --- Pydantic Schemas ---


class RunSchema(BaseModel):
    id: int
    started_at: datetime
    finished_at: datetime | None
    status: str
    trigger: str
    files_scanned: int
    duplicates_found: int
    links_created: int
    space_saved: int
    existing_links_found: int
    existing_space_saved: int
    error_message: str | None

    class Config:
        from_attributes = True


class HardlinkSchema(BaseModel):
    id: int
    run_id: int
    created_at: datetime
    source_path: str
    linked_path: str
    file_size: int
    hash: str
    device_id: int
    is_existing: bool

    class Config:
        from_attributes = True


class StatsSchema(BaseModel):
    total_space_saved: int
    total_links_created: int
    total_existing_space_saved: int
    total_runs: int
    last_run: RunSchema | None
    next_run_at: datetime | None


class SettingsSchema(BaseModel):
    scan_dirs: list[str]
    min_size: int
    min_size_human: str
    schedule: str
    base_url: str
    db_path: str
