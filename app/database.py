from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.models import Base


def init_db(db_path: str):
    engine = create_engine(
        f"sqlite:///{db_path}",
        connect_args={"check_same_thread": False},
        echo=False,
    )
    Base.metadata.create_all(engine)
    session_factory = sessionmaker(bind=engine)
    return engine, session_factory
