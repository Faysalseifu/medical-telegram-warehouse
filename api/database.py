from collections.abc import Iterator

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app_config import get_config, normalize_sqlalchemy_url

DATABASE_URL = normalize_sqlalchemy_url(get_config().database.sqlalchemy_url)

# Engine is created once per process; adjust pool settings as needed.
engine = create_engine(DATABASE_URL, future=True)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)


def get_db() -> Iterator[Session]:
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
