import os

from sqlalchemy import create_engine
from sqlalchemy.engine.url import make_url
from sqlalchemy.orm import sessionmaker


def _normalize_url(url: str) -> str:
	"""Ensure SQLAlchemy uses psycopg driver when available.

	SQLAlchemy defaults to psycopg2 for "postgresql://" URLs. Since this
	project installs psycopg v3, append the driver if missing.
	"""

	try:
		parsed = make_url(url)
		if parsed.drivername == "postgresql":
			parsed = parsed.set(drivername="postgresql+psycopg")
		elif parsed.drivername == "postgres":
			parsed = parsed.set(drivername="postgresql+psycopg")
		return str(parsed)
	except Exception:
		return url


raw_url = os.getenv("DATABASE_URL", "postgresql://postgres:password@localhost:5432/postgres")
DATABASE_URL = _normalize_url(raw_url)

# Engine is created once per process; adjust pool settings as needed.
engine = create_engine(DATABASE_URL, future=True)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)
