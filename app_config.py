from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Final

from dotenv import load_dotenv
from sqlalchemy.engine.url import make_url

PROJECT_ROOT: Final[Path] = Path(__file__).resolve().parent
DATA_DIR: Final[Path] = PROJECT_ROOT / "data"
LOGS_DIR: Final[Path] = PROJECT_ROOT / "logs"
RAW_TELEGRAM_MESSAGES_DIR: Final[Path] = DATA_DIR / "raw" / "telegram_messages"
RAW_IMAGES_DIR: Final[Path] = DATA_DIR / "raw" / "images"
YOLO_OUTPUT_CSV: Final[Path] = DATA_DIR / "enriched" / "yolo_detections.csv"
SCRAPER_LOG_FILE: Final[Path] = LOGS_DIR / "scraper.log"

DEFAULT_DATABASE_URL: Final[str] = "postgresql://postgres:postgres@localhost:5432/medical_warehouse"
DEFAULT_SQLALCHEMY_DATABASE_URL: Final[str] = "postgresql+psycopg://postgres:password@localhost:5432/postgres"
DEFAULT_CHANNELS: Final[tuple[str, ...]] = (
    "CheMed123",
    "lobelia4cosmetics",
    "Thequorachannel",
)
DEFAULT_MODEL: Final[str] = "yolov8n.pt"
DEFAULT_IMAGE_EXTENSIONS: Final[tuple[str, ...]] = (".jpg", ".jpeg", ".png")


def ensure_directory(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


def raw_message_dir(date_folder: str) -> Path:
    return RAW_TELEGRAM_MESSAGES_DIR / date_folder


def raw_image_dir(channel_name: str) -> Path:
    return RAW_IMAGES_DIR / channel_name


def _load_env() -> None:
    load_dotenv(override=True)


def _env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None:
        return default
    try:
        return int(value)
    except ValueError:
        return default


def _env_str(name: str, default: str) -> str:
    return os.getenv(name, default)


def normalize_sqlalchemy_url(url: str) -> str:
    try:
        parsed = make_url(url)
        if parsed.drivername in {"postgresql", "postgres"}:
            parsed = parsed.set(drivername="postgresql+psycopg")
        return str(parsed)
    except Exception:
        return url


@dataclass(frozen=True)
class ScraperConfig:
    api_id: int
    api_hash: str
    phone_number: str
    session_name: str = "telegram_scraper"
    channels: tuple[str, ...] = DEFAULT_CHANNELS
    days_back: int = 5
    max_messages: int = 1000
    batch_size: int = 100
    request_delay_seconds: float = 1.5
    flood_wait_buffer_seconds: int = 5
    retry_delay_seconds: int = 10


@dataclass(frozen=True)
class DatabaseConfig:
    raw_url: str = DEFAULT_DATABASE_URL
    sqlalchemy_url: str = DEFAULT_SQLALCHEMY_DATABASE_URL


@dataclass(frozen=True)
class YoloConfig:
    model_name: str = DEFAULT_MODEL
    image_dir: Path = RAW_IMAGES_DIR
    output_csv: Path = YOLO_OUTPUT_CSV
    image_extensions: tuple[str, ...] = DEFAULT_IMAGE_EXTENSIONS
    person_class: int = 0
    bottle_class: int = 39
    cup_class: int = 41
    vase_class: int = 86

    @property
    def relevant_classes(self) -> frozenset[int]:
        return frozenset({self.person_class, self.bottle_class, self.cup_class, self.vase_class})


@dataclass(frozen=True)
class AppConfig:
    scraper: ScraperConfig
    database: DatabaseConfig
    yolo: YoloConfig


@lru_cache(maxsize=1)
def get_config() -> AppConfig:
    _load_env()
    return AppConfig(
        scraper=ScraperConfig(
            api_id=_env_int("API_ID", 0),
            api_hash=_env_str("API_HASH", ""),
            phone_number=_env_str("PHONE_NUMBER", ""),
        ),
        database=DatabaseConfig(
            raw_url=_env_str("DATABASE_URL", DEFAULT_DATABASE_URL),
            sqlalchemy_url=_env_str("DATABASE_URL", DEFAULT_SQLALCHEMY_DATABASE_URL),
        ),
        yolo=YoloConfig(),
    )