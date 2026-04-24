from __future__ import annotations

from pathlib import Path

from app_config import (
    PROJECT_ROOT,
    get_config,
    normalize_sqlalchemy_url,
    raw_image_dir,
    raw_message_dir,
    ensure_directory,
)


def test_normalize_sqlalchemy_url_updates_postgres_driver() -> None:
    url = normalize_sqlalchemy_url("postgresql://user:pass@localhost:5432/example")

    assert url.startswith("postgresql+psycopg://")


def test_path_helpers_build_expected_locations() -> None:
    assert raw_message_dir("2026-01-18") == PROJECT_ROOT / "data" / "raw" / "telegram_messages" / "2026-01-18"
    assert raw_image_dir("example-channel") == PROJECT_ROOT / "data" / "raw" / "images" / "example-channel"


def test_ensure_directory_creates_path(tmp_path: Path) -> None:
    target = tmp_path / "nested" / "folder"

    result = ensure_directory(target)

    assert result == target
    assert target.exists()


def test_get_config_returns_dataclass_settings(monkeypatch) -> None:
    monkeypatch.setenv("API_ID", "123")
    monkeypatch.setenv("API_HASH", "hash")
    monkeypatch.setenv("PHONE_NUMBER", "+251000000000")
    get_config.cache_clear()

    config = get_config()

    assert config.scraper.api_id == 123
    assert config.scraper.api_hash == "hash"
    assert config.scraper.phone_number == "+251000000000"