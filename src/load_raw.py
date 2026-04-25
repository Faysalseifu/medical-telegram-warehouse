import json
import logging
import sys
from pathlib import Path
from typing import Iterable

import psycopg

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app_config import RAW_TELEGRAM_MESSAGES_DIR, get_config  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

DATABASE_URL = get_config().database.raw_url
RAW_BASE = RAW_TELEGRAM_MESSAGES_DIR


def ensure_schema_and_table(conn: psycopg.Connection) -> None:
    """Create raw schema/table with a primary key to de-dupe inserts."""
    with conn.cursor() as cur:
        cur.execute("CREATE SCHEMA IF NOT EXISTS raw;")
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS raw.telegram_messages (
                channel_name    TEXT NOT NULL,
                message_id      BIGINT NOT NULL,
                message_date    TIMESTAMPTZ,
                message_text    TEXT,
                has_media       BOOLEAN,
                image_path      TEXT,
                views           INTEGER,
                forwards        INTEGER,
                loaded_at       TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                CONSTRAINT telegram_messages_pk PRIMARY KEY (channel_name, message_id)
            );
            """
        )
        conn.commit()


def yield_records(base_dir: Path) -> Iterable[list[tuple[object, ...]]]:
    """Yield batches of records from JSON files under data/raw/telegram_messages/YYYY-MM-DD."""
    if not base_dir.exists():
        logger.warning("Raw data directory does not exist: %s", base_dir)
        return

    for date_folder in sorted(base_dir.iterdir()):
        if not date_folder.is_dir():
            continue
        for json_file in sorted(date_folder.glob("*.json")):
            logger.info("Processing %s", json_file)
            try:
                with json_file.open("r", encoding="utf-8") as f:
                    messages = json.load(f)
            except Exception as exc:  # noqa: BLE001
                logger.error("Failed to read %s: %s", json_file, exc)
                continue

            if not messages:
                continue

            batch: list[tuple[object, ...]] = []
            for msg in messages:
                batch.append(
                    (
                        msg.get("channel_name"),
                        msg.get("message_id"),
                        msg.get("message_date"),
                        msg.get("message_text"),
                        msg.get("has_media"),
                        msg.get("image_path"),
                        msg.get("views"),
                        msg.get("forwards"),
                    )
                )

            if batch:
                yield batch


def load_json_to_postgres() -> None:
    inserted = 0
    with psycopg.connect(DATABASE_URL) as conn:
        ensure_schema_and_table(conn)
        with conn.cursor() as cur:
            for batch in yield_records(RAW_BASE):
                try:
                    cur.executemany(
                        """
                        INSERT INTO raw.telegram_messages (
                            channel_name,
                            message_id,
                            message_date,
                            message_text,
                            has_media,
                            image_path,
                            views,
                            forwards
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (channel_name, message_id) DO NOTHING
                        """,
                        batch,
                    )
                    inserted += len(batch)
                    conn.commit()
                    logger.info("Inserted %s rows (total %s)", len(batch), inserted)
                except Exception as exc:  # noqa: BLE001
                    conn.rollback()
                    logger.error("Insert failed, rolled back: %s", exc)

    logger.info("Finished load. Total inserted: %s", inserted)


if __name__ == "__main__":
    load_json_to_postgres()
