import asyncio
import json
import structlog
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

from telethon import TelegramClient
from telethon.errors import ChannelPrivateError, FloodWaitError
from telethon.tl.functions.messages import GetHistoryRequest
from telethon.tl.types import Photo

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app_config import ensure_directory, get_config, raw_image_dir, raw_message_dir, SCRAPER_LOG_FILE  # noqa: E402

SETTINGS = get_config().scraper

# Ensure directories exist
ensure_directory(SCRAPER_LOG_FILE.parent)

# Set up logging
logger = structlog.get_logger(__name__)

async def scrape_channel(
    client: TelegramClient,
    channel_username: str,
    days_back: int = SETTINGS.days_back,
    max_messages: int = SETTINGS.max_messages,
    since_id: int | None = None,
) -> list[dict[str, object]]:
    """
    Scrape messages and images from a single channel.
    """
    messages: list[dict[str, object]] = []
    try:
        entity = await client.get_entity(channel_username)
        logger.info("Accessed channel", channel=channel_username)
    except ChannelPrivateError:
        logger.error("Channel private or invalid", channel=channel_username)
        return messages
    except Exception as e:
        logger.error("Error accessing channel", channel=channel_username, error=str(e))
        return messages

    offset_id = 0
    limit = SETTINGS.batch_size
    # Telethon message dates are timezone-aware (UTC); compare using aware datetime
    min_date = datetime.now(timezone.utc) - timedelta(days=days_back) if days_back > 0 else None

    while True:
        try:
            history = await client(GetHistoryRequest(
                peer=entity,
                offset_id=offset_id,
                offset_date=None,
                add_offset=0,
                limit=limit,
                max_id=0,
                min_id=0,
                hash=0
            ))

            if not history.messages:
                break

            batch_messages: list[dict[str, object]] = []
            for msg in history.messages:
                if since_id is not None and msg.id <= since_id:
                    logger.info("Reached since_id", channel=channel_username)
                    return messages
                if min_date and msg.date < min_date:
                    logger.info("Reached min_date", channel=channel_username)
                    return messages

                msg_data = {
                    "message_id": msg.id,
                    "channel_name": channel_username,
                    "message_date": msg.date.isoformat(),
                    "message_text": msg.message or "",
                    "has_media": msg.media is not None,
                    "image_path": None,  # To be filled in download step
                    "views": getattr(msg, "views", 0) or 0,
                    "forwards": getattr(msg, "forwards", 0) or 0
                }

                # Download image if present
                # Telethon exposes photos via msg.photo; download using the message
                if getattr(msg, "photo", None):
                    img_dir = ensure_directory(raw_image_dir(channel_username))
                    img_path = img_dir / f"{msg.id}.jpg"

                    try:
                        await client.download_media(msg, str(img_path))
                        msg_data["image_path"] = str(img_path)
                        logger.info("Downloaded image", image_path=str(img_path))
                    except Exception as e:
                        logger.error("Failed to download image", msg_id=msg.id, error=str(e))

                batch_messages.append(msg_data)

            messages.extend(batch_messages)
            offset_id = history.messages[-1].id
            logger.info("Scraped messages batch", channel=channel_username, batch_size=len(batch_messages), total=len(messages))

            if len(messages) >= max_messages:
                logger.info("Reached max_messages", channel=channel_username)
                break

            await asyncio.sleep(SETTINGS.request_delay_seconds)  # Avoid rate limits

        except FloodWaitError as e:
            logger.warning("Flood wait sleeping", seconds=e.seconds)
            await asyncio.sleep(e.seconds + SETTINGS.flood_wait_buffer_seconds)
        except Exception as e:
            logger.error("Error during scraping", channel=channel_username, error=str(e))
            await asyncio.sleep(SETTINGS.retry_delay_seconds)

    return messages


def _load_scrape_state(state_path: Path) -> dict[str, int]:
    if not state_path.exists():
        return {}
    try:
        with state_path.open("r", encoding="utf-8") as f:
            raw = json.load(f)
        return {key: int(value) for key, value in raw.items()}
    except Exception as exc:  # noqa: BLE001
        logger.warning("Failed to read scrape state", state_path=str(state_path), error=str(exc))
        return {}


def _save_scrape_state(state_path: Path, state: dict[str, int]) -> None:
    ensure_directory(state_path.parent)
    tmp_path = state_path.with_suffix(state_path.suffix + ".tmp")
    with tmp_path.open("w", encoding="utf-8") as f:
        json.dump(state, f, indent=2, sort_keys=True)
    tmp_path.replace(state_path)


async def main() -> None:
    channels = SETTINGS.channels
    state = _load_scrape_state(SETTINGS.state_path)

    async with TelegramClient(SETTINGS.session_name, SETTINGS.api_id, SETTINGS.api_hash) as client:
        # First-run login
        if not await client.is_user_authorized():
            await client.send_code_request(SETTINGS.phone_number)
            code = input("Enter the code: ")
            await client.sign_in(SETTINGS.phone_number, code)

        for channel in channels:
            logger.info("Starting scrape", channel=channel)
            since_id = state.get(channel)
            messages = await scrape_channel(client, channel, since_id=since_id)

            if messages:
                today_str = datetime.now().strftime("%Y-%m-%d")
                raw_dir = ensure_directory(raw_message_dir(today_str))
                timestamp = datetime.now().strftime("%H%M%S")
                json_path = raw_dir / f"{channel}_{timestamp}.json"

                with open(json_path, "w", encoding="utf-8") as f:
                    json.dump(messages, f, ensure_ascii=False, indent=2)

                max_id = max(int(msg["message_id"]) for msg in messages if msg.get("message_id") is not None)
                state[channel] = max(max_id, state.get(channel, 0))
                logger.info("Saved messages", count=len(messages), path=str(json_path))
            else:
                logger.warning("No messages scraped", channel=channel)

        _save_scrape_state(SETTINGS.state_path, state)


if __name__ == "__main__":
    asyncio.run(main())
