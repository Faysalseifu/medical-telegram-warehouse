import asyncio
import json
import logging
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

from dotenv import load_dotenv
from telethon import TelegramClient
from telethon.errors import ChannelPrivateError, FloodWaitError
from telethon.tl.functions.messages import GetHistoryRequest
from telethon.tl.types import Photo

# Load environment variables
load_dotenv()

# Ensure directories exist
Path("logs").mkdir(parents=True, exist_ok=True)

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("logs/scraper.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

API_ID = int(os.getenv("API_ID", "0"))
API_HASH = os.getenv("API_HASH", "")
PHONE_NUMBER = os.getenv("PHONE_NUMBER", "")
SESSION_NAME = "telegram_scraper"


async def scrape_channel(client: TelegramClient, channel_username: str, days_back: int = 7, max_messages: int = 5000):
    """
    Scrape messages and images from a single channel.
    """
    messages: list[dict] = []
    try:
        entity = await client.get_entity(channel_username)
        logger.info(f"Accessed channel: {channel_username}")
    except ChannelPrivateError:
        logger.error(f"Channel {channel_username} is private or invalid")
        return messages
    except Exception as e:
        logger.error(f"Error accessing {channel_username}: {e}")
        return messages

    offset_id = 0
    limit = 100  # Batch size to avoid floods
    # Telethon message dates are timezone-aware (UTC); compare using aware datetime
    min_date = datetime.now(timezone.utc) - timedelta(days=days_back)

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

            batch_messages: list[dict] = []
            for msg in history.messages:
                if msg.date < min_date:
                    logger.info(f"Reached min_date for {channel_username}")
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
                    img_dir = Path(f"data/raw/images/{channel_username}")
                    img_dir.mkdir(parents=True, exist_ok=True)
                    img_path = img_dir / f"{msg.id}.jpg"

                    try:
                        await client.download_media(msg, str(img_path))
                        msg_data["image_path"] = str(img_path)
                        logger.info(f"Downloaded image: {img_path}")
                    except Exception as e:
                        logger.error(f"Failed to download image for msg {msg.id}: {e}")

                batch_messages.append(msg_data)

            messages.extend(batch_messages)
            offset_id = history.messages[-1].id
            logger.info(f"Scraped {len(batch_messages)} messages from {channel_username}. Total: {len(messages)}")

            if len(messages) >= max_messages:
                logger.info(f"Reached max_messages for {channel_username}")
                break

            await asyncio.sleep(1.5)  # Avoid rate limits

        except FloodWaitError as e:
            logger.warning(f"Flood wait: sleeping for {e.seconds} seconds")
            await asyncio.sleep(e.seconds + 5)
        except Exception as e:
            logger.error(f"Error during scraping {channel_username}: {e}")
            await asyncio.sleep(10)

    return messages


async def main():
    channels = [
        "CheMed123",
        "lobelia4cosmetics",
        "Thequorachannel",
    ]

    async with TelegramClient(SESSION_NAME, API_ID, API_HASH) as client:
        # First-run login
        if not await client.is_user_authorized():
            await client.send_code_request(PHONE_NUMBER)
            code = input("Enter the code: ")
            await client.sign_in(PHONE_NUMBER, code)

        for channel in channels:
            logger.info(f"Starting scrape for {channel}")
            messages = await scrape_channel(client, channel, days_back=5, max_messages=1000)

            if messages:
                today_str = datetime.now().strftime("%Y-%m-%d")
                raw_dir = Path(f"data/raw/telegram_messages/{today_str}")
                raw_dir.mkdir(parents=True, exist_ok=True)
                json_path = raw_dir / f"{channel}.json"

                with open(json_path, "w", encoding="utf-8") as f:
                    json.dump(messages, f, ensure_ascii=False, indent=2)

                logger.info(f"Saved {len(messages)} messages to {json_path}")
            else:
                logger.warning(f"No messages scraped for {channel}")


if __name__ == "__main__":
    asyncio.run(main())
