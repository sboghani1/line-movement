"""
Telegram image fetcher for the capper analyzer pipeline.

Fetches images from the CAPPERS FREE Telegram channel since the last run.
Returns a list of dicts (oldest first) with image bytes, capper name,
source ref, and timestamps.
"""
import asyncio
import io
import os
import re
from datetime import datetime
from typing import List, Optional
from zoneinfo import ZoneInfo

from dotenv import load_dotenv
from telethon import TelegramClient
from telethon.sessions import StringSession

load_dotenv()

API_ID_ENV = os.environ.get("TELEGRAM_API_ID", "")
API_HASH = os.environ.get("TELEGRAM_API_HASH", "")
SESSION = os.environ.get("TELEGRAM_SESSION", "")
CHANNEL_ID_ENV = os.environ.get("TELEGRAM_CHANNEL_ID", "")
SOURCE_NAME = "telegram_cappers_free"


def _channel_id() -> int:
    """Return the Telegram channel ID with -100 prefix for supergroups/channels."""
    raw = CHANNEL_ID_ENV.lstrip("-")
    return int(f"-100{raw}")


async def _fetch_images_async(since_timestamp: Optional[datetime]) -> List[dict]:
    """Async core: iterate channel messages newest-first, stop at cutoff."""
    client = TelegramClient(StringSession(SESSION), int(API_ID_ENV), API_HASH)
    await client.start()

    channel_id = _channel_id()
    results = []
    eastern = ZoneInfo("America/New_York")
    last_capper = "UNKNOWN"
    # Map grouped_id -> capper for album grouping
    album_capper: dict = {}

    async for msg in client.iter_messages(channel_id):
        # iter_messages is newest-first; stop once we're past the cutoff
        if since_timestamp and msg.date <= since_timestamp:
            break

        # Update running capper name from text messages / captions
        if msg.text:
            first_line = re.sub(r"\*+", "", msg.text.strip().split("\n")[0]).strip()
            if first_line and len(first_line) <= 40:
                last_capper = first_line.upper()
                if msg.grouped_id:
                    album_capper[msg.grouped_id] = last_capper

        if not msg.photo:
            continue

        # Resolve capper: prefer group-level capper, fall back to last seen
        if msg.grouped_id and msg.grouped_id in album_capper:
            capper = album_capper[msg.grouped_id]
        else:
            capper = last_capper

        buf = io.BytesIO()
        await client.download_media(msg, file=buf)

        sent_at_et = msg.date.astimezone(eastern).strftime("%Y-%m-%d %H:%M:%S")

        results.append({
            "source_ref": f"telegram:{channel_id}:{msg.id}",
            "image_bytes": buf.getvalue(),
            "media_type": "image/jpeg",
            "sent_at": sent_at_et,
            "capper_name": capper,
            "message_content": msg.text or "",
            "message_dt": msg.date,
            "source": SOURCE_NAME,
        })

    await client.disconnect()
    # Return oldest-first (consistent with Discord fetcher)
    return list(reversed(results))


def fetch_images_since(since_timestamp: Optional[datetime]) -> List[dict]:
    """Fetch Telegram images posted after since_timestamp.

    Returns a list of dicts (oldest first) with keys:
      source_ref, image_bytes, media_type, sent_at, capper_name,
      message_content, message_dt, source

    Returns [] if Telegram env vars are not configured.
    """
    if not all([API_ID_ENV, API_HASH, SESSION, CHANNEL_ID_ENV]):
        print("  Telegram credentials not configured, skipping")
        return []
    return asyncio.run(_fetch_images_async(since_timestamp))
