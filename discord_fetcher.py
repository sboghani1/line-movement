"""
Discord image fetcher for the capper analyzer pipeline.

Fetches images posted in the Discord picks channel since the last run,
extracting capper name and image URL from message content and embeds.
Returns a list of (image_url, sent_at_eastern, capper_name, content, message_dt)
tuples in chronological order.
"""
import os
import re
import time
from datetime import datetime
from typing import List, Optional, Tuple
from zoneinfo import ZoneInfo

import requests
from dotenv import load_dotenv

load_dotenv()

DISCORD_USER_TOKEN = os.environ.get("DISCORD_USER_TOKEN", "")
DISCORD_CHANNEL_ID = "1384768734727508019"
IMAGE_EXTENSIONS = (".png", ".jpg", ".jpeg", ".gif", ".webp")


def fetch_recent_messages(limit: int = 100, before: Optional[str] = None) -> list:
    """Fetch recent messages from the Discord channel using user token.

    Args:
        limit: Number of messages to fetch (max 100)
        before: Message ID to fetch messages before (for pagination)
    """
    if not DISCORD_USER_TOKEN:
        raise ValueError("DISCORD_USER_TOKEN environment variable not set")

    headers = {
        "Authorization": DISCORD_USER_TOKEN,  # User token, no "Bot" prefix
        "Content-Type": "application/json",
    }

    url = f"https://discord.com/api/v10/channels/{DISCORD_CHANNEL_ID}/messages"
    params = {"limit": limit}
    if before:
        params["before"] = before

    response = requests.get(url, headers=headers, params=params)

    if response.status_code == 401:
        raise ValueError("Invalid token or unauthorized access")
    elif response.status_code == 403:
        raise ValueError("No permission to access this channel")
    elif response.status_code != 200:
        raise ValueError(f"Discord API error: {response.status_code} - {response.text}")

    return response.json()


def fetch_all_messages_since(
    since_timestamp: Optional[datetime], max_pages: int = 10
) -> list:
    """Fetch all messages since the given timestamp using pagination.

    Args:
        since_timestamp: Fetch messages newer than this timestamp
        max_pages: Maximum number of API calls to make (safety limit)

    Returns:
        List of all messages since the timestamp, newest first
    """
    all_messages = []
    last_message_id = None

    for page in range(max_pages):
        messages = fetch_recent_messages(limit=100, before=last_message_id)

        if not messages:
            break

        # Check if we've gone past the since_timestamp
        reached_cutoff = False
        for msg in messages:
            msg_time_str = msg.get("timestamp", "")
            if msg_time_str:
                msg_dt = datetime.fromisoformat(msg_time_str.replace("Z", "+00:00"))
                if since_timestamp and msg_dt <= since_timestamp:
                    reached_cutoff = True
                    break
            all_messages.append(msg)

        if reached_cutoff:
            print(
                f"  Pagination: Reached cutoff after {page + 1} page(s), {len(all_messages)} messages"
            )
            break

        # Get the ID of the last message for pagination
        last_message_id = messages[-1].get("id")

        # Rate limit - be respectful to Discord API
        time.sleep(0.5)

        if page > 0:
            print(
                f"  Pagination: Fetched page {page + 1}, {len(all_messages)} messages so far..."
            )

    return all_messages


def parse_discord_timestamp(timestamp_str: str) -> str:
    """Convert Discord ISO timestamp to Eastern time formatted string."""
    dt = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
    eastern = ZoneInfo("America/New_York")
    dt_eastern = dt.astimezone(eastern)
    return dt_eastern.strftime("%Y-%m-%d %H:%M:%S")


def get_most_recent_image() -> Optional[Tuple[str, str]]:
    """Find and return the URL and sent time of the most recent image in the channel."""
    messages = fetch_recent_messages(limit=100)

    for message in messages:
        message_time = message.get("timestamp", "")

        # Check attachments
        for attachment in message.get("attachments", []):
            url = attachment.get("url", "")
            if url.lower().endswith(IMAGE_EXTENSIONS):
                sent_at = parse_discord_timestamp(message_time) if message_time else ""
                return url, sent_at

        # Check embeds (for linked images)
        for embed in message.get("embeds", []):
            if embed.get("type") == "image":
                image_url = embed.get("url") or embed.get("thumbnail", {}).get("url")
                if image_url:
                    sent_at = (
                        parse_discord_timestamp(message_time) if message_time else ""
                    )
                    return image_url, sent_at
            # Also check embed image field
            if "image" in embed:
                sent_at = parse_discord_timestamp(message_time) if message_time else ""
                return embed["image"].get("url"), sent_at

    return None


def get_messages_with_images_since(
    since_timestamp: Optional[datetime],
) -> List[Tuple[str, str, str, str, datetime]]:
    """Get all messages with images since the given timestamp.

    Returns list of (image_url, message_sent_at_eastern, capper_name, message_content, message_datetime_utc) tuples.
    """
    messages = fetch_all_messages_since(since_timestamp)
    results = []

    for message in messages:
        message_time_str = message.get("timestamp", "")
        if not message_time_str:
            continue

        message_dt = datetime.fromisoformat(message_time_str.replace("Z", "+00:00"))

        if since_timestamp and message_dt <= since_timestamp:
            continue

        content = message.get("content", "").strip()
        # Remove role mentions and user mentions
        clean_content = re.sub(r"<@[&!]?\d+>", "", content).strip()

        # Get capper from first non-empty line of cleaned content
        capper_name = "UNKNOWN"
        if clean_content:
            for line in clean_content.split("\n"):
                line = line.strip()
                # Strip markdown bold markers
                line = re.sub(r"\*\*(.+?)\*\*", r"\1", line)
                line = line.strip()
                if line and len(line) <= 30:
                    # Skip URLs, common non-capper text, and links
                    skip_patterns = ["http", "tracker", "click here", "[", "]"]
                    if not any(p in line.lower() for p in skip_patterns):
                        capper_name = line.upper()
                        break

        # Check attachments
        for attachment in message.get("attachments", []):
            url = attachment.get("url", "")
            if url.lower().endswith(IMAGE_EXTENSIONS):
                sent_at = parse_discord_timestamp(message_time_str)
                results.append((url, sent_at, capper_name, clean_content, message_dt))

        # Check embeds (for linked images)
        for embed in message.get("embeds", []):
            image_url = None
            if embed.get("type") == "image":
                image_url = embed.get("url") or embed.get("thumbnail", {}).get("url")
            elif "image" in embed:
                image_url = embed["image"].get("url")

            if image_url:
                sent_at = parse_discord_timestamp(message_time_str)
                # Try to get capper from embed title/description/author if not found in content
                embed_capper = capper_name
                if embed_capper == "UNKNOWN":
                    # Check embed title
                    embed_title = embed.get("title", "").strip()
                    if (
                        embed_title
                        and len(embed_title) <= 30
                        and not embed_title.startswith("http")
                    ):
                        embed_capper = embed_title.upper()
                    # Check embed description first line (strip ### heading markers)
                    if embed_capper == "UNKNOWN":
                        embed_desc = embed.get("description", "").strip()
                        if embed_desc:
                            first_line = embed_desc.split("\n")[0].strip()
                            first_line = re.sub(r"^#{1,3}\s*", "", first_line).strip()
                            if (
                                first_line
                                and len(first_line) <= 30
                                and not first_line.startswith("http")
                            ):
                                embed_capper = first_line.upper()
                    # Check embed author name
                    if embed_capper == "UNKNOWN":
                        author_name = embed.get("author", {}).get("name", "").strip()
                        if author_name and len(author_name) <= 30:
                            embed_capper = author_name.upper()

                # Build full content including embed text for OCR context
                embed_content = clean_content
                if embed.get("title"):
                    embed_content = f"{embed.get('title')}\n{embed_content}"
                if embed.get("description"):
                    embed_content = f"{embed_content}\n{embed.get('description')}"

                results.append(
                    (image_url, sent_at, embed_capper, embed_content, message_dt)
                )

    # Return in chronological order (oldest first)
    return sorted(results, key=lambda x: x[3])
