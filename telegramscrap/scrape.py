"""Async Telegram scraper (terminal port of the original notebook cells 1-3)."""

import asyncio
import json
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from telethon import TelegramClient

from telegramscrap.config import Credentials
from telegramscrap.datafiles import clean_xml_text, format_duration, save_table

SEP = "-" * 80


@dataclass
class ScrapeParams:
    channels: list[str]
    date_min: datetime
    date_max: datetime
    name: str
    keyword: str = ""
    max_messages: int = 1_000_000
    timeout: int = 21_600  # seconds; 0 disables the limit
    fmt: str = "parquet"
    out_dir: Path = Path("output")
    session: str = "telegramscrap"
    with_comments: bool = True


def channel_slug(channel: str) -> str:
    """Reduce '@name', 't.me/name', 'https://t.me/name/123?x=1' etc. to a bare 'name'."""
    s = channel.strip()
    for prefix in ("https://", "http://"):
        if s.startswith(prefix):
            s = s[len(prefix):]
    for prefix in ("t.me/", "telegram.me/", "telegram.dog/"):
        if s.startswith(prefix):
            s = s[len(prefix):]
    s = s.split("?")[0].split("#")[0]
    s = s.strip("/").split("/")[0]
    return s.lstrip("@")


def parse_date(value: str, *, end_of_day: bool = False) -> datetime:
    dt = datetime.fromisoformat(value)
    if end_of_day and dt.time() == datetime.min.time():
        dt = dt.replace(hour=23, minute=59, second=59)
    return dt.replace(tzinfo=timezone.utc)


def _reactions_to_string(reactions) -> str:
    if not reactions:
        return ""
    parts = []
    for result in reactions.results:
        reaction = result.reaction
        emoji = getattr(reaction, "emoticon", None)
        if emoji is None:
            doc_id = getattr(reaction, "document_id", None)
            emoji = f"[custom:{doc_id}]" if doc_id is not None else "[stars]"
        parts.append(f"{emoji} {result.count}")
    return " ".join(parts) + (" " if parts else "")


async def _collect_comments(client, channel: str, slug: str, message) -> list[dict]:
    """Replies to one post that has a linked discussion thread."""
    comments = []
    try:
        async for c in client.iter_messages(channel, reply_to=message.id):
            comments.append(
                {
                    "Type": "comment",
                    "Comment Group": f"@{slug}",
                    "Comment Author ID": c.sender_id,
                    "Comment Content": (c.text or "").replace("'", '"'),
                    "Comment Date": c.date.strftime("%Y-%m-%d %H:%M:%S"),
                    "Comment Message ID": c.id,
                    "Comment Author": c.post_author,
                    "Comment Views": c.views,
                    "Comment Reactions": _reactions_to_string(c.reactions),
                    "Comment Shares": c.forwards,
                    "Comment Media": bool(c.media),
                    "Comment Url": f"https://t.me/{slug}/{message.id}?comment={c.id}",
                }
            )
    except Exception as exc:  # transient (flood wait, thread just removed, ...)
        print(f"  ! comments for {slug}/{message.id}: {exc}")
    return comments


async def _scrape(creds: Credentials, params: ScrapeParams) -> pd.DataFrame:
    params.out_dir.mkdir(parents=True, exist_ok=True)

    data: list[dict] = []
    t_index = 0
    start_time = time.time()

    def time_is_up() -> bool:
        return bool(params.timeout) and time.time() - start_time > params.timeout

    client = TelegramClient(params.session, creds.api_id, creds.api_hash)
    await client.start(phone=creds.phone, password=creds.password)

    try:
        for i, channel in enumerate(params.channels):
            if t_index >= params.max_messages or time_is_up():
                break

            loop_start = time.time()
            c_index = 0
            slug = channel_slug(channel)
            try:
                async for message in client.iter_messages(channel, search=params.keyword or None):
                    if message.date < params.date_min:
                        break
                    if message.date > params.date_max:
                        continue

                    has_thread = bool(message.replies and message.replies.comments)
                    comments = (
                        await _collect_comments(client, channel, slug, message)
                        if params.with_comments and has_thread
                        else []
                    )
                    date_str = message.date.strftime("%Y-%m-%d %H:%M:%S")
                    data.append(
                        {
                            "Type": "text",
                            "Group": f"@{slug}",
                            "Author ID": message.sender_id,
                            "Content": clean_xml_text(message.text),
                            "Date": date_str,
                            "Message ID": message.id,
                            "Author": message.post_author,
                            "Views": message.views,
                            "Reactions": _reactions_to_string(message.reactions),
                            "Shares": message.forwards,
                            "Media": bool(message.media),
                            "Url": f"https://t.me/{slug}/{message.id}",
                            "Comments List": clean_xml_text(json.dumps(comments)),
                        }
                    )
                    c_index += 1
                    t_index += 1

                    elapsed = format_duration(time.time() - start_time)
                    print(
                        f"{channel}: {c_index:05} here / {t_index:05} total "
                        f"| id {message.id} | {date_str} | elapsed {elapsed}"
                    )

                    if t_index % 1000 == 0:
                        backup = params.out_dir / (
                            f"backup_{params.name}_until_{t_index:05}_"
                            f"{slug}_ID{message.id:07}"
                        )
                        save_table(pd.DataFrame(data), backup, params.fmt)
                        print(f"  -> backup written: {backup.name}")

                    if t_index >= params.max_messages or time_is_up():
                        break

                print(f"##### {channel}: done, {c_index:05} posts #####")
                partial = params.out_dir / f"complete_{slug}_in_{params.name}_until_{t_index:05}"
                save_table(pd.DataFrame(data), partial, params.fmt)
            except Exception as exc:
                print(f"{channel} error: {exc}")

            # be gentle: at least 60s per channel
            spent = time.time() - loop_start
            if spent < 60 and i < len(params.channels) - 1:
                await asyncio.sleep(60 - spent)
    finally:
        await client.disconnect()

    print(SEP)
    print(f"Concluded: {t_index:05} posts scraped")
    print(SEP)
    return pd.DataFrame(data)


def run(creds: Credentials, params: ScrapeParams) -> Path:
    df = asyncio.run(_scrape(creds, params))
    final = params.out_dir / f"FINAL_{params.name}_with_{len(df):05}"
    path = save_table(df, final, params.fmt)
    print(f"Final file: {path}")
    return path
