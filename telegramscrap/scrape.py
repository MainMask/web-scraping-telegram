"""Async Telegram scraper (terminal port of the original notebook cells 1-3)."""

import asyncio
import json
import shlex
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import NamedTuple

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from telethon import TelegramClient, utils
from telethon.errors import FloodWaitError, ServerError, TimedOutError
from telethon.tl.functions.messages import GetMessageReactionsListRequest
from telethon.tl.types import PeerChannel, PeerUser, User

from telegramscrap.config import Credentials
from telegramscrap.datafiles import clean_xml_text, format_duration, save_table

SEP = "-" * 80

# Telethon auto-sleeps and retries the exact request on FLOOD_WAIT up to this many
# seconds (its default is only 60), so ordinary rate limits and short soft bans are
# waited out transparently instead of skipping the data. Longer waits raise
# FloodWaitError, which the channel loop checkpoints and waits out itself.
FLOOD_SLEEP_THRESHOLD = 3600
# a FloodWaitError longer than this (or too many in a row) stops the run with a
# --resume hint rather than sleeping for the better part of a day.
FLOOD_MAX_WAIT = 6 * 3600
FLOOD_MAX_ATTEMPTS = 12
FLOOD_RETRY_BUFFER = 5  # extra seconds slept on top of the ban so we don't re-trip it
# small pause after each reaction-list request to keep the burst rate down
REACTOR_CALL_DELAY = 0.5

# A channel post's comment counter (message.replies.replies) can briefly lag behind
# reality on a very fresh post. Older than this, a 0 count is trusted as "no
# comments" and the GetReplies call is skipped (saves one request per empty post
# and keeps the log clean); within it, the thread is always fetched.
RECENT_POST_WINDOW = timedelta(hours=48)

# Keep reconnecting through long outages instead of aborting the run. Bounded
# wall time ~ CONNECTION_RETRIES * (RETRY_DELAY + connect time) ~ 8-14h, enough
# for a multi-hour outage, still finite (never None/negative, i.e. infinite).
CONNECTION_RETRIES = 2000  # Telethon default 5
RETRY_DELAY = 15           # seconds between reconnect attempts; default 1
REQUEST_RETRIES = 10       # per-request retries across reconnects; default 5

# In-run automatic resume: restart a channel from the last checkpointed message
# id when a connection error escapes iter_messages.
RESUME_MAX_ATTEMPTS = 5    # per-channel restarts before re-raising
RESUME_BASE_WAIT = 30      # wait = min(RESUME_BASE_WAIT * attempt, RESUME_MAX_WAIT)
RESUME_MAX_WAIT = 300

# a dropped connection: never swallowed as a per-message skip, always bubbles up
# to the channel-level retry loop so the work is redone rather than lost.
NET_ERRORS = (ConnectionError, OSError, asyncio.TimeoutError)

# Transient server-side RPC failures (500 RPC_CALL_FAIL / RPC_MCGET_FAIL,
# 503 TIMEOUT): Telegram is telling us to retry, so treat them like a dropped
# connection — redo the whole post/channel rather than keep a half-scraped thread.
# 400 (MSG_ID_INVALID on posts with no comments) and 403 (BROADCAST_FORBIDDEN on
# channel-post reactors) are deliberately NOT here: those stay per-message skips.
RETRYABLE_RPC = (ServerError, TimedOutError)


def _progress_bar(frac: float, width: int = 20) -> str:
    frac = min(max(frac, 0.0), 1.0)
    filled = round(frac * width)
    return "█" * filled + "░" * (width - filled)


# How often (in scraped posts) to flush the in-memory buffer to a checkpoint shard.
CHECKPOINT_EVERY = 1000

# One (person, message, reaction) is unique; a --resume overlap or a repeated
# reaction-list page can re-emit a row, so exact repeats on this key are dropped.
REACTOR_DEDUP_KEY = ["Group", "Message ID", "Reactor ID", "Reaction"]


def _atomic_parquet(df: pd.DataFrame, dest: Path) -> None:
    """Write via a temp file + rename, so an OOM-kill mid-write can't leave a
    half-written shard behind."""
    tmp = dest.with_name(dest.name + ".tmp")
    df.to_parquet(tmp, index=False)
    tmp.replace(dest)


def _atomic_write_text(dest: Path, text: str) -> None:
    tmp = dest.with_name(dest.name + ".tmp")
    tmp.write_text(text, encoding="utf-8")
    tmp.replace(dest)


def _shard_paths(ckpt_dir: Path, base: str) -> list[Path]:
    return sorted(ckpt_dir.glob(f"{base}_part_*.parquet"))


def _shard_num(p: Path) -> int:
    return int(p.stem.rsplit("_", 1)[1])


def _read_shards(ckpt_dir: Path, base: str, start: int = 0) -> pd.DataFrame:
    """Concatenate `<base>_part_NNNNN.parquet` shards with index >= start into one
    frame. O(total) once — used for the posts output and the excel fallback.

    pandas concat (not a single pyarrow read): a column that is all-null in one
    batch and typed in another gets inferred as different parquet types per shard,
    which pyarrow refuses to stitch but pandas coerces cleanly."""
    paths = [p for p in _shard_paths(ckpt_dir, base) if _shard_num(p) >= start]
    if not paths:
        return pd.DataFrame()
    return pd.concat((pd.read_parquet(p) for p in paths), ignore_index=True)


def _consolidate_reactors(ckpt_dir: Path, dest: Path) -> tuple[Path, int]:
    """Stream the reactor shards into one parquet file, deduplicating without ever
    holding the whole table:
      * within a shard — on the full row key (a repeated reaction-list page);
      * across shards — at message level: every reactor row of a message is written
        to a single shard (a message's rows are appended together, before the
        `t_index % CHECKPOINT_EVERY` flush), so the same (Group, Message ID) turning
        up in a later shard is a --resume-overlap re-scrape and its copy is dropped.
    Memory: O(distinct reacted messages) for the seen-set, never O(reactor rows).

    Caller guarantees at least one non-empty reactor shard, so `writer` is always
    created (every shard is written from a non-empty buffer)."""
    seen: set = set()
    tmp = dest.with_name(dest.name + ".tmp")
    writer = None
    total = 0
    try:
        for p in _shard_paths(ckpt_dir, "reactors"):
            part = pd.read_parquet(p).drop_duplicates(subset=REACTOR_DEDUP_KEY)
            msg_keys = list(zip(part["Group"].astype(str), part["Message ID"]))
            fresh = [k not in seen for k in msg_keys]
            part = part[fresh]
            if part.empty:
                continue
            seen.update(k for k, f in zip(msg_keys, fresh) if f)
            table = pa.Table.from_pandas(part, preserve_index=False)
            if writer is None:
                writer = pq.ParquetWriter(tmp, table.schema)
            else:
                table = table.cast(writer.schema)
            writer.write_table(table)
            total += len(part)
    finally:
        if writer is not None:
            writer.close()
    tmp.replace(dest)  # atomic: no half-written final file if this step is killed
    return dest, total


def _count_shard_rows(ckpt_dir: Path, base: str) -> int:
    """Row total across `<base>` shards, from parquet footer metadata only — no row
    data is read into memory."""
    return sum(pq.ParquetFile(p).metadata.num_rows for p in _shard_paths(ckpt_dir, base))


def _next_shard_index(ckpt_dir: Path) -> int:
    idxs = [_shard_num(p)
            for base in ("posts", "reactors")
            for p in _shard_paths(ckpt_dir, base)]
    return max(idxs) + 1 if idxs else 0


def _migrate_legacy_checkpoint(ckpt_dir: Path) -> None:
    """A pre-shard checkpoint kept a single overwriting `posts.parquet` /
    `reactors.parquet`. Promote each to shard 0 so `--resume` keeps that data
    without ever loading it."""
    for base in ("posts", "reactors"):
        legacy = ckpt_dir / f"{base}.parquet"
        if legacy.exists() and not _shard_paths(ckpt_dir, base):
            legacy.rename(ckpt_dir / f"{base}_part_00000.parquet")


def _clear_checkpoint(ckpt_dir: Path) -> None:
    """Remove a previous job's checkpoint artefacts: a fresh (non-resume) run under
    the same --name must not inherit stale shards, and a clean finish leaves nothing
    behind."""
    if not ckpt_dir.exists():
        return
    for p in (*ckpt_dir.glob("*.parquet"), *ckpt_dir.glob("*.tmp")):
        p.unlink()
    (ckpt_dir / "resume.json").unlink(missing_ok=True)


@dataclass
class ScrapeParams:
    channels: list[str]
    date_min: datetime
    date_max: datetime
    name: str
    keyword: str = ""
    max_messages: int = 1_000_000
    timeout: int = 0  # stop after this many seconds; 0 disables the limit
    fmt: str = "parquet"
    out_dir: Path = Path("output")
    session: str = "telegramscrap"
    with_comments: bool = True
    with_reactors: bool = True
    with_participants: bool = True
    resume: bool = False
    channels_file: str = ""  # original --channels-file, if any (only for the resume-command hint)


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


class _ChannelRef(NamedTuple):
    arg: str | int   # what to pass to Telethon: int for a numeric ID, the raw string otherwise
    slug: str        # Group column value + output filename component
    url_base: str    # a message URL is f"{url_base}/{message_id}"


def _channel_ref(raw: str) -> _ChannelRef:
    """Resolve a user-supplied channel (@name / t.me URL / numeric ID) to how we
    address it and how we render it. Numeric IDs (e.g. '-1001629147115', as shown
    by Telegram clients) become `t.me/c/<short_id>` links and a 'c<short_id>' slug."""
    s = raw.strip()
    body = s[1:] if s.startswith("-") else s
    if body.isdigit():
        cid = int(s)
        short = utils.resolve_id(cid)[0] if cid < 0 else cid  # -100… marker -> bare id
        return _ChannelRef(cid, f"c{short}", f"https://t.me/c/{short}")
    name = channel_slug(s)
    return _ChannelRef(raw, name, f"https://t.me/{name}")


def parse_date(value: str, *, end_of_day: bool = False) -> datetime:
    """Accept 'DD.MM.YYYY' or an ISO date ('YYYY-MM-DD')."""
    value = value.strip()
    try:
        dt = datetime.fromisoformat(value)
    except ValueError:
        try:
            dt = datetime.strptime(value, "%d.%m.%Y")
        except ValueError:
            raise SystemExit(f"Bad date {value!r}: use DD.MM.YYYY or YYYY-MM-DD")
    if end_of_day and dt.time() == datetime.min.time():
        dt = dt.replace(hour=23, minute=59, second=59)
    return dt.replace(tzinfo=timezone.utc)


def _reaction_emoji(reaction) -> str:
    """Human-readable label for one Reaction: the emoji, '[custom:<id>]' or '[stars]'."""
    emoji = getattr(reaction, "emoticon", None)
    if emoji is None:
        doc_id = getattr(reaction, "document_id", None)
        emoji = f"[custom:{doc_id}]" if doc_id is not None else "[stars]"
    return emoji


def _reactions_to_string(reactions) -> str:
    if not reactions:
        return ""
    parts = [f"{_reaction_emoji(r.reaction)} {r.count}" for r in reactions.results]
    return " ".join(parts) + (" " if parts else "")


def _sender_username(msg) -> str:
    """Username of a message's sender. '' for a user without one; a marker when the
    sender is a channel (anonymous admin / linked channel) or is unavailable."""
    sender = getattr(msg, "sender", None)
    if sender is None:
        return "[anonymous]"
    if isinstance(sender, User):
        return sender.username or ""
    return "[channel]"


def _entity_name(entity) -> str:
    """Display name: a User's first + last name, else a Channel/Chat title; '' if unknown."""
    if entity is None:
        return ""
    name = " ".join(
        p for p in (getattr(entity, "first_name", None), getattr(entity, "last_name", None)) if p
    )
    return name or getattr(entity, "title", "") or ""


def _sender_name(msg) -> str:
    return _entity_name(getattr(msg, "sender", None))


async def _collect_reactors(client, peer, ref: _ChannelRef, post_id: int, msg, target: str) -> list[dict]:
    """Per-user reaction list for one message (`target` is 'post' or 'comment').

    Telegram refuses this for broadcast-channel posts (BroadcastForbiddenError); like
    _collect_comments, any RPC error is logged and that message is skipped.
    """
    if not getattr(msg, "reactions", None):  # nothing reacted -> no API call
        return []
    url = (
        f"{ref.url_base}/{post_id}?comment={msg.id}"
        if target == "comment"
        else f"{ref.url_base}/{post_id}"
    )
    rows: list[dict] = []
    offset = None
    try:
        while True:
            res = await client(
                GetMessageReactionsListRequest(peer=peer, id=msg.id, limit=100, offset=offset)
            )
            await asyncio.sleep(REACTOR_CALL_DELAY)
            entities = {e.id: e for e in (*res.users, *res.chats)}
            for pr in res.reactions:
                peer_id = pr.peer_id
                eid = (
                    getattr(peer_id, "user_id", None)
                    or getattr(peer_id, "channel_id", None)
                    or getattr(peer_id, "chat_id", None)
                )
                ent = entities.get(eid)
                if isinstance(peer_id, PeerUser):
                    rid, uname = peer_id.user_id, (getattr(ent, "username", "") or "")
                else:  # PeerChannel / PeerChat
                    rid, uname = utils.get_peer_id(peer_id), "[channel]"
                rows.append(
                    {
                        "Type": "reactor",
                        "Target": target,
                        "Group": f"@{ref.slug}",
                        "Message ID": msg.id,
                        "Post ID": post_id,
                        "Url": url,
                        "Reactor ID": rid,
                        "Reactor Username": uname,
                        "Reactor Name": _entity_name(ent),
                        "Reaction": _reaction_emoji(pr.reaction),
                        "Date": pr.date.strftime("%Y-%m-%d %H:%M:%S") if pr.date else "",
                    }
                )
            if not res.next_offset:
                break
            offset = res.next_offset
    except (*NET_ERRORS, FloodWaitError, *RETRYABLE_RPC):  # disconnect, soft ban, or a
        raise                             # transient 500/503: redo the post, don't skip
    except Exception as exc:  # BroadcastForbidden, thread removed, ...
        print(f"  ! reactors for {ref.slug}/{post_id} ({target} {msg.id}): {exc}")
    return rows


async def _collect_comments(
    client, ref: _ChannelRef, message, *, with_reactors: bool = False
) -> tuple[list[dict], list[dict]]:
    """Replies to one post that has a linked discussion thread.

    Returns (comments, reactor_rows); reactor_rows is empty unless with_reactors.
    """
    comments: list[dict] = []
    reactors: list[dict] = []
    # The linked discussion group: authoritative for where the replies live.
    # (message.input_chat / c.input_chat point back at the broadcast channel when
    # iterating with reply_to=, so they can't be trusted for the reactions call.)
    disc_id = getattr(message.replies, "channel_id", None)
    disc_peer = PeerChannel(disc_id) if disc_id else None
    try:
        async for c in client.iter_messages(ref.arg, reply_to=message.id):
            comments.append(
                {
                    "Type": "comment",
                    "Comment Group": f"@{ref.slug}",
                    "Comment Author ID": c.sender_id,
                    "Comment Author Username": _sender_username(c),
                    "Comment Author Name": _sender_name(c),
                    "Comment Content": (c.text or "").replace("'", '"'),
                    "Comment Date": c.date.strftime("%Y-%m-%d %H:%M:%S"),
                    "Comment Message ID": c.id,
                    "Comment Author": c.post_author,
                    "Comment Views": c.views,
                    "Comment Reactions": _reactions_to_string(c.reactions),
                    "Comment Shares": c.forwards,
                    "Comment Media": bool(c.media),
                    "Comment Url": f"{ref.url_base}/{message.id}?comment={c.id}",
                }
            )
            if with_reactors:
                peer = disc_peer or getattr(c, "input_chat", None) or ref.arg
                reactors += await _collect_reactors(client, peer, ref, message.id, c, "comment")
    except (*NET_ERRORS, FloodWaitError, *RETRYABLE_RPC):  # disconnect, soft ban, or a
        raise                             # transient 500/503: redo the post, don't skip
    except Exception as exc:  # thread just removed, ...
        print(f"  ! comments for {ref.slug}/{message.id}: {exc}")
    return comments, reactors


async def _collect_post(client, ref: _ChannelRef, message, params: ScrapeParams) -> tuple[dict, list[dict]]:
    """One post row plus its reactor rows (comment reactors first, then post reactors)."""
    # a 0 comment count is trusted only once the post has had time to settle
    fresh = datetime.now(timezone.utc) - message.date < RECENT_POST_WINDOW
    has_thread = bool(
        message.replies and message.replies.comments
        and (message.replies.replies or fresh)
    )
    comments, reactors = (
        await _collect_comments(client, ref, message, with_reactors=params.with_reactors)
        if params.with_comments and has_thread
        else ([], [])
    )
    if params.with_reactors:
        peer = getattr(message, "input_chat", None) or ref.arg
        reactors += await _collect_reactors(client, peer, ref, message.id, message, "post")
    row = {
        "Type": "text",
        "Group": f"@{ref.slug}",
        "Author ID": message.sender_id,
        "Content": clean_xml_text(message.text),
        "Date": message.date.strftime("%Y-%m-%d %H:%M:%S"),
        "Message ID": message.id,
        "Author": message.post_author,
        "Views": message.views,
        "Reactions": _reactions_to_string(message.reactions),
        "Shares": message.forwards,
        "Media": bool(message.media),
        "Url": f"{ref.url_base}/{message.id}",
        "Comments List": clean_xml_text(json.dumps(comments)),
    }
    return row, reactors


async def _scrape(creds: Credentials, params: ScrapeParams) -> pd.DataFrame:
    params.out_dir.mkdir(parents=True, exist_ok=True)
    # per-channel history snapshots go in partial_dir; the resume machinery goes one
    # level down in ckpt_dir, so `combine --input <name>_partial` only sees the snapshots
    partial_dir = params.out_dir / f"{params.name}_partial"
    ckpt_dir = partial_dir / "checkpoint"

    data: list[dict] = []
    reactors: list[dict] = []
    t_index = 0
    start_time = time.monotonic()

    n_channels = len(params.channels)
    span = (params.date_max - params.date_min).total_seconds()

    def _date_frac(msg_date) -> float:
        if span <= 0:
            return 1.0
        return min(max((params.date_max - msg_date).total_seconds() / span, 0.0), 1.0)

    def time_is_up() -> bool:
        return bool(params.timeout) and time.monotonic() - start_time > params.timeout

    shard_index = 0

    def _write_checkpoint(channel_index: int, last_id: int) -> None:
        nonlocal shard_index
        if not data and not reactors and not (ckpt_dir / "resume.json").exists():
            return  # nothing scraped yet and no cursor to advance
        ckpt_dir.mkdir(parents=True, exist_ok=True)
        # append-only: write ONLY the rows gathered since the previous checkpoint to
        # a fresh shard, then free them. Earlier shards are never reread or rewritten,
        # so a checkpoint costs O(batch) regardless of run size. Always parquet:
        # lossless and format-stable so --resume finds it whatever the run's --format.
        wrote = False
        if data:
            _atomic_parquet(pd.DataFrame(data),
                            ckpt_dir / f"posts_part_{shard_index:05}.parquet")
            data.clear()
            wrote = True
        if reactors:
            _atomic_parquet(pd.DataFrame(reactors),
                            ckpt_dir / f"reactors_part_{shard_index:05}.parquet")
            reactors.clear()
            wrote = True
        if wrote:
            shard_index += 1
        _atomic_write_text(ckpt_dir / "resume.json", json.dumps({
            "name": params.name,
            "channels": params.channels,
            "keyword": params.keyword,
            "date_min": params.date_min.isoformat(),
            "date_max": params.date_max.isoformat(),
            "channel_index": channel_index,
            "last_id": last_id,
            "t_index": t_index,
            "updated": datetime.now(timezone.utc).isoformat(),
        }, indent=2))

    resume_channel_index = 0
    resume_last_id = 0
    resume_meta_ok = params.resume and (ckpt_dir / "resume.json").exists()
    if params.resume and not resume_meta_ok:
        print(f"  ! --resume: {ckpt_dir / 'resume.json'} not found; starting a fresh run")
    if not resume_meta_ok:
        _clear_checkpoint(ckpt_dir)  # fresh run: never inherit a previous job's shards
    else:
        rj = ckpt_dir / "resume.json"
        meta = json.loads(rj.read_text(encoding="utf-8"))
        if (meta.get("channels") != params.channels
                or meta.get("keyword", "") != params.keyword
                or meta.get("date_min") != params.date_min.isoformat()
                or meta.get("date_max") != params.date_max.isoformat()):
            raise SystemExit("--resume: resume.json does not match the current "
                             "arguments (channels / keyword / dates). Re-run with "
                             "the same command as the interrupted job.")
        _migrate_legacy_checkpoint(ckpt_dir)
        if not _shard_paths(ckpt_dir, "posts") and int(meta.get("t_index", 0)) > 0:
            raise SystemExit(f"--resume: checkpoint shards are missing from {ckpt_dir} "
                             f"but resume.json reports {meta['t_index']} scraped posts "
                             f"— cannot resume safely.")
        shard_index = _next_shard_index(ckpt_dir)
        t_index = _count_shard_rows(ckpt_dir, "posts")  # footer metadata only, no data
        n_reactor_rows = _count_shard_rows(ckpt_dir, "reactors")
        resume_channel_index = int(meta["channel_index"])
        resume_last_id = int(meta["last_id"])
        print(SEP)
        print(f"Resuming '{params.name}': channel {resume_channel_index + 1}/{n_channels}, "
              f"{t_index} posts + {n_reactor_rows} reactor rows already saved in "
              f"{shard_index} shard(s), continuing from id {resume_last_id or 'newest'}")
        print(SEP)

    client = TelegramClient(params.session, creds.api_id, creds.api_hash,
                            flood_sleep_threshold=FLOOD_SLEEP_THRESHOLD,
                            connection_retries=CONNECTION_RETRIES,
                            retry_delay=RETRY_DELAY,
                            request_retries=REQUEST_RETRIES)
    await client.start(phone=creds.phone, password=creds.password)

    i, last_id = resume_channel_index, resume_last_id  # for the Ctrl-C handler below
    snapshot_from = 0  # first shard index not yet written to an `_until_` snapshot
    try:
        for i, channel in enumerate(params.channels):
            if i < resume_channel_index:
                continue
            if t_index >= params.max_messages or time_is_up():
                break

            loop_start = time.monotonic()
            c_index = 0
            last_id = resume_last_id if i == resume_channel_index else 0
            attempt = 0
            flood_attempts = 0
            done_channel = False
            try:
                ref = _channel_ref(channel)
                try:
                    title = getattr(await client.get_entity(ref.arg), "title", None)
                except Exception:
                    title = None
                label = f'"{title}" ({channel})' if title else channel
                print(f"=== ch {i + 1}/{n_channels}: {label} ===")
                while True:
                    try:
                        async for message in client.iter_messages(
                                ref.arg, search=params.keyword or None, offset_id=last_id):
                            if message.date < params.date_min:
                                done_channel = True
                                break
                            if message.date > params.date_max:
                                continue

                            row, row_reactors = await _collect_post(client, ref, message, params)
                            data.append(row)
                            reactors.extend(row_reactors)
                            c_index += 1
                            t_index += 1
                            last_id = message.id
                            date_str = row["Date"]

                            now = time.monotonic() - start_time
                            cf = _date_frac(message.date)                  # current channel fraction
                            overall = (i + cf) / n_channels
                            eta = (format_duration(now / overall - now)
                                   if overall > 0.02 and now > 1 else "estimating")
                            print(
                                f"|{_progress_bar(overall)}| {overall * 100:5.1f}%  "
                                f"ch {i + 1}/{n_channels} ({cf * 100:3.0f}%) "
                                f"| {c_index:05} here / {t_index:05} total | id {message.id} | {date_str} "
                                f"| elapsed {format_duration(now)} | ETA {eta}"
                            )

                            if t_index % CHECKPOINT_EVERY == 0:
                                _write_checkpoint(i, last_id)
                                print(f"  -> checkpoint: {t_index} posts")

                            if t_index >= params.max_messages or time_is_up():
                                break
                        else:
                            done_channel = True
                    except FloodWaitError as exc:  # a soft ban longer than FLOOD_SLEEP_THRESHOLD
                        flood_attempts += 1
                        _write_checkpoint(i, last_id)
                        wait = exc.seconds + FLOOD_RETRY_BUFFER
                        if wait > FLOOD_MAX_WAIT or flood_attempts > FLOOD_MAX_ATTEMPTS:
                            print(f"  ! {label}: FLOOD_WAIT {exc.seconds}s - giving up "
                                  f"(checkpoint saved at {t_index} posts)")
                            raise
                        print(f"  ! {label}: FLOOD_WAIT {exc.seconds}s - checkpoint saved at "
                              f"{t_index} posts, waiting it out "
                              f"({flood_attempts}/{FLOOD_MAX_ATTEMPTS}), then resuming from id "
                              f"{last_id or 'newest'}")
                        await asyncio.sleep(wait)
                        try:
                            if not client.is_connected():
                                await client.connect()
                        except Exception as ce:
                            print(f"  ! reconnect failed: {ce}")
                        continue
                    except (*NET_ERRORS, *RETRYABLE_RPC) as exc:
                        attempt += 1
                        _write_checkpoint(i, last_id)
                        if attempt > RESUME_MAX_ATTEMPTS:
                            print(f"  ! {label}: {exc} - giving up after {attempt - 1} retries")
                            raise
                        wait = min(RESUME_BASE_WAIT * attempt, RESUME_MAX_WAIT)
                        print(f"  ! {label}: {exc} - checkpoint saved at {t_index} posts, "
                              f"retry {attempt}/{RESUME_MAX_ATTEMPTS} from id "
                              f"{last_id or 'newest'} in {wait}s")
                        await asyncio.sleep(wait)
                        try:
                            if not client.is_connected():
                                await client.connect()  # Telethon won't recover a hard _disconnect on its own
                        except Exception as ce:
                            print(f"  ! reconnect failed: {ce}")
                        continue
                    break  # iter_messages finished without a disconnect -> channel done

                print(f"##### {label}: done, {c_index:05} posts | "
                      f"overall {((i + 1) / n_channels) * 100:.0f}% #####")
                # flush the tail buffer to a shard and advance the resume cursor
                # (on every exit path from the channel, not just a clean finish)
                _write_checkpoint(i + 1 if done_channel else i,
                                  0 if done_channel else last_id)
                partial_dir.mkdir(exist_ok=True)
                partial = partial_dir / f"{ref.slug}_until_{t_index:05}"
                # only this channel's new shards (after a --resume the first one
                # covers everything not yet snapshotted, once)
                save_table(_read_shards(ckpt_dir, "posts", start=snapshot_from),
                           partial, params.fmt)
                snapshot_from = shard_index
            except (*NET_ERRORS, FloodWaitError, *RETRYABLE_RPC):  # bubble to the Ctrl-C/finally scope and out to run()
                raise
            except Exception as exc:
                print(f"{channel} error: {exc}")

            # be gentle: at least 60s per channel
            spent = time.monotonic() - loop_start
            if spent < 60 and i < len(params.channels) - 1:
                await asyncio.sleep(60 - spent)
    except KeyboardInterrupt:
        print()
        _write_checkpoint(i, last_id)
        raise
    finally:
        await client.disconnect()

    # reached only on a run that finished without an exception (a crash bubbles to
    # run() before this and prints the --resume hint). Posts need the whole frame
    # for normalize/sort; reactors are consolidated shard-by-shard in run().
    posts_df = _read_shards(ckpt_dir, "posts")
    print(SEP)
    print(f"Concluded: {t_index:05} posts scraped")
    print(SEP)
    return posts_df


def _resume_command(params: ScrapeParams) -> str:
    """The `telegramscrap scrape … --resume` line that continues this run.

    Dates go out as full ISO (`YYYY-MM-DDTHH:MM:SS`) so re-parsing reproduces the
    exact stored bounds — including a `--date-max` that isn't end-of-day.
    """
    src = (["--channels-file", params.channels_file] if params.channels_file
           else ["--channels", ",".join(params.channels)])
    parts = ["telegramscrap", "scrape", *src,
             "--date-min", params.date_min.strftime("%Y-%m-%dT%H:%M:%S"),
             "--date-max", params.date_max.strftime("%Y-%m-%dT%H:%M:%S"),
             "--name", params.name]
    if params.keyword:
        parts += ["--keyword", params.keyword]
    if params.max_messages != ScrapeParams.__dataclass_fields__["max_messages"].default:
        parts += ["--max-messages", str(params.max_messages)]
    if params.timeout:
        parts += ["--timeout", str(params.timeout)]
    if params.fmt == "excel":
        parts += ["--format", "excel"]
    if params.out_dir != Path("output"):
        parts += ["--out-dir", str(params.out_dir)]
    if params.session != "telegramscrap":
        parts += ["--session", params.session]
    if not params.with_comments:
        parts.append("--no-comments")
    if not params.with_reactors:
        parts.append("--no-reactors")
    if not params.with_participants:
        parts.append("--no-participants")
    parts.append("--resume")
    return " ".join(shlex.quote(p) for p in parts)


def run(creds: Credentials, params: ScrapeParams) -> Path:
    from telegramscrap.analysis import normalize_posts, participants

    ckpt_dir = params.out_dir / f"{params.name}_partial" / "checkpoint"
    rj = ckpt_dir / "resume.json"
    try:
        df = asyncio.run(_scrape(creds, params))
    except (*NET_ERRORS, FloodWaitError, KeyboardInterrupt, *RETRYABLE_RPC) as exc:
        detail = f": {exc}" if str(exc) else ""
        print(SEP)
        print(f"Run stopped: {type(exc).__name__}{detail}")
        if rj.exists():
            print(f"\nContinue from the last checkpoint ({rj}) with:\n\n"
                  f"    {_resume_command(params)}\n")
        raise SystemExit(1)
    if not df.empty:
        df = normalize_posts(df)
    path = save_table(df, params.out_dir / f"{params.name}_posts", params.fmt)
    print(f"Posts:    {path}  ({len(df)} rows)")

    r_path = None
    if params.with_reactors and _shard_paths(ckpt_dir, "reactors"):
        if params.fmt == "parquet":
            # streams the shards, deduplicating on REACTOR_DEDUP_KEY (see _consolidate_reactors)
            r_path, n_reactors = _consolidate_reactors(
                ckpt_dir, params.out_dir / f"{params.name}_reactors.parquet")
        else:  # excel: small runs only, keep the in-memory path
            rdf = _read_shards(ckpt_dir, "reactors").drop_duplicates(
                subset=REACTOR_DEDUP_KEY, ignore_index=True)
            r_path = save_table(rdf, params.out_dir / f"{params.name}_reactors", params.fmt)
            n_reactors = len(rdf)
        print(f"Reactors: {r_path}  ({n_reactors} rows)")

    if params.with_participants:
        p_out = params.out_dir / f"{params.name}_participants"
        try:
            participants(str(path), str(p_out),
                         reactors=str(r_path) if r_path else "", fmt=params.fmt)
        except SystemExit as exc:  # nothing to build (no comments, no reactors, ...)
            print(f"Participants: skipped ({exc})")

    _clear_checkpoint(ckpt_dir)  # clean finish -> drop the resume cursor and shards
    return path
