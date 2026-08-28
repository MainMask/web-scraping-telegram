"""Async Telegram scraper (terminal port of the original notebook cells 1-3)."""

import asyncio
import json
import shlex
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import NamedTuple

import pandas as pd
from telethon import TelegramClient, utils
from telethon.tl.functions.messages import GetMessageReactionsListRequest
from telethon.tl.types import PeerChannel, PeerUser, User

from telegramscrap.config import Credentials
from telegramscrap.datafiles import clean_xml_text, format_duration, read_table, save_table

SEP = "-" * 80

# Telethon auto-sleeps and retries on FLOOD_WAIT up to this many seconds (its
# default is only 60), so ordinary rate limits are waited out instead of
# skipping the data. Longer waits (a soft ban) still raise and are logged.
FLOOD_SLEEP_THRESHOLD = 600
# small pause after each reaction-list request to keep the burst rate down
REACTOR_CALL_DELAY = 0.5

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


def _progress_bar(frac: float, width: int = 20) -> str:
    frac = min(max(frac, 0.0), 1.0)
    filled = round(frac * width)
    return "█" * filled + "░" * (width - filled)


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
    except NET_ERRORS:  # a real disconnect: let the channel loop retry, don't skip data
        raise
    except Exception as exc:  # BroadcastForbidden, FloodWait past the threshold, thread removed, ...
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
    except NET_ERRORS:  # a real disconnect: let the channel loop retry, don't skip data
        raise
    except Exception as exc:  # transient (flood wait, thread just removed, ...)
        print(f"  ! comments for {ref.slug}/{message.id}: {exc}")
    return comments, reactors


async def _collect_post(client, ref: _ChannelRef, message, params: ScrapeParams) -> tuple[dict, list[dict]]:
    """One post row plus its reactor rows (comment reactors first, then post reactors)."""
    has_thread = bool(message.replies and message.replies.comments)
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


async def _scrape(creds: Credentials, params: ScrapeParams) -> tuple[pd.DataFrame, pd.DataFrame]:
    params.out_dir.mkdir(parents=True, exist_ok=True)
    # per-channel history snapshots go in partial_dir; the resume machinery goes one
    # level down in ckpt_dir, so `combine --input <name>_partial` only sees the snapshots
    partial_dir = params.out_dir / f"{params.name}_partial"
    ckpt_dir = partial_dir / "checkpoint"

    data: list[dict] = []
    reactors: list[dict] = []
    t_index = 0
    start_time = time.time()

    n_channels = len(params.channels)
    span = (params.date_max - params.date_min).total_seconds()

    def _date_frac(msg_date) -> float:
        if span <= 0:
            return 1.0
        return min(max((params.date_max - msg_date).total_seconds() / span, 0.0), 1.0)

    def time_is_up() -> bool:
        return bool(params.timeout) and time.time() - start_time > params.timeout

    def _write_checkpoint(channel_index: int, last_id: int) -> None:
        if not data:
            return
        ckpt_dir.mkdir(parents=True, exist_ok=True)
        # always parquet: lossless (excel would truncate long cells) and format-stable
        # so --resume finds it regardless of the run's --format
        save_table(pd.DataFrame(data), ckpt_dir / "posts", "parquet")
        if reactors:
            save_table(pd.DataFrame(reactors), ckpt_dir / "reactors", "parquet")
        (ckpt_dir / "resume.json").write_text(json.dumps({
            "name": params.name,
            "channels": params.channels,
            "keyword": params.keyword,
            "date_min": params.date_min.isoformat(),
            "date_max": params.date_max.isoformat(),
            "channel_index": channel_index,
            "last_id": last_id,
            "t_index": t_index,
            "updated": datetime.now(timezone.utc).isoformat(),
        }, indent=2), encoding="utf-8")

    resume_channel_index = 0
    resume_last_id = 0
    if params.resume:
        rj = ckpt_dir / "resume.json"
        if not rj.exists():
            print(f"  ! --resume: {rj} not found; starting a fresh run")
        else:
            meta = json.loads(rj.read_text(encoding="utf-8"))
            if (meta.get("channels") != params.channels
                    or meta.get("keyword", "") != params.keyword
                    or meta.get("date_min") != params.date_min.isoformat()
                    or meta.get("date_max") != params.date_max.isoformat()):
                raise SystemExit("--resume: resume.json does not match the current "
                                 "arguments (channels / keyword / dates). Re-run with "
                                 "the same command as the interrupted job.")
            p_ck = ckpt_dir / "posts.parquet"
            r_ck = ckpt_dir / "reactors.parquet"
            if not p_ck.exists() and int(meta.get("t_index", 0)) > 0:
                raise SystemExit(f"--resume: {p_ck} is missing but resume.json reports "
                                 f"{meta['t_index']} scraped posts — cannot resume safely.")
            if p_ck.exists():
                data = read_table(p_ck).to_dict("records")
            if r_ck.exists():
                reactors = read_table(r_ck).to_dict("records")
            t_index = len(data)  # the checkpoint parquet is the source of truth, not meta
            resume_channel_index = int(meta["channel_index"])
            resume_last_id = int(meta["last_id"])
            print(SEP)
            print(f"Resuming '{params.name}': channel {resume_channel_index + 1}/{n_channels}, "
                  f"{len(data)} posts + {len(reactors)} reactor rows reloaded, "
                  f"continuing from id {resume_last_id or 'newest'}")
            print(SEP)

    client = TelegramClient(params.session, creds.api_id, creds.api_hash,
                            flood_sleep_threshold=FLOOD_SLEEP_THRESHOLD,
                            connection_retries=CONNECTION_RETRIES,
                            retry_delay=RETRY_DELAY,
                            request_retries=REQUEST_RETRIES)
    await client.start(phone=creds.phone, password=creds.password)

    i, last_id = resume_channel_index, resume_last_id  # for the Ctrl-C handler below
    try:
        for i, channel in enumerate(params.channels):
            if i < resume_channel_index:
                continue
            if t_index >= params.max_messages or time_is_up():
                break

            loop_start = time.time()
            c_index = 0
            last_id = resume_last_id if i == resume_channel_index else 0
            attempt = 0
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

                            now = time.time() - start_time
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

                            if t_index % 1000 == 0:
                                _write_checkpoint(i, last_id)
                                print(f"  -> checkpoint: {t_index} posts")

                            if t_index >= params.max_messages or time_is_up():
                                break
                        else:
                            done_channel = True
                    except NET_ERRORS as exc:
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
                partial_dir.mkdir(exist_ok=True)
                partial = partial_dir / f"{ref.slug}_until_{t_index:05}"
                save_table(pd.DataFrame(data), partial, params.fmt)
                if done_channel:
                    _write_checkpoint(i + 1, 0)
            except NET_ERRORS:  # bubble to the Ctrl-C/finally scope and out to run()
                raise
            except Exception as exc:
                print(f"{channel} error: {exc}")

            # be gentle: at least 60s per channel
            spent = time.time() - loop_start
            if spent < 60 and i < len(params.channels) - 1:
                await asyncio.sleep(60 - spent)
    except KeyboardInterrupt:
        print()
        _write_checkpoint(i, last_id)
        raise
    finally:
        await client.disconnect()

    print(SEP)
    print(f"Concluded: {t_index:05} posts scraped")
    print(SEP)
    return pd.DataFrame(data), pd.DataFrame(reactors)


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

    rj = params.out_dir / f"{params.name}_partial" / "checkpoint" / "resume.json"
    try:
        df, reactors = asyncio.run(_scrape(creds, params))
    except (*NET_ERRORS, KeyboardInterrupt) as exc:
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
    if params.with_reactors and not reactors.empty:
        # a --resume overlap or a repeated reaction-list page can re-emit a row;
        # one (person, message, reaction) is unique, so drop exact repeats
        reactors = reactors.drop_duplicates(
            subset=["Group", "Message ID", "Reactor ID", "Reaction"], ignore_index=True
        )
        r_path = save_table(reactors, params.out_dir / f"{params.name}_reactors", params.fmt)
        print(f"Reactors: {r_path}  ({len(reactors)} rows)")

    if params.with_participants:
        p_out = params.out_dir / f"{params.name}_participants"
        try:
            participants(str(path), str(p_out),
                         reactors=str(r_path) if r_path else "", fmt=params.fmt)
        except SystemExit as exc:  # nothing to build (no comments, no reactors, ...)
            print(f"Participants: skipped ({exc})")

    rj.unlink(missing_ok=True)  # clean finish -> no resume pointer left behind
    return path
