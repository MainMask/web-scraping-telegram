"""Check a scraped posts file against the live channel.

`scrape` walks the channel with `iter_messages` (messages.getHistory). This probes
every id that is *absent* from the scrape with `get_messages(ids=...)`
(messages.getMessages) — an independent API path — so a gap that is a real,
un-scraped post can be told apart from an ordinary deletion.
"""

import asyncio
from dataclasses import dataclass
from datetime import datetime

import pandas as pd
from telethon import TelegramClient
from telethon.errors import FloodWaitError
from telethon.tl.types import MessageService

from telegram_scraper.analysis import _count_comments
from telegram_scraper.config import Credentials
from telegram_scraper.datafiles import read_table, resolve_inputs, save_table
from telegram_scraper.scrape import (
    CONNECTION_RETRIES,
    FLOOD_SLEEP_THRESHOLD,
    NET_ERRORS,
    REQUEST_RETRIES,
    RETRY_DELAY,
    RETRYABLE_RPC,
    _channel_ref,
)

SEP = "-" * 80
ID_BATCH = 200          # messages.getMessages accepts up to 200 ids per call
BATCH_PAUSE = 0.3       # gentle spacing between probe batches
BOUND_PROBE_CAP = 5000  # cap the id sweep beyond the scraped range


@dataclass
class VerifyParams:
    input: str
    channel: str
    date_min: datetime
    date_max: datetime
    session: str = "telegram-scraper"
    output: str = ""
    comment_sample: int = 0


def _chunks(seq, n):
    for i in range(0, len(seq), n):
        yield seq[i:i + n]


def _load_saved(pattern: str) -> pd.DataFrame:
    df = pd.concat([read_table(p) for p in resolve_inputs(pattern)], ignore_index=True)
    if "Message ID" not in df.columns:
        raise SystemExit(f"{pattern}: no 'Message ID' column — not a scraped posts file")
    if "Reactor ID" in df.columns:
        raise SystemExit(f"{pattern}: looks like a *_reactors file — pass the *_posts file")
    df = df[df["Message ID"].notna()].drop_duplicates(subset="Message ID").copy()
    df["_id"] = df["Message ID"].astype(int)
    return df


async def _classify_absent(client, entity, ids, params: VerifyParams):
    """Probe `ids`; return ([(id, date), ...] real in-window posts, counts dict)."""
    missed, counts = [], {"deleted": 0, "service": 0, "out_of_window": 0}
    for batch in _chunks(ids, ID_BATCH):
        for mid, m in zip(batch, await client.get_messages(entity, ids=batch)):
            if m is None:
                counts["deleted"] += 1
            elif isinstance(m, MessageService) or getattr(m, "action", None) is not None:
                counts["service"] += 1
            elif not (params.date_min <= m.date <= params.date_max):
                counts["out_of_window"] += 1
            else:
                missed.append((mid, m.date))
        await asyncio.sleep(BATCH_PAUSE)
    return missed, counts


async def _check_comments(client, entity, df: pd.DataFrame, params: VerifyParams):
    """Sample scraped threads, compare captured comment counts to the server's."""
    if "Comments" in df.columns:
        nc = df.set_index("_id")["Comments"].astype(int)
    elif "Comments List" in df.columns:
        nc = df.set_index("_id")["Comments List"].apply(_count_comments)
    else:
        print("comment check: input has no 'Comments'/'Comments List' column — skipped")
        return []
    with_c = nc[nc > 0]
    if with_c.empty:
        return []
    n = min(params.comment_sample, len(with_c))
    ids = sorted(with_c.sample(n, random_state=0).index)
    print(f"comment threads: re-checking {n} of {len(with_c)} threads against the server...")
    short = []
    for batch in _chunks(ids, ID_BATCH):
        for m in await client.get_messages(entity, ids=batch):
            if m is None or not getattr(m, "replies", None):
                continue
            expected = m.replies.replies or 0
            got = int(nc.get(m.id, 0))
            if got < expected * 0.9 and expected - got > 5:
                short.append((m.id, m.date, got, expected))
        await asyncio.sleep(BATCH_PAUSE)
    for mid, mdate, got, exp in short:
        print(f"    ~ short thread id {mid}  captured {got} / server {exp}")
    return short


async def _verify(creds: Credentials, params: VerifyParams):
    df = _load_saved(params.input)
    saved = set(df["_id"])
    id_min, id_max = min(saved), max(saved)

    client = TelegramClient(params.session, creds.api_id, creds.api_hash,
                            flood_sleep_threshold=FLOOD_SLEEP_THRESHOLD,
                            connection_retries=CONNECTION_RETRIES,
                            retry_delay=RETRY_DELAY,
                            request_retries=REQUEST_RETRIES)
    await client.start(phone=creds.phone, password=creds.password)

    flagged = []          # (id, date, reason)
    short_threads = []
    try:
        ref = _channel_ref(params.channel)
        entity = await client.get_entity(ref.arg)
        newest = await client.get_messages(entity, limit=1)
        oldest = await client.get_messages(entity, limit=1, reverse=True)
        total = (await client.get_messages(entity, limit=0)).total
        newest = newest[0] if newest else None
        oldest = oldest[0] if oldest else None

        print(SEP)
        print(f'verify "{params.input}"  vs  {params.channel} '
              f'("{getattr(entity, "title", "")}")')
        print(f"  saved posts:   {len(saved):>8}   (id {id_min}..{id_max})")
        print(f"  channel total: {total:>8}   "
              f"(newest id {getattr(newest, 'id', '?')}, oldest id {getattr(oldest, 'id', '?')})")
        print(SEP)

        # check: every id absent from the scrape, within the scraped range
        absent = [i for i in range(id_min, id_max + 1) if i not in saved]
        note = "  (this will take a few minutes)" if len(absent) > 20_000 else ""
        print(f"id range {id_min}..{id_max}: {len(absent)} absent id(s), probing...{note}")
        missed, counts = await _classify_absent(client, entity, absent, params)
        print(f"  {counts['deleted']} deleted/never existed, {counts['service']} service, "
              f"{counts['out_of_window']} outside dates, {len(missed)} REAL POSTS MISSED")
        for mid, mdate in missed:
            print(f"    ! missed id {mid}  {mdate:%Y-%m-%d %H:%M}")
            flagged.append((mid, mdate, "missed"))

        # check: the scrape reached the channel's oldest in-window message
        if oldest and oldest.id < id_min and oldest.date >= params.date_min:
            capped = id_min - oldest.id > BOUND_PROBE_CAP
            lo = range(oldest.id, min(id_min, oldest.id + BOUND_PROBE_CAP))
            lo_missed, _ = await _classify_absent(client, entity, list(lo), params)
            print(f"lower bound: channel starts at id {oldest.id} < first saved {id_min} -> "
                  f"{'>=' if capped else ''}{len(lo_missed)} in-window post(s) before the scrape")
            for mid, mdate in lo_missed:
                flagged.append((mid, mdate, "before-first-saved"))

        # check: nothing in-window newer than the last saved id
        if newest and newest.id > id_max and newest.date <= params.date_max:
            hi = range(id_max + 1, min(newest.id + 1, id_max + 1 + BOUND_PROBE_CAP))
            hi_missed, _ = await _classify_absent(client, entity, list(hi), params)
            print(f"upper bound: {len(hi_missed)} in-window post(s) after the last saved id {id_max}")
            for mid, mdate in hi_missed:
                flagged.append((mid, mdate, "after-last-saved"))

        if params.comment_sample:
            short_threads = await _check_comments(client, entity, df, params)
    finally:
        await client.disconnect()

    return flagged, short_threads


def run(creds: Credentials, params: VerifyParams) -> None:
    try:
        flagged, short_threads = asyncio.run(_verify(creds, params))
    except (KeyboardInterrupt, *NET_ERRORS, FloodWaitError, *RETRYABLE_RPC) as exc:
        print(SEP)
        print(f"Verification interrupted ({type(exc).__name__}) — re-run to finish.")
        raise SystemExit(1)
    print(SEP)

    if params.output and (flagged or short_threads):
        rows = [{"Message ID": i, "Date": d, "Reason": r} for i, d, r in flagged]
        rows += [{"Message ID": i, "Date": d, "Reason": "short-thread"}
                 for i, d, _, _ in short_threads]
        out = save_table(pd.DataFrame(rows), params.output, "parquet")
        print(f"flagged ids -> {out}")

    if flagged:
        print(f"RESULT: {len(flagged)} message(s) missed by the scrape — re-scrape "
              f"the affected date range(s).")
        raise SystemExit(1)
    if short_threads:
        print(f"RESULT: 0 posts missed; {len(short_threads)} thread(s) look short "
              f"(server counts include deleted comments — usually benign).")
        return
    print("RESULT: 0 posts missed — the scrape is complete for this id range.")
