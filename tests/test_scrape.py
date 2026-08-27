"""Offline test of the scrape loop with a fake Telethon client (no network)."""

import types
from datetime import datetime, timezone

import pandas as pd
import pytest

import telegramscrap.scrape as scrape
from telegramscrap.config import Credentials
from telegramscrap.scrape import ScrapeParams


def _msg(mid, date, text, *, replies=0, custom_reaction=False):
    return types.SimpleNamespace(
        id=mid,
        date=date,
        text=text,
        sender_id=-100,
        post_author="Author",
        views=1,
        forwards=0,
        media=False,
        reactions=types.SimpleNamespace(
            results=[types.SimpleNamespace(
                reaction=types.SimpleNamespace(document_id=555) if custom_reaction
                else types.SimpleNamespace(emoticon="👍"),
                count=3,
            )]
        ),
        replies=types.SimpleNamespace(comments=True, replies=replies) if replies else None,
    )


class FakeClient:
    def __init__(self, *a, **k):
        pass

    async def start(self, **k):
        return self

    async def disconnect(self):
        return None

    def iter_messages(self, channel, search=None, reply_to=None):
        async def gen():
            if reply_to is not None:
                if reply_to == 20:
                    yield _msg(999, datetime(2024, 6, 5, tzinfo=timezone.utc), "a reply")
                return
            yield _msg(40, datetime(2025, 1, 1, tzinfo=timezone.utc), "too new")
            yield _msg(30, datetime(2024, 6, 6, tzinfo=timezone.utc), "keep", custom_reaction=True)
            yield _msg(20, datetime(2024, 6, 5, tzinfo=timezone.utc), None, replies=1)
            yield _msg(10, datetime(2023, 1, 1, tzinfo=timezone.utc), "too old")
        return gen()


@pytest.fixture
def fake_client(monkeypatch):
    monkeypatch.setattr(scrape, "TelegramClient", FakeClient)


def test_scrape_end_to_end(fake_client, tmp_path):
    params = ScrapeParams(
        channels=["https://t.me/SomeChannel/"],
        date_min=datetime(2024, 1, 1, tzinfo=timezone.utc),
        date_max=datetime(2024, 12, 31, 23, 59, 59, tzinfo=timezone.utc),
        name="unit.test",
        fmt="parquet",
        out_dir=tmp_path,
    )
    path = scrape.run(Credentials(1, "hash"), params)
    assert path.name == "FINAL_unit.test_with_00002.parquet"

    df = pd.read_parquet(path)
    assert list(df["Message ID"]) == [30, 20]           # newer skipped, older breaks
    assert set(df["Group"]) == {"@SomeChannel"}          # URL form -> slug
    assert df.iloc[0]["Url"] == "https://t.me/SomeChannel/30"
    assert df.iloc[1]["Content"] == ""                   # None text -> ""
    assert "[custom:" in df.iloc[0]["Reactions"]
    assert '"Comment Content": "a reply"' in df.iloc[1]["Comments List"]  # post 20 had a thread
    assert df.iloc[0]["Comments List"] == "[]"           # post 30 had no thread


def test_scrape_no_comments_flag(fake_client, tmp_path):
    params = ScrapeParams(
        channels=["@SomeChannel"],
        date_min=datetime(2024, 1, 1, tzinfo=timezone.utc),
        date_max=datetime(2024, 12, 31, 23, 59, 59, tzinfo=timezone.utc),
        name="x",
        fmt="parquet",
        out_dir=tmp_path,
        with_comments=False,
    )
    df = pd.read_parquet(scrape.run(Credentials(1, "h"), params))
    assert list(df["Comments List"]) == ["[]", "[]"]
