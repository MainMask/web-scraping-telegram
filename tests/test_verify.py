"""Offline tests for `telegram-scraper verify` (fake client, no Telegram network)."""

import types
from datetime import datetime, timezone

import pandas as pd
import pytest
from telethon.errors import FloodWaitError

import telegram_scraper.verify as verify
from telegram_scraper.cli import build_parser, cmd_verify
from telegram_scraper.config import Credentials
from telegram_scraper.verify import VerifyParams


def _m(mid, y, mo, d, *, service=False, replies=None):
    return types.SimpleNamespace(
        id=mid,
        date=datetime(y, mo, d, 12, 0, tzinfo=timezone.utc),
        action="pinned" if service else None,
        replies=types.SimpleNamespace(replies=replies, comments=True) if replies is not None else None,
    )


# The live channel: id -> message. Ids not here come back as None from get_messages.
CHANNEL = {
    100: _m(100, 2024, 1, 5),
    98:  _m(98, 2024, 1, 4),
    97:  _m(97, 2024, 1, 4, service=True),   # service message (not a post)
    95:  _m(95, 2023, 6, 1),                 # exists but older than date_min
    90:  _m(90, 2024, 1, 2, replies=40),     # a post with a 40-comment thread
    88:  _m(88, 2024, 1, 1),
}
REAL_IN_WINDOW = [88, 90, 98, 100]  # what a complete scrape of 88..100 should hold


class _TotalList(list):
    total = 0


class FakeVerifyClient:
    def __init__(self, *a, **k):
        pass

    async def start(self, **k):
        return self

    async def disconnect(self):
        return None

    async def get_entity(self, arg):
        return types.SimpleNamespace(title="Fake Ch")

    async def get_messages(self, entity, ids=None, limit=None, reverse=False, **k):
        if ids is not None:
            return [CHANNEL.get(i) for i in ids]
        ordered = [CHANNEL[k] for k in sorted(CHANNEL, reverse=not reverse)]
        out = _TotalList(ordered[:limit] if limit else [])
        out.total = len(CHANNEL)
        return out


class FloodVerifyClient(FakeVerifyClient):
    async def get_messages(self, *a, **k):
        raise FloodWaitError(request=None)


@pytest.fixture(autouse=True)
def _fake(monkeypatch):
    monkeypatch.setattr(verify, "TelegramClient", FakeVerifyClient)
    monkeypatch.setattr(verify, "BATCH_PAUSE", 0)


def _write_input(tmp_path, ids, **cols):
    df = pd.DataFrame({"Message ID": list(ids), **cols})
    p = tmp_path / "posts.parquet"
    df.to_parquet(p, index=False)
    return str(p)


def _params(tmp_path, ids, **kw):
    return VerifyParams(
        input=_write_input(tmp_path, ids, **kw.pop("cols", {})),
        channel="-100123",
        date_min=datetime(2024, 1, 1, tzinfo=timezone.utc),
        date_max=datetime(2024, 12, 31, 23, 59, 59, tzinfo=timezone.utc),
        **kw,
    )


def test_verify_clean(tmp_path, capsys):
    verify.run(Credentials(1, "h"), _params(tmp_path, REAL_IN_WINDOW))
    assert "RESULT: 0 posts missed" in capsys.readouterr().out


def test_verify_detects_missed_post(tmp_path, capsys):
    out = tmp_path / "missed.parquet"
    with pytest.raises(SystemExit):
        verify.run(Credentials(1, "h"),
                   _params(tmp_path, [88, 90, 100], output=str(out)))
    log = capsys.readouterr().out
    assert "1 REAL POSTS MISSED" in log and "missed id 98" in log
    flagged = pd.read_parquet(out)
    assert flagged["Message ID"].tolist() == [98]
    assert flagged["Reason"].tolist() == ["missed"]


def test_verify_ignores_service_and_out_of_window(tmp_path, capsys):
    verify.run(Credentials(1, "h"), _params(tmp_path, REAL_IN_WINDOW))
    log = capsys.readouterr().out
    assert "1 service" in log and "1 outside dates" in log
    assert "0 REAL POSTS MISSED" in log


def test_verify_detects_short_scrape(tmp_path, capsys):
    with pytest.raises(SystemExit):
        verify.run(Credentials(1, "h"), _params(tmp_path, [90, 98, 100]))
    assert "channel starts at id 88" in capsys.readouterr().out


def test_verify_comment_sample(tmp_path, capsys):
    verify.run(Credentials(1, "h"),
               _params(tmp_path, REAL_IN_WINDOW,
                       comment_sample=5,
                       cols={"Comments": [0, 10, 0, 0]}))  # post 90 -> only 10 of 40
    log = capsys.readouterr().out
    assert "short thread id 90  captured 10 / server 40" in log
    assert "0 posts missed; 1 thread(s) look short" in log


def test_verify_rejects_reactors_file(tmp_path):
    p = tmp_path / "r.parquet"
    pd.DataFrame({"Message ID": [1], "Reactor ID": [99]}).to_parquet(p, index=False)
    with pytest.raises(SystemExit, match="_reactors"):
        verify.run(Credentials(1, "h"), VerifyParams(
            input=str(p), channel="-100123",
            date_min=datetime(2024, 1, 1, tzinfo=timezone.utc),
            date_max=datetime(2024, 12, 31, tzinfo=timezone.utc)))


def test_verify_handles_flood(tmp_path, capsys, monkeypatch):
    monkeypatch.setattr(verify, "TelegramClient", FloodVerifyClient)
    with pytest.raises(SystemExit):
        verify.run(Credentials(1, "h"), _params(tmp_path, REAL_IN_WINDOW))
    out = capsys.readouterr().out
    assert "Verification interrupted" in out and "FloodWaitError" in out


def test_verify_subcommand_parses():
    args = build_parser().parse_args(
        ["verify", "--input", "x.parquet", "--channel", "@c",
         "--date-min", "01.01.2024", "--date-max", "31.12.2024"])
    assert args.func is cmd_verify
