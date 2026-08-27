"""Offline test of the scrape loop with a fake Telethon client (no network)."""

import types
from datetime import datetime, timezone

import pandas as pd
import pytest
from telethon import utils
from telethon.errors import BroadcastForbiddenError, FloodWaitError
from telethon.tl.types import Channel, PeerChannel, PeerUser, ReactionEmoji, User

import telegramscrap.scrape as scrape
from telegramscrap.config import Credentials
from telegramscrap.scrape import ScrapeParams

_CHANNEL_PEER_ID = utils.get_peer_id(PeerChannel(888))


def _msg(mid, date, text, *, replies=0, custom_reaction=False, sender=None, reacts=True):
    return types.SimpleNamespace(
        id=mid,
        date=date,
        text=text,
        sender_id=-100,
        sender=sender,
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
        ) if reacts else None,
        replies=types.SimpleNamespace(comments=True, replies=replies, channel_id=1001) if replies else None,
    )


def _reactions_list_response():
    """A fake messages.MessageReactionsList: one user + one channel reactor."""
    return types.SimpleNamespace(
        reactions=[
            types.SimpleNamespace(
                peer_id=PeerUser(777),
                date=datetime(2024, 6, 5, tzinfo=timezone.utc),
                reaction=ReactionEmoji(emoticon="🔥"),
            ),
            types.SimpleNamespace(
                peer_id=PeerChannel(888),
                date=None,
                reaction=ReactionEmoji(emoticon="👍"),
            ),
        ],
        users=[User(id=777, username="bob", first_name="Bob", last_name="Ivanov")],
        chats=[Channel(id=888, title="Disc Grp", photo=None, date=None)],
        next_offset=None,
        count=2,
    )


class FakeClient:
    comment_ids = {999, 998}
    reaction_peers = []  # peers passed to GetMessageReactionsListRequest; reset per instance

    def __init__(self, *a, **k):
        type(self).reaction_peers = []

    async def start(self, **k):
        return self

    async def disconnect(self):
        return None

    async def __call__(self, request):
        if type(request).__name__ != "GetMessageReactionsListRequest":
            raise NotImplementedError(request)
        type(self).reaction_peers.append(request.peer)
        if request.id in self.comment_ids:
            return _reactions_list_response()
        raise BroadcastForbiddenError(request=None)  # channel posts: Telegram says no

    def iter_messages(self, channel, search=None, reply_to=None):
        async def gen():
            if reply_to is not None:
                if reply_to == 20:
                    yield _msg(999, datetime(2024, 6, 5, tzinfo=timezone.utc), "a reply",
                               sender=User(id=777, username="bob", first_name="Bob", last_name="Ivanov"))
                    yield _msg(998, datetime(2024, 6, 5, tzinfo=timezone.utc), "anon reply",
                               sender=Channel(id=888, title="disc", photo=None, date=None),
                               reacts=False)
                return
            yield _msg(40, datetime(2025, 1, 1, tzinfo=timezone.utc), "too new")
            yield _msg(30, datetime(2024, 6, 6, tzinfo=timezone.utc), "keep", custom_reaction=True)
            yield _msg(20, datetime(2024, 6, 5, tzinfo=timezone.utc), None, replies=1)
            yield _msg(10, datetime(2023, 1, 1, tzinfo=timezone.utc), "too old")
        return gen()


class FloodClient(FakeClient):
    async def __call__(self, request):
        raise FloodWaitError(request=None)


@pytest.fixture
def fake_client(monkeypatch):
    monkeypatch.setattr(scrape, "TelegramClient", FakeClient)


def _params(tmp_path, **kw):
    return ScrapeParams(
        channels=kw.pop("channels", ["https://t.me/SomeChannel/"]),
        date_min=datetime(2024, 1, 1, tzinfo=timezone.utc),
        date_max=datetime(2024, 12, 31, 23, 59, 59, tzinfo=timezone.utc),
        name="unit.test",
        fmt="parquet",
        out_dir=tmp_path,
        **kw,
    )


def test_scrape_end_to_end(fake_client, tmp_path):
    path = scrape.run(Credentials(1, "hash"), _params(tmp_path))
    assert path.name == "unit.test_posts.parquet"

    df = pd.read_parquet(path)
    assert list(df["Message ID"]) == [30, 20]           # newer skipped, older breaks
    assert set(df["Group"]) == {"@SomeChannel"}          # URL form -> slug
    assert df.iloc[0]["Url"] == "https://t.me/SomeChannel/30"
    assert df.iloc[1]["Content"] == ""                   # None text -> ""
    assert "[custom:" in df.iloc[0]["Reactions"]
    assert '"Comment Content": "a reply"' in df.iloc[1]["Comments List"]  # post 20 had a thread
    assert '"Comment Author Username": "bob"' in df.iloc[1]["Comments List"]
    assert '"Comment Author Name": "Bob Ivanov"' in df.iloc[1]["Comments List"]
    assert '"Comment Author Username": "[channel]"' in df.iloc[1]["Comments List"]  # anon reply
    assert df.iloc[0]["Comments List"] == "[]"           # post 30 had no thread


def test_scrape_no_comments_flag(fake_client, tmp_path):
    df = pd.read_parquet(scrape.run(Credentials(1, "h"), _params(tmp_path, with_comments=False)))
    assert list(df["Comments List"]) == ["[]", "[]"]


def test_scrape_numeric_channel_id(fake_client, tmp_path):
    path = scrape.run(Credentials(1, "h"), _params(tmp_path, channels=["-1001629147115"]))
    df = pd.read_parquet(path)
    assert set(df["Group"]) == {"@c1629147115"}
    assert df.iloc[0]["Url"] == "https://t.me/c/1629147115/30"
    assert (tmp_path / "unit.test_partial" / "c1629147115_until_00002.parquet").exists()


def test_scrape_collect_reactors(fake_client, tmp_path):
    scrape.run(Credentials(1, "h"), _params(tmp_path, with_reactors=True))

    files = list(tmp_path.glob("unit.test_reactors.parquet"))
    assert len(files) == 1
    r = pd.read_parquet(files[0])

    # channel posts are 403; only comment 999 carried reactions (998 had none)
    assert set(r["Target"]) == {"comment"}
    assert set(r["Message ID"]) == {999}
    assert set(r["Post ID"]) == {20}
    assert (r["Url"] == "https://t.me/SomeChannel/20?comment=999").all()

    by_id = r.set_index("Reactor ID")
    assert by_id.loc[777, "Reactor Username"] == "bob"
    assert by_id.loc[777, "Reactor Name"] == "Bob Ivanov"
    assert by_id.loc[777, "Reaction"] == "🔥"
    assert by_id.loc[777, "Date"] == "2024-06-05 00:00:00"
    assert by_id.loc[_CHANNEL_PEER_ID, "Reactor Username"] == "[channel]"
    assert by_id.loc[_CHANNEL_PEER_ID, "Reactor Name"] == "Disc Grp"

    # comment reactions must target the discussion group (replies.channel_id), not
    # the broadcast channel — otherwise Telegram answers BroadcastForbiddenError
    assert any(getattr(p, "channel_id", None) == 1001 for p in FakeClient.reaction_peers)


def test_scrape_reactors_skipped_on_flood_wait(monkeypatch, tmp_path):
    monkeypatch.setattr(scrape, "TelegramClient", FloodClient)
    path = scrape.run(Credentials(1, "h"), _params(tmp_path, with_reactors=True))
    assert path.exists()                                  # posts still written
    assert not list(tmp_path.glob("*reactors*"))          # nothing collected -> no file


def test_scrape_reactors_without_comments(fake_client, tmp_path):
    # only per-post attempts happen (all 403); must not crash, no reactors file
    scrape.run(Credentials(1, "h"), _params(tmp_path, with_reactors=True, with_comments=False))
    assert not list(tmp_path.glob("*reactors*"))
