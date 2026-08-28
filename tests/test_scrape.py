"""Offline test of the scrape loop with a fake Telethon client (no network)."""

import asyncio
import json
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
        sender_id=getattr(sender, "id", -100),
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


def _main_messages():
    return [
        _msg(40, datetime(2025, 1, 1, tzinfo=timezone.utc), "too new"),
        _msg(30, datetime(2024, 6, 6, tzinfo=timezone.utc), "keep", custom_reaction=True),
        _msg(20, datetime(2024, 6, 5, tzinfo=timezone.utc), None, replies=1),
        _msg(10, datetime(2023, 1, 1, tzinfo=timezone.utc), "too old"),
    ]


def _thread_replies(reply_to):
    if reply_to != 20:
        return []
    return [
        _msg(999, datetime(2024, 6, 5, tzinfo=timezone.utc), "a reply",
             sender=User(id=777, username="bob", first_name="Bob", last_name="Ivanov")),
        _msg(998, datetime(2024, 6, 5, tzinfo=timezone.utc), "anon reply",
             sender=Channel(id=888, title="disc", photo=None, date=None), reacts=False),
    ]


class FakeClient:
    comment_ids = {999, 998}
    reaction_peers = []  # peers passed to GetMessageReactionsListRequest; reset per instance
    calls = []           # (channel, offset_id) for each main-branch iter_messages; reset per instance
    init_kwargs = {}     # kwargs the last instance was constructed with

    def __init__(self, *a, **k):
        type(self).reaction_peers = []
        type(self).calls = []
        type(self).init_kwargs = k

    async def start(self, **k):
        return self

    async def disconnect(self):
        return None

    async def connect(self):
        return None

    def is_connected(self):
        return True

    async def get_entity(self, arg):
        return types.SimpleNamespace(title="Fake Channel")

    async def __call__(self, request):
        if type(request).__name__ != "GetMessageReactionsListRequest":
            raise NotImplementedError(request)
        type(self).reaction_peers.append(request.peer)
        if request.id in self.comment_ids:
            return _reactions_list_response()
        raise BroadcastForbiddenError(request=None)  # channel posts: Telegram says no

    def _main_gen(self, offset_id):
        """Overridable: the main-branch messages an instance yields."""
        async def gen():
            for m in _main_messages():
                if offset_id and m.id >= offset_id:
                    continue
                yield m
        return gen()

    def iter_messages(self, channel, search=None, reply_to=None, offset_id=0):
        if reply_to is not None:
            async def replies():
                for m in _thread_replies(reply_to):
                    yield m
            return replies()
        type(self).calls.append((channel, offset_id))
        return self._main_gen(offset_id)


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
        fmt=kw.pop("fmt", "parquet"),
        out_dir=tmp_path,
        with_reactors=kw.pop("with_reactors", False),
        **kw,
    )


def test_scrape_end_to_end(fake_client, tmp_path, capsys):
    path = scrape.run(Credentials(1, "hash"), _params(tmp_path))
    assert path.name == "unit.test_posts.parquet"

    log = capsys.readouterr().out
    assert "%" in log and "ETA" in log and ("█" in log or "░" in log)  # progress bar

    df = pd.read_parquet(path)
    assert list(df["Message ID"]) == ["30", "20"]        # newer skipped, older breaks
    assert list(df["Comments"]) == [0, 2]                # post 30 no thread, post 20 two replies
    assert set(df["Group"]) == {"@SomeChannel"}          # URL form -> slug
    assert df.iloc[0]["Url"] == "https://t.me/SomeChannel/30"
    assert df.iloc[1]["Content"] == ""                   # None text -> ""
    assert "[custom:" in df.iloc[0]["Reactions"]
    assert '"Comment Content": "a reply"' in df.iloc[1]["Comments List"]  # post 20 had a thread
    assert '"Comment Author Username": "bob"' in df.iloc[1]["Comments List"]
    assert '"Comment Author Name": "Bob Ivanov"' in df.iloc[1]["Comments List"]
    assert '"Comment Author Username": "[channel]"' in df.iloc[1]["Comments List"]  # anon reply
    assert df.iloc[0]["Comments List"] == "[]"           # post 30 had no thread

    people = pd.read_parquet(tmp_path / "unit.test_participants.parquet").set_index("ID")
    assert people.loc[777, "Username"] == "bob"
    assert people.loc[777, "Name"] == "Bob Ivanov"
    assert people.loc[777, "Comments"] == 1


def test_scrape_no_comments_flag(fake_client, tmp_path):
    df = pd.read_parquet(scrape.run(Credentials(1, "h"), _params(tmp_path, with_comments=False)))
    assert list(df["Comments List"]) == ["[]", "[]"]


def test_scrape_numeric_channel_id(fake_client, tmp_path, capsys):
    path = scrape.run(Credentials(1, "h"), _params(tmp_path, channels=["-1001629147115"]))
    df = pd.read_parquet(path)
    assert set(df["Group"]) == {"@c1629147115"}
    assert df.iloc[0]["Url"] == "https://t.me/c/1629147115/30"
    assert (tmp_path / "unit.test_partial" / "c1629147115_until_00002.parquet").exists()
    assert '"Fake Channel" (-1001629147115)' in capsys.readouterr().out  # title in the header


def test_scrape_reactors(fake_client, tmp_path):
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

    people = pd.read_parquet(tmp_path / "unit.test_participants.parquet").set_index("ID")
    assert people.loc[777, "Reactions"] >= 1             # reactor folded into participants

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
    assert not list(tmp_path.glob("*participants*"))      # nothing to build -> skipped


# --- connection resilience + resume -----------------------------------------

class FlakyClient(FakeClient):
    """Drops the connection once, mid-iteration, then recovers."""
    recovered = False

    def __init__(self, *a, **k):
        super().__init__(*a, **k)
        type(self).recovered = False

    def _main_gen(self, offset_id):
        async def gen():
            for m in _main_messages():
                if offset_id and m.id >= offset_id:
                    continue
                yield m
                if m.id == 30 and not type(self).recovered:
                    type(self).recovered = True
                    raise ConnectionError("boom")
        return gen()


class DeadClient(FakeClient):
    """Never gets past the first post."""

    def _main_gen(self, offset_id):
        async def gen():
            if not offset_id:
                yield _msg(40, datetime(2025, 1, 1, tzinfo=timezone.utc), "too new")
                yield _msg(30, datetime(2024, 6, 6, tzinfo=timezone.utc), "keep")
            raise ConnectionError("dead")
        return gen()


class ResumeClient(FakeClient):
    """After a resume (offset_id set) one extra older post appears."""

    def _main_gen(self, offset_id):
        async def gen():
            if offset_id:
                yield _msg(15, datetime(2024, 6, 4, tzinfo=timezone.utc), "resumed extra")
            for m in _main_messages():
                if offset_id and m.id >= offset_id:
                    continue
                yield m
        return gen()


class CtrlCClient(FakeClient):
    """User hits Ctrl-C after the first post."""

    def _main_gen(self, offset_id):
        async def gen():
            yield _msg(40, datetime(2025, 1, 1, tzinfo=timezone.utc), "too new")
            yield _msg(30, datetime(2024, 6, 6, tzinfo=timezone.utc), "keep")
            raise KeyboardInterrupt
        return gen()


class ReactorDropClient(FakeClient):
    """The connection drops once, inside the per-message reactions call."""
    dropped = False

    def __init__(self, *a, **k):
        super().__init__(*a, **k)
        type(self).dropped = False

    async def __call__(self, request):
        if not type(self).dropped:
            type(self).dropped = True
            raise ConnectionError("boom during reactions")
        return await super().__call__(request)


def _partial(tmp_path):
    return tmp_path / "unit.test_partial"


def _ckpt(tmp_path):
    return _partial(tmp_path) / "checkpoint"


def _resume_meta(tmp_path, **over):
    meta = {
        "name": "unit.test",
        "channels": ["https://t.me/SomeChannel/"],
        "keyword": "",
        "date_min": datetime(2024, 1, 1, tzinfo=timezone.utc).isoformat(),
        "date_max": datetime(2024, 12, 31, 23, 59, 59, tzinfo=timezone.utc).isoformat(),
        "channel_index": 0,
        "last_id": 0,
        "t_index": 0,
    }
    meta.update(over)
    return meta


def _seed_checkpoint(tmp_path, rows, meta):
    d = _ckpt(tmp_path)
    d.mkdir(parents=True, exist_ok=True)
    if rows:
        pd.DataFrame(rows).to_parquet(d / "posts.parquet")
    (d / "resume.json").write_text(json.dumps(meta), encoding="utf-8")


_CK_ROW_30 = {"Type": "text", "Group": "@SomeChannel", "Content": "c30",
              "Date": "2024-06-06 00:00:00", "Message ID": 30, "Comments List": "[]",
              "Url": "https://t.me/SomeChannel/30"}
_CK_ROW_20 = {"Type": "text", "Group": "@SomeChannel", "Content": "c20",
              "Date": "2024-06-05 00:00:00", "Message ID": 20, "Comments List": "[]",
              "Url": "https://t.me/SomeChannel/20"}


def test_connection_kwargs_passed(fake_client, tmp_path):
    scrape.run(Credentials(1, "h"), _params(tmp_path))
    k = FakeClient.init_kwargs
    assert k["connection_retries"] == scrape.CONNECTION_RETRIES
    assert k["retry_delay"] == scrape.RETRY_DELAY
    assert k["request_retries"] == scrape.REQUEST_RETRIES
    assert k["flood_sleep_threshold"] == scrape.FLOOD_SLEEP_THRESHOLD


def test_scrape_default_offset_id_zero(fake_client, tmp_path):
    scrape.run(Credentials(1, "h"), _params(tmp_path))
    assert FakeClient.calls[0][1] == 0


def test_scrape_resumes_after_connection_error(monkeypatch, tmp_path, capsys):
    monkeypatch.setattr(scrape, "TelegramClient", FlakyClient)
    monkeypatch.setattr(scrape, "RESUME_BASE_WAIT", 0)
    path = scrape.run(Credentials(1, "h"), _params(tmp_path, with_reactors=True))

    df = pd.read_parquet(path)
    assert list(df["Message ID"]) == ["30", "20"]        # no duplicate 30
    assert len(FlakyClient.calls) == 2
    assert FlakyClient.calls[1][1] == 30                  # restarted just past the last saved id
    assert "retry 1/" in capsys.readouterr().out

    r = pd.read_parquet(tmp_path / "unit.test_reactors.parquet")
    assert set(r["Message ID"]) == {999}                  # comment reactors collected once
    assert len(r) == 2 and not r.duplicated().any()


def test_scrape_gives_up_and_hints_resume(monkeypatch, tmp_path, capsys):
    monkeypatch.setattr(scrape, "TelegramClient", DeadClient)
    monkeypatch.setattr(scrape, "RESUME_BASE_WAIT", 0)
    monkeypatch.setattr(scrape, "RESUME_MAX_ATTEMPTS", 2)

    with pytest.raises(SystemExit):
        scrape.run(Credentials(1, "h"), _params(tmp_path))

    out = capsys.readouterr().out
    assert "telegramscrap scrape" in out and "--resume" in out and "--name unit.test" in out
    meta = json.loads((_ckpt(tmp_path) / "resume.json").read_text())
    assert meta["last_id"] == 30 and meta["channel_index"] == 0
    assert len(pd.read_parquet(_ckpt(tmp_path) / "posts.parquet")) == 1


def test_keyboard_interrupt_checkpoints_and_hints(monkeypatch, tmp_path, capsys):
    monkeypatch.setattr(scrape, "TelegramClient", CtrlCClient)

    with pytest.raises(SystemExit):
        scrape.run(Credentials(1, "h"), _params(tmp_path))

    out = capsys.readouterr().out
    assert "telegramscrap scrape" in out and "--resume" in out
    meta = json.loads((_ckpt(tmp_path) / "resume.json").read_text())
    assert meta["last_id"] == 30


def test_resume_command_is_parseable(tmp_path):
    from telegramscrap.cli import build_parser

    p = _params(tmp_path, keyword="war", with_reactors=False, timeout=99)
    argv = scrape._resume_command(p).split()[1:]  # drop the "telegramscrap" prog name
    ns = build_parser().parse_args(argv)
    assert ns.resume is True
    assert ns.name == "unit.test" and ns.keyword == "war" and ns.timeout == 99
    assert ns.no_reactors is True


def test_resume_command_uses_channels_file(tmp_path):
    from telegramscrap.cli import build_parser

    argv = scrape._resume_command(_params(tmp_path, channels_file="chans.txt")).split()[1:]
    assert "--channels-file" in argv and "chans.txt" in argv and "--channels" not in argv
    build_parser().parse_args(argv)  # mutually-exclusive group still satisfied


def test_resume_command_dates_roundtrip(tmp_path):
    p = _params(tmp_path)
    argv = scrape._resume_command(p).split()[1:]
    assert scrape.parse_date(argv[argv.index("--date-min") + 1]) == p.date_min
    assert scrape.parse_date(argv[argv.index("--date-max") + 1], end_of_day=True) == p.date_max


def test_connection_error_during_reactions_triggers_retry(monkeypatch, tmp_path, capsys):
    monkeypatch.setattr(scrape, "TelegramClient", ReactorDropClient)
    monkeypatch.setattr(scrape, "RESUME_BASE_WAIT", 0)

    path = scrape.run(Credentials(1, "h"), _params(tmp_path, with_reactors=True))

    assert list(pd.read_parquet(path)["Message ID"]) == ["30", "20"]  # nothing skipped
    assert "retry 1/" in capsys.readouterr().out
    assert len(ReactorDropClient.calls) == 2                          # channel restarted once


def test_resume_flag_reloads_checkpoint(monkeypatch, tmp_path, capsys):
    monkeypatch.setattr(scrape, "TelegramClient", ResumeClient)
    _seed_checkpoint(tmp_path, [_CK_ROW_30, _CK_ROW_20],
                     _resume_meta(tmp_path, last_id=20, t_index=2))

    path = scrape.run(Credentials(1, "h"), _params(tmp_path, resume=True))

    assert "Resuming" in capsys.readouterr().out
    assert ResumeClient.calls[0][1] == 20
    assert list(pd.read_parquet(path)["Message ID"]) == ["30", "20", "15"]


def test_resume_flag_skips_completed_channel(monkeypatch, tmp_path):
    monkeypatch.setattr(scrape, "TelegramClient", FakeClient)
    chans = ["@one", "@two"]
    _seed_checkpoint(tmp_path, [_CK_ROW_30],
                     _resume_meta(tmp_path, channels=chans, channel_index=1, t_index=1))

    scrape.run(Credentials(1, "h"), _params(tmp_path, resume=True, channels=chans))
    assert [c[0] for c in FakeClient.calls] == ["@two"]


def test_resume_flag_param_mismatch(monkeypatch, tmp_path):
    monkeypatch.setattr(scrape, "TelegramClient", FakeClient)
    _seed_checkpoint(tmp_path, [_CK_ROW_30], _resume_meta(tmp_path, channels=["@old"]))

    with pytest.raises(SystemExit, match="does not match"):
        scrape.run(Credentials(1, "h"), _params(tmp_path, resume=True))


def test_resume_flag_missing_json(fake_client, tmp_path, capsys):
    path = scrape.run(Credentials(1, "h"), _params(tmp_path, resume=True))
    assert "not found" in capsys.readouterr().out
    assert list(pd.read_parquet(path)["Message ID"]) == ["30", "20"]


def test_scrape_success_clears_resume_json(fake_client, tmp_path):
    path = scrape.run(Credentials(1, "h"), _params(tmp_path))
    assert path.exists()
    assert not (_ckpt(tmp_path) / "resume.json").exists()


def test_checkpoint_is_parquet_even_for_excel(fake_client, tmp_path):
    scrape.run(Credentials(1, "h"), _params(tmp_path, fmt="excel", with_participants=False))
    d = _ckpt(tmp_path)
    assert (d / "posts.parquet").exists()
    assert not (d / "posts.xlsx").exists()


def test_resume_flag_missing_checkpoint_refuses(monkeypatch, tmp_path):
    monkeypatch.setattr(scrape, "TelegramClient", FakeClient)
    d = _ckpt(tmp_path)
    d.mkdir(parents=True)
    (d / "resume.json").write_text(json.dumps(_resume_meta(tmp_path, t_index=500, last_id=42)))

    with pytest.raises(SystemExit, match="missing"):
        scrape.run(Credentials(1, "h"), _params(tmp_path, resume=True))


def test_run_dedups_reactor_rows(monkeypatch, tmp_path):
    posts = pd.DataFrame([{"Type": "text", "Group": "@c", "Message ID": 1,
                           "Date": "2024-01-01 00:00:00", "Comments List": "[]", "Url": "u"}])
    dup = pd.DataFrame([
        {"Group": "@c", "Message ID": 1, "Reactor ID": 7, "Reaction": "🔥", "Date": "d"},
        {"Group": "@c", "Message ID": 1, "Reactor ID": 7, "Reaction": "🔥", "Date": "d"},
        {"Group": "@c", "Message ID": 1, "Reactor ID": 8, "Reaction": "👍", "Date": "d"},
    ])

    async def fake_scrape(creds, params):
        return posts, dup

    monkeypatch.setattr(scrape, "_scrape", fake_scrape)
    scrape.run(Credentials(1, "h"), _params(tmp_path, with_reactors=True, with_participants=False))
    r = pd.read_parquet(tmp_path / "unit.test_reactors.parquet")
    assert len(r) == 2


def test_combine_ignores_resume_checkpoint(tmp_path):
    from telegramscrap import analysis

    pdir = _partial(tmp_path)
    (pdir / "checkpoint").mkdir(parents=True)
    pd.DataFrame([_CK_ROW_30, _CK_ROW_20]).to_parquet(pdir / "SomeChannel_until_00002.parquet")
    pd.DataFrame([{  # reactor-shaped row that must NOT be pulled into a post merge
        "Type": "reactor", "Target": "comment", "Group": "@SomeChannel", "Message ID": 999,
        "Post ID": 20, "Url": "u", "Reactor ID": 7, "Reaction": "🔥", "Date": "2024-06-05 00:00:00",
    }]).to_parquet(pdir / "checkpoint" / "reactors.parquet")

    out = tmp_path / "combined.parquet"
    analysis.combine(str(pdir), str(out), ["Group", "Message ID"])
    assert sorted(pd.read_parquet(out)["Message ID"]) == ["20", "30"]  # reactor row 999 excluded


def test_collect_post_returns_row_and_reactor_rows(tmp_path):
    ref = scrape._channel_ref("https://t.me/SomeChannel/")
    msg = _msg(20, datetime(2024, 6, 5, tzinfo=timezone.utc), "body", replies=1)
    row, reactors = asyncio.run(
        scrape._collect_post(FakeClient(), ref, msg, _params(tmp_path, with_reactors=True))
    )
    assert row["Message ID"] == 20 and row["Group"] == "@SomeChannel"
    assert row["Url"] == "https://t.me/SomeChannel/20"
    assert json.loads(row["Comments List"])[0]["Comment Author Username"] == "bob"
    assert {r["Message ID"] for r in reactors} == {999} and len(reactors) == 2
