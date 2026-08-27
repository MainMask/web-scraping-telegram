"""Offline tests for the pure helpers (no Telegram network, no credentials)."""

import pandas as pd
import pytest

from telegramscrap.analysis import _TME_BASE_RE, _TME_RE, _count_comments, combine
from telegramscrap.cli import build_parser
from telegramscrap.datafiles import clean_xml_text, format_duration, read_table, save_table
from telegramscrap.scrape import _channel_ref, channel_slug, parse_date


def test_clean_xml_text_handles_none_and_control_chars():
    assert clean_xml_text(None) == ""
    assert clean_xml_text("a\x00b\x07c") == "abc"
    assert clean_xml_text("normal текст 😀") == "normal текст 😀"


def test_format_duration():
    assert format_duration(90061) == "01:01:01:01"


@pytest.mark.parametrize("fmt", ["parquet", "xlsx", "csv"])
def test_save_read_roundtrip(tmp_path, fmt):
    df = pd.DataFrame({"Group": ["@a", "@b"], "Content": ["hi", "yo"]})
    path = save_table(df, tmp_path / "out", fmt)
    back = read_table(path)
    assert list(back["Content"]) == ["hi", "yo"]


def test_parse_date_end_of_day_is_utc():
    d = parse_date("2025-01-15", end_of_day=True)
    assert (d.hour, d.minute, d.second) == (23, 59, 59)
    assert d.tzinfo is not None


def test_parse_date_accepts_dotted_and_iso():
    dotted = parse_date("10.07.2015")
    assert (dotted.year, dotted.month, dotted.day) == (2015, 7, 10)
    assert dotted == parse_date("2015-07-10")
    assert parse_date("10.07.2015", end_of_day=True).hour == 23


def test_parse_date_rejects_garbage():
    with pytest.raises(SystemExit):
        parse_date("not-a-date")


def test_count_comments_from_json_string():
    payload = '[{"Type": "comment"}, {"Type": "comment"}, {"Type": "text"}]'
    assert _count_comments(payload) == 2
    assert _count_comments(None) == 0


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("@durov", "durov"),
        ("durov", "durov"),
        ("https://t.me/durov", "durov"),
        ("http://t.me/durov/", "durov"),
        ("t.me/durov/123?comment=1", "durov"),
        ("https://t.me/+AbCdEf", "+AbCdEf"),
        ("  https://telegram.me/durov  ", "durov"),
    ],
)
def test_channel_slug(raw, expected):
    assert channel_slug(raw) == expected


@pytest.mark.parametrize(
    "raw,arg,slug,url_base",
    [
        ("@durov", "@durov", "durov", "https://t.me/durov"),
        ("https://t.me/durov/9", "https://t.me/durov/9", "durov", "https://t.me/durov"),
        ("-1001629147115", -1001629147115, "c1629147115", "https://t.me/c/1629147115"),
        ("1629147115", 1629147115, "c1629147115", "https://t.me/c/1629147115"),
    ],
)
def test_channel_ref(raw, arg, slug, url_base):
    ref = _channel_ref(raw)
    assert (ref.arg, ref.slug, ref.url_base) == (arg, slug, url_base)


def test_save_table_keeps_dotted_name(tmp_path):
    df = pd.DataFrame({"a": [1]})
    out = save_table(df, tmp_path / "my.data.2024", "parquet")
    assert out.name == "my.data.2024.parquet"
    assert read_table(out)["a"].tolist() == [1]


def test_combine_errors_on_empty_inputs(tmp_path):
    pd.DataFrame().to_parquet(tmp_path / "empty.parquet")
    with pytest.raises(SystemExit):
        combine(str(tmp_path / "*.parquet"), str(tmp_path / "out.parquet"), ["Group", "Message ID"])


def test_combine_custom_dedup_cols_without_message_id(tmp_path):
    pd.DataFrame(
        {"Url": ["a", "b", "a"], "Date": ["2024-01-01", "2024-01-02", "2024-01-01"],
         "Comments List": [None, None, None]}
    ).to_parquet(tmp_path / "f.parquet")
    out = tmp_path / "out.parquet"
    combine(str(tmp_path / "*.parquet"), str(out), ["Url"])
    assert len(read_table(out)) == 2


def test_scrape_rejects_negative_timeout():
    argv = ["scrape", "--channels", "@x", "--date-min", "2024-01-01",
            "--date-max", "2024-01-02", "--name", "t", "--timeout", "-1"]
    with pytest.raises(SystemExit):
        build_parser().parse_args(argv)


def test_tme_link_extraction_and_normalisation():
    text = "join https://t.me/foo/123 and https://t.me/bar?x=1"
    links = _TME_RE.findall(text)
    assert len(links) == 2
    assert _TME_BASE_RE.match(links[0]).group(1) == "https://t.me/foo"
