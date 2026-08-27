"""Offline tests for the interactive menu (no real input(), no Telegram)."""

import pytest

from telegramscrap.cli import build_parser, main
from telegramscrap.menu import Prompt, run_menu


def _prompt(answers):
    it = iter(answers)
    return Prompt(input_fn=lambda _msg: next(it))


def _valid(argv):
    """The menu must only ever emit argv that argparse accepts."""
    build_parser().parse_args(argv)


def test_scrape_argv_minimal():
    from telegramscrap.menu import _scrape_argv

    argv = _scrape_argv(_prompt(["@durov", "2025-08-01", "2025-08-07", "test",
                                 "", "", "", "", "", "", "", ""]))
    assert argv == ["scrape", "--channels", "@durov", "--date-min", "2025-08-01",
                    "--date-max", "2025-08-07", "--name", "test", "--format", "parquet"]
    _valid(argv)


def test_scrape_argv_extras_disabled():
    from telegramscrap.menu import _scrape_argv

    argv = _scrape_argv(_prompt(["@a", "2025-01-01", "2025-01-02", "t",
                                 "", "", "", "", "", "n", "n", "n"]))
    assert "--no-comments" in argv
    assert "--no-reactors" in argv
    assert "--no-participants" in argv
    _valid(argv)


def test_scrape_argv_non_default_options():
    from telegramscrap.menu import _scrape_argv

    argv = _scrape_argv(_prompt(["@a", "2025-01-01", "2025-01-02", "t",
                                 "trump", "50", "0", "data", "excel", "", "", ""]))
    assert argv[argv.index("--keyword") + 1] == "trump"
    assert argv[argv.index("--max-messages") + 1] == "50"
    assert argv[argv.index("--timeout") + 1] == "0"
    assert argv[argv.index("--out-dir") + 1] == "data"
    assert argv[argv.index("--format") + 1] == "excel"
    _valid(argv)


def test_scrape_argv_numeric_id():
    from telegramscrap.menu import _scrape_argv

    argv = _scrape_argv(_prompt(["-1001629147115", "2025-01-01", "2025-01-02", "t",
                                 "", "", "", "", "", "", "", ""]))
    assert argv[:3] == ["scrape", "--channels", "-1001629147115"]
    _valid(argv)  # argparse must accept the negative-number value


def test_scrape_argv_channels_file(tmp_path):
    from telegramscrap.menu import _scrape_argv

    f = tmp_path / "chans.txt"
    f.write_text("@a\n@b\n", encoding="utf-8")
    argv = _scrape_argv(_prompt([str(f), "2025-01-01", "2025-01-02", "t",
                                 "", "", "", "", "", "", "", ""]))
    assert argv[:3] == ["scrape", "--channels-file", str(f)]
    _valid(argv)


def test_read_argv_with_conversion(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)  # no data files -> plain text prompt
    from telegramscrap.menu import _read_argv

    argv = _read_argv(_prompt(["output/f.parquet", "20", "2"]))  # "2" -> excel
    assert argv == ["read", "output/f.parquet", "--head", "20", "--to", "excel"]
    _valid(argv)


def test_read_argv_defaults(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    from telegramscrap.menu import _read_argv

    argv = _read_argv(_prompt(["output/f.parquet", "", ""]))
    assert argv == ["read", "output/f.parquet"]
    _valid(argv)


def test_read_argv_picks_file_by_number(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    (tmp_path / "output").mkdir()
    (tmp_path / "output" / "FINAL_a.parquet").write_bytes(b"x")
    from telegramscrap.menu import _read_argv

    argv = _read_argv(_prompt(["1", "", ""]))
    assert argv[1] == "output/FINAL_a.parquet"
    assert argv[0] == "read"


def test_read_argv_typed_path_when_files_listed(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    (tmp_path / "output").mkdir()
    (tmp_path / "output" / "FINAL_a.parquet").write_bytes(b"x")
    from telegramscrap.menu import _read_argv

    argv = _read_argv(_prompt(["some/other.parquet", "", ""]))
    assert argv[1] == "some/other.parquet"


def test_comments_argv(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    (tmp_path / "output").mkdir()
    (tmp_path / "output" / "FINAL_a.parquet").write_bytes(b"x")
    from telegramscrap.menu import _comments_argv

    argv = _comments_argv(_prompt(["1", "output/a_comments.parquet", ""]))
    assert argv == ["comments", "--input", "output/FINAL_a.parquet",
                    "--output", "output/a_comments.parquet"]
    _valid(argv)


def test_participants_argv(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    (tmp_path / "output").mkdir()
    (tmp_path / "output" / "FINAL_a.parquet").write_bytes(b"x")
    from telegramscrap.menu import _participants_argv

    argv = _participants_argv(_prompt(["1", "output/people.xlsx", "2"]))  # "2" -> excel
    assert argv == ["participants", "--input", "output/FINAL_a.parquet",
                    "--output", "output/people.xlsx", "--format", "excel"]
    _valid(argv)


def test_menu_runs_selected_command_then_quits():
    calls = []
    answers = ["1", "@a", "2025-01-01", "2025-01-02", "t",
               "", "", "", "", "", "", "", "", "", "0"]
    run_menu(_prompt(answers), dispatch=calls.append)
    assert calls == [["scrape", "--channels", "@a", "--date-min", "2025-01-01",
                      "--date-max", "2025-01-02", "--name", "t", "--format", "parquet"]]


def test_menu_survives_command_error(capsys):
    def boom(argv):
        raise SystemExit("boom")

    run_menu(_prompt(["2", "some/file.parquet", "", "", "", "0"]), dispatch=boom)  # must not raise
    assert "  ! boom" in capsys.readouterr().out


def test_menu_hides_numeric_exit_code(capsys):
    def argparse_style(argv):
        raise SystemExit(2)  # argparse already printed its own message

    run_menu(_prompt(["2", "some/file.parquet", "", "", "", "0"]), dispatch=argparse_style)
    assert "! 2" not in capsys.readouterr().out


def test_menu_quit_immediately():
    calls = []
    run_menu(_prompt(["0"]), dispatch=calls.append)
    assert calls == []


def test_menu_unknown_option_reprompts():
    calls = []
    run_menu(_prompt(["99", "0"]), dispatch=calls.append)
    assert calls == []


@pytest.mark.parametrize("argv", [[], ["menu"]])
def test_cli_opens_menu(monkeypatch, argv):
    called = []
    monkeypatch.setattr("telegramscrap.menu.run_menu", lambda: called.append(argv))
    main(argv)
    assert called == [argv]


def test_cli_scrape_still_requires_flags():
    with pytest.raises(SystemExit):
        build_parser().parse_args(["scrape"])
