"""Interactive input()-based wizard that builds and runs a `telegramscrap` command."""

import re
import shlex
from pathlib import Path

SEP = "-" * 60


class Prompt:
    """Thin wrapper over an input function so the menu can be tested offline."""

    def __init__(self, input_fn=input):
        self._input = input_fn

    def text(self, msg: str, default: str = "", *, required: bool = False) -> str:
        while True:
            raw = self._input(f"{msg}{f' [{default}]' if default else ''}: ").strip()
            value = raw or default
            if value or not required:
                return value
            print("  ! required")

    def yes_no(self, msg: str, default: bool) -> bool:
        raw = self._input(f"{msg} [{'Y/n' if default else 'y/N'}]: ").strip().lower()
        return default if not raw else raw.startswith(("y", "д"))

    def choice(self, msg: str, options: list[str], default: str) -> str:
        print(f"{msg}:")
        for i, opt in enumerate(options, 1):
            print(f"  {i}) {opt}{'  (default)' if opt == default else ''}")
        raw = self._input("Choose: ").strip()
        if not raw:
            return default
        if raw.isdigit() and 1 <= int(raw) <= len(options):
            return options[int(raw) - 1]
        return raw if raw in options else default


def _opt(argv: list[str], flag: str, value: str, default: str) -> None:
    """Append `flag value` only when it differs from the CLI default."""
    if value and value != default:
        argv += [flag, value]


_DATA_EXT = (".parquet", ".xlsx", ".csv")


def _data_files() -> list[Path]:
    """Data files in ./output and the current directory, newest first (max 40)."""
    found: dict = {}
    for d in (Path("output"), Path(".")):
        if d.is_dir():
            for f in d.iterdir():
                if f.is_file() and f.suffix.lower() in _DATA_EXT:
                    found[f.resolve()] = f
    return sorted(found.values(), key=lambda f: f.stat().st_mtime, reverse=True)[:40]


def _ask_data_file(p: Prompt, msg: str) -> str:
    """Prompt for an input file, offering a numbered list of found data files."""
    files = _data_files()
    if not files:
        return p.text(msg, required=True)
    print(f"\n{msg}:")
    for i, f in enumerate(files, 1):
        print(f"  {i:>2}) {f}")
    raw = p.text("Pick a number or type a path", required=True)
    if raw.isdigit() and 1 <= int(raw) <= len(files):
        return str(files[int(raw) - 1])
    return raw


def _scrape_argv(p: Prompt) -> list[str]:
    argv = ["scrape"]
    channels = p.text("Channels: @name / numeric id (-100...), comma-separated, or path to a .txt file",
                      required=True)
    argv += ["--channels-file", channels] if Path(channels).is_file() else ["--channels", channels]
    argv += ["--date-min", p.text("Date from (DD.MM.YYYY)", required=True)]
    argv += ["--date-max", p.text("Date to (DD.MM.YYYY)", required=True)]
    argv += ["--name", p.text("Output file base name", required=True)]
    _opt(argv, "--keyword", p.text("Keyword filter", ""), "")
    _opt(argv, "--max-messages", p.text("Max messages", "1000000"), "1000000")
    _opt(argv, "--timeout", p.text("Timeout seconds (0 = no limit)", "0"), "0")
    _opt(argv, "--out-dir", p.text("Output directory", "output"), "output")
    argv += ["--format", p.choice("Format", ["parquet", "excel"], "parquet")]
    if not p.yes_no("Fetch comments (commenter id + username + name)?", True):
        argv.append("--no-comments")
    if not p.yes_no("Collect reactors (id + username; slow, one API call per reacted message)?", True):
        argv.append("--no-reactors")
    if not p.yes_no("Also build the participants table (id + username + name)?", True):
        argv.append("--no-participants")
    if p.yes_no("Resume an interrupted run with this name?", False):
        argv.append("--resume")
    return argv


def _read_argv(p: Prompt) -> list[str]:
    argv = ["read", _ask_data_file(p, "Data file to preview")]
    _opt(argv, "--head", p.text("Rows to show", "10"), "10")
    convert = p.choice("Also convert to", ["(none)", "excel", "parquet", "csv"], "(none)")
    if convert != "(none)":
        argv += ["--to", convert]
    return argv


def _combine_argv(p: Prompt) -> list[str]:
    argv = ["combine"]
    argv += ["--input", p.text("Input file, directory or glob of .parquet", required=True)]
    argv += ["--output", p.text("Output .parquet path", required=True)]
    _opt(argv, "--dedup-cols", p.text("Dedup columns", "Group,Message ID"), "Group,Message ID")
    return argv


def _summary_argv(p: Prompt) -> list[str]:
    argv = ["summary"]
    argv += ["--input", _ask_data_file(p, "Input data file")]
    argv += ["--output-base", p.text("Output path prefix (_contents.xlsx etc. appended)", required=True)]
    return argv


def _sample_argv(p: Prompt) -> list[str]:
    argv = ["sample"]
    argv += ["--input", _ask_data_file(p, "Input data file")]
    argv += ["--output", p.text("Output .xlsx path", required=True)]
    _opt(argv, "--sample-size", p.text("Sample size", "10000"), "10000")
    return argv


def _filter_argv(p: Prompt) -> list[str]:
    argv = ["filter"]
    argv += ["--input", _ask_data_file(p, "Input data file")]
    argv += ["--output", p.text("Output base name (_unique.xlsx / _part_N.xlsx appended)", required=True)]
    argv += ["--keywords", p.text("Keywords (comma-separated)", required=True)]
    return argv


def _links_argv(p: Prompt) -> list[str]:
    argv = ["links"]
    argv += ["--input", _ask_data_file(p, "Input data file")]
    argv += ["--output", p.text("Output .xlsx path", required=True)]
    return argv


def _comments_argv(p: Prompt) -> list[str]:
    argv = ["comments", "--input", _ask_data_file(p, "Scraped posts file (the *_posts file, not *_reactors)")]
    argv += ["--output", p.text("Output path for the new file (e.g. output/Baza_comments)", required=True)]
    fmt = p.choice("Format", ["parquet", "excel"], "parquet")
    if fmt != "parquet":
        argv += ["--format", fmt]
    return argv


def _participants_argv(p: Prompt) -> list[str]:
    argv = ["participants", "--input",
            _ask_data_file(p, "Scraped posts file (the *_posts file, not *_reactors — "
                              "the reactors file next to it is picked up automatically)")]
    argv += ["--output", p.text("Output path for the new file (e.g. output/Baza_participants)", required=True)]
    fmt = p.choice("Format", ["parquet", "excel"], "parquet")
    if fmt != "parquet":
        argv += ["--format", fmt]
    return argv


def _guess_verify_defaults(path: str) -> dict:
    """Best-effort channel + date bounds from a scraped posts file, so the menu can
    pre-fill `verify`'s arguments instead of making the user retype them."""
    from telegramscrap.datafiles import read_table
    import pandas as pd

    out: dict = {}
    p = Path(path)
    try:
        try:
            df = pd.read_parquet(p, columns=["Group", "Date"])
        except Exception:
            df = read_table(p)
        groups = df["Group"].dropna() if "Group" in df.columns else []
        if len(groups):
            g = str(groups.iloc[0]).lstrip("@")
            out["channel"] = (f"-100{g[1:]}" if g[:1] == "c" and g[1:].isdigit()
                              else f"@{g}")
        dates = pd.to_datetime(df["Date"], errors="coerce").dropna() if "Date" in df.columns else []
        if len(dates):
            out["date_min"] = dates.min().strftime("%d.%m.%Y")
            out["date_max"] = dates.max().strftime("%d.%m.%Y")
        stem = re.sub(r"(_posts)?\.(parquet|xlsx|csv)$", "", p.name)
        out["output"] = str(p.with_name(f"{stem}_missed.parquet"))
    except Exception:
        return {}
    return out


def _verify_argv(p: Prompt) -> list[str]:
    src = _ask_data_file(p, "Scraped posts file to verify (the *_posts file)")
    g = _guess_verify_defaults(src)
    argv = ["verify", "--input", src,
            "--channel", p.text("Channel (@name or numeric id) — a guess from the file",
                                g.get("channel", ""), required=True),
            "--date-min", p.text("Date from (DD.MM.YYYY) — the scrape's --date-min",
                                 g.get("date_min", ""), required=True),
            "--date-max", p.text("Date to (DD.MM.YYYY) — the scrape's --date-max",
                                 g.get("date_max", ""), required=True)]
    out = p.text("Write missed ids to (blank = skip)", g.get("output", ""))
    if out:
        argv += ["--output", out]
    _opt(argv, "--comment-sample", p.text("Also re-check N random comment threads (0 = skip)", "0"), "0")
    return argv


def _login_argv(p: Prompt) -> list[str]:
    return ["login"]


# key -> (label, one-line help, argv builder)
_ACTIONS = {
    "1": ("scrape", "channels -> posts + participants + reactors in one run", _scrape_argv),
    "2": ("read", "preview a data file, optionally convert it", _read_argv),
    "3": ("combine", "merge .parquet files, drop duplicates", _combine_argv),
    "4": ("comments", "flatten Comments List into one row per comment", _comments_argv),
    "5": ("participants", "ID + username + name of commenters & reactors", _participants_argv),
    "6": ("summary", "per-group monthly tables", _summary_argv),
    "7": ("sample", "proportional per-category sample to .xlsx", _sample_argv),
    "8": ("filter", "keep rows matching keywords", _filter_argv),
    "9": ("links", "extract and count t.me links", _links_argv),
    "10": ("verify", "probe the channel for posts the scrape missed", _verify_argv),
    "11": ("login", "(re)authorise and save the session", _login_argv),
}


def _dispatch(argv: list[str]) -> None:
    from telegramscrap import cli

    cli.main(argv)


def _print_menu() -> None:
    print(f"\n{SEP}\n telegramscrap\n{SEP}")
    for key, (label, description, _) in _ACTIONS.items():
        print(f"  {key}) {label:<8} - {description}")
    print("  0) quit")


def run_menu(prompt: Prompt | None = None, dispatch=None) -> None:
    prompt = prompt or Prompt()
    dispatch = dispatch or _dispatch

    while True:
        _print_menu()
        try:
            choice = prompt.text("Choose", "1")
            if choice in ("0", "q", "quit", ""):
                return
            entry = _ACTIONS.get(choice)
            if entry is None:
                print("  ! unknown option")
                continue
            argv = entry[2](prompt)
            print("\n  telegramscrap " + " ".join(shlex.quote(a) for a in argv) + "\n")
            run = prompt.yes_no("Run now?", True)
        except (EOFError, KeyboardInterrupt):
            print()
            return

        if not run:
            continue
        try:
            dispatch(argv)
        except SystemExit as exc:  # our own raise SystemExit("msg"); argparse prints its own
            if isinstance(exc.code, str):
                print(f"  ! {exc.code}")
        except Exception as exc:  # keep the menu alive on any command failure
            print(f"  ! {type(exc).__name__}: {exc}")
