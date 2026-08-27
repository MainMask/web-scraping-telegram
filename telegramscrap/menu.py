"""Interactive input()-based wizard that builds and runs a `telegramscrap` command."""

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
    _opt(argv, "--timeout", p.text("Timeout seconds (0 = no limit)", "21600"), "21600")
    _opt(argv, "--out-dir", p.text("Output directory", "output"), "output")
    argv += ["--format", p.choice("Format", ["parquet", "excel"], "parquet")]
    if not p.yes_no("Fetch comments (commenter id + username)?", True):
        argv.append("--no-comments")
    if p.yes_no("Collect reactors (id + username + emoji; slow)?", False):
        argv.append("--collect-reactors")
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
    argv = ["comments", "--input", _ask_data_file(p, "Scraped posts file")]
    argv += ["--output", p.text("Output path", required=True)]
    fmt = p.choice("Format", ["parquet", "excel"], "parquet")
    if fmt != "parquet":
        argv += ["--format", fmt]
    return argv


def _participants_argv(p: Prompt) -> list[str]:
    argv = ["participants", "--input", _ask_data_file(p, "Scraped posts file")]
    argv += ["--output", p.text("Output path", required=True)]
    fmt = p.choice("Format", ["parquet", "excel"], "parquet")
    if fmt != "parquet":
        argv += ["--format", fmt]
    return argv


def _login_argv(p: Prompt) -> list[str]:
    return ["login"]


# key -> (label, one-line help, argv builder)
_ACTIONS = {
    "1": ("scrape", "collect posts / comments / reactors from channels", _scrape_argv),
    "2": ("read", "preview a data file, optionally convert it", _read_argv),
    "3": ("combine", "merge .parquet files, drop duplicates", _combine_argv),
    "4": ("comments", "flatten Comments List into one row per comment", _comments_argv),
    "5": ("participants", "ID + username + name of commenters & reactors", _participants_argv),
    "6": ("summary", "per-group monthly tables", _summary_argv),
    "7": ("sample", "proportional per-category sample to .xlsx", _sample_argv),
    "8": ("filter", "keep rows matching keywords", _filter_argv),
    "9": ("links", "extract and count t.me links", _links_argv),
    "10": ("login", "(re)authorise and save the session", _login_argv),
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
        except SystemExit as exc:  # argparse errors / raise SystemExit in analysis
            if exc.code not in (0, None):
                print(f"  ! {exc}")
        except Exception as exc:  # keep the menu alive on any command failure
            print(f"  ! {type(exc).__name__}: {exc}")
