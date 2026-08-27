"""Command-line entry point: `telegramscrap <command>`."""

import argparse
import re
from pathlib import Path

from telegramscrap import __version__


def _positive_int(value: str) -> int:
    n = int(value)
    if n <= 0:
        raise argparse.ArgumentTypeError("must be a positive integer")
    return n


def _non_negative_int(value: str) -> int:
    n = int(value)
    if n < 0:
        raise argparse.ArgumentTypeError("must be zero or a positive integer")
    return n


def _read_channels(args) -> list[str]:
    if args.channels_file:
        raw = Path(args.channels_file).read_text(encoding="utf-8")
    else:
        raw = args.channels
    channels = [c.strip() for c in re.split(r"[,\s]+", raw) if c.strip()]
    if not channels:
        raise SystemExit("No channels given (use --channels or --channels-file).")
    return channels


def cmd_scrape(args) -> None:
    from telegramscrap.config import load_credentials
    from telegramscrap.scrape import ScrapeParams, parse_date, run

    params = ScrapeParams(
        channels=_read_channels(args),
        date_min=parse_date(args.date_min),
        date_max=parse_date(args.date_max, end_of_day=True),
        name=args.name,
        keyword=args.keyword,
        max_messages=args.max_messages,
        timeout=args.timeout,
        fmt=args.format,
        out_dir=Path(args.out_dir),
        session=args.session,
        with_comments=not args.no_comments,
    )
    run(load_credentials(), params)


def cmd_login(args) -> None:
    from telegramscrap.config import load_credentials
    from telegramscrap.login import login

    login(load_credentials(), args.session)


def cmd_read(args) -> None:
    from telegramscrap.datafiles import read_table, save_table

    df = read_table(args.input)
    print(df.head(args.head).to_string())
    print(f"[{len(df)} rows x {len(df.columns)} columns]")
    if args.to:
        out = save_table(df, Path(args.input).with_suffix(""), args.to)
        print(f"Converted: {out}")


def cmd_combine(args) -> None:
    from telegramscrap.analysis import combine

    combine(args.input, args.output, [c.strip() for c in args.dedup_cols.split(",")])


def cmd_summary(args) -> None:
    from telegramscrap.analysis import summary

    summary(args.input, args.output_base, args.date_col, args.group_col, args.comments_col)


def cmd_sample(args) -> None:
    from telegramscrap.analysis import sample

    sample(args.input, args.output, args.text_col, args.category_col, args.sample_size, args.min_length)


def cmd_filter(args) -> None:
    from telegramscrap.analysis import filter_keywords

    filter_keywords(
        args.input, args.output, args.content_col,
        [k.strip() for k in args.keywords.split(",") if k.strip()], args.max_rows_per_file,
    )


def cmd_links(args) -> None:
    from telegramscrap.analysis import links

    links(args.input, args.output)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="telegramscrap", description=__doc__)
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    sub = parser.add_subparsers(dest="command", required=True)

    lg = sub.add_parser("login", help="authorise once (asks for the Telegram code) and save the session")
    lg.add_argument("--session", default="telegramscrap",
                    help="session name/path (default: ./telegramscrap.session)")
    lg.set_defaults(func=cmd_login)

    s = sub.add_parser("scrape", help="scrape channels/groups into parquet or xlsx")
    g = s.add_mutually_exclusive_group(required=True)
    g.add_argument("--channels", help="comma-separated list, e.g. '@a, @b'")
    g.add_argument("--channels-file", help="text file with channels (comma- or newline-separated)")
    s.add_argument("--date-min", required=True, help="ISO date, inclusive (YYYY-MM-DD)")
    s.add_argument("--date-max", required=True, help="ISO date, inclusive (YYYY-MM-DD)")
    s.add_argument("--name", required=True, help="base name for output files")
    s.add_argument("--keyword", default="", help="only messages containing this term")
    s.add_argument("--max-messages", type=_positive_int, default=1_000_000)
    s.add_argument("--timeout", type=_non_negative_int, default=21_600, help="seconds; 0 = no limit")
    s.add_argument("--format", choices=["excel", "parquet"], default="parquet",
                   help="parquet (default, lossless) or excel (capped at 32k chars/cell)")
    s.add_argument("--out-dir", default="output")
    s.add_argument("--session", default="telegramscrap",
                   help="session name/path (default: ./telegramscrap.session)")
    s.add_argument("--no-comments", action="store_true", help="skip fetching per-message comments (much faster)")
    s.set_defaults(func=cmd_scrape)

    r = sub.add_parser("read", help="print the head of a data file and optionally convert it")
    r.add_argument("input")
    r.add_argument("--to", choices=["excel", "xlsx", "parquet", "csv"], help="also write a converted copy")
    r.add_argument("--head", type=int, default=10)
    r.set_defaults(func=cmd_read)

    c = sub.add_parser("combine", help="merge parquet files, drop duplicates, recount comments")
    c.add_argument("--input", required=True, help="file, directory, or glob of .parquet files")
    c.add_argument("--output", required=True)
    c.add_argument("--dedup-cols", default="Group,Message ID")
    c.set_defaults(func=cmd_combine)

    m = sub.add_parser("summary", help="per-group monthly summary tables (contents/comments/total)")
    m.add_argument("--input", required=True)
    m.add_argument("--output-base", required=True, help="path prefix; _contents.xlsx etc. are appended")
    m.add_argument("--date-col", default="Date")
    m.add_argument("--group-col", default="Group")
    m.add_argument("--comments-col", default="Comments")
    m.set_defaults(func=cmd_summary)

    p = sub.add_parser("sample", help="proportional per-category sample to xlsx")
    p.add_argument("--input", required=True)
    p.add_argument("--output", required=True)
    p.add_argument("--text-col", default="Content")
    p.add_argument("--category-col", default="Group")
    p.add_argument("--sample-size", type=int, default=10_000)
    p.add_argument("--min-length", type=int, default=20)
    p.set_defaults(func=cmd_sample)

    f = sub.add_parser("filter", help="filter rows by keywords, add one indicator column per keyword")
    f.add_argument("--input", required=True)
    f.add_argument("--output", required=True, help="base name; _unique.xlsx / _part_N.xlsx appended")
    f.add_argument("--content-col", default="Content")
    f.add_argument("--keywords", required=True, help="comma-separated")
    f.add_argument("--max-rows-per-file", type=int, default=1_000_000)
    f.set_defaults(func=cmd_filter)

    lk = sub.add_parser("links", help="extract and count t.me links from Content")
    lk.add_argument("--input", required=True)
    lk.add_argument("--output", required=True)
    lk.set_defaults(func=cmd_links)

    return parser


def main(argv: list[str] | None = None) -> None:
    args = build_parser().parse_args(argv)
    args.func(args)


if __name__ == "__main__":
    main()
