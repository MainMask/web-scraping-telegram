# TelegramScrap — terminal edition

A command-line tool for scraping and analysing data from **Telegram channels, groups and chats**
using the [Telethon](https://docs.telethon.dev/) library. It extracts message content, author
information, reactions, views, shares and comments, and stores the result as **Apache Parquet**
(`.parquet`) or **Excel** (`.xlsx`).

This is a full rewrite of the original Jupyter/Colab notebook so it runs from a normal terminal:
credentials come from a `.env` file, run parameters are command-line flags, and the five former
analysis scripts are now sub-commands of a single `telegramscrap` CLI.

> Original notebook and academic project by **Ergon Cugler de Moraes Silva** —
> <https://github.com/ergoncugler/web-scraping-telegram/>. See *Citation* below.

---

## Install

Requires Python 3.11+.

```bash
python -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate
pip install -e .                   # or: pip install -r requirements.txt
```

## Configure credentials (once)

Generate `api_id` and `api_hash` at <https://my.telegram.org/apps>, then:

```bash
cp .env.example .env
# edit .env and set TG_API_ID, TG_API_HASH (TG_PHONE and TG_PASSWORD are optional)
```

Then authorise once:

```bash
telegramscrap login
```

Telethon asks for the login code (and 2FA password) in the terminal and saves the session to
`./telegramscrap.session`, so every later command runs non-interactively. (`scrape` also
triggers this login on its first run if you skip `login`.) Keep that file private; deleting it
just means logging in again.

---

## Usage

```
telegramscrap <command> [options]        # or:  python -m telegramscrap <command>
```

| Command   | What it does |
|-----------|--------------|
| `menu`    | interactive wizard that asks questions, then builds and runs a command |
| `login`   | authorise once (asks for the Telegram code), save the session |
| `scrape`  | scrape channels/groups into `.parquet` or `.xlsx` |
| `read`    | print the head of a data file, optionally convert it |
| `combine` | merge many `.parquet` files, drop duplicates, recount comments |
| `comments`| flatten `Comments List` into a table with one row per comment |
| `summary` | per-group monthly tables (contents / comments / total) |
| `sample`  | proportional per-category sample to `.xlsx` |
| `filter`  | keep rows matching keywords, add one 0/1 column per keyword |
| `links`   | extract and count `t.me` links from `Content` (snowball sampling) |

Run `telegramscrap <command> --help` for the full flag list.

### Interactive menu

Not sure which flags you need? Run `telegramscrap` with no arguments (or
`telegramscrap menu`): it walks you through the options, prints the equivalent
`telegramscrap …` command, and runs it. Every flag below still works directly.

### Scrape

```bash
telegramscrap scrape \
  --channels "@LulanoTelegram, @jairbolsonarobrasil" \
  --date-min 2024-10-15 --date-max 2025-01-15 \
  --name Test \
  --out-dir output
```

A channel may be given as `@name`, `t.me/name` or a full `https://t.me/name` URL — all are
reduced to `name`, and the `Group` column is stored normalised as `@name`.

It may also be a **numeric ID** such as `-1001629147115` (the form Telegram clients and
`t.me/c/1629147115/…` links use) — handy for private channels that have no username. The
logged-in account must already be a member of that channel (or have it in its dialogs) for
the ID to resolve. For an ID-only channel the `Group` column and file names use
`@c<short_id>` and links are `https://t.me/c/<short_id>/…`.

Useful flags: `--channels-file channels.txt` (comma- or newline-separated), `--keyword <term>`,
`--max-messages <n>`, `--timeout <seconds>` (`0` = no limit), `--no-comments` (skip the
per-post comment fetch), `--collect-reactors` (see below), `--session <name>`,
`--format {parquet,excel}`.

`--collect-reactors` also writes a separate `FINAL_<name>_reactors_with_<n>` file with one
row per *(user, message, reaction)*: who (`id` + `username`) put which emoji, and on what.
It is **opt-in and slow** — one extra API call per message that has reactions. Telegram
refuses the reactors list for **broadcast-channel posts** (to prevent de-anonymisation), so
those are skipped with a note; in practice this flag captures the reactions left on the
**comments**, and therefore needs the comment fetch enabled (not `--no-comments`).

On a busy channel this is easily thousands of requests; Telegram may answer with
`FloodWaitError` (logged and skipped) or, worst case, a temporary soft ban. Keep runs
small with `--max-messages` / a narrow date range.

Output is **parquet** by default. Use `--format excel` only for small runs — Excel truncates
any cell over 32,767 characters (the `Comments List` of a busy post easily exceeds that);
`telegramscrap read <file> --to excel` converts a parquet afterwards.

Comments are fetched only for posts that actually have a linked discussion thread.

Dates may be written as `DD.MM.YYYY` (e.g. `15.10.2024`) or ISO `YYYY-MM-DD`. Both
`--date-min` and `--date-max` are **inclusive**; `--date-max` covers the whole day (UTC).

Output files land in `--out-dir` (`<slug>` is the channel name without `@`, or
`c<short_id>` for a numeric-ID channel):

- `backup_<name>_until_<n>_<slug>_ID<id>.<ext>` — written every 1,000 messages
- `complete_<slug>_in_<name>_until_<n>.<ext>` — after each channel finishes
- `FINAL_<name>_with_<n>.<ext>` — the full run
- `FINAL_<name>_reactors_with_<n>.<ext>` — only with `--collect-reactors`; one row per
  *(user, message, reaction)*, columns:
  `Type, Target, Group, Message ID, Post ID, Url, Reactor ID, Reactor Username, Reaction, Date`

### Analyse

```bash
telegramscrap combine  --input 'output/*.parquet' --output output/unified.parquet
telegramscrap comments --input output/unified.parquet --output output/comments.parquet
telegramscrap summary  --input output/unified.parquet --output-base output/resume
telegramscrap filter   --input output/unified.parquet --output output/kw --keywords "Trump,Biden,Kamala"
telegramscrap sample   --input output/unified.parquet --output output/sample.xlsx --sample-size 10000
telegramscrap links    --input output/unified.parquet --output output/links.xlsx
telegramscrap read     output/unified.parquet --head 20 --to xlsx
```

The `Comments List` column holds comments as a JSON string — `telegramscrap comments`
explodes it into a flat table (one row per comment, with `Comment Author ID` /
`Comment Author Username`), `--format excel` for a spreadsheet.

---

## Output columns

`Type, Group, Author ID, Content, Date, Message ID, Author, Views, Reactions, Shares, Media, Url, Comments List`

Each entry inside the `Comments List` JSON carries `Comment Author ID` **and**
`Comment Author Username` (`""` if the commenter has none, `[channel]` / `[anonymous]`
when the reply was sent by a channel or anonymously).

## Notes

- **Telegram soft ban:** scraping more than ~150–200 communities in one block can trigger a
  24-hour soft ban. There is no practical limit on the number of messages from fewer communities.
- Parquet handles very large datasets and long text far better than spreadsheets; `.xlsx` is
  capped at 1,048,576 rows and ~32k characters per cell.
- Respect Telegram's Terms of Service and applicable data-protection law. Responsibility for use
  lies with the user.

## Tests

```bash
pip install pytest
pytest -q
```

The tests are offline (no Telegram, no credentials) and cover the pure helpers.

---

## Project layout

```
telegramscrap/
  cli.py         argparse entry point + sub-command dispatch
  menu.py        interactive input()-based wizard over the CLI flags
  config.py      load TG_* credentials from .env
  scrape.py      async scraper (asyncio.run)
  analysis.py    combine / comments / summary / sample / filter / links
  datafiles.py   read/write parquet·xlsx·csv, text cleaning
tests/           offline tests for the helpers
```

## Citation

If you use this tool in research, please cite the original work:

> SILVA, Ergon Cugler de Moraes. *TelegramScrap: A comprehensive tool for scraping Telegram data*.
> (Feb) 2023. Available at: <https://doi.org/10.48550/arXiv.2412.16786>.

The original code has supported peer-reviewed studies and technical notes on disinformation,
conspiracy communities and political discourse; see the upstream repository for the full list.

## License

Free and open-source. Provide appropriate credit when using or modifying.
