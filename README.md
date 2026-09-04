# telegram-scraper

A command-line tool for scraping and analysing data from **Telegram channels, groups and chats**
using the [Telethon](https://docs.telethon.dev/) library. It extracts message content, author
information, reactions, views, shares and comments, and stores the result as **Apache Parquet**
(`.parquet`) or **Excel** (`.xlsx`).

This is a full rewrite of the original Jupyter/Colab notebook so it runs from a normal terminal:
credentials come from a `.env` file, run parameters are command-line flags, and the five former
analysis scripts are now sub-commands of a single `telegram-scraper` CLI.

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

## Docker

Deploy on a server without a local Python setup. The Telethon session and every scraped
file live in `./data/` on the host.

```bash
cp .env.example .env        # set TG_API_ID, TG_API_HASH
docker compose build

# authorise once (asks for the Telegram code) — session saved to ./data/
docker compose run --rm telegram-scraper login

# scrape (writes to ./data/output/)
docker compose run --rm telegram-scraper scrape \
  --channels '@channel' --date-min 01.01.2024 --date-max 31.01.2024 --name Test

# interactive menu
docker compose run --rm telegram-scraper menu

# analysis commands work the same (paths are relative to /data)
# the _<from>-<to> suffix is the span of posts actually collected — check output/ for the real name
docker compose run --rm telegram-scraper read output/Test_posts_02.01.2024-30.01.2024.parquet --head 20
```

- `./data/` holds the session and all output — back it up, keep it private.
- Stopping a scrape: `Ctrl-C` during `docker compose run` sends SIGINT and the run writes
  a resume checkpoint; re-run the same command with `--resume`. `docker stop` (SIGTERM)
  keeps only the periodic in-run checkpoints.
- Without compose:
  `docker build -t telegram-scraper . && docker run --rm -it --init --env-file .env -v "$PWD/data:/data" telegram-scraper login`

## Configure credentials (once)

Generate `api_id` and `api_hash` at <https://my.telegram.org/apps>, then:

```bash
cp .env.example .env
# edit .env and set TG_API_ID, TG_API_HASH (TG_PHONE and TG_PASSWORD are optional)
```

Then authorise once:

```bash
telegram-scraper login
```

Telethon asks for the login code (and 2FA password) in the terminal and saves the session to
`./telegram-scraper.session`, so every later command runs non-interactively. (`scrape` also
triggers this login on its first run if you skip `login`.) Keep that file private; deleting it
just means logging in again.

> **Upgrading from 2.0** — the command is now `telegram-scraper` (was `telegramscrap`), so
> `python -m telegram_scraper` replaces `python -m telegramscrap` and you need to reinstall
> (`pip install .` / `docker compose build`) to refresh the entry point. The default session
> file is now `telegram-scraper.session`; rename the old `telegramscrap.session` (or run
> `login` again). Scrape outputs now carry a `_<from>-<to>` date span in their names.

---

## Usage

```
telegram-scraper <command> [options]        # or:  python -m telegram_scraper <command>
```

| Command   | What it does |
|-----------|--------------|
| `menu`    | interactive wizard that asks questions, then builds and runs a command |
| `login`   | authorise once (asks for the Telegram code), save the session |
| `scrape`  | scrape channels/groups into `.parquet` or `.xlsx` |
| `read`    | print the head of a data file, optionally convert it |
| `combine` | merge many `.parquet` files, drop duplicates, recount comments |
| `comments`| flatten `Comments List` into a table with one row per comment |
| `participants`| unique `ID` + `username` + `name` of everyone who commented or reacted |
| `summary` | per-group monthly tables (contents / comments / total) |
| `sample`  | proportional per-category sample to `.xlsx` |
| `filter`  | keep rows matching keywords, add one 0/1 column per keyword |
| `links`   | extract and count `t.me` links from `Content` (snowball sampling) |
| `verify`  | probe the live channel for posts a scrape missed (id-gap + bounds check) |

Run `telegram-scraper <command> --help` for the full flag list.

### Interactive menu

Not sure which flags you need? Run `telegram-scraper` with no arguments (or
`telegram-scraper menu`): it walks you through the options, prints the equivalent
`telegram-scraper …` command, and runs it. Every flag below still works directly.

### Scrape

```bash
telegram-scraper scrape \
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
`--max-messages <n>`, `--timeout <seconds>` (stop after N seconds; `0`, the default, = no limit), `--no-comments` (skip the
per-post comment fetch), `--no-reactors` (see below), `--no-participants` (skip the
`<name>_participants` table), `--session <name>`, `--format {parquet,excel}`,
`--resume` (see *Interruptions* below).

By default the run also writes a separate `<name>_reactors` file with one
row per *(user, message, reaction)*: who (`id` + `username`) put which emoji, and on what.
It is **slow** — one extra API call per message that has reactions. Telegram
refuses the reactors list for **broadcast-channel posts** (to prevent de-anonymisation),
so those are skipped — without even the extra request when the channel reports its
reactor list as hidden; in practice this captures the reactions left on the
**comments** — with `--no-comments` there is almost nothing left to collect for a channel.
Pass `--no-reactors` to skip it.

On a busy channel this is easily thousands of requests. `FLOOD_WAIT` responses of up
to an hour are waited out and retried automatically (no data lost) — the wait is
printed to the terminal (`… WARNING  Sleeping for Ns …`); a small pause between
reaction-list calls keeps the burst rate down. A longer wait (a temporary soft ban)
is checkpointed and waited out too — up to 6 hours, then the run stops with a
ready-to-paste `--resume` command; nothing is skipped. Keep runs small with
`--max-messages` / a narrow date range, or add `--no-reactors`.

Output is **parquet** by default. Use `--format excel` only for small runs — Excel truncates
any cell over 32,767 characters (the `Comments List` of a busy post easily exceeds that);
`telegram-scraper read <file> --to excel` converts a parquet afterwards.

Comments are fetched only for posts that actually have a linked discussion thread.

Dates may be written as `DD.MM.YYYY` (e.g. `15.10.2024`) or ISO `YYYY-MM-DD`. Both
`--date-min` and `--date-max` are **inclusive**; `--date-max` covers the whole day (UTC).

Output files land in `--out-dir`, named after `--name` with the scraped post-date
span appended as `_<from>-<to>` (both `DD.MM.YYYY`, the first and last post actually
collected):

- `<name>_posts_<from>-<to>.<ext>` — the full run: one row per post, comments in the `Comments List` column.
  Normalised on the way out (like `combine`): duplicates dropped on `Group` + `Message ID`,
  a `Comments` count column added, `Date` parsed to a real datetime, rows sorted newest-first.
- `<name>_participants_<from>-<to>.<ext>` — unless `--no-participants`; one row per unique person who
  commented or reacted: `ID, Username, Name, Comments, Reactions, Total`.
  Skipped with a note when there is nothing to build.
- `<name>_reactors_<from>-<to>.<ext>` — unless `--no-reactors`; one row per *(user, message, reaction)*:
  `Type, Target, Group, Message ID, Post ID, Url, Reactor ID, Reactor Username, Reactor Name, Reaction, Date`
- `<name>_partial/` — `<slug>_until_NNNNN` snapshots written after each channel
  (post-shaped, so `combine --input <name>_partial` merges just these). The resume
  machinery lives in `<name>_partial/checkpoint/` — append-only
  `posts_part_NNNNN.parquet` / `reactors_part_NNNNN.parquet` shards (always parquet,
  whatever `--format` is) plus a `resume.json` cursor, written every 1,000 messages
  and on every reconnect. Each checkpoint only writes the batch since the previous
  one, so its cost and `--resume`'s memory stay flat no matter how much has been
  scraped. A pre-shard `posts.parquet` / `reactors.parquet` from an older run is
  migrated to a shard automatically on `--resume`. The whole `checkpoint/` folder is
  removed on a clean finish.

Re-running overwrites the previous `<name>_posts` / `<name>_participants` /
`<name>_reactors` only when the post-date span comes out identical; a different span
lands in new files.

### Interruptions

A dropped connection is waited out and retried for hours before the run gives up
(`FLOOD_WAIT` soft bans are handled the same way, see above), and if `iter_messages`
still fails the current channel is restarted from the last checkpointed message —
so a passing outage costs nothing.

If the run does die (a very long outage, a crash, `Ctrl-C`) it prints the ready-to-paste
command that resumes it — the same arguments plus `--resume` — which reads the
checkpoint cursor in `<name>_partial/` and continues from it. `Ctrl-C` writes a fresh
checkpoint on the way out. `--resume` refuses to run if the channels / keyword / dates
differ from the interrupted job. (After a hard power-off, nothing is printed — add
`--resume` to your original command by hand.)

### Analyse

```bash
telegram-scraper combine      --input 'output/*_posts_*.parquet' --output output/unified.parquet
telegram-scraper comments     --input output/unified.parquet --output output/comments.parquet
telegram-scraper participants --input output/Test_posts_02.01.2024-30.01.2024.parquet --output output/people.xlsx
telegram-scraper summary  --input output/unified.parquet --output-base output/resume
telegram-scraper filter   --input output/unified.parquet --output output/kw --keywords "Trump,Biden,Kamala"
telegram-scraper sample   --input output/unified.parquet --output output/sample.xlsx --sample-size 10000
telegram-scraper links    --input output/unified.parquet --output output/links.xlsx
telegram-scraper read     output/unified.parquet --head 20 --to xlsx
```

The `Comments List` column holds comments as a JSON string — `telegram-scraper comments`
explodes it into a flat table (one row per comment, with `Comment Author ID` /
`Comment Author Username` / `Comment Author Name`), `--format excel` for a spreadsheet.

`telegram-scraper participants` goes further: it merges the commenters with the
`<name>_reactors` file it finds next to `--input` and writes one row per
person — `ID, Username, Name, Comments, Reactions, Total`. `scrape` runs this
step for you (`<name>_participants`) unless you pass `--no-participants`. `Username` / `Name` are
blank when Telegram has none for that account (only the numeric `ID` identifies them);
`Name` is only populated for data scraped after this feature was added.

### Verify

```bash
telegram-scraper verify --input output/Test_posts_02.01.2024-30.01.2024.parquet --channel @Test \
  --date-min 01.01.2020 --date-max 31.12.2024 --output output/Test_missed.parquet
```

`scrape` walks the channel with `iter_messages`; `verify` cross-checks the result
with `get_messages(ids=…)` (a different Telegram API path). It takes every message
id absent from the scrape within the scraped range and asks the server what that id
is: a deleted/never-existed id and a service message are fine, but a real message
inside `--date-min…--date-max` is a **miss** and gets listed (and written to
`--output`). It also checks that the scrape reached the channel's oldest in-window
message. Exit code is non-zero if anything was missed. `--comment-sample N`
additionally re-checks `N` random threads against the server's reply count.
Needs a free session (not while a `--resume` run is using it).

---

## Output columns

`Type, Group, Author ID, Content, Date, Message ID, Author, Views, Reactions, Shares, Media, Url, Comments List`

plus a `Comments` count column added when the file is normalised on write.

Each entry inside the `Comments List` JSON carries `Comment Author ID`,
`Comment Author Username` **and** `Comment Author Name` (`""` if the commenter
has none, `[channel]` / `[anonymous]` when the reply was sent by a channel or
anonymously).

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
telegram_scraper/
  cli.py         argparse entry point + sub-command dispatch
  menu.py        interactive input()-based wizard over the CLI flags
  config.py      load TG_* credentials from .env
  scrape.py      async scraper (asyncio.run)
  verify.py      cross-check a scrape against the live channel for missed posts
  analysis.py    combine / comments / participants / summary / sample / filter / links
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
