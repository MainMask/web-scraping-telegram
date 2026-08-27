"""Post-processing tools for scraped data (terminal ports of the original helper scripts)."""

import json
import re
from pathlib import Path

import numpy as np
import pandas as pd
from tqdm import tqdm

from telegramscrap.datafiles import read_table, resolve_inputs, save_table

_URL_RE = re.compile(r"http\S+|www\S+")
_TME_RE = re.compile(r"(https?://t\.me/[^\s]+)")
_TME_BASE_RE = re.compile(r"(https?://t\.me/[\w\d\+]+)")


def _require_columns(df: pd.DataFrame, columns, source: str) -> None:
    missing = [c for c in columns if c not in df.columns]
    if missing:
        raise SystemExit(
            f"{source}: missing column(s) {missing}. Available: {list(df.columns)}"
        )


def _count_comments(comments_list) -> int:
    if pd.isna(comments_list):
        return 0
    if isinstance(comments_list, str):
        comments_list = json.loads(comments_list)
    return sum(1 for item in comments_list if item.get("Type") == "comment")


def combine(inputs: str, output: str, dedup_cols: list[str]) -> None:
    """Concatenate parquet files, drop duplicates, recompute the Comments count."""
    paths = resolve_inputs(inputs)
    frames = []
    for p in tqdm(paths, desc="Reading files"):
        df = pd.read_parquet(p)
        if not df.empty and not df.isna().all().all():
            frames.append(df)
    if not frames:
        raise SystemExit(f"No non-empty parquet files found in: {inputs}")
    combined = pd.concat(frames, ignore_index=True)
    _require_columns(combined, dedup_cols + ["Date"], inputs)

    if "Message ID" in combined.columns:
        combined["Message ID"] = combined["Message ID"].astype(str)
    if "Group" in combined.columns:
        combined["Group"] = combined["Group"].apply(lambda x: x if str(x).startswith("@") else "@" + str(x))

    print(f"Rows before dedup: {len(combined)}")
    print(f"Duplicates on {dedup_cols}: {combined.duplicated(subset=dedup_cols).sum()}")
    combined = combined.drop_duplicates(subset=dedup_cols)
    print(f"Rows after dedup: {len(combined)}")

    combined["Comments"] = [
        _count_comments(row.get("Comments List"))
        for row in tqdm(combined.to_dict(orient="records"), desc="Counting comments")
    ]
    combined["Comments"] = combined["Comments"].astype(int)
    if "Media" in combined.columns:
        combined["Media"] = combined["Media"].astype(bool)
    combined["Date"] = pd.to_datetime(combined["Date"])
    combined = combined.sort_values(by="Date", ascending=False)

    n_comments = int(combined["Comments"].sum())
    print(f"Rows: {len(combined)} | comments: {n_comments} | total: {len(combined) + n_comments}")

    save_table(combined, output, "parquet")
    print(f"Saved: {output}")


def _save(df: pd.DataFrame, output: str, fmt: str) -> Path:
    """Honour an explicit .parquet/.xlsx/.csv suffix on `output`, otherwise use `fmt`."""
    if Path(output).suffix.lower() in (".parquet", ".xlsx", ".csv"):
        fmt = None
    return save_table(df, output, fmt)


def _comment_pairs(df: pd.DataFrame) -> list[tuple[dict, dict]]:
    """(post_row, comment_dict) for every comment in the Comments List column."""
    pairs = []
    for post in df.to_dict(orient="records"):
        raw = post.get("Comments List")
        if isinstance(raw, str):
            items = json.loads(raw) if raw.strip() else []
        elif isinstance(raw, list):
            items = raw
        else:
            items = []
        pairs += [(post, c) for c in items if c.get("Type") == "comment"]
    return pairs


def explode_comments(input_path: str, output: str, fmt: str = "parquet") -> None:
    """Flatten the Comments List JSON into a flat table: one row per comment."""
    df = read_table(input_path)
    _require_columns(df, ["Comments List", "Group", "Message ID"], input_path)
    rows = []
    for post, c in tqdm(_comment_pairs(df), desc="Exploding comments"):
        rows.append({
            "Group": post["Group"],
            "Post ID": post["Message ID"],
            "Post Url": post.get("Url", ""),
            "Comment Author ID": c.get("Comment Author ID"),
            "Comment Author Username": c.get("Comment Author Username", ""),
            "Comment Author Name": c.get("Comment Author Name", ""),
            "Comment Content": c.get("Comment Content", ""),
            "Comment Date": c.get("Comment Date", ""),
            "Comment Message ID": c.get("Comment Message ID"),
            "Comment Author": c.get("Comment Author"),
            "Comment Views": c.get("Comment Views"),
            "Comment Reactions": c.get("Comment Reactions", ""),
            "Comment Shares": c.get("Comment Shares"),
            "Comment Media": c.get("Comment Media"),
            "Comment Url": c.get("Comment Url", ""),
        })
    if not rows:
        raise SystemExit(f"{input_path}: no comments in 'Comments List'.")
    out = pd.DataFrame(rows)
    for col in ("Comment Author ID", "Comment Message ID", "Comment Views", "Comment Shares"):
        out[col] = pd.to_numeric(out[col], errors="coerce").astype("Int64")  # keep ints, allow <NA>
    print(f"Saved: {_save(out, output, fmt)} ({len(rows)} comments)")


_NON_USER = {"[channel]", "[anonymous]"}


def _first_nonempty(series) -> str:
    return next((x for x in series if x), "")


def _sibling_reactors(input_path: str) -> list[Path]:
    d = Path(input_path).parent
    return sorted(d.glob("*reactors*.parquet")) + sorted(d.glob("*reactors*.xlsx"))


def participants(input_path: str, output: str, reactors: str | None = None,
                 fmt: str = "parquet") -> None:
    """One row per unique person who commented or reacted: ID, username, name, counts."""
    df = read_table(input_path)
    _require_columns(df, ["Comments List"], input_path)

    rows = [
        {"ID": c.get("Comment Author ID"), "Username": c.get("Comment Author Username") or "",
         "Name": c.get("Comment Author Name") or "", "Comments": 1, "Reactions": 0}
        for _post, c in _comment_pairs(df)
    ]
    for rf in ([Path(reactors)] if reactors else _sibling_reactors(input_path)):
        rdf = read_table(rf)
        _require_columns(rdf, ["Reactor ID", "Reactor Username"], str(rf))
        rows += [
            {"ID": r.get("Reactor ID"), "Username": r.get("Reactor Username") or "",
             "Name": r.get("Reactor Name") or "", "Comments": 0, "Reactions": 1}
            for r in rdf.to_dict(orient="records")
        ]
        print(f"  + reactors from {rf.name}")

    if not rows:
        raise SystemExit(f"{input_path}: no commenters or reactors found.")

    p = pd.DataFrame(rows)
    p["ID"] = pd.to_numeric(p["ID"], errors="coerce").astype("Int64")
    p = p[p["ID"].notna() & (p["ID"] > 0)]  # drop anonymous + channel/chat entities (negative IDs)
    p["Username"] = p["Username"].apply(
        lambda u: "" if not isinstance(u, str) or u in _NON_USER else u
    )
    p["Name"] = p["Name"].apply(lambda n: n if isinstance(n, str) else "")

    agg = p.groupby("ID").agg(
        Username=("Username", _first_nonempty),
        Name=("Name", _first_nonempty),
        Comments=("Comments", "sum"),
        Reactions=("Reactions", "sum"),
    ).reset_index()
    agg["Total"] = agg["Comments"] + agg["Reactions"]
    agg = agg.sort_values("Total", ascending=False, ignore_index=True)

    print(f"Saved: {_save(agg, output, fmt)} ({len(agg)} people)")


def summary(input_path: str, output_base: str, date_col: str, group_col: str, comments_col: str) -> None:
    """Per-group monthly counts of contents, comments and their total."""
    df = read_table(input_path)
    _require_columns(df, [date_col, group_col, comments_col], input_path)
    df[date_col] = pd.to_datetime(df[date_col])
    df["MonthYear"] = df[date_col].dt.to_period("M")

    contents = df.groupby([group_col, "MonthYear"]).size().unstack().fillna(0)
    comments = df.groupby([group_col, "MonthYear"])[comments_col].sum().unstack().fillna(0)
    total = contents.add(comments, fill_value=0)

    months = pd.period_range(start=contents.columns.min(), end=contents.columns.max(), freq="M")
    out_dir = Path(output_base).parent
    out_dir.mkdir(parents=True, exist_ok=True)
    for name, table in (("contents", contents), ("comments", comments), ("total", total)):
        table = table.reindex(columns=months, fill_value=0)
        table.columns = table.columns.astype(str)
        path = f"{output_base}_{name}.xlsx"
        table.to_excel(path, index=True)
        print(f"Saved: {path}")


def _sample_proportionally(df, text_column, category_column, sample_size):
    parts = []
    total_rows = len(df)
    for category in tqdm(df[category_column].unique(), desc="Sampling categories"):
        cat_df = df[df[category_column] == category]
        target = max(1, int(np.ceil(len(cat_df) / total_rows * sample_size)))
        non_empty = cat_df[cat_df[text_column].notna() & (cat_df[text_column].str.strip() != "")]
        if len(non_empty) >= target:
            parts.append(non_empty.sample(target))
        elif not non_empty.empty:
            rest = cat_df[~cat_df.index.isin(non_empty.index)]
            parts.append(pd.concat([non_empty, rest.sample(target - len(non_empty), replace=True)]))
        else:
            parts.append(cat_df.sample(target, replace=True))
    return pd.concat(parts)


def sample(input_path: str, output: str, text_col: str, category_col: str, sample_size: int, min_length: int) -> None:
    """Proportional sample per category, prioritising rows that have text."""
    df = read_table(input_path)
    _require_columns(df, [text_col, category_col], input_path)
    df = df[df[text_col].str.len() > min_length].copy()
    df[text_col] = df[text_col].apply(lambda t: _URL_RE.sub("", str(t)))
    if "Comments List" in df.columns:
        df["Comments List"] = df["Comments List"].apply(lambda x: json.loads(x) if pd.notnull(x) else x)
    sampled = _sample_proportionally(df, text_col, category_col, sample_size)
    save_table(sampled, output, "excel")
    print(f"Saved: {output} ({len(sampled)} rows)")


def filter_keywords(input_path: str, output: str, content_col: str, keywords: list[str], max_rows_per_file: int) -> None:
    """Keep rows containing any keyword; add one 0/1 column per keyword."""
    df = read_table(input_path)
    _require_columns(df, [content_col], input_path)
    if "Comments List" in df.columns:
        df["Comments List"] = df["Comments List"].apply(lambda x: json.loads(x) if pd.notnull(x) else x)
    for kw in tqdm(keywords, desc="Keyword columns"):
        df[kw] = df[content_col].astype(str).apply(lambda x: 1 if kw in x else 0)
    df["Keyword_Count"] = df[keywords].sum(axis=1)
    filtered = df[df["Keyword_Count"] > 0]
    print(f"Matched rows: {len(filtered)}")

    num_files = (len(filtered) // max_rows_per_file) + 1
    for i in range(num_files):
        chunk = filtered.iloc[i * max_rows_per_file:(i + 1) * max_rows_per_file]
        if chunk.empty:
            continue
        suffix = "unique" if num_files == 1 else f"part_{i + 1}"
        path = save_table(chunk, f"{Path(output).with_suffix('')}_{suffix}", "excel")
        print(f"Saved: {path}")


def links(input_path: str, output: str) -> None:
    """Extract, normalise and count t.me links found in Content (snowball sampling)."""
    df = read_table(input_path)
    _require_columns(df, ["Content"], input_path)
    found = df["Content"].astype(str).apply(_TME_RE.findall)
    normalised = []
    for sublist in tqdm(found.tolist(), desc="Normalising links"):
        for link in sublist:
            m = _TME_BASE_RE.match(link)
            if m:
                normalised.append(m.group(1))
    counts = pd.Series(normalised).value_counts().reset_index()
    counts.columns = ["Telegram Link", "Frequency"]
    save_table(counts, output, "excel")
    print(f"Saved: {output} ({len(counts)} unique links)")
