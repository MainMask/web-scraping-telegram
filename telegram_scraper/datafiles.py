"""Shared helpers for reading/writing tabular data and cleaning text."""

import glob
import re
from pathlib import Path

import pandas as pd

_EXT_FOR_FORMAT = {"xlsx": "xlsx", "excel": "xlsx", "parquet": "parquet", "csv": "csv"}

# Characters that are valid in XML 1.0 (and therefore storable in .xlsx). Anything
# outside these ranges is stripped before writing.
_INVALID_XML_CHARS = re.compile(
    "[^\\u0009\\u000A\\u000D\\u0020-\\uD7FF\\uE000-\\uFFFD\\U00010000-\\U0010FFFF]"
)


def clean_xml_text(text: str | None) -> str:
    """Drop characters that Excel / XML cannot store. None becomes an empty string."""
    if not text:
        return ""
    return _INVALID_XML_CHARS.sub("", text)


def format_duration(seconds: float) -> str:
    seconds = int(seconds)
    days, rem = divmod(seconds, 86400)
    hours, rem = divmod(rem, 3600)
    minutes, secs = divmod(rem, 60)
    return f"{days:02}:{hours:02}:{minutes:02}:{secs:02}"


_EXCEL_CELL_LIMIT = 32_767


def _warn_if_excel_would_truncate(df: pd.DataFrame) -> None:
    for col in df.columns:
        if df[col].dtype == object:
            longest = df[col].dropna().astype(str).str.len().max()
            if pd.notna(longest) and longest > _EXCEL_CELL_LIMIT:
                print(
                    f"  ! WARNING: column {col!r} has cells up to {longest} chars; "
                    f"Excel truncates at {_EXCEL_CELL_LIMIT}. Use --format parquet to keep full data."
                )


def save_table(df: pd.DataFrame, path: str | Path, fmt: str | None = None) -> Path:
    """Write a DataFrame as parquet, xlsx or csv (inferred from the extension unless fmt given)."""
    path = Path(path)
    ext = _EXT_FOR_FORMAT.get((fmt or path.suffix.lstrip(".")).lower())
    if ext is None:
        raise ValueError(f"Unsupported format: {fmt!r}")
    # append the extension without clobbering dots that are part of the name
    if path.suffix.lower() != f".{ext}":
        path = path.with_name(f"{path.name}.{ext}")
    if ext == "xlsx":
        _warn_if_excel_would_truncate(df)
        df.to_excel(path, index=False, engine="openpyxl")
    elif ext == "parquet":
        df.to_parquet(path, index=False)
    else:
        df.to_csv(path, index=False)
    return path


def read_table(path: str | Path) -> pd.DataFrame:
    path = Path(path)
    suffix = path.suffix.lower()
    if suffix == ".parquet":
        return pd.read_parquet(path)
    if suffix in (".xlsx", ".xls"):
        return pd.read_excel(path)
    if suffix == ".csv":
        return pd.read_csv(path)
    raise ValueError(f"Unsupported file type: {path.name}")


def resolve_inputs(pattern: str) -> list[Path]:
    """Expand a file, a directory (its *.parquet files) or a glob into a sorted list of paths."""
    p = Path(pattern)
    if p.is_dir():
        return sorted(p.glob("*.parquet"))
    if p.exists():
        return [p]
    matches = sorted(Path(m) for m in glob.glob(pattern))
    if not matches:
        raise SystemExit(f"No files match: {pattern}")
    return matches
