"""
src/_utils.py  ─  Shared pipeline utilities
─────────────────────────────────────────────────────────────
Provides
  setup_logging            – configure root logger (console + daily file)
  latest_file              – newest file matching a glob pattern
  extract_date_from_filename – pull YYYY-MM-DD from a filename
  to_snake                 – normalise a column name to snake_case
  build_rename_map         – de-dup snake_case rename dict for a column list
  read_excel_with_header   – read Excel, promote first row as header
  ensure_schema            – select / cast / fill-null to match a schema dict
  write_parquet_atomic     – write Parquet via tmp-then-replace
  run_backup               – copy raw + processed into a dated backup folder
"""

from __future__ import annotations

import logging
import re
import shutil
from datetime import date, datetime
from pathlib import Path

import polars as pl

logger = logging.getLogger(__name__)

__all__ = [
    "setup_logging",
    "latest_file",
    "extract_date_from_filename",
    "to_snake",
    "build_rename_map",
    "read_excel_with_header",
    "ensure_schema",
    "write_parquet_atomic",
    "run_backup",
]


# ─────────────────────────────────────────────────────────────
# LOGGING SETUP
# ─────────────────────────────────────────────────────────────

def setup_logging(log_dir: Path | str | None = None) -> None:
    """
    Configure the root logger with a StreamHandler and (optionally) a
    daily rotating FileHandler.  Call once from the pipeline entry point
    before any ETL stage runs.
    """
    fmt     = "%(asctime)s  %(levelname)-8s  %(name)-22s  %(message)s"
    datefmt = "%Y-%m-%d %H:%M:%S"

    handlers: list[logging.Handler] = [logging.StreamHandler()]

    if log_dir is not None:
        log_dir = Path(log_dir)
        log_dir.mkdir(parents=True, exist_ok=True)
        log_file = log_dir / f"pipeline_{datetime.today().strftime('%Y-%m-%d')}.log"
        fh = logging.FileHandler(log_file, encoding="utf-8")
        fh.setFormatter(logging.Formatter(fmt, datefmt=datefmt))
        handlers.append(fh)
        logger.info("log file  : %s", log_file)

    logging.basicConfig(
        level=logging.INFO,
        format=fmt,
        datefmt=datefmt,
        handlers=handlers,
        force=True,
    )


# ─────────────────────────────────────────────────────────────
# FILE HELPERS
# ─────────────────────────────────────────────────────────────

def latest_file(directory: Path | str, pattern: str) -> Path:
    """Return the newest file (by mtime) matching *pattern* inside *directory*."""
    files = sorted(Path(directory).glob(pattern), key=lambda p: p.stat().st_mtime, reverse=True)
    if not files:
        raise FileNotFoundError(f"No files match {pattern!r} in {directory}")
    return files[0]


def extract_date_from_filename(
    path: str | Path,
    *,
    regex: str = r"\d{4}-\d{2}-\d{2}",
    date_format: str = "%Y-%m-%d",
) -> date:
    """Pull first YYYY-MM-DD token from *path.name* and return as datetime.date."""
    fname = Path(path).name
    m = re.search(regex, fname)
    if not m:
        raise ValueError(f"No date matching {regex!r} in filename {fname!r}")
    return datetime.strptime(m.group(0), date_format).date()


# ─────────────────────────────────────────────────────────────
# COLUMN NAME HELPERS
# ─────────────────────────────────────────────────────────────

def to_snake(name: str) -> str:
    """Normalise a column name to lower_snake_case."""
    s = str(name).strip().lower()
    s = re.sub(r"[^\w\s]", " ", s)
    s = re.sub(r"\s+", "_", s)
    return re.sub(r"_+", "_", s).strip("_") or "col"


def build_rename_map(columns: list[str]) -> dict[str, str]:
    """
    Return a {original: snake_case} rename dict for *columns*.
    Duplicate snake names are suffixed _1, _2, …
    """
    counts: dict[str, int] = {}
    result: dict[str, str] = {}
    for col in columns:
        snake = to_snake(col)
        if snake in counts:
            counts[snake] += 1
            result[col] = f"{snake}_{counts[snake]}"
        else:
            counts[snake] = 0
            result[col] = snake
    return result


# ─────────────────────────────────────────────────────────────
# EXCEL READER
# ─────────────────────────────────────────────────────────────

def read_excel_with_header(
    path: Path | str,
    *,
    skip_rows: int = 0,
    engine: str = "calamine",
) -> pl.DataFrame:
    """
    Read an Excel file, skip *skip_rows* preamble rows, then promote the
    next row as the column header.  Returns a DataFrame with the original
    header strings as column names (no renaming applied here).
    """
    df = pl.read_excel(
        path,
        has_header=False,
        read_options={"skip_rows": skip_rows},
        engine=engine,
    )
    headers = [str(v) for v in df.row(0)]
    return df.slice(1).rename(dict(zip(df.columns, headers)))


# ─────────────────────────────────────────────────────────────
# SCHEMA ENFORCEMENT
# ─────────────────────────────────────────────────────────────

def ensure_schema(
    df: pl.DataFrame,
    schema: dict[str, pl.DataType],
    name: str = "",
) -> pl.DataFrame:
    """
    Select and cast *df* to exactly match *schema*.
    Columns missing from *df* are added as typed nulls.
    *name* is used only in warning messages.
    """
    if df.height == 0:
        return pl.DataFrame(schema=schema)

    existing = set(df.columns)
    missing  = [c for c in schema if c not in existing]
    if missing:
        label = f"{name}: " if name else ""
        logger.warning("%smissing columns filled with null: %s", label, missing)

    return df.select([
        pl.lit(None).cast(dtype).alias(col)
        if col in missing
        else pl.col(col).cast(dtype, strict=False)
        for col, dtype in schema.items()
    ])


# ─────────────────────────────────────────────────────────────
# PARQUET WRITER
# ─────────────────────────────────────────────────────────────

def write_parquet_atomic(
    df: pl.DataFrame,
    target: Path,
    *,
    compression: str = "zstd",
) -> None:
    """
    Write a Parquet file atomically: write to a .tmp file in the same
    directory then replace the target on success.
    """
    if target.suffix != ".parquet":
        raise ValueError(f"Target path must end with '.parquet', got: {target}")
    tmp = target.with_name(target.name + ".tmp")
    try:
        df.write_parquet(tmp, compression=compression)
        tmp.replace(target)
    finally:
        if tmp.exists():
            tmp.unlink()


# ─────────────────────────────────────────────────────────────
# BACKUP
# ─────────────────────────────────────────────────────────────

def run_backup() -> None:
    """
    Copy data/raw and data/processed into a date-stamped folder one level
    above the project root (YYYY-MM-DD).  Overwrites if already exists.
    """
    today        = datetime.today().strftime("%Y-%m-%d")
    project_root = Path(__file__).resolve().parents[1]
    backup_dest  = project_root.parent / today

    if backup_dest.exists():
        shutil.rmtree(backup_dest)
        logger.info("  backup : overwriting existing %s", backup_dest)

    backup_dest.mkdir(parents=True)

    for folder in ("raw", "processed"):
        src = project_root / "data" / folder
        if src.exists():
            shutil.copytree(src, backup_dest / folder)
            logger.info("  backup : %s → %s", src.name, backup_dest / folder)
        else:
            logger.warning("  backup : source not found, skipped: %s", src)

    logger.info("  backup : done → %s", backup_dest)
