"""
common/utils.py  ──  Re-usable helpers for all ETL pipelines
-------------------------------------------------------------------
Everything lives in ETLUtils; thin aliases are exported for convenience.
"""

from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

###############################################################################
# Logging (arrow-style format already set by root)                            #
###############################################################################
logger = logging.getLogger("ETLUtils")

###############################################################################
# Public re-exports (so old imports won’t break)                              #
###############################################################################
__all__ = [
    # convenience aliases
    "sanitize_id",
    "latest_file",
    "extract_date_from_filename",
    "add_date_slices",
    # full class
    "ETLUtils",
]


###############################################################################
# ETLUtils – now contains ALL shared helpers                                  #
###############################################################################
class ETLUtils:
    """
    Static helpers for lightweight, in-process ETL work.
    All methods return **new** DataFrames and never mutate inputs.
    """

    # ──────────────────────────────
    # 0.  Generic file / string utils
    # ──────────────────────────────
    @staticmethod
    def build_ee_pos_id(df: pd.DataFrame) -> pd.Series:
        emp = sanitize_id(df["Employee ID"])
        pos = df["Position ID"].astype(str)
        return emp + pos

    @staticmethod
    def sanitize_id(series: pd.Series, *, strip_leading_zeros: bool = False) -> pd.Series:
        """Pure string ID (strip '.0', trim, optional zero-lstrip)."""
        out = series.astype(str).str.replace(r"\.0$", "", regex=True).str.strip()
        if strip_leading_zeros:
            out = out.str.lstrip("0")
        return out

    @staticmethod
    def latest_file(directory: Path, pattern: str) -> Path:
        """Newest file (by mtime) matching *pattern* inside *directory*."""
        files = sorted(directory.glob(pattern), key=lambda p: p.stat().st_mtime, reverse=True)
        if not files:
            raise FileNotFoundError(f"No files match {pattern!r} in {directory}")
        return files[0]

    @staticmethod
    def extract_date_from_filename(
        path: str | Path,
        *,
        regex: str = r"\d{4}-\d{2}-\d{2}",
        date_format: str = "%Y-%m-%d",
    ) -> pd.Timestamp:
        """Pull first YYYY-MM-DD in *path.name* and return as Timestamp."""
        fname = Path(path).name
        m = re.search(regex, fname)
        if not m:
            raise ValueError(f"No date like {regex!r} in {fname!r}")
        return pd.to_datetime(m.group(0), format=date_format, errors="raise").normalize()

    @staticmethod
    def add_date_slices(
        df: pd.DataFrame,
        *,
        date_col: str,
        prefix: str = "",
    ) -> pd.DataFrame:
        """
        Append *_year / *_mmmyy / *_yyyymm columns derived from *date_col*.

        Pass the *prefix* you want, e.g.
        • jobreq.py →  prefix="thd_"
        • hire.py   →  prefix="start_"
        """
        out = df.copy()
        out[f"{prefix}year"] = out[date_col].dt.year
        out[f"{prefix}month_year"] = out[date_col].dt.strftime("%b %Y")
        out[f"{prefix}yyyymm"] = out[date_col].dt.strftime("%Y%m")
        return out


    @staticmethod
    def apply_filters(df: pd.DataFrame, conditions: dict[str, Any]) -> pd.DataFrame:

        if not conditions:
            return df.copy()

        mask = pd.Series(True, index=df.index)

        for col, cond in conditions.items():
            if col not in df.columns:
                logger.warning("Filter skipped – column missing: %s", col)
                continue

            if callable(cond):
                mask &= df[col].apply(cond)
            elif isinstance(cond, (list, tuple, set, np.ndarray, pd.Series)):
                mask &= df[col].isin(cond)
            else:
                mask &= df[col].eq(cond)

        logger.info("Applied filters: kept %d / %d rows", mask.sum(), len(df))
        return df[mask].copy()

    # ──────────────────────────────
    # 1.  FILE READERS (unchanged)   …
    #    ‣ read_excel / read_csv / read_json
    # ──────────────────────────────
    @staticmethod
    def read_excel(
        file_path: str | Path,
        *,
        sheet_name: str | int | None = 0,
        usecols: str | list[str] | None = None,
        filter_condition: dict[str, Any] | None = None,
        skiprows: int | list[int] | None = None,
        nrows: int | None = None,
        dtype: dict[str, str] | None = None,
        **kwargs,
    ) -> pd.DataFrame:
        try:
            df = pd.read_excel(
                file_path,
                sheet_name=sheet_name,
                usecols=usecols,
                skiprows=skiprows,
                nrows=nrows,
                dtype=dtype,
                engine=kwargs.pop("engine", None),
                **kwargs,
            )
            logger.info("Read Excel (%s): %s  (rows=%d)", sheet_name, Path(file_path).name, len(df))
            return ETLUtils.apply_filters(df, filter_condition) if filter_condition else df
        except Exception:
            logger.exception("Failed reading Excel %s", file_path)
            raise

    @staticmethod
    def read_csv(
        file_path: str | Path,
        *,
        usecols: list[str] | None = None,
        filter_condition: dict[str, Any] | None = None,
        dtype: dict[str, str] | None = None,
        encoding: str = "utf-8",
        sep: str = ",",
        **kwargs,
    ) -> pd.DataFrame:
        try:
            df = pd.read_csv(
                file_path,
                usecols=usecols,
                dtype=dtype,
                encoding=encoding,
                sep=sep,
                **kwargs,
            )
            logger.info("Read CSV: %s  (rows=%d)", Path(file_path).name, len(df))
            return ETLUtils.apply_filters(df, filter_condition) if filter_condition else df
        except Exception:
            logger.exception("Failed reading CSV %s", file_path)
            raise

    @staticmethod
    def read_json(
        file_path: str | Path,
        *,
        filter_condition: dict[str, Any] | None = None,
        **kwargs,
    ) -> pd.DataFrame:
        try:
            df = pd.read_json(file_path, **kwargs)
            logger.info("Read JSON: %s  (rows=%d)", Path(file_path).name, len(df))
            return ETLUtils.apply_filters(df, filter_condition) if filter_condition else df
        except Exception:
            logger.exception("Failed reading JSON %s", file_path)
            raise

    # ──────────────────────────────
    # 2.  TRANSFORMERS  (apply_filters, …)  – unchanged
    # 3.  TYPE HELPERS  (convert_types, …)  – unchanged
    # 4.  AGG / JOIN    – unchanged
    # 5.  SAVE / QA     – unchanged
    # ──────────────────────────────
    # … (the rest of the previously-shared ETLUtils code stays intact) …


###############################################################################
# Aliases so old top-level imports still work                                 #
###############################################################################
sanitize_id = ETLUtils.sanitize_id
latest_file = ETLUtils.latest_file
extract_date_from_filename = ETLUtils.extract_date_from_filename
add_date_slices = ETLUtils.add_date_slices
