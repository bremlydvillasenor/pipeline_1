# erp.py — ERP Headcount ETL

## Overview

Processes the monthly **headcount masterfile** Excel export, flags each
employee-month record with whether that employee made a qualifying referral, and
writes the result as a parquet file for downstream reporting.

This module runs **after** `referral.py`, which provides the referral lookup
table used for flagging.

---

## Inputs

| Source | Config Key | Description |
|---|---|---|
| Headcount Excel | `HEADCOUNT_PATTERN` | Monthly BIA masterfile; located in `HISTORICAL_DATA_ROOT` |
| `unique_referrals_df` | *(in-memory)* | Output of `referral.run_referral()`; one row per `headcount_id` with `has_referral=1` |

### Raw Excel columns consumed

| Raw Column | Internal Name |
|---|---|
| `Masterfile[Reporting Date]` | `reporting_date` |
| `Masterfile[Location]` | `location` |
| `Masterfile[Function]` | `function` |
| `Masterfile[Sub-function]` | `sub_function` |
| `Masterfile[Job Grade]` | `job_grade` |
| `Masterfile[Employee ID]` | `employee_id` |

---

## Output

| Output | Config Key | Description |
|---|---|---|
| Headcount parquet | `OUTPUT_MHEADCOUNT_PARQUET` | Headcount rows with `has_referral` flag, zstd compressed |

---

## ETL Flow

```
Headcount Excel                unique_referrals_df
(HISTORICAL_DATA_ROOT)         (from referral.py)
       │
       ▼
_read_excel_promote_header()
  • skip 1 preamble row
  • promote next row as header
  • select only expected columns
  • normalize names to snake_case
       │
       ▼
extract()
  • rename to internal column names
  • parse reporting_date → pl.Date
       │
       ▼
transform()
  │
  ├─ _extend_headcount()
  │    • compares latest reporting_date to today's month-end
  │    • if current month is missing, duplicates latest month's rows
  │      and stamps them with today's month-end date
  │    • ensures reporting always has an up-to-date snapshot
  │
  ├─ Prefix job_grade with "Level "
  │    e.g. "5" → "Level 5"
  │
  ├─ Build headcount_id = employee_id + '_' + YYYY-MM
  │    (derived from reporting_date)
  │
  └─ Left-join unique_referrals_df on headcount_id
       • matched rows: has_referral = 1
       • unmatched rows: has_referral = 0
       │
       ▼
load()
  • write_parquet(zstd) → OUTPUT_MHEADCOUNT_PARQUET
```

---

## Key Identifier

### `headcount_id`
Links an employee to a specific reporting month:
```
{employee_id}_{YYYY-MM}
```
where `YYYY-MM` is derived from `reporting_date`. This is the join key used to
attach referral flags from `referral.py`.

---

## Month Extension Logic (`_extend_headcount`)

BIA delivers headcount data for completed months only. To keep dashboards
current, the pipeline detects when the current calendar month-end is absent from
the data and auto-extends:

1. Compute `target` = last day of the current month.
2. If `target` is already in the data → do nothing.
3. Otherwise, copy the latest month's rows and overwrite their `reporting_date`
   with `target`, then append and sort.

---

## Output Columns

| Column | Type | Notes |
|---|---|---|
| `reporting_date` | `Date` | Month-end snapshot date |
| `location` | `Utf8` | |
| `function` | `Utf8` | |
| `sub_function` | `Utf8` | |
| `job_grade` | `Utf8` | Prefixed with "Level " |
| `employee_id` | `Utf8` | |
| `headcount_id` | `Utf8` | `employee_id_YYYY-MM` |
| `has_referral` | `Int8` | `1` if employee referred someone that month, else `0` |

---

## Public Interface

```python
from src.erp import run_erp_pipeline

headcount_df = run_erp_pipeline(config, unique_referrals_df)
```

| Parameter | Type | Description |
|---|---|---|
| `config` | `dict` | Pipeline configuration from `load_config()` |
| `unique_referrals_df` | `pl.DataFrame` | Second return value of `run_referral()` |

| Return value | Type | Description |
|---|---|---|
| `headcount_df` | `pl.DataFrame` | Headcount with `has_referral` flags |

---

## Dependencies

- **Upstream:** `referral.py` must run first to produce `unique_referrals_df`
- **Downstream:** None; this is the final stage of the pipeline
