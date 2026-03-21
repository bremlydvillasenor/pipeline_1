# CLAUDE.md — pipeline_1

## Project Overview

This is a Python-based HR/Recruiting data pipeline that processes Workday exports into clean parquet/CSV outputs for reporting and analytics. It covers job requisitions, hires, applications, referrals and ERP (Employee Referral Participation) Data.

## How to Run

```bat
# Windows (from project root)
run.bat

# Or directly
python -m pipeline
```

The virtual environment is located at `..\.venv` (one level above the project root).

## Project Structure

```
pipeline_1/
├── pipeline.py          # Entry point — orchestrates all pipeline stages
├── config.yaml          # All file patterns, output names, and settings
├── run.bat              # Windows launcher script
├── src/
│   ├── __init__.py      # Exports all run_* functions and load_config
│   ├── _config.py       # Loads config.yaml and resolves all paths
│   ├── _utils.py        # Shared utilities (run_backup, helpers)
│   ├── jobreq.py        # Job Requisitions ETL
│   ├── hire.py          # Hires ETL
│   ├── app.py           # Applications ETL + funnel transform
│   ├── referral.py      # Referrals ETL
│   └── erp.py           # ERP movement data ETL
└── data/
    ├── raw/             # Source Excel/CSV exports from Workday
    ├── historical/      # Historical reference files (e.g., hiresdb.parquet)
    ├── processed/       # Output parquet/CSV files
    └── external/        # External reference data
```

## Pipeline Stages (in order)

1. `run_jobreq(config)` — Processes job requisition data
2. `run_hire(config)` — Processes hire/movement data
3. `run_app(config)` — Processes daily application data + builds funnel
4. `run_referral(config)` — Processes referral data
5. `run_erp(config)` — Processes ERP data
6. `run_backup()` — Backs up the project

## Config System

All configuration lives in `config.yaml` and is loaded via `src/_config.py`.

`load_config("config.yaml")` returns a single flat dict with:
- **Path keys** (uppercase): `RAW_DATA_ROOT`, `PROCESSED_DATA_ROOT`, `SCRUM_FILE`, `OUTPUT_APPS_PAR`, etc.
- **Pattern keys** (uppercase): `ALL_STATUS_PATTERN`, `DAILY_APP_PATTERN`, etc.
- **Settings**: `REQS_YEARS_SCOPE`

Always use `config["KEY"]` (uppercase) when referencing paths and patterns inside pipeline modules.

## Key Libraries

- **Polars** — primary dataframe library
- **PyYAML** — config loading
- **openpyxl / xlsx support** — for reading Excel source files

## Data Sources (Raw Inputs)

All raw files are matched by glob patterns from `config.yaml`:

| Pattern Key | Description |
|---|---|
| `ALL_STATUS_PATTERN` | Workday requisition all-status report |
| `FULFILLMENT_PATTERN` | Fulfillment report |
| `INTX_HIRES_PATTERN` | Internal/external hires report |
| `DAILY_APP_PATTERN` | Daily application CSV (skip_rows=16) |
| `PROSPECT_PATTERN` | Referrals without job applications |
| `HEADCOUNT_PATTERN` | Monthly headcount file |

The `ta_scrum_file.xlsx` (in `external/`) contains reference sheets:
- `disposition_mapping` — maps raw disposition reasons to consolidated categories
- `source_mapping` — maps source to consolidated_channel, channel_sort, internal_external

## Application Funnel Logic (`app.py`)

The funnel transforms one application row into multiple stage rows:
- Valid stages: `Review(1) → Screen(2) → Assessment(3) → Interview(4) → Offer(6) → Ready for Hire(8)`
- Stage coercions: `5 → 4`, `7 → 6`
- Candidates with status **"Application in Process"** are exploded only for stages **before** their current stage
- Only the last stage row retains true status/disposition; earlier rows get `status='Passed'`

## Important Conventions

- Use `latest_file(directory, pattern)` from `_utils` to find the most recent matching file
- Output files are always written to `PROCESSED_DATA_ROOT` as `.parquet` (zstd compression)
- The `hiresdb.parquet` accumulates historical hire records and lives in `HISTORICAL_DATA_ROOT`
- Level 9 compensation grade records are always excluded from applications
- Applications are scoped to the last 4 years from the refresh date
