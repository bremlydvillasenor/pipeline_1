# Pseudocode: `src/app.py` — Applications ETL Pipeline

## Overview

Processes daily application CSV exports from Workday into two output datasets:

1. **`applications.parquet`** — One row per candidate-application, enriched and cleaned
2. **`app_funnel.parquet`** — One row per candidate-per-stage reached (exploded view for funnel analytics)
3. **`applications_offer_accepts.parquet`** — One row per offer data, enrich with recruiter who deliver offer 

```
Daily Application CSV (skip 16 rows)
            │
            ▼
        EXTRACT
    ├── App CSV         → LazyFrame (renamed columns)
            │
            ▼
        TRANSFORM
    ├── Fill nulls, parse dates, filter cutoff + Level 1 to Level 8
    ├── Audit originals (snapshot before corrections)
    ├── Apply rescind corrections
    ├── Derive stage numbers
    ├── Derive: recruiter_completed_offer_id (digits-only)
    └── Reorder final columns
            │
            ├── apps LazyFrame ──────────────────────────► applications.parquet / .csv
            ├── offer_accepts LazyFrame (candidate_recruiting_status = "Offer Accepted") ──────► applications_offer_accepts.parquet
            └── funnel LazyFrame (_transform_funnel) ───► app_funnel.parquet
```

---

## Constants

### Stage Map

```
STAGE_MAP = {
    "Review":           1,
    "Screen":           2,
    "Assessment":       3,
    "Interview":        4,
    "Reference Check":  5,
    "Offer":            6,
    "Background Check": 7,
    "Ready for Hire":   8,
}
```

### Funnel Valid Stages

```
FUNNEL_VALID_STAGES = [1, 2, 3, 4, 6, 7, 8]
```

### Column Groups

```
DATE_COLS = [
    added_date, job_application_date, offer_accepted_date,
    candidate_start_date, target_hire_date,
]

ENGINEERED_COLS = [
    last_recruiting_stage_orig, candidate_recruiting_status_orig,
    disposition_reason_orig, recruiter_completed_offer_id,
    last_stage_number
]
```

---

## Stage 1: EXTRACT — `extract(config)`

```
FUNCTION extract(config):

    # Determine refresh date and application lookback window
    status_path  ← latest_file(RAW_DATA_ROOT, ALL_STATUS_PATTERN)
    run_date ← current date
    app_cutoff   ← date(run_date.year - 4, month=1, day=1)
        # Applications older than 4 years from refresh date are excluded

    # Load daily application CSV
    app_path ← latest_file(RAW_DATA_ROOT, DAILY_APP_PATTERN)
    app_lf   ← scan_csv(app_path, skip_rows=16)
                .select(APP_RENAME_MAP.keys())
                .rename(APP_RENAME_MAP)
        # skip_rows=16: Workday CSV has 16 metadata header rows before data

    RETURN {
        app_lf, app_cutoff, run_date
    }
```

---

## Stage 2: TRANSFORM — `transform(raw, config)`

### 2a. Baseline Cleaning

```
lf ← raw["app_lf"]

# Null-fill source before joins (prevents NULL key fan-out)
source ← FILL NULL → "zzz_unknown"

# Parse all date columns (multi-format, non-strict — nulls on failure)
FOR EACH col IN DATE_COLS WHERE col IN schema:
    col ← COALESCE(
        strptime(col, "%Y-%m-%d"),
        strptime(col, "%Y/%m/%d"),
        strptime(col, "%m/%d/%Y"),
    )

# Scope filter: exclude records before the 4-year lookback window
FILTER: job_application_date >= app_cutoff

# Grade exclusion: Level 9 is always removed from applications
FILTER: compensation_grade in "Level 1" to "Level 8" 
```

### 2b. Audit Snapshots (Pre-Correction)

```
# Preserve raw values before any corrections are applied
last_recruiting_stage_orig        ← last_recruiting_stage
candidate_recruiting_status_orig  ← candidate_recruiting_status
disposition_reason_orig           ← disposition_reason
```

### 2c. Rescind Correction — `_apply_rescind_correction(lf)`

Corrects records where an accepted offer was later rescinded but Workday
still shows the candidate as "Ready for Hire" with "Offer Accepted" status.

```
FUNCTION _apply_rescind_correction(lf):
    REQUIRED columns:
        hire_transaction_status, hired,
        last_recruiting_stage_orig, candidate_recruiting_status_orig,
        disposition_reason

    IF any required column is absent → RETURN lf unchanged

    rescind_mask ←
        hire_transaction_status == "Rescinded"
        AND hired IS NULL
        AND last_recruiting_stage_orig == "Ready for Hire"
        AND candidate_recruiting_status_orig == "Offer Accepted"

    stage_update_mask ←
        (last_recruiting_stage_orig == "Ready for Hire" AND disposition_reason IS NOT NULL)
        OR rescind_mask

    APPLY:
        last_recruiting_stage ←
            IF stage_update_mask → "Offer"      # Roll back stage to Offer
            ELSE                 → unchanged

        disposition_reason ←
            IF rescind_mask → "Offer Accepted to Rescinded"   # Assign explicit disposition
            ELSE            → unchanged

    RETURN corrected lf
```

### 2d. Stage Number Derivation

```
last_stage_number ← STAGE_MAP.replace(last_recruiting_stage, default=NULL).cast(Int8)
    # Unmapped stage names → NULL
```

### 2f. Derived Fields (Post-Join)

```
# Recruiter ID: digits only from "Name (12345)" format; blank → sentinel
cleaned ← recruiter_completed_offer.replace_all(r"\D+", "")
recruiter_completed_offer_id ←
    IF cleaned == "" → "zzz_blank"
    ELSE             → cleaned

```

### 2g. Final Column Selection

```
final_cols ← [ c for c in (RAW_COLS_SNAKE + ENGINEERED_COLS) if c in schema ]
lf ← lf.select(final_cols)
    # Columns absent from this run's schema are silently excluded
```

### 2h. Derived Sub-Frames

```
# Offer-Accepts: candidates who have Accepted Offer
offer_lf ← lf.filter(candidate_recruiting_status = "Offer Accepted")

# Funnel transform (collect → transform → lazy)
funnel_lf ← _transform_funnel(lf.collect(), config).lazy()
```

---

## Stage 3: FUNNEL TRANSFORM — `_transform_funnel(df, config)`

Explodes each application row into one row per funnel stage reached,
enabling stage-level conversion and in-process tracking.

### 3a. Guard Checks

```
FUNCTION _transform_funnel(df, config):
    IF df is not a Polars DataFrame → RAISE TypeError
    IF df is empty                  → RETURN df unchanged

    _check_funnel_schema(df)
        # Raises ValueError if any FUNNEL_REQUIRED_COLS are missing

    df ← df.select(FUNNEL_REQUIRED_COLS)   # Keep only required columns
```

### 3c. Date Truncation

```
job_application_date ← truncate to first day of month
    # Normalises all apps in same month to same date for cohort grouping
```

### 3d. In-Process Tagging

```
_in_process ←
    candidate_recruiting_status.strip().lower() == "application in process"
```

### 3e. Stage List Generation (Per Row)

```
FOR EACH row:
    IF last_stage_number IS NULL:
        _stage_list ← []

    ELIF _in_process == True:
        _stage_list ← [ s for s in FUNNEL_VALID_STAGES if s < last_stage_number ]
        # In-process: explode only COMPLETED prior stages (current stage not yet done)

    ELSE:
        _stage_list ← [ s for s in FUNNEL_VALID_STAGES if s <= last_stage_number ]
        # Completed: explode all stages up to and including current stage
```

> **Key rule:** A candidate "in process" at stage 1 (Review) produces
> **zero rows** and is absent from the funnel entirely — they have not
> completed any stage yet.

### 3f. Stage Explosion

```
long ← df.explode(_stage_list).rename(_stage_list → stage_number)
FILTER: stage_number IS NOT NULL
DROP: _in_process

# Add human-readable stage label
long ← long LEFT JOIN FUNNEL_STAGE_MAP ON stage_number
    → adds "stage" column: "Review", "Screen", ..., "Ready for Hire"
```

### 3g. Status and Disposition Masking

```
is_last ← (last_stage_number == stage_number)

candidate_recruiting_status ←
    IF is_last → keep original status
    ELSE       → "Passed"
    # Earlier stage rows always show "Passed"

disposition_reason ←
    IF is_last → keep original disposition
    ELSE       → NULL

```

### 3j. Final Column Ordering

```
RETURN long.select([c for c in ORDER_COLS if c in long.columns])
```

---

## Stage 4: LOAD — `load(dfs, config)`

```
FUNCTION load(dfs, config):

    # Applications — full cleaned dataset
    apps_df ← dfs["apps"].collect()
    apps_df.write_csv(OUTPUT_APPS_CSV)
    apps_df.write_parquet(OUTPUT_APPS_PAR, compression="zstd")

    # Offer-accepts slice — candidates who Offer Accepts
    dfs["offer_stage"].collect()
        .write_parquet(OUTPUT_APPS_OFFER_PAR, compression="zstd")

    # Funnel — one row per candidate per stage
    dfs["funnel"].collect()
        .write_parquet(OUTPUT_FUNNEL_PAR)
        # Note: no explicit compression specified (Polars default applies)
```

---

## Main Orchestrator — `run_app(config)`

```
FUNCTION run_app(config):
    LOG "🚀 App ETL started..."
    t0 ← now()

    [EXTRACT]  ext  ← extract(config)
    [TRANSFORM] dfs ← transform(ext, config)
    [LOAD]           load(dfs, config)

    LOG "✅ App ETL completed • duration: Xs"
    RETURN dfs      # { "apps", "offer_stage", "funnel" }
```

---

## Decision Logic Summary

| Decision Point | Condition | Outcome |
|---|---|---|
| Date parsing failure | Value doesn't match any format | Field becomes NULL (non-strict, no error) |
| Missing source | `source` IS NULL | Filled with `"zzz_unknown"` before join |
| Level 1 to Level 8 filter | `compensation_grade in "Level 1" to "Level 8"` |
| Application scope | `job_application_date < 4-year cutoff` | Row dropped |
| Rescind correction | Rescinded + NULL hired + RFH stage + Offer Accepted status | Stage rolled back to "Offer"; disposition set to "Offer Accepted to Rescinded" |
| Stage rollback (RFH + disposition) | Stage is "Ready for Hire" AND disposition is not null | Stage rolled back to "Offer" |
| Unmapped stage name | Stage name not in STAGE_MAP | `last_stage_number` = NULL |
| In-process funnel rows | `candidate_recruiting_status == "Application in Process"` | Explode only stages **before** current; zero rows if at stage 1 |
| Last stage vs earlier rows | `stage_number == last_stage_number` | Keeps true status/disposition; earlier rows → "Passed" + NULL disposition |
| Missing enrichment columns | Column absent from join result | Silently excluded from final column selection |
| Empty recruiter ID | Digits-only extraction yields `""` | Replaced with `"zzz_blank"` |

---

## Output Schema

### `applications.parquet` / `applications.csv`

One row per candidate application.

| Column | Type | Source | Notes |
|---|---|---|---|
| `candidate_id` | Utf8 | Raw CSV | |
| `candidate_name` | Utf8 | Raw CSV | |
| `job_requisition_id` | Utf8 | Raw CSV | |
| `job_requisition` | Utf8 | Raw CSV | |
| `recruiting_instruction` | Utf8 | Raw CSV | |
| `job_family` | Utf8 | Raw CSV | |
| `compensation_grade` | Utf8 | Raw CSV | Level 9 excluded |
| `worker_type_hiring_requirement` | Utf8 | Raw CSV | |
| `worker_sub_type_hiring_requirement` | Utf8 | Raw CSV | |
| `target_hire_date` | Date | Raw CSV | |
| `added_date` | Date | Raw CSV | |
| `job_application_date` | Date | Raw CSV | Scoped to last 4 years |
| `offer_accepted_date` | Date | Raw CSV | |
| `candidate_start_date` | Date | Raw CSV | |
| `recruiter_employee_id` | Utf8 | Raw CSV | |
| `recruiter_completed_offer` | Utf8 | Raw CSV | |
| `disposition_reason` | Utf8 | Raw CSV (corrected) | |
| `candidate_recruiting_status` | Utf8 | Raw CSV (corrected) | |
| `last_recruiting_stage` | Utf8 | Raw CSV (corrected) | |
| `hired` | Utf8 | Raw CSV | |
| `hire_transaction_status` | Utf8 | Raw CSV | |
| `source` | Utf8 | Raw CSV | NULLs → "zzz_unknown" |
| `referred_by_employee_id` | Utf8 | Raw CSV | |
| `referred_by` | Utf8 | Raw CSV | |
| `recruiting_agency` | Utf8 | Raw CSV | |
| `last_employer` | Utf8 | Raw CSV | |
| `school_name` | Utf8 | Raw CSV | |
| `is_cancelled` | Utf8 | Raw CSV | |
| `mbps_teams` | Utf8 | Raw CSV | |
| `last_recruiting_stage_orig` | Utf8 | Engineered | Pre-correction snapshot |
| `candidate_recruiting_status_orig` | Utf8 | Engineered | Pre-correction snapshot |
| `disposition_reason_orig` | Utf8 | Engineered | Pre-correction snapshot |
| `recruiter_completed_offer_id` | Utf8 | Engineered | Digits only; blank → "zzz_blank" |
| `last_stage_number` | Int8 | Engineered | Mapped from stage name via STAGE_MAP |
| `consolidated_disposition` | Utf8 | dispo_map join | |
| `consolidated_disposition_2` | Utf8 | dispo_map join | |
| `consolidated_channel` | Utf8 | source_map join | |
| `channel_sort` | Utf8 | source_map join | |
| `internal_external` | Utf8 | source_map join | |
| `is_non_auto_dispo` | Utf8 | dispo_map join | |
| `is_candidate_driven_dispo` | Utf8 | dispo_map join | |
| `function` | Utf8 | jobreq join | |
| `sub_function` | Utf8 | jobreq join | |
| `rag_target_offer_acceptance_date` | Date | jobreq join | |
| `on_time_offer_accept` | Utf8 | Engineered | "Yes" or "" |
| `complexity` | Utf8 | jobreq join | |

### `app_funnel.parquet`

One row per candidate per funnel stage reached. Derived from `applications`.

| Column | Type | Notes |
|---|---|---|
| `job_requisition_id` | Utf8 | |
| `candidate_id` | Utf8 | |
| `added_date` | Date | |
| `job_application_date` | Date | Truncated to month-start |
| `last_stage_number` | Int16 | Coerced (5→4, 7→6) |
| `stage_number` | Int16 | Stage for this row (1,2,3,4,6,8) |
| `stage` | Utf8 | Label: "Review", "Screen", etc. |
| `candidate_recruiting_status` | Utf8 | "Passed" for non-last rows; true status for last |
| `recruiting_agency` | Utf8 | |
| `source` | Utf8 | |
| `consolidated_channel` | Utf8 | |
| `internal_external` | Utf8 | |
| `disposition_reason` | Utf8 | NULL for non-last rows |
| `is_dispo` | Utf8 | From dispo_map join |
| `consolidated_disposition` | Utf8 | From dispo_map join |
| `consolidated_disposition_2` | Utf8 | From dispo_map join |
| `is_non_auto_dispo` | Utf8 | From dispo_map join |
| `is_candidate_driven_dispo` | Utf8 | From dispo_map join |
| `in_process_count` | Int8 | 1 if still in-process at this stage |
| `completed_count` | Int8 | 1 if completed this stage |
| `on_time_offer_accept` | Utf8 | NULL for non-last rows |

### `applications_offer_accepts.parquet`

Filtered slice of `applications` where candidate_recruiting_status = "Offer Accepted".
Schema is identical to `applications.parquet`.

---

## Data Flow Summary

```
config["RAW_DATA_ROOT"]
    │
    ├── DAILY_APP_PATTERN CSV  (skip 16 rows)
    │       rename columns → parse dates
    │       filter: job_application_date >= 4-year cutoff
    │       filter: compensation_grade = "Level 1" to "Level 8"
    │       snapshot originals (stage, status, disposition)
    │       apply rescind correction
    │       derive: recruiter_completed_offer_id
    │       select final column order
    │
    ├── applications.csv / applications.parquet  ←── full apps LazyFrame
    │
    ├── applications_offer_accepts.parquet               ←── filter(candidate_recruiting_status = "Offer Accepted")
    │
    └── app_funnel.parquet                       ←── _transform_funnel(apps_df)
                │
                ├── truncate job_application_date to month
                ├── tag in-process candidates
                ├── build per-row stage list
                │       in-process → stages BEFORE current
                │       completed  → stages UP TO AND INCLUDING current
                ├── explode to one row per stage
                ├── join FUNNEL_STAGE_MAP → stage label

```
