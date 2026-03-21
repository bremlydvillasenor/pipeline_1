# Pseudocode: `src/hire.py` — Hires ETL Pipeline

## Overview

Processes incoming Workday hire records and merges them into a master historical
hires database (`hiresdb.parquet`). The pipeline is exception-aware: manually
overridden values in the master take precedence over incoming updates.

```
Raw Excel Files
    │
    ├── Fulfillment Report   ──┐
    └── INTX Hires Report    ──┴──► build_incoming_hires_file()
                                          │
                                          ▼
                               Incoming LazyFrame (joined, deduped)
                                          │
                                          ▼
                               upsert_merge(master, incoming, key="jrpos_id")
                                          │
                                          ▼
                               Merged DataFrame
                                    │          │
                                    ▼          ▼
                         discrepancy     write_parquet_atomic()
                           report        → hiresdb.parquet
                                         → hires.parquet
```

---

## Constants and Schemas

### Composite Key Definitions

| Key Column  | Formula                                        | Purpose                        |
|-------------|------------------------------------------------|--------------------------------|
| `jrpos_id`  | `job_requisition_id + "_" + position_id`       | Primary merge key              |
| `jree_id`   | `job_requisition_id + "_" + employee_id`       | JR-to-employee linkage         |
| `eehire_id` | `employee_id + "_" + candidate_start_date(YYYYMM)` | Employee-month join key   |

### Exception Columns (Preserve Manual Overrides)

Certain columns have a paired `*_exception` column in the master. During upsert,
the exception value always wins over the incoming value:

| Base Column           | Exception Column              |
|-----------------------|-------------------------------|
| `is_hire_on_time`     | `is_hire_on_time_exception`   |
| `rfh_recruiter_id`    | `rfh_recruiter_id_exception`  |
| `source`              | `source_exception`            |

### Date Parse Rules (column-specific formats)

| Column                  | Format                          |
|-------------------------|---------------------------------|
| `offer_accepted_date`   | `%Y/%m/%d`                      |
| `job_application_date`  | `%Y-%m-%d %H:%M:%S%.f` → date  |
| `ready_for_hire_date`   | `%Y-%m-%d %H:%M:%S%.f` → date  |
| All other `*_date`      | Try `%Y-%m-%d %H:%M:%S`, fallback to `%Y-%m-%d` |

---

## Stage 1: EXTRACT — `extract_incoming_hires_files(config)`

```
FUNCTION extract_incoming_hires_files(config):
    raw_root ← config["RAW_DATA_ROOT"]

    fulfillment_path ← latest_file(raw_root, FULFILLMENT_PATTERN)
    intx_path        ← latest_file(raw_root, INTX_HIRES_PATTERN)

    fulfillment ← read_excel(fulfillment_path, skip_rows=7)
    intx        ← read_excel(intx_path,        skip_rows=10)

    LOG "Extracted — fulfillment: N rows, intx: M rows"

    RETURN { "fulfillment": fulfillment, "intx": intx }
```

### `read_excel(path, skip_rows)` Helper

```
FUNCTION read_excel(path, skip_rows):
    raw ← pl.read_excel(path, has_header=False, skip_rows=skip_rows)
    header ← raw.row(0)           # First row after skip = real header
    df ← raw.slice(1)             # Remaining rows = data
    df.columns ← header
    RETURN df
```

> **Why:** Workday Excel exports have metadata rows at the top before the actual
> column headers, so headers must be extracted manually.

---

## Stage 2: TRANSFORM — `build_incoming_hires_file(fulfillment, intx)`

### 2a. Shared Setup

```
cutoff ← first day of month, 3 months ago
```

> Only hires with `candidate_start_date >= cutoff` are retained.

### 2b. Fulfillment Branch

```
fulfillment_lf ← select_rename(fulfillment, FULFILLMENT_RENAME_MAP)

FOR EACH column ending in "_date":
    parse_date(column)          # in-place date casting

FILTER:
    candidate_start_date >= cutoff
    AND grade_proposed NOT IN ["Level 9", "Level 10"]

DERIVE:
    business_process_reason ← split(" > ")[0]   # Keep only top-level category
    jrpos_id  ← job_requisition_id + "_" + position_id
    eehire_id ← employee_id + "_" + YYYYMM(candidate_start_date)
    jree_id   ← job_requisition_id + "_" + employee_id
```

### 2c. INTX Branch

```
intx_lf ← select_rename(intx, INTX_RENAME_MAP)

FOR EACH column ending in "_date":
    parse_date(column)

FILTER:
    candidate_start_date >= cutoff

DERIVE:
    rfh_recruiter_id ←
        IF recruiter_completed_offer matches r"\((\d+)\)"  → extract digits
        ELSE                                               → recruiter_employee_id

    days_frozen ←
        IF jr_unfreeze_date IS NULL → 0
        ELSE                        → days_frozen (as-is)

    offer_accepted_date ←
        COALESCE(offer_accepted_date, ready_for_hire_date)
        # Falls back to RFH date when offer date is missing

    eehire_id ← employee_id + "_" + YYYYMM(candidate_start_date)

DROP: candidate_start_date, employee_id, recruiter_employee_id
    # These came from fulfillment; INTX versions are redundant after derivation
```

### 2d. Join and Deduplicate

```
incoming ←
    fulfillment_lf
    LEFT JOIN intx_lf ON eehire_id
    SORT BY (candidate_start_date, employee_id)
    DEDUPLICATE ON eehire_id, KEEP last
    # Sort-then-keep-last ensures the most recent record per employee-month wins
```

---

## Stage 3: UPSERT MERGE — `upsert_merge(master, incoming, key_col)`

Merges incoming records into the master database with exception-column awareness.

```
FUNCTION upsert_merge(master, incoming, key_col="jrpos_id"):

    update_cols ← columns in BOTH incoming AND master, EXCLUDING key_col

    joined ← master LEFT JOIN incoming ON key_col (suffix "_upd" for incoming cols)

    FOR EACH col IN update_cols:
        exc_col ← col + "_exception"

        IF exc_col EXISTS in master:
            # Exception-aware resolution: manual override wins
            new_val ←
                IF   exc_col  IS NOT NULL → exc_col       # Manual override
                ELIF col_upd  IS NOT NULL → col_upd       # Incoming update
                ELSE                      → col (master)  # Keep existing
        ELSE:
            # Standard resolution: incoming wins if present
            new_val ← COALESCE(col_upd, col)

        SET col ← new_val

    updated_rows ← joined WITH coalesced columns, SELECT master column order

    # Append brand-new records not yet in master
    new_rows ← incoming ANTI JOIN master ON key_col
    new_rows_aligned ← new_rows + NULL columns for any schema gaps

    VALIDATE schema of new_rows_aligned against HIRES_SCHEMA

    RETURN CONCAT(updated_rows, new_rows_aligned)
```

### Decision Logic for Exception-Aware Merge

```
Priority (highest to lowest):
  1. *_exception column  → manual analyst override, never overwritten
  2. Incoming (_upd)     → fresh Workday data
  3. Master (existing)   → historical value, kept as fallback
```

---

## Stage 4: SCHEMA VALIDATION

### `validate_and_align_schema(df, expected_schema, df_name)`

```
FUNCTION validate_and_align_schema(df, expected_schema, df_name):
    IF df is empty:
        WARN and RETURN empty DataFrame with expected_schema

    missing_cols ← columns in expected_schema NOT in df
    IF missing_cols:
        WARN "missing columns added as NULL: [...]"

    FOR EACH (col, dtype) in expected_schema:
        IF col is missing → add as NULL cast to dtype
        ELSE              → cast existing col to dtype (non-strict)

    RETURN df reordered and cast to expected_schema
```

### `schema_check_lf(lf, target_schema)` — Hard Validation

```
FUNCTION schema_check_lf(lf, target_schema):
    actual_schema ← lf.collect_schema()

    missing      ← target_schema keys NOT in actual
    type_mismatch ← columns where actual dtype ≠ expected dtype

    IF missing OR type_mismatch:
        RAISE ValueError with detail of missing + mismatched columns
    # Extra columns are allowed (logged but not an error)
```

---

## Stage 5: DISCREPANCY REPORT — `generate_discrepancy_report(hires_df, jobreq_df)`

Cross-checks the merged hires count per JR against the `positions_filled_hire_selected`
count from the job requisitions table.

```
FUNCTION generate_discrepancy_report(hires_df, jobreq_df):

    hires_agg ←
        hires_df
        GROUP BY job_requisition_id
        AGGREGATE count(*) AS hires_count

    jobreq_sel ←
        jobreq_df
        SELECT job_requisition_id, positions_filled_hire_selected (fill NULL → 0)
        DEDUPLICATE ON job_requisition_id

    discrepancies ←
        jobreq_sel
        LEFT JOIN hires_agg ON job_requisition_id
        hires_count ← fill NULL → 0
        discrepancy ← ABS(jobreq_filled_count - hires_count)
        FILTER discrepancy != 0

    IF discrepancies > 0:
        WARN "⚠️  N JR discrepancies detected"
    ELSE:
        LOG  "✅ No discrepancies found."

    RETURN discrepancies DataFrame
```

---

## Stage 6: LOAD — `write_parquet_atomic(df, target)`

```
FUNCTION write_parquet_atomic(df, target, compression="zstd"):
    ASSERT target ends with ".parquet"

    tmp ← target + ".tmp"

    TRY:
        df.write_parquet(tmp, compression=compression)
        tmp.rename → target          # Atomic replace
    EXCEPT:
        RAISE
    FINALLY:
        IF tmp still exists → DELETE tmp   # Always clean up temp file
```

> **Why atomic:** Prevents a partially written file from being read if the
> pipeline crashes mid-write. The target is only replaced after a full
> successful write.

---

## Main Orchestrator — `run_hire_pipeline(config, incoming_df, master_df)`

```
FUNCTION run_hire_pipeline(config, incoming_df=None, master_df=None):
    LOG "🚀 Hires ETL started"

    # ── EXTRACT ──────────────────────────────────────────
    IF incoming_df is None:
        raw        ← extract_incoming_hires_files(config)
        incoming   ← build_incoming_hires_file(raw.fulfillment, raw.intx)
    ELSE:
        incoming ← incoming_df   # Injected (e.g., for testing)

    # ── LOAD MASTER ──────────────────────────────────────
    IF master_df is None:
        hiresdb_path ← config["OUTPUT_HIRESDB_PAR"]
        IF NOT exists → RAISE FileNotFoundError
        master_df ← read_parquet(hiresdb_path)

    master_df ← validate_and_align_schema(master_df, HIRES_SCHEMA, "Master")

    # ── MERGE ─────────────────────────────────────────────
    merged_df ← upsert_merge(master_df, incoming, key_col="jrpos_id").collect()

    # ── DISCREPANCY CHECK ─────────────────────────────────
    jobreq_df ← validate_and_align_schema(
        read_parquet(config["OUTPUT_JOBREQS_PAR"]),
        JOBREQ_SCHEMA,
        "Jobreq",
    )
    PRINT generate_discrepancy_report(merged_df, jobreq_df)

    # ── DEBUG DUMP ────────────────────────────────────────
    incoming.collect().write_parquet("incoming.parquet")   # Dev artifact

    # ── PERSIST ───────────────────────────────────────────
    write_parquet_atomic(merged_df, config["OUTPUT_HIRESDB_PAR"])  # Accumulating DB
    write_parquet_atomic(merged_df, config["OUTPUT_HIRES_PAR"])    # Current snapshot

    LOG "✅ Hires ETL completed — N rows"
    RETURN merged_df
```

### Key Decision Points in Orchestrator

| Decision | Condition | Outcome |
|---|---|---|
| Skip extract | `incoming_df` passed in | Use provided DataFrame (enables testing without files) |
| Skip master load | `master_df` passed in | Use provided DataFrame (enables testing without files) |
| Master missing | `hiresdb.parquet` not found | Hard fail — cannot merge without a base |
| Schema gap | Master missing expected columns | Add as NULLs, log warning, continue |
| Schema mismatch | Type conflict or missing in new rows | Hard fail with `ValueError` |
| Discrepancy found | JR fill count ≠ hires count | Log warning, continue (non-blocking) |
| Write failure | Exception during parquet write | Temp file cleaned up, exception re-raised |

---

## Data Flow Summary

```
config["RAW_DATA_ROOT"]
    │
    ├── Fulfillment Report (skip 7 rows)
    │       select/rename → parse dates → filter cutoff + grade
    │       derive: jrpos_id, eehire_id, jree_id, business_process_reason
    │
    └── INTX Hires Report (skip 10 rows)
            select/rename → parse dates → filter cutoff
            derive: rfh_recruiter_id, days_frozen, offer_accepted_date, eehire_id
            drop: candidate_start_date, employee_id, recruiter_employee_id
            │
            └── LEFT JOIN on eehire_id
                    sort → deduplicate (keep last per eehire_id)
                    │
                    └── INCOMING LazyFrame
                              │
                    UPSERT MERGE (key: jrpos_id)
                              │
                    master hiresdb.parquet ──────────┘
                              │
                    MERGED DataFrame
                    ├── discrepancy_report (vs jobreqs.parquet)
                    ├── write → hiresdb.parquet  (historical accumulation)
                    └── write → hires.parquet    (current snapshot)
```
