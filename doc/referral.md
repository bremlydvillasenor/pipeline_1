# referral.py — Referral ETL

## Overview

Builds a complete ERP/Referral recruiting funnel spanning **Prospect (stage 0)
through Ready for Hire (stage 8)**. Combines two source datasets — a pre-funnel
prospect Excel export and the application funnel parquet produced by `app.py` —
into a single deduplicated output, and produces a slim lookup table consumed by
`erp.py`.

---

## Inputs

| Source | Config Key | Description |
|---|---|---|
| Prospect Excel | `PROSPECT_PATTERN` | Referred candidates not yet in the ATS funnel (stage 0) |
| Funnel parquet | `OUTPUT_FUNNEL_PAR` | Full application funnel from `app.py`; filtered to ERP/Referral sources (stages 1–8) |

The source filter uses the regex `(?i)ERP|Referral` applied to the `source` column.

---

## Outputs

| Output | Config Key | Description |
|---|---|---|
| Referrals parquet | `OUTPUT_REFERRALS_PARQUET` | Full deduplicated ERP funnel (Prospect → Ready for Hire), zstd compressed |
| `unique_referrals_df` | *(in-memory)* | One row per `headcount_id` with `has_referral=1`; passed directly to `erp.py` |

---

## Stage Numbering

| Stage Label | `last_stage_number` |
|---|---|
| Prospect | 0 |
| Review | 1 |
| Screen | 2 |
| Assessment | 3 |
| Interview | 4 |
| Offer | 6 |
| Ready for Hire | 8 |

---

## ETL Flow

```
Prospect Excel                 Funnel parquet (app.py output)
      │                               │
      ▼                               ▼
_extract_prospect()          _extract_erp_funnel()
  stage = 0                    filter: source ~ ERP|Referral
                                stages 1–8
      │                               │
      └──────────── extract() ────────┘
                        │
                        ▼
            _apply_common_transforms()  (both DataFrames)
              • align to REFERRAL_SCHEMA
              • strip employee ID from referred_by
                "John Smith (12345)" → "John Smith"
              • null-safe key columns
                        │
                        ▼
            review_df = erp_funnel_df[ last_stage_number == 1 ]
                        │
                        ▼
            prospect_review_df = concat(prospect_df, review_df)
              • all rows stamped last_stage_number = 0
              • acts as top-of-funnel bridge
                        │
                        ▼
            full_erp_funnel_df = concat(erp_funnel_df, prospect_review_df)
              • spans stages 0 → 8
                        │
                        ▼
            Build erp_id  =  job_requisition_id + '_'
                            + referred_by_employee_id + '_'
                            + candidate_id
                        │
                        ▼
            Build headcount_id  =  referred_by_employee_id + '_' + YYYY-MM
              (derived from added_date)
                        │
                        ▼
            Deduplicate on erp_id  (keep last)
                        │
              ┌─────────┴──────────┐
              ▼                    ▼
      referrals_df          unique_referrals_df
   (full funnel)        (headcount_id, has_referral=1)
              │
              ▼
      write_parquet(zstd)
```

---

## Key Identifiers

### `erp_id`
Composite deduplication key for a single referral record:
```
{job_requisition_id}_{referred_by_employee_id}_{candidate_id}
```

### `headcount_id`
Links a referring employee to a calendar month, used to join into the ERP
headcount table:
```
{referred_by_employee_id}_{YYYY-MM}
```
where `YYYY-MM` is derived from `added_date`.

---

## Output Schema (`REFERRAL_SCHEMA`)

| Column | Type | Notes |
|---|---|---|
| `candidate_id` | `Utf8` | |
| `job_requisition_id` | `Utf8` | |
| `referred_by_employee_id` | `Utf8` | |
| `referred_by` | `Utf8` | Employee ID suffix stripped |
| `added_date` | `Date` | Parsed from multiple date formats |
| `source` | `Utf8` | Contains "ERP" or "Referral" |
| `disposition_reason` | `Utf8` | |
| `consolidated_disposition` | `Utf8` | From funnel only; null for prospects |
| `candidate_recruiting_status` | `Utf8` | |
| `last_stage_number` | `Int16` | 0 for all prospect_review rows |

---

## Public Interface

```python
from src.referral import run_referral

referrals_df, unique_referrals_df = run_referral(config)
```

| Return value | Type | Description |
|---|---|---|
| `referrals_df` | `pl.DataFrame` | Full deduplicated ERP funnel |
| `unique_referrals_df` | `pl.DataFrame` | `headcount_id` + `has_referral=1`; input to `erp.py` |

---

## Dependencies

- **Upstream:** `app.py` must run first to produce `OUTPUT_FUNNEL_PAR`
- **Downstream:** `erp.py` consumes `unique_referrals_df`
