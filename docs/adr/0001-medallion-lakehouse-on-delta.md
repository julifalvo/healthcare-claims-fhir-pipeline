# ADR 0001: Medallion lakehouse on Delta Lake

- Status: accepted
- Date: 2026-09-29

## Context

Claims data arrives as daily FHIR Bulk Data exports. Analysts need typed, deduplicated tables,
finance needs numbers that do not change between refreshes without a reason, and engineering
needs to replay history after fixing a bug. A plain Parquet folder gives none of ACID writes,
upserts or time travel.

## Decision

Store every layer as Delta Lake tables, organized as a medallion:

| Layer | Content | Write pattern |
|---|---|---|
| Landing | Bulk Data NDJSON + `manifest.json`, one folder per `export_date` | atomic folder swap |
| Bronze | One raw JSON line per row plus lineage (file, hash, run) | `replaceWhere` on `export_date` partitions |
| Silver | Typed entities, validated, deduplicated | `MERGE` (SCD1), SCD2 for patients |
| Gold | Star schema and business marts | full overwrite (one atomic commit) |

## Consequences

- Every stage is idempotent: re-running a window replaces exactly the same partitions or keys.
- Bronze keeps the untouched payload, so any silver/gold bug is fixed by replaying bronze. No re-export is needed.
- Gold is cheap to rebuild at this volume. If it grows, `fact_claim` switches to `MERGE` by
  `claim_id`, restricted to the claims touched in the window. The public schema stays the same.
- Delta OSS tables are readable without Spark (delta-rs), which is how the dashboard and the
  worklist export read gold.
