# ADR 0004: Quarantine bad records, gate gold on quality

- Status: accepted
- Date: 2026-09-29

## Context

Source systems send malformed JSON, missing references, impossible dates and invalid codes.
Dropping those records silently skews revenue metrics. Failing the whole load for one bad row
blocks everyone.

## Decision

- Rules are declarative Spark SQL predicates (`quality/rules.py`). A NULL result counts as a failure.
- Every failing record goes to `silver/_quarantine` with the rules it broke and its raw payload,
  keyed by content hash so re-runs never duplicate it. Valid records continue to silver.
- Per rule and per export date, `checked`/`failed` counts are stored in `silver/_dq_results` and
  published as `gold.mart_data_quality`.
- A quality gate between silver and gold fails the run (`AirflowFailException`, no retries) when
  any entity quarantines more than `MAX_QUARANTINE_RATE` (default 5%). Gold keeps serving the
  last good version.
- Both DQ tables are partitioned by entity and created by DDL before the parallel silver tasks.
  Each MERGE adds `t.entity = '<entity>'`, so concurrent writers never conflict.

## Consequences

- Every rejected record is explainable and replayable after a fix.
- A systemic upstream break (for example, a schema change that nulls every patient reference) stops
  the pipeline before it reaches dashboards. Isolated noise does not.
