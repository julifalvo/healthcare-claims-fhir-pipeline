# Runbook

Operational guide for `claims_lakehouse` and its downstream DAGs.

## Schedules

| DAG | Schedule | Purpose |
|---|---|---|
| `claims_lakehouse` | daily 03:00 UTC | Process the previous day's `$export` end to end |
| `denials_worklist` | on update of asset `gold_fact_claim` | Write the appeals worklist CSV to `lakehouse/exports/` |
| `lakehouse_maintenance` | Sundays 05:00 UTC | `OPTIMIZE ... ZORDER BY`, `VACUUM` (7-day retention) on silver |

## First deployment

Incremental exports only carry changes, so an empty lakehouse cannot be built from a single day.
`resolve_window` detects the empty lake (no `silver/claim/_delta_log`) and widens the first run
back to the first export date. Unpause the DAG and wait for the first run (about 6 min locally).

## Backfill or reprocess a range

Trigger with params. Every stage is idempotent, so overlapping an already processed range is safe:

```bash
docker compose exec airflow-scheduler airflow dags trigger claims_lakehouse \
  --conf '{"start_date": "2026-03-01", "end_date": "2026-03-31"}'
```

The UI's "Trigger" form shows the same two date params.

## The quality gate failed

`quality_gate` fails without retrying when an entity quarantines more than `MAX_QUARANTINE_RATE`
of its records. Gold is not rebuilt, so the dashboard keeps showing the last good data.

1. Read the task log. It prints the rate per entity.
2. Find the breaking rule:
   ```sql
   SELECT entity, rule, sum(failed) FROM delta.`/opt/airflow/lakehouse/silver/_dq_results`
   WHERE export_date >= DATE'2026-09-01' GROUP BY ALL ORDER BY 3 DESC
   ```
3. Inspect raw payloads in `silver/_quarantine` (`raw` column).
4. Either fix upstream and re-export the day, or fix the parser/rule and re-run the window.
   Bronze keeps the original lines, so no re-export is needed for a parser fix.

## Bronze reconciliation failed

`ingest_bronze[<Resource>]` raises `ReconciliationError` when the rows landed differ from the
manifest `count`. The export is incomplete or corrupt: re-run `extract_fhir_bulk_export` for the
day (it overwrites the folder atomically) and clear the downstream tasks.

## A task failed for an infrastructure reason

Spark tasks retry twice with exponential backoff. If retries are exhausted, clear the task (and
downstream) from the UI. The stage re-processes the same window idempotently.

## Useful checks

```bash
make status                       # recent runs
docker compose logs -f airflow-scheduler
docker compose exec airflow-scheduler ls /opt/airflow/lakehouse/exports
```
