## What and why

<!-- The change and the business or engineering reason for it. -->

## Data impact

- [ ] Schema change in silver/gold (describe migration or backfill needed)
- [ ] New or changed data quality rule
- [ ] Changes gold metric definitions (dashboard or worklist consumers affected)

## Checklist

- [ ] `make lint` and `make test` pass
- [ ] Re-running the same window is still idempotent (no duplicated rows)
- [ ] Docs / ADR updated if a design decision changed
