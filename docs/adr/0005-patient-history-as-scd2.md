# ADR 0005: Patient history as SCD Type 2

- Status: accepted
- Date: 2026-09-29

## Context

Patients move, and address drives network, state regulation and population-health reporting.
Overwriting the address loses the state at the time of service.

## Decision

`silver.patient` keeps Type 2 history keyed on `(patient_id, valid_from)` with `valid_to` and
`is_current`. A change is detected by hashing the tracked attributes, so a new FHIR version that
changes nothing tracked does not create history.

The merge rebuilds the timeline of only the patients present in the batch (existing rows + new
versions), then applies it in a single Delta `MERGE` that upserts the new timeline and deletes
rows it supersedes.

## Consequences

- Late or replayed versions land in the correct place. Re-running a batch is a no-op. The unit
  tests cover initial load, change, replay and "new version, no change".
- `gold.dim_patient` exposes the current row. Point-in-time joins can use `valid_from`/`valid_to`.
