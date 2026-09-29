# ADR 0002: FHIR R4 Bulk Data as the ingestion contract

- Status: accepted
- Date: 2026-09-29

## Context

US payers and providers exchange clinical and claims data through HL7 FHIR. CMS interoperability
rules push payers toward FHIR APIs, and FHIR Bulk Data (`$export`) is the standard way to move
population-level data: NDJSON files, one per resource type, plus a manifest.

## Decision

The pipeline ingests FHIR R4 Bulk Data exports: `Patient`, `Organization`, `Encounter`,
`Condition`, `Claim` and `ExplanationOfBenefit`. Exports are incremental (`_since`), so each daily
folder carries only resources whose `meta.lastUpdated` falls on that day. The synthetic generator
produces this format, and a test validates the resources against the official R4B models
(`fhir.resources`).

- Bronze reconciles its row count against the manifest `count` and fails the task on any
  mismatch. Files are never partially loaded.
- Silver parses with explicit schemas that name only the fields the model uses. Unknown fields are
  ignored, so additive changes to the FHIR profile never break parsing.
- Adjudications (`ExplanationOfBenefit`) arrive days or weeks after their `Claim`, as they do in
  production. Silver upserts by id, and gold always joins the latest state.

## Consequences

- Swapping the generator for a real `$export` endpoint (EHR, payer API, Synthea) only changes the
  extract task.
- Incremental exports mean an empty lakehouse must load the history first. The DAG detects this
  and bootstraps from the first export date automatically (see the runbook).
