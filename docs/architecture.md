# Architecture

## Data flow

```mermaid
flowchart LR
    subgraph src["Source: FHIR R4 Bulk Data"]
        EXP["$export?_since=day<br/>NDJSON per resource<br/>+ manifest.json"]
    end

    subgraph lake["Delta Lake lakehouse"]
        direction LR
        B[("Bronze<br/>raw JSON lines<br/>+ lineage")]
        S[("Silver<br/>typed entities<br/>MERGE / SCD2")]
        Q[("Quarantine<br/>+ DQ metrics")]
        G[("Gold<br/>star schema<br/>+ marts")]
    end

    subgraph consume["Consumers"]
        D["Streamlit dashboard<br/>(delta-rs)"]
        W["Appeals worklist CSV<br/>(asset-triggered DAG)"]
    end

    EXP -->|"reconcile counts<br/>vs manifest"| B
    B -->|"parse · validate"| S
    B -.->|"rule failures"| Q
    S -->|"quality gate<br/>≤ 5% quarantined"| G
    G --> D
    G --> W
```

## One daily run

```mermaid
sequenceDiagram
    autonumber
    participant S as Scheduler
    participant X as extract
    participant B as ingest_bronze[×6]
    participant V as conform_silver.*
    participant Q as quality_gate
    participant G as build_gold
    participant W as denials_worklist DAG

    S->>X: resolve_window (yesterday, or full history if the lake is empty)
    X->>X: write export_date=YYYY-MM-DD/*.ndjson atomically
    X->>B: dynamic task mapping, one task per resource type (pool: spark=2)
    B->>B: replaceWhere(export_date) + reconcile with manifest
    B->>V: 6 silver tasks in parallel (after DDL for shared DQ tables)
    V->>V: parse → rules → quarantine | dedupe → MERGE
    V->>Q: all silver done
    alt quarantine rate > threshold
        Q-->>S: AirflowFailException (no retries), gold untouched
    else healthy
        Q->>G: rebuild gold in Spark SQL (atomic overwrite)
        G-->>W: asset event gold_fact_claim → triggers worklist export
    end
```

## Late-arriving adjudications

A claim is exported on the day it is submitted. Its ExplanationOfBenefit shows up in a later
export, between 10 and 70 days afterwards depending on the payer. Silver upserts EOBs by id,
and gold joins each claim to its latest adjudication on every run. So:

- claims without an EOB are `pending` and age in `mart_ar_aging`;
- when the EOB lands, the next run flips the claim to `paid`, `denied` or `partially_denied`, with
  no special handling;
- about 1.5% of claims never get a response. They surface as 90+ day A/R, which is exactly the
  follow-up list a billing team needs.

## Idempotency by layer

| Stage | Mechanism | Re-run effect |
|---|---|---|
| Extract | write to `.tmp_export_date=…`, then rename | same files |
| Bronze | `replaceWhere export_date IN (…)` | same partitions replaced |
| Silver | `MERGE` on business key, newer `last_updated` wins | no-op |
| Patient SCD2 | rebuild the affected timelines, one `MERGE` with upsert + delete | no-op |
| Quarantine / DQ | `MERGE` on `(entity, record_hash)` / `(entity, export_date, rule)` | no-op |
| Gold | overwrite (single Delta commit) | identical tables |

The end-to-end test runs the pipeline twice over the same window and asserts identical silver
row counts and gold outputs. A separate test proves the generator gives the same output across
processes: a set-iteration-order bug once shifted ids between runs and broke the MERGEs.

## Scaling notes

- Spark runs in local mode inside the Airflow worker. Point `SPARK_MASTER` at a cluster, or swap
  the task bodies for `SparkSubmitOperator` / Databricks operators. The job modules do not change (ADR 0003).
- Bronze and silver are processed per export-date window, so daily runs touch one partition.
- Gold is a full rebuild because it is small. The documented path to incremental is a `MERGE` of
  `fact_claim` restricted to claims touched in the window.
- `lakehouse_maintenance` compacts and Z-orders silver on the join keys used by gold
  (`patient_id`, `payer_id`, `claim_id`).
