# ADR 0003: Spark in local mode inside Airflow tasks

- Status: accepted
- Date: 2026-09-29

## Context

The project must run on a laptop with one command and still use the engine a production claims
platform would use at scale.

## Decision

Each Spark task starts a local-mode `SparkSession` inside the Airflow worker (LocalExecutor), with
Delta Lake jars resolved at image build time. An Airflow pool (`spark`, 2 slots) bounds
concurrent JVMs, so memory stays predictable no matter how many tasks are ready.

Jobs are plain functions in `claims_pipeline.jobs` that receive a `SparkSession`. Airflow tasks,
the CLI and the tests all call the same functions.

## Consequences

- To move to a cluster, set `SPARK_MASTER` or replace the `@task` bodies with
  `SparkSubmitOperator` / Databricks / EMR operators that call the same job modules.
  The job code does not change.
- Each task pays about 10 s of JVM startup. That is acceptable for a daily batch. A long-lived
  Spark Connect server would remove it.
- Parallel tasks writing to shared Delta tables need care: shared tables are created by a DDL
  task before the fan-out, and MERGEs carry literal partition predicates so Delta can prove the
  writes are disjoint (see ADR 0004).
