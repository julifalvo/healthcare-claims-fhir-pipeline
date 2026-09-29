from datetime import date

import pytest

pytestmark = pytest.mark.spark

START, END = date(2026, 2, 1), date(2026, 2, 10)


def _silver_counts(spark):
    from claims_pipeline.jobs.silver import ENTITIES, silver_table

    return {
        name: spark.read.format("delta").load(silver_table(name)).count()
        for name in [*ENTITIES, "claim_line"]
    }


def test_full_pipeline_is_idempotent_and_reconciled(spark):
    from claims_pipeline.config import GOLD_DIR, table_path
    from claims_pipeline.pipeline import run_all

    first = run_all(spark, START, END, run_id="test-1")
    counts_after_first = _silver_counts(spark)
    second = run_all(spark, START, END, run_id="test-2")

    # An empty lakehouse bootstraps from the simulation start, then runs incrementally.
    assert first["window"]["start"] == "2026-01-01"
    assert second["window"]["start"] == START.isoformat()
    assert _silver_counts(spark) == counts_after_first
    assert first["gold"] == second["gold"]
    assert all(b["rows"] == b["expected"] for b in first["bronze"])
    assert all(m["rate"] < 0.05 for m in first["quality_gate"].values())

    fact = spark.read.format("delta").load(table_path(GOLD_DIR, "fact_claim"))
    assert fact.count() == fact.select("claim_id").distinct().count()
    statuses = {r["claim_status"] for r in fact.select("claim_status").distinct().collect()}
    assert statuses <= {"paid", "denied", "partially_denied", "pending"}
    assert "pending" in statuses and "paid" in statuses
