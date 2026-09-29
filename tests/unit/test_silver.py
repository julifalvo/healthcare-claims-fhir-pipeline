import json
from datetime import datetime

import pytest

pytestmark = pytest.mark.spark


def _eob(line_reasons: list[str | None]) -> str:
    def adjudication(category, amount, reason=None):
        entry = {"category": {"coding": [{"code": category}]}, "amount": {"value": amount}}
        if reason:
            entry["reason"] = {"coding": [{"code": reason}]}
        return entry

    items = [
        {
            "sequence": i,
            "adjudication": [
                adjudication("submitted", 100.0),
                adjudication("eligible", 0.0 if reason else 60.0),
                adjudication("benefit", 0.0 if reason else 50.0, reason),
            ],
        }
        for i, reason in enumerate(line_reasons, 1)
    ]
    return json.dumps(
        {
            "resourceType": "ExplanationOfBenefit",
            "id": "eob-1",
            "meta": {"versionId": "1", "lastUpdated": "2026-02-01T10:00:00Z"},
            "claim": {"reference": "Claim/clm-1"},
            "created": "2026-02-01T10:00:00Z",
            "item": items,
            "total": [
                {
                    "category": {"coding": [{"code": "submitted"}]},
                    "amount": {"value": 100.0 * len(items)},
                },
                {"category": {"coding": [{"code": "benefit"}]}, "amount": {"value": 50.0}},
            ],
        }
    )


@pytest.mark.parametrize(
    ("reasons", "status", "denied_amount", "reason_code"),
    [
        ([None, None], "paid", 0, None),
        (["197", "197"], "denied", 200, "197"),
        ([None, "50"], "partially_denied", 100, "50"),
    ],
)
def test_eob_parsing_derives_claim_status(spark, reasons, status, denied_amount, reason_code):
    from claims_pipeline.jobs.silver import ENTITIES, parse

    bronze = spark.createDataFrame(
        [(_eob(reasons), "f", datetime(2026, 2, 1).date(), "h")],
        "raw STRING, source_file STRING, export_date DATE, record_hash STRING",
    )
    row = parse(bronze, ENTITIES["eob"]).collect()[0]

    assert row["claim_id"] == "clm-1"
    assert row["claim_status"] == status
    assert row["denied_amount"] == denied_amount
    assert row["denial_reason_code"] == reason_code


def test_patient_scd2_keeps_history_and_is_idempotent(spark, tmp_path):
    from claims_pipeline.jobs.silver import scd2_merge

    path = str(tmp_path / "patient")
    schema = "patient_id STRING, state STRING, version_id INT, last_updated TIMESTAMP"

    def load(rows):
        df = spark.createDataFrame(rows, schema)
        scd2_merge(spark, df, path, "patient_id", ("state",))
        return {
            (r["patient_id"], r["state"], r["is_current"])
            for r in spark.read.format("delta").load(path).collect()
        }

    v1 = ("p1", "TX", 1, datetime(2026, 1, 1))
    v2 = ("p1", "OK", 2, datetime(2026, 3, 1))
    v3_same_state = ("p1", "OK", 3, datetime(2026, 4, 1))

    assert load([v1]) == {("p1", "TX", True)}
    moved = {("p1", "TX", False), ("p1", "OK", True)}
    assert load([v2]) == moved
    assert load([v2]) == moved, "replaying a batch must not duplicate history"
    assert load([v3_same_state]) == moved, "a version without attribute changes is not new history"

    history = spark.read.format("delta").load(path).orderBy("valid_from").collect()
    assert history[0]["valid_to"] == history[1]["valid_from"]
    assert history[1]["valid_to"] is None
