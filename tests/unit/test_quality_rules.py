import pytest

pytestmark = pytest.mark.spark


def test_rules_flag_every_broken_rule_and_keep_valid_records(spark):
    from claims_pipeline.quality import rules

    df = spark.createDataFrame(
        [
            ("ok", "pat-1", 100.0, 2, True),
            ("no-patient", None, 100.0, 1, True),
            ("negative-and-empty", "pat-1", -5.0, 0, True),
            ("corrupt", None, None, 0, False),
        ],
        "claim_id STRING, patient_id STRING, total_billed DOUBLE, line_count INT, "
        "_is_valid_json BOOLEAN",
    )
    evaluated = rules.evaluate(df, "claim")
    failed = {r["claim_id"]: sorted(r["_failed_rules"]) for r in evaluated.collect()}

    assert failed["ok"] == []
    assert failed["no-patient"] == ["claim_has_patient"]
    assert failed["negative-and-empty"] == ["claim_has_lines", "claim_total_non_negative"]
    # A corrupt line only fails the JSON rule, so per-rule metrics are not inflated.
    assert failed["corrupt"] == ["valid_fhir_json"]

    valid, rejected = rules.split(evaluated)
    assert [r["claim_id"] for r in valid.collect()] == ["ok"]
    assert rejected.count() == 3


def test_icd10_format_rule(spark):
    from claims_pipeline.quality import rules

    codes = ["I10", "S72.001A", "E11.9", "I509X", "UNKNOWN", "1234", None]
    df = spark.createDataFrame(
        [(c, "pat-1", True) for c in codes],
        "icd10_code STRING, patient_id STRING, _is_valid_json BOOLEAN",
    )
    valid, _ = rules.split(rules.evaluate(df, "condition"))
    assert sorted(r["icd10_code"] for r in valid.collect()) == ["E11.9", "I10", "S72.001A"]
