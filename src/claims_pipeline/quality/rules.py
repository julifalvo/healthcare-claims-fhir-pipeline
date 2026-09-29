"""Declarative record-level data quality rules.

Each rule is a Spark SQL predicate that a *valid* record satisfies. A NULL result counts as a
failure. Failing records are routed to quarantine with the list of rules they broke, so nothing
is silently dropped and every rejection is explainable.
"""

from dataclasses import dataclass
from datetime import UTC, datetime

from delta.tables import DeltaTable
from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from claims_pipeline.config import SILVER_DIR, table_path

QUARANTINE_TABLE = table_path(SILVER_DIR, "_quarantine")
DQ_RESULTS_TABLE = table_path(SILVER_DIR, "_dq_results")
RECORDS_METRIC = "__records__"

DDL = {
    QUARANTINE_TABLE: "entity STRING, record_id STRING, failed_rules ARRAY<STRING>, raw STRING, "
    "source_file STRING, export_date DATE, record_hash STRING, quarantined_at TIMESTAMP",
    DQ_RESULTS_TABLE: "export_date DATE, entity STRING, rule STRING, checked BIGINT, "
    "failed BIGINT, run_id STRING, evaluated_at TIMESTAMP",
}


@dataclass(frozen=True)
class Rule:
    name: str
    entity: str
    predicate: str
    description: str


def _json_rule(entity: str) -> Rule:
    return Rule(
        "valid_fhir_json",
        entity,
        "_is_valid_json",
        "Line is well-formed JSON (truncated or corrupt exports are caught here).",
    )


RULES: list[Rule] = [
    *[_json_rule(e) for e in ("patient", "organization", "encounter", "condition", "claim", "eob")],
    Rule("id_present", "patient", "patient_id IS NOT NULL", "Patient has a logical id."),
    Rule(
        "birth_date_not_in_future",
        "patient",
        "birth_date <= current_date()",
        "Birth date is known and not in the future.",
    ),
    Rule(
        "birth_date_plausible",
        "patient",
        "birth_date >= DATE'1900-01-01'",
        "Birth date is after 1900.",
    ),
    Rule("id_present", "organization", "organization_id IS NOT NULL", "Organization has an id."),
    Rule(
        "encounter_has_patient",
        "encounter",
        "patient_id IS NOT NULL",
        "Encounter.subject references a Patient.",
    ),
    Rule(
        "encounter_period_ordered",
        "encounter",
        "period_end >= period_start",
        "Encounter ends after it starts.",
    ),
    Rule(
        "icd10_code_format",
        "condition",
        r"icd10_code RLIKE '^[A-TV-Z][0-9][0-9A-Z](\\.[0-9A-Z]{1,4})?$'",
        "Diagnosis is a syntactically valid ICD-10-CM code.",
    ),
    Rule(
        "condition_has_patient",
        "condition",
        "patient_id IS NOT NULL",
        "Condition.subject references a Patient.",
    ),
    Rule("claim_has_patient", "claim", "patient_id IS NOT NULL", "Claim.patient is present."),
    Rule(
        "claim_total_non_negative",
        "claim",
        "total_billed >= 0",
        "Billed total is zero or positive.",
    ),
    Rule("claim_has_lines", "claim", "line_count > 0", "Claim has at least one line item."),
    Rule(
        "eob_references_claim",
        "eob",
        "claim_id IS NOT NULL",
        "ExplanationOfBenefit points to its Claim.",
    ),
    Rule(
        "eob_paid_within_billed",
        "eob",
        "paid_amount <= submitted_amount",
        "Payer never pays more than was billed.",
    ),
]


def rules_for(entity: str) -> list[Rule]:
    return [r for r in RULES if r.entity == entity]


def _failed(rule: Rule) -> Column:
    broken = ~F.coalesce(F.expr(rule.predicate), F.lit(False))
    if rule.name != "valid_fhir_json":
        # Corrupt lines fail only the JSON rule, so per-rule metrics stay meaningful.
        broken = F.col("_is_valid_json") & broken
    return F.when(broken, F.lit(rule.name))


def evaluate(df: DataFrame, entity: str) -> DataFrame:
    """Add `_failed_rules` (array<string>) listing every rule the record breaks."""
    rules = rules_for(entity)
    return df.withColumn("_failed_rules", F.array_compact(F.array(*[_failed(r) for r in rules])))


def split(evaluated: DataFrame) -> tuple[DataFrame, DataFrame]:
    valid = evaluated.where(F.size("_failed_rules") == 0).drop("_failed_rules")
    rejected = evaluated.where(F.size("_failed_rules") > 0)
    return valid, rejected


def metrics(evaluated: DataFrame, entity: str, run_id: str) -> DataFrame:
    """Checked/failed counts per export date and rule, plus a `__records__` row per date."""
    per_rule = [
        F.sum(F.array_contains("_failed_rules", r.name).cast("int")).alias(r.name)
        for r in rules_for(entity)
    ]
    wide = evaluated.groupBy("export_date").agg(
        F.count(F.lit(1)).alias("checked"),
        F.sum((F.size("_failed_rules") > 0).cast("int")).alias(RECORDS_METRIC),
        *per_rule,
    )
    names = [RECORDS_METRIC, *[r.name for r in rules_for(entity)]]
    stacked = F.expr(
        "stack({n}, {pairs}) AS (rule, failed)".format(
            n=len(names), pairs=", ".join(f"'{n}', `{n}`" for n in names)
        )
    )
    return wide.select(
        "export_date",
        F.lit(entity).alias("entity"),
        "checked",
        stacked,
    ).select(
        "export_date",
        "entity",
        "rule",
        F.col("checked").cast("long").alias("checked"),
        F.coalesce(F.col("failed"), F.lit(0)).cast("long").alias("failed"),
        F.lit(run_id).alias("run_id"),
        # A literal, not current_timestamp(): MERGE sources must be deterministic.
        F.lit(datetime.now(UTC)).alias("evaluated_at"),
    )


def persist(
    spark: SparkSession, rejected: DataFrame, dq_metrics: DataFrame, entity: str, id_column: str
) -> None:
    """Idempotently store quarantined records and DQ metrics (re-runs overwrite, never duplicate)."""
    quarantine = rejected.select(
        F.lit(entity).alias("entity"),
        F.col(id_column).alias("record_id"),
        F.col("_failed_rules").alias("failed_rules"),
        "raw",
        "source_file",
        "export_date",
        "record_hash",
        F.lit(datetime.now(UTC)).alias("quarantined_at"),
    )
    _merge(spark, quarantine, QUARANTINE_TABLE, entity, ["record_hash"])
    _merge(spark, dq_metrics, DQ_RESULTS_TABLE, entity, ["export_date", "rule"])


def ensure_tables(spark: SparkSession) -> None:
    """Create the shared DQ tables up front: parallel first writes to an empty Delta path race."""
    for path, columns in DDL.items():
        spark.sql(
            f"CREATE TABLE IF NOT EXISTS delta.`{path}` ({columns}) USING DELTA PARTITIONED BY (entity)"
        )


def _merge(spark: SparkSession, df: DataFrame, path: str, entity: str, keys: list[str]) -> None:
    """Silver tasks run in parallel and share these tables. Partitioning by entity plus a literal
    partition predicate in the MERGE lets Delta prove the writes are disjoint (no conflicts)."""
    condition = " AND ".join([f"t.entity = '{entity}'", *(f"t.{k} = s.{k}" for k in keys)])
    (
        DeltaTable.forPath(spark, path)
        .alias("t")
        .merge(df.alias("s"), condition)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
