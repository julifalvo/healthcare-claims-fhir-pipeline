"""Silver: typed, validated, deduplicated FHIR entities stored as Delta tables.

bronze (raw JSON lines) -> parse with explicit schemas -> DQ rules -> quarantine | dedupe -> MERGE
"""

import logging
from collections.abc import Callable
from dataclasses import dataclass

from delta.tables import DeltaTable
from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from claims_pipeline.config import SILVER_DIR, table_path
from claims_pipeline.fhir import codes
from claims_pipeline.jobs.bronze import bronze_table
from claims_pipeline.quality import rules
from claims_pipeline.spark import table_exists, upsert

logger = logging.getLogger(__name__)

META = "meta STRUCT<versionId: STRING, lastUpdated: STRING>"
REF = "STRUCT<reference: STRING>"
CODING = "STRUCT<system: STRING, code: STRING, display: STRING>"
CONCEPT = f"STRUCT<coding: ARRAY<{CODING}>>"
AMOUNT = "STRUCT<value: DOUBLE, currency: STRING>"

SCHEMAS = {
    "Patient": f"""id STRING, {META},
        identifier ARRAY<STRUCT<system: STRING, value: STRING>>,
        name ARRAY<STRUCT<family: STRING, given: ARRAY<STRING>>>,
        gender STRING, birthDate STRING,
        address ARRAY<STRUCT<city: STRING, state: STRING, postalCode: STRING>>""",
    "Organization": f"""id STRING, {META}, active BOOLEAN, name STRING,
        type ARRAY<{CONCEPT}>, address ARRAY<STRUCT<state: STRING>>""",
    "Encounter": f"""id STRING, {META}, status STRING, `class` {CODING},
        subject {REF}, serviceProvider {REF},
        period STRUCT<start: STRING, `end`: STRING>, reasonCode ARRAY<{CONCEPT}>,
        hospitalization STRUCT<dischargeDisposition: {CONCEPT}>""",
    "Condition": f"""id STRING, {META}, clinicalStatus {CONCEPT}, code {CONCEPT},
        subject {REF}, encounter {REF}, recordedDate STRING""",
    "Claim": f"""id STRING, {META}, status STRING, type {CONCEPT}, patient {REF},
        billablePeriod STRUCT<start: STRING, `end`: STRING>, created STRING,
        insurer {REF}, provider {REF},
        diagnosis ARRAY<STRUCT<sequence: INT, diagnosisCodeableConcept: {CONCEPT}>>,
        item ARRAY<STRUCT<sequence: INT, productOrService: {CONCEPT}, servicedDate: STRING,
            quantity: STRUCT<value: DOUBLE>, unitPrice: {AMOUNT}, net: {AMOUNT},
            encounter: ARRAY<{REF}>>>,
        total {AMOUNT}""",
    "ExplanationOfBenefit": f"""id STRING, {META}, status STRING, type {CONCEPT},
        patient {REF}, created STRING, insurer {REF}, provider {REF}, claim {REF},
        outcome STRING,
        item ARRAY<STRUCT<sequence: INT, productOrService: {CONCEPT},
            adjudication: ARRAY<STRUCT<category: {CONCEPT}, amount: {AMOUNT},
                reason: {CONCEPT}>>>>,
        total ARRAY<STRUCT<category: {CONCEPT}, amount: {AMOUNT}>>,
        payment STRUCT<date: STRING, amount: {AMOUNT}>""",
}


def _ref_id(col: Column) -> Column:
    return F.nullif(F.regexp_extract(col, r"^[A-Za-z]+/(.+)$", 1), F.lit(""))


def _ts(col: Column) -> Column:
    return F.try_to_timestamp(col)


def _date(col: Column) -> Column:
    return col.try_cast("date")


def _first_code(concept: Column) -> Column:
    return F.get(concept.getField("coding"), 0).getField("code")


def _money(col: Column) -> Column:
    return F.round(col, 2).cast("decimal(14,2)")


def _category(col: Column) -> Column:
    return _first_code(col.getField("category"))


def _common(r: Column) -> list[Column]:
    return [
        r.meta.versionId.try_cast("int").alias("version_id"),
        _ts(r.meta.lastUpdated).alias("last_updated"),
    ]


def parse_patient(r: Column) -> list[Column]:
    address = F.get(r.address, 0)
    name = F.get(r["name"], 0)
    return [
        r.id.alias("patient_id"),
        F.get(r.identifier, 0).getField("value").alias("mrn"),
        name.getField("family").alias("family_name"),
        F.get(name.getField("given"), 0).alias("given_name"),
        r.gender.alias("gender"),
        _date(r.birthDate).alias("birth_date"),
        address.getField("city").alias("city"),
        address.getField("state").alias("state"),
        address.getField("postalCode").alias("postal_code"),
        *_common(r),
    ]


def parse_organization(r: Column) -> list[Column]:
    codings = F.flatten(F.transform(r["type"], lambda t: t.getField("coding")))

    def code_from(system: str) -> Column:
        return F.get(F.filter(codings, lambda c: c.getField("system") == system), 0).getField(
            "code"
        )

    return [
        r.id.alias("organization_id"),
        r["name"].alias("name"),
        code_from(codes.ORG_TYPE_SYSTEM).alias("org_type"),
        code_from(codes.ORG_SEGMENT_SYSTEM).alias("segment"),
        F.get(r.address, 0).getField("state").alias("state"),
        r.active.alias("active"),
        *_common(r),
    ]


def parse_encounter(r: Column) -> list[Column]:
    start, end = _ts(r.period.start), _ts(r.period.getField("end"))
    return [
        r.id.alias("encounter_id"),
        _ref_id(r.subject.reference).alias("patient_id"),
        _ref_id(r.serviceProvider.reference).alias("provider_id"),
        r.getField("class").getField("code").alias("encounter_class"),
        r.status.alias("status"),
        start.alias("period_start"),
        end.alias("period_end"),
        F.datediff(end, start).alias("length_of_stay_days"),
        _first_code(F.get(r.reasonCode, 0)).alias("reason_code"),
        _first_code(r.hospitalization.dischargeDisposition).alias("discharge_disposition"),
        *_common(r),
    ]


def parse_condition(r: Column) -> list[Column]:
    coding = F.get(r.code.coding, 0)
    return [
        r.id.alias("condition_id"),
        _ref_id(r.subject.reference).alias("patient_id"),
        _ref_id(r.encounter.reference).alias("encounter_id"),
        coding.getField("code").alias("icd10_code"),
        coding.getField("display").alias("icd10_display"),
        _first_code(r.clinicalStatus).alias("clinical_status"),
        _ts(r.recordedDate).alias("recorded_at"),
        *_common(r),
    ]


def parse_claim(r: Column) -> list[Column]:
    dx_codes = F.transform(
        r.diagnosis, lambda d: _first_code(d.getField("diagnosisCodeableConcept"))
    )
    first_item = F.get(r.item, 0)
    return [
        r.id.alias("claim_id"),
        _ref_id(r.patient.reference).alias("patient_id"),
        _ref_id(r.provider.reference).alias("provider_id"),
        _ref_id(r.insurer.reference).alias("payer_id"),
        _ref_id(F.get(first_item.getField("encounter"), 0).getField("reference")).alias(
            "encounter_id"
        ),
        _first_code(r["type"]).alias("claim_type"),
        r.status.alias("status"),
        _ts(r.created).alias("created_at"),
        _ts(r.billablePeriod.start).alias("service_start"),
        _ts(r.billablePeriod.getField("end")).alias("service_end"),
        F.get(dx_codes, 0).alias("primary_dx_code"),
        dx_codes.alias("dx_codes"),
        F.coalesce(F.size(r.item), F.lit(0)).alias("line_count"),
        _money(r.total.value).alias("total_billed"),
        r.item.alias("_items"),
        *_common(r),
    ]


def parse_eob(r: Column) -> list[Column]:
    def total(category: str) -> Column:
        match = F.filter(r.total, lambda t: _category(t) == category)
        return _money(F.get(match, 0).getField("amount").getField("value"))

    def adjudicated(item: Column, category: str) -> Column:
        match = F.filter(item.getField("adjudication"), lambda a: _category(a) == category)
        return F.get(match, 0)

    lines = F.transform(
        r.item,
        lambda i: F.struct(
            adjudicated(i, "submitted").getField("amount").getField("value").alias("submitted"),
            _first_code(adjudicated(i, "benefit").getField("reason")).alias("reason"),
        ),
    )
    denied_lines = F.filter(lines, lambda x: x.getField("reason").isNotNull())
    denied = F.transform(denied_lines, lambda x: x.getField("reason"))
    denied_amount = F.aggregate(
        denied_lines,
        F.lit(0.0),
        lambda acc, x: acc + F.coalesce(x.getField("submitted"), F.lit(0.0)),
    )
    line_count, denied_count = F.size(r.item), F.size(denied)
    return [
        r.id.alias("eob_id"),
        _ref_id(r.claim.reference).alias("claim_id"),
        _ref_id(r.patient.reference).alias("patient_id"),
        _ref_id(r.insurer.reference).alias("payer_id"),
        _ref_id(r.provider.reference).alias("provider_id"),
        r.outcome.alias("outcome"),
        _ts(r.created).alias("adjudicated_at"),
        total("submitted").alias("submitted_amount"),
        total("eligible").alias("allowed_amount"),
        total("benefit").alias("paid_amount"),
        _money(denied_amount).alias("denied_amount"),
        _date(r.payment.date).alias("payment_date"),
        line_count.alias("line_count"),
        denied_count.alias("denied_line_count"),
        F.get(denied, 0).alias("denial_reason_code"),
        F.when(denied_count == 0, "paid")
        .when(denied_count == line_count, "denied")
        .otherwise("partially_denied")
        .alias("claim_status"),
        *_common(r),
    ]


@dataclass(frozen=True)
class Entity:
    name: str
    resource_type: str
    key: str
    parser: Callable[[Column], list[Column]]
    scd2_tracked: tuple[str, ...] = ()


ENTITIES = {
    e.name: e
    for e in [
        Entity(
            "patient",
            "Patient",
            "patient_id",
            parse_patient,
            scd2_tracked=(
                "mrn",
                "family_name",
                "given_name",
                "gender",
                "birth_date",
                "city",
                "state",
                "postal_code",
            ),
        ),
        Entity("organization", "Organization", "organization_id", parse_organization),
        Entity("encounter", "Encounter", "encounter_id", parse_encounter),
        Entity("condition", "Condition", "condition_id", parse_condition),
        Entity("claim", "Claim", "claim_id", parse_claim),
        Entity("eob", "ExplanationOfBenefit", "eob_id", parse_eob),
    ]
}
LINEAGE = ["export_date", "source_file", "record_hash"]


def silver_table(name: str) -> str:
    return table_path(SILVER_DIR, name)


def parse(bronze: DataFrame, entity: Entity) -> DataFrame:
    parsed = bronze.withColumn("_is_valid_json", F.expr("try_parse_json(raw)").isNotNull())
    parsed = parsed.withColumn("r", F.from_json("raw", SCHEMAS[entity.resource_type]))
    return parsed.select(*entity.parser(F.col("r")), "_is_valid_json", "raw", *LINEAGE)


def latest_per_key(df: DataFrame, keys: list[str]) -> DataFrame:
    w = Window.partitionBy(*keys).orderBy(
        F.col("last_updated").desc(), F.col("version_id").desc(), F.col("export_date").desc()
    )
    return df.withColumn("_rn", F.row_number().over(w)).where("_rn = 1").drop("_rn")


def scd2_merge(
    spark: SparkSession, incoming: DataFrame, path: str, key: str, tracked: tuple[str, ...]
) -> None:
    """Type 2 history keyed on (key, valid_from). Rebuilds the timeline of affected keys only,
    so late or replayed versions land in the right place and re-runs are idempotent."""
    fingerprint = F.concat_ws(
        "||", *[F.coalesce(F.col(c).cast("string"), F.lit("<null>")) for c in tracked]
    )
    incoming = incoming.withColumn("row_hash", F.sha2(fingerprint, 256)).withColumn(
        "valid_from", F.col("last_updated")
    )
    columns = incoming.columns
    exists = table_exists(spark, path)
    existing = (
        DeltaTable.forPath(spark, path)
        .toDF()
        .join(incoming.select(key).distinct(), key, "left_semi")
        if exists
        else None
    )
    combined = incoming if existing is None else existing.select(*columns).unionByName(incoming)

    w = Window.partitionBy(key).orderBy("valid_from")
    history = (
        combined.dropDuplicates([key, "valid_from"])
        .withColumn("_prev_hash", F.lag("row_hash").over(w))
        .where(F.col("_prev_hash").isNull() | (F.col("_prev_hash") != F.col("row_hash")))
        .drop("_prev_hash")
        .withColumn("valid_to", F.lead("valid_from").over(w))
        .withColumn("is_current", F.col("valid_to").isNull())
    )
    if existing is None:
        history.write.format("delta").save(path)
        return

    stale = existing.join(history.select(key, "valid_from"), [key, "valid_from"], "left_anti")
    source = history.withColumn("_action", F.lit("upsert")).unionByName(
        stale.select(key, "valid_from").withColumn("_action", F.lit("delete")),
        allowMissingColumns=True,
    )
    assignments = {c: f"s.{c}" for c in history.columns}
    (
        DeltaTable.forPath(spark, path)
        .alias("t")
        .merge(source.alias("s"), f"t.{key} = s.{key} AND t.valid_from = s.valid_from")
        .whenMatchedDelete(condition="s._action = 'delete'")
        .whenMatchedUpdate(set=assignments)
        .whenNotMatchedInsert(condition="s._action = 'upsert'", values=assignments)
        .execute()
    )


def _claim_lines(valid_claims: DataFrame) -> DataFrame:
    item = F.col("item")
    coding = F.get(item.getField("productOrService").getField("coding"), 0)
    return valid_claims.select(
        "claim_id", "version_id", "last_updated", F.explode("_items").alias("item"), *LINEAGE
    ).select(
        "claim_id",
        item.getField("sequence").alias("line_number"),
        coding.getField("system").alias("code_system"),
        coding.getField("code").alias("procedure_code"),
        coding.getField("display").alias("procedure_display"),
        item.getField("quantity").getField("value").cast("int").alias("quantity"),
        _money(item.getField("unitPrice").getField("value")).alias("unit_price"),
        _money(item.getField("net").getField("value")).alias("net_amount"),
        _date(item.getField("servicedDate")).alias("serviced_date"),
        "version_id",
        "last_updated",
        *LINEAGE,
    )


def run(spark: SparkSession, entity_name: str, export_dates: list[str], run_id: str) -> dict:
    entity = ENTITIES[entity_name]
    source = bronze_table(entity.resource_type)
    if not table_exists(spark, source):
        return {"entity": entity_name, "read": 0, "valid": 0, "quarantined": 0}

    dates = [F.to_date(F.lit(d)) for d in export_dates]
    bronze = spark.read.format("delta").load(source).where(F.col("export_date").isin(dates))
    evaluated = rules.evaluate(parse(bronze, entity), entity_name).cache()
    try:
        valid, rejected = rules.split(evaluated)
        rules.persist(
            spark, rejected, rules.metrics(evaluated, entity_name, run_id), entity_name, entity.key
        )
        valid = valid.drop("_is_valid_json", "raw")

        if entity_name == "claim":
            lines = latest_per_key(_claim_lines(valid), ["claim_id", "line_number"])
            upsert(spark, lines, silver_table("claim_line"), ["claim_id", "line_number"])
            valid = valid.drop("_items")

        if entity.scd2_tracked:
            versions = latest_per_key(valid, [entity.key, "version_id"])
            scd2_merge(spark, versions, silver_table(entity_name), entity.key, entity.scd2_tracked)
        else:
            upsert(
                spark, latest_per_key(valid, [entity.key]), silver_table(entity_name), [entity.key]
            )

        result = {
            "entity": entity_name,
            "read": evaluated.count(),
            "quarantined": rejected.count(),
        }
    finally:
        evaluated.unpersist()
    result["valid"] = result["read"] - result["quarantined"]
    logger.info("Silver %s: %s", entity_name, result)
    return result
