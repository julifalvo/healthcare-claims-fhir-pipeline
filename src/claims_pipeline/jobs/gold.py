"""Gold: star schema and revenue-cycle marts, recomputed from silver in Spark SQL.

Gold is rebuilt atomically on every run (Delta overwrite = single commit). Readers never see a
half-written table, and the logic stays declarative.
"""

import logging

from pyspark.sql import SparkSession

from claims_pipeline.config import GOLD_DIR, table_path
from claims_pipeline.fhir.codes import DENIAL_ACTIONS, DENIAL_REASONS
from claims_pipeline.jobs.silver import silver_table
from claims_pipeline.quality.rules import DQ_RESULTS_TABLE

logger = logging.getLogger(__name__)

ICD10_CHAPTER = """CASE
    WHEN {c} RLIKE '^[AB]' THEN 'Infectious diseases'
    WHEN {c} RLIKE '^(C|D[0-4])' THEN 'Neoplasms'
    WHEN {c} RLIKE '^E' THEN 'Endocrine & metabolic'
    WHEN {c} RLIKE '^F' THEN 'Mental & behavioral'
    WHEN {c} RLIKE '^I' THEN 'Circulatory system'
    WHEN {c} RLIKE '^J' THEN 'Respiratory system'
    WHEN {c} RLIKE '^M' THEN 'Musculoskeletal'
    WHEN {c} RLIKE '^N' THEN 'Genitourinary'
    WHEN {c} RLIKE '^R' THEN 'Symptoms & signs'
    WHEN {c} RLIKE '^[ST]' THEN 'Injury'
    WHEN {c} RLIKE '^Z' THEN 'Preventive & screening'
    ELSE 'Other' END"""

GOLD_TABLES: dict[str, str] = {
    "dim_payer": """
        SELECT organization_id AS payer_id, name AS payer_name, segment AS payer_segment, state
        FROM organization WHERE org_type = 'pay'""",
    "dim_provider": """
        SELECT organization_id AS provider_id, name AS provider_name,
               segment AS facility_type, state
        FROM organization WHERE org_type = 'prov'""",
    "dim_patient": """
        SELECT patient_id, mrn, gender, birth_date, age,
               CASE WHEN age < 18 THEN '0-17' WHEN age < 45 THEN '18-44'
                    WHEN age < 65 THEN '45-64' WHEN age < 80 THEN '65-79' ELSE '80+' END AS age_band,
               city, state, valid_from AS current_since, address_versions
        FROM (
            SELECT p.*, floor(months_between(DATE'{as_of}', birth_date) / 12) AS age,
                   count(*) OVER (PARTITION BY patient_id) AS address_versions
            FROM patient_history p
        ) WHERE is_current""",
    "dim_diagnosis": f"""
        SELECT icd10_code, max_by(icd10_display, last_updated) AS description,
               {ICD10_CHAPTER.format(c="icd10_code")} AS chapter
        FROM condition GROUP BY icd10_code""",
    "dim_denial_reason": "SELECT * FROM denial_reason_ref",
    "fact_claim": f"""
        SELECT
            c.claim_id, c.patient_id, c.provider_id, c.payer_id, c.encounter_id,
            e.encounter_class, c.claim_type, c.primary_dx_code,
            {ICD10_CHAPTER.format(c="c.primary_dx_code")} AS dx_chapter,
            to_date(c.service_start) AS service_date,
            to_date(c.created_at) AS submitted_date,
            trunc(to_date(c.created_at), 'MM') AS submitted_month,
            datediff(to_date(c.created_at), to_date(c.service_end)) AS days_to_submit,
            c.line_count, c.total_billed AS billed_amount,
            coalesce(x.allowed_amount, 0) AS allowed_amount,
            coalesce(x.paid_amount, 0) AS paid_amount,
            coalesce(x.denied_amount, 0) AS denied_amount,
            coalesce(x.claim_status, 'pending') AS claim_status,
            CASE WHEN x.denial_reason_code IS NOT NULL
                 THEN concat('CO-', x.denial_reason_code) END AS denial_reason_code,
            to_date(x.adjudicated_at) AS adjudicated_date,
            datediff(to_date(x.adjudicated_at), to_date(c.created_at)) AS days_to_adjudication,
            x.payment_date,
            CASE WHEN x.eob_id IS NULL
                 THEN datediff(DATE'{{as_of}}', to_date(c.created_at)) END AS ar_age_days,
            coalesce(x.claim_status = 'paid', false) AS is_clean_claim
        FROM claim c
        LEFT JOIN eob x ON x.claim_id = c.claim_id
        LEFT JOIN encounter e ON e.encounter_id = c.encounter_id
        WHERE to_date(c.created_at) <= DATE'{{as_of}}'""",
    "fact_inpatient_stay": f"""
        WITH stays AS (
            SELECT encounter_id, patient_id, provider_id, reason_code AS principal_dx,
                   to_date(period_start) AS admit_date, to_date(period_end) AS discharge_date,
                   length_of_stay_days
            FROM encounter WHERE encounter_class = 'IMP'
        ), sequenced AS (
            SELECT *,
                   lead(admit_date) OVER w AS next_admit_date,
                   lead(encounter_id) OVER w AS readmission_encounter_id
            FROM stays
            WINDOW w AS (PARTITION BY patient_id ORDER BY admit_date, encounter_id)
        )
        SELECT *,
               {ICD10_CHAPTER.format(c="principal_dx")} AS dx_chapter,
               trunc(discharge_date, 'MM') AS discharge_month,
               datediff(next_admit_date, discharge_date) AS days_to_readmission,
               coalesce(datediff(next_admit_date, discharge_date) BETWEEN 0 AND 30, false)
                   AS readmitted_30d,
               discharge_date <= date_sub(DATE'{{as_of}}', 30) AS is_eligible_index
        FROM sequenced""",
    "mart_payer_monthly": """
        SELECT f.submitted_month, f.payer_id, p.payer_name, p.payer_segment,
               count(*) AS claims,
               count_if(claim_status <> 'pending') AS adjudicated_claims,
               count_if(claim_status = 'denied') AS denied_claims,
               count_if(claim_status = 'partially_denied') AS partially_denied_claims,
               CAST(try_divide(count_if(claim_status = 'denied'),
                               count_if(claim_status <> 'pending')) AS DOUBLE) AS denial_rate,
               CAST(try_divide(count_if(is_clean_claim),
                               count_if(claim_status <> 'pending')) AS DOUBLE) AS clean_claim_rate,
               sum(billed_amount) AS billed_amount,
               sum(allowed_amount) AS allowed_amount,
               sum(paid_amount) AS paid_amount,
               sum(denied_amount) AS denied_amount,
               CAST(try_divide(sum(paid_amount), sum(allowed_amount)) AS DOUBLE)
                   AS net_collection_rate,
               avg(days_to_adjudication) AS avg_days_to_adjudication
        FROM fact_claim f JOIN dim_payer p USING (payer_id)
        GROUP BY ALL""",
    "mart_denials": """
        SELECT p.payer_name, f.denial_reason_code, r.description AS denial_reason,
               r.category AS denial_category, r.recoverable, f.encounter_class,
               f.submitted_month,
               count(*) AS denied_claims, sum(f.denied_amount) AS denied_amount
        FROM fact_claim f
        JOIN dim_payer p USING (payer_id)
        LEFT JOIN dim_denial_reason r USING (denial_reason_code)
        WHERE f.claim_status IN ('denied', 'partially_denied')
        GROUP BY ALL""",
    "mart_ar_aging": """
        SELECT p.payer_name,
               CASE WHEN ar_age_days <= 30 THEN '0-30' WHEN ar_age_days <= 60 THEN '31-60'
                    WHEN ar_age_days <= 90 THEN '61-90' ELSE '90+' END AS aging_bucket,
               count(*) AS open_claims, sum(billed_amount) AS outstanding_amount
        FROM fact_claim f JOIN dim_payer p USING (payer_id)
        WHERE claim_status = 'pending'
        GROUP BY ALL""",
    "mart_readmissions": """
        SELECT s.discharge_month, s.principal_dx, d.description AS diagnosis, s.dx_chapter,
               count(*) AS index_stays,
               count_if(readmitted_30d) AS readmissions,
               CAST(try_divide(count_if(readmitted_30d), count(*)) AS DOUBLE) AS readmission_rate,
               avg(length_of_stay_days) AS avg_length_of_stay
        FROM fact_inpatient_stay s
        LEFT JOIN dim_diagnosis d ON d.icd10_code = s.principal_dx
        WHERE is_eligible_index
        GROUP BY ALL""",
    "mart_data_quality": """
        SELECT export_date, entity, rule, checked, failed,
               CAST(try_divide(failed, checked) AS DOUBLE) AS failure_rate
        FROM dq_results""",
}


def _register_sources(spark: SparkSession) -> None:
    for name in ["organization", "encounter", "condition", "claim", "eob", "claim_line"]:
        spark.read.format("delta").load(silver_table(name)).createOrReplaceTempView(name)
    spark.read.format("delta").load(silver_table("patient")).createOrReplaceTempView(
        "patient_history"
    )
    spark.read.format("delta").load(DQ_RESULTS_TABLE).createOrReplaceTempView("dq_results")
    reasons = [
        (f"CO-{code}", DENIAL_REASONS[code], *DENIAL_ACTIONS[code]) for code in DENIAL_REASONS
    ]
    spark.createDataFrame(
        reasons,
        "denial_reason_code STRING, description STRING, category STRING, "
        "recommended_action STRING, recoverable BOOLEAN",
    ).createOrReplaceTempView("denial_reason_ref")


def build(spark: SparkSession, as_of: str) -> dict[str, int]:
    _register_sources(spark)
    counts = {}
    for name, sql in GOLD_TABLES.items():
        df = spark.sql(sql.replace("{as_of}", as_of))
        (
            df.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .save(table_path(GOLD_DIR, name))
        )
        # Later marts read earlier gold tables, so expose each one as it is written.
        spark.read.format("delta").load(table_path(GOLD_DIR, name)).createOrReplaceTempView(name)
        counts[name] = spark.table(name).count()
    logger.info("Gold rebuilt as of %s: %s", as_of, counts)
    return counts
