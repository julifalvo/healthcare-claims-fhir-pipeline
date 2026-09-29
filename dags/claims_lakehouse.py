"""FHIR claims lakehouse: bulk export -> bronze -> silver -> quality gate -> gold.

Runs daily for the previous day's `$export`. Trigger manually with `start_date`/`end_date`
params to backfill a range in a single run; every stage is idempotent, so re-runs are safe.
"""

import logging
from datetime import date, datetime, timedelta

from airflow.sdk import Asset, Metadata, Param, dag, get_current_context, task, task_group
from airflow.sdk.exceptions import AirflowFailException

from claims_pipeline.config import RESOURCE_TYPES

log = logging.getLogger(__name__)

LANDING_ASSET = Asset(name="fhir_bulk_export", uri="file:///opt/airflow/lakehouse/landing/fhir")
GOLD_ASSETS = [
    Asset(name=f"gold_{t}", uri=f"file:///opt/airflow/lakehouse/gold/{t}")
    for t in ("fact_claim", "mart_payer_monthly", "mart_denials", "mart_readmissions")
]
SPARK_POOL = "spark"


def notify_failure(context) -> None:
    ti = context["ti"]
    log.error(
        "ALERT data-platform: %s.%s failed (try %s). Logs: %s",
        ti.dag_id,
        ti.task_id,
        ti.try_number,
        ti.log_url,
    )


DEFAULT_ARGS = {
    "owner": "data-platform",
    "retries": 2,
    "retry_delay": timedelta(seconds=30),
    "retry_exponential_backoff": True,
    "execution_timeout": timedelta(minutes=45),
    "on_failure_callback": notify_failure,
}


@dag(
    dag_id="claims_lakehouse",
    schedule="0 3 * * *",
    start_date=datetime(2026, 1, 2),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    params={
        "start_date": Param(
            None,
            type=["null", "string"],
            format="date",
            description="Backfill start (YYYY-MM-DD). Empty = previous day.",
        ),
        "end_date": Param(
            None,
            type=["null", "string"],
            format="date",
            description="Backfill end, inclusive. Empty = previous day.",
        ),
    },
    tags=["healthcare", "fhir", "spark", "delta-lake"],
    doc_md=__doc__,
)
def claims_lakehouse():
    @task
    def resolve_window() -> dict:
        from claims_pipeline.pipeline import date_range, resolve_window

        ctx = get_current_context()
        params = ctx["params"]
        interval_start = ctx.get("data_interval_start")
        default_day = (interval_start or datetime.now()).date() - timedelta(days=1)
        start = date.fromisoformat(params["start_date"]) if params["start_date"] else default_day
        end = date.fromisoformat(params["end_date"]) if params["end_date"] else default_day
        start, end = resolve_window(start, end)
        return {
            "start": start.isoformat(),
            "end": end.isoformat(),
            "dates": date_range(start, end),
            "run_id": ctx["run_id"],
        }

    @task(outlets=[LANDING_ASSET])
    def extract_fhir_bulk_export(window: dict):
        from claims_pipeline.pipeline import extract

        counts = extract(window["dates"])
        yield Metadata(LANDING_ASSET, {"days": len(window["dates"]), **counts})

    @task(pool=SPARK_POOL, max_active_tis_per_dagrun=2, map_index_template="{{ resource_type }}")
    def ingest_bronze(resource_type: str, window: dict) -> dict:
        from claims_pipeline.jobs import bronze
        from claims_pipeline.spark import spark_session

        with spark_session(f"bronze-{resource_type}") as spark:
            return bronze.ingest(spark, resource_type, window["dates"], window["run_id"])

    @task(pool=SPARK_POOL)
    def prepare_quality_tables() -> None:
        """DDL before fan-out: the silver tasks write to shared quarantine/DQ tables in parallel."""
        from claims_pipeline.quality import rules
        from claims_pipeline.spark import spark_session

        with spark_session("ddl") as spark:
            rules.ensure_tables(spark)

    @task_group(tooltip="Parse, validate, quarantine and MERGE each FHIR resource into Delta")
    def conform_silver(window: dict):
        from claims_pipeline.pipeline import SILVER_ENTITIES

        for entity in SILVER_ENTITIES:

            @task(task_id=entity, pool=SPARK_POOL)
            def conform(entity_name: str, window: dict) -> dict:
                from claims_pipeline.jobs import silver
                from claims_pipeline.spark import spark_session

                with spark_session(f"silver-{entity_name}") as spark:
                    return silver.run(spark, entity_name, window["dates"], window["run_id"])

            conform(entity, window)

    @task(pool=SPARK_POOL)
    def quality_gate(window: dict) -> dict:
        from claims_pipeline.quality import gate
        from claims_pipeline.spark import spark_session

        with spark_session("quality-gate") as spark:
            try:
                return gate.check(spark, window["dates"])
            except gate.QualityGateError as err:
                # Bad data will not fix itself on retry: fail fast, keep gold untouched.
                raise AirflowFailException(str(err)) from err

    @task(pool=SPARK_POOL, outlets=GOLD_ASSETS)
    def build_gold(window: dict) -> dict:
        from claims_pipeline.jobs import gold
        from claims_pipeline.spark import spark_session

        with spark_session("gold") as spark:
            return gold.build(spark, as_of=window["end"])

    window = resolve_window()
    bronze = ingest_bronze.partial(window=window).expand(resource_type=list(RESOURCE_TYPES))
    extract_fhir_bulk_export(window) >> bronze
    silver = conform_silver(window)
    [bronze, prepare_quality_tables()] >> silver >> quality_gate(window) >> build_gold(window)


claims_lakehouse()
