"""Weekly Delta Lake housekeeping: compact small files, Z-order hot join keys, vacuum old files."""

from datetime import datetime, timedelta

from airflow.sdk import dag, task


@dag(
    dag_id="lakehouse_maintenance",
    schedule="0 5 * * 0",
    start_date=datetime(2026, 1, 4),
    catchup=False,
    default_args={"owner": "data-platform", "retries": 1, "retry_delay": timedelta(minutes=1)},
    tags=["delta-lake", "maintenance"],
    doc_md=__doc__,
)
def lakehouse_maintenance():
    @task(pool="spark")
    def optimize_and_vacuum() -> dict:
        from claims_pipeline.jobs.maintenance import optimize
        from claims_pipeline.spark import spark_session

        with spark_session("maintenance") as spark:
            return optimize(spark)

    optimize_and_vacuum()


lakehouse_maintenance()
