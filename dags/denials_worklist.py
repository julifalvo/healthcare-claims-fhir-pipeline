"""Data-aware downstream DAG: as soon as gold is republished, hand the billing team a prioritized
worklist of recoverable denials still inside their appeal window."""

from datetime import date, datetime, timedelta

from airflow.sdk import Asset, dag, task

FACT_CLAIM = Asset(name="gold_fact_claim", uri="file:///opt/airflow/lakehouse/gold/fact_claim")


@dag(
    dag_id="denials_worklist",
    schedule=[FACT_CLAIM],
    start_date=datetime(2026, 1, 2),
    catchup=False,
    default_args={"owner": "revenue-cycle", "retries": 1, "retry_delay": timedelta(seconds=30)},
    tags=["healthcare", "revenue-cycle", "asset-triggered"],
    doc_md=__doc__,
)
def denials_worklist():
    @task
    def export_worklist() -> dict:
        from claims_pipeline.jobs.exports import read_gold, write_denials_worklist

        as_of = read_gold("fact_claim")["submitted_date"].max()
        return write_denials_worklist(as_of if isinstance(as_of, date) else date.today())

    export_worklist()


denials_worklist()
