"""Stage entry points shared by the Airflow DAGs and the CLI."""

import logging
from datetime import date, timedelta

from pyspark.sql import SparkSession

from claims_pipeline.config import LANDING_DIR, RESOURCE_TYPES, SILVER_DIR
from claims_pipeline.fhir.generator import DEFAULT_SETTINGS, export_day
from claims_pipeline.jobs import bronze, gold, silver
from claims_pipeline.quality import gate, rules

logger = logging.getLogger(__name__)

SILVER_ENTITIES = tuple(silver.ENTITIES)


def date_range(start: date, end: date) -> list[str]:
    if end < start:
        raise ValueError(f"end ({end}) is before start ({start})")
    return [(start + timedelta(days=i)).isoformat() for i in range((end - start).days + 1)]


def extract(export_dates: list[str]) -> dict[str, int]:
    totals: dict[str, int] = dict.fromkeys(RESOURCE_TYPES, 0)
    for d in export_dates:
        for resource_type, n in export_day(date.fromisoformat(d), LANDING_DIR).items():
            totals[resource_type] += n
    return totals


def is_bootstrapped() -> bool:
    return (SILVER_DIR / "claim" / "_delta_log").exists()


def resolve_window(start: date, end: date) -> tuple[date, date]:
    """Incremental exports only carry changes, so an empty lakehouse needs the full history
    (patients and organizations are exported once, on the first day)."""
    if not is_bootstrapped() and start > DEFAULT_SETTINGS.start:
        logger.info("Empty lakehouse: bootstrapping from %s", DEFAULT_SETTINGS.start)
        return DEFAULT_SETTINGS.start, end
    return start, end


def run_all(spark: SparkSession, start: date, end: date, run_id: str) -> dict:
    """Full pipeline in one Spark session (CLI / CI). Airflow runs the same stages as tasks."""
    start, end = resolve_window(start, end)
    dates = date_range(start, end)
    report: dict = {"window": {"start": start.isoformat(), "end": end.isoformat()}}
    report["extract"] = extract(dates)
    report["bronze"] = [bronze.ingest(spark, rt, dates, run_id) for rt in RESOURCE_TYPES]
    rules.ensure_tables(spark)
    report["silver"] = [silver.run(spark, e, dates, run_id) for e in SILVER_ENTITIES]
    report["quality_gate"] = gate.check(spark, dates)
    report["gold"] = gold.build(spark, as_of=end.isoformat())
    return report
