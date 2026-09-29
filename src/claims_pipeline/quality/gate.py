from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from claims_pipeline.config import MAX_QUARANTINE_RATE
from claims_pipeline.quality.rules import DQ_RESULTS_TABLE, RECORDS_METRIC


class QualityGateError(RuntimeError):
    pass


def check(
    spark: SparkSession, export_dates: list[str], max_rate: float = MAX_QUARANTINE_RATE
) -> dict[str, dict]:
    """Block publishing to gold when any entity quarantines more than `max_rate` of its records."""
    dates = [F.to_date(F.lit(d)) for d in export_dates]
    rows = (
        spark.read.format("delta")
        .load(DQ_RESULTS_TABLE)
        .where(F.col("export_date").isin(dates) & (F.col("rule") == RECORDS_METRIC))
        .groupBy("entity")
        .agg(F.sum("checked").alias("checked"), F.sum("failed").alias("quarantined"))
        .collect()
    )
    report = {
        r["entity"]: {
            "checked": r["checked"],
            "quarantined": r["quarantined"],
            "rate": round(r["quarantined"] / r["checked"], 5) if r["checked"] else 0.0,
        }
        for r in rows
    }
    breaches = {e: m for e, m in report.items() if m["rate"] > max_rate}
    if breaches:
        raise QualityGateError(f"Quarantine rate above {max_rate:.1%}: {breaches}")
    return report
