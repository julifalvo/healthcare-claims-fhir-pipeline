"""Delta table housekeeping: compaction, Z-ordering and vacuum."""

import logging

from pyspark.sql import SparkSession

from claims_pipeline.jobs.silver import silver_table
from claims_pipeline.spark import table_exists

logger = logging.getLogger(__name__)

# Columns most used in joins/filters downstream; Z-ordering co-locates them for data skipping.
ZORDER = {
    "claim": ["patient_id", "payer_id"],
    "eob": ["claim_id"],
    "encounter": ["patient_id"],
    "condition": ["patient_id"],
    "claim_line": ["claim_id"],
}


def optimize(spark: SparkSession, retain_hours: int = 168) -> dict[str, dict]:
    report = {}
    for table, columns in ZORDER.items():
        path = silver_table(table)
        if not table_exists(spark, path):
            continue
        metrics = spark.sql(f"OPTIMIZE delta.`{path}` ZORDER BY ({', '.join(columns)})").collect()[
            0
        ]["metrics"]
        spark.sql(f"VACUUM delta.`{path}` RETAIN {retain_hours} HOURS")
        report[table] = {
            "files_added": metrics["numFilesAdded"],
            "files_removed": metrics["numFilesRemoved"],
        }
    logger.info("Maintenance report: %s", report)
    return report
