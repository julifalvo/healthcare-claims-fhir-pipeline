import argparse
import json
import logging
import uuid
from datetime import date

from claims_pipeline.config import LANDING_DIR
from claims_pipeline.pipeline import date_range, extract


def main() -> None:
    parser = argparse.ArgumentParser(prog="claims-pipeline")
    sub = parser.add_subparsers(dest="command", required=True)
    for name, help_text in [
        ("export", "Write synthetic FHIR Bulk Data exports to the landing zone."),
        ("run", "Run export -> bronze -> silver -> quality gate -> gold in one Spark session."),
    ]:
        cmd = sub.add_parser(name, help=help_text)
        cmd.add_argument("--start", type=date.fromisoformat, required=True)
        cmd.add_argument("--end", type=date.fromisoformat, required=True)
    sub.add_parser("worklist", help="Write the denials worklist CSV from gold.").add_argument(
        "--as-of", type=date.fromisoformat, required=True
    )
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")

    if args.command == "export":
        print(json.dumps(extract(date_range(args.start, args.end)), indent=2))
        print(f"Landing zone: {LANDING_DIR}")
    elif args.command == "run":
        from claims_pipeline.pipeline import run_all
        from claims_pipeline.spark import spark_session

        with spark_session("claims-pipeline-cli") as spark:
            report = run_all(spark, args.start, args.end, run_id=f"cli-{uuid.uuid4().hex[:8]}")
        print(json.dumps(report, indent=2, default=str))
    else:
        from claims_pipeline.jobs.exports import write_denials_worklist

        print(json.dumps(write_denials_worklist(args.as_of), indent=2))


if __name__ == "__main__":
    main()
