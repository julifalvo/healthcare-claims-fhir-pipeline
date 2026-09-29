"""Operational extracts built from gold with delta-rs + pandas (no Spark needed)."""

import logging
from datetime import date
from pathlib import Path

import pandas as pd
import pyarrow as pa
from deltalake import DeltaTable

from claims_pipeline.config import EXPORTS_DIR, GOLD_DIR, table_path

logger = logging.getLogger(__name__)

APPEAL_WINDOW_DAYS = 90


def read_gold(name: str) -> pd.DataFrame:
    """Gold table as pandas; money columns (Delta decimals) become floats for analytics."""
    table = DeltaTable(table_path(GOLD_DIR, name)).to_pyarrow_table()
    schema = pa.schema(
        pa.field(f.name, pa.float64()) if pa.types.is_decimal(f.type) else f for f in table.schema
    )
    return table.cast(schema).to_pandas()


def denials_worklist(as_of: date, lookback_days: int = 60) -> pd.DataFrame:
    """Recoverable denials still inside the appeal window, highest dollar value first."""
    claims = read_gold("fact_claim")
    reasons = read_gold("dim_denial_reason")
    payers = read_gold("dim_payer")[["payer_id", "payer_name"]]
    providers = read_gold("dim_provider")[["provider_id", "provider_name"]]

    denied = claims[claims["claim_status"].isin(["denied", "partially_denied"])].merge(
        reasons, on="denial_reason_code"
    )
    adjudicated = pd.to_datetime(denied["adjudicated_date"])
    as_of_ts = pd.Timestamp(as_of)
    denied = denied.assign(
        days_since_denial=(as_of_ts - adjudicated).dt.days,
        appeal_deadline=(adjudicated + pd.Timedelta(days=APPEAL_WINDOW_DAYS)).dt.date,
    )
    recent = denied["days_since_denial"].between(0, lookback_days)
    worklist = (
        denied[denied["recoverable"] & recent]
        .merge(payers, on="payer_id")
        .merge(providers, on="provider_id")
        .sort_values(["denied_amount", "days_since_denial"], ascending=[False, False])
    )
    return worklist[
        [
            "claim_id",
            "payer_name",
            "provider_name",
            "patient_id",
            "submitted_date",
            "denial_reason_code",
            "description",
            "recommended_action",
            "denied_amount",
            "days_since_denial",
            "appeal_deadline",
        ]
    ].reset_index(drop=True)


def write_denials_worklist(as_of: date, exports_dir: Path = EXPORTS_DIR) -> dict:
    worklist = denials_worklist(as_of)
    exports_dir.mkdir(parents=True, exist_ok=True)
    path = exports_dir / f"denials_worklist_{as_of.isoformat()}.csv"
    worklist.to_csv(path, index=False)
    at_risk = float(worklist["denied_amount"].sum())
    logger.info("Wrote %s claims (USD %.2f recoverable) to %s", len(worklist), at_risk, path)
    return {"path": str(path), "claims": len(worklist), "recoverable_usd": round(at_risk, 2)}
