import os
from pathlib import Path

LAKEHOUSE_DIR = Path(os.getenv("LAKEHOUSE_DIR", Path.cwd() / "lakehouse")).resolve()

LANDING_DIR = LAKEHOUSE_DIR / "landing" / "fhir"
BRONZE_DIR = LAKEHOUSE_DIR / "bronze"
SILVER_DIR = LAKEHOUSE_DIR / "silver"
GOLD_DIR = LAKEHOUSE_DIR / "gold"
EXPORTS_DIR = LAKEHOUSE_DIR / "exports"

RESOURCE_TYPES = (
    "Organization",
    "Patient",
    "Encounter",
    "Condition",
    "Claim",
    "ExplanationOfBenefit",
)

# Fail the run when more than this share of silver records is quarantined.
MAX_QUARANTINE_RATE = float(os.getenv("MAX_QUARANTINE_RATE", "0.05"))


def table_path(layer_dir: Path, name: str) -> str:
    return (layer_dir / name).as_posix()
