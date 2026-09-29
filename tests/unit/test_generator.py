import json
import os
import subprocess
import sys
from datetime import date

import pytest

from claims_pipeline.fhir.generator import SimulationSettings, build_world, export_day

SMALL = SimulationSettings(n_patients=300)
CLEAN = SimulationSettings(n_patients=300, dq_error_rate=0.0, malformed_line_rate=0.0)

FINGERPRINT = """
import hashlib, json
from claims_pipeline.fhir.generator import SimulationSettings, build_world
world = build_world(SimulationSettings(n_patients=300))
payload = json.dumps({d.isoformat(): v for d, v in sorted(world.items())}, sort_keys=True)
print(hashlib.sha256(payload.encode()).hexdigest())
"""


def _fingerprint(hash_seed: str) -> str:
    env = {**os.environ, "PYTHONHASHSEED": hash_seed}
    return subprocess.run(
        [sys.executable, "-c", FINGERPRINT], env=env, capture_output=True, text=True, check=True
    ).stdout.strip()


def test_world_is_identical_across_processes_and_hash_seeds():
    # Regression: iterating a set of str made ids shift between runs and broke idempotent MERGEs.
    assert _fingerprint("1") == _fingerprint("2")


def test_resources_conform_to_fhir_r4():
    fhir = pytest.importorskip("fhir.resources.R4B")
    checked = 0
    for day_resources in build_world(CLEAN).values():
        for resource_type, resources in day_resources.items():
            model = fhir.get_fhir_model_class(resource_type)
            for resource in resources[:25]:
                model.model_validate(resource)
                checked += 1
    assert checked > 1_000


def test_adjudications_arrive_after_their_claims():
    world = build_world(CLEAN)
    claims = {c["id"]: c["created"] for day in world.values() for c in day.get("Claim", [])}
    eobs = [e for day in world.values() for e in day.get("ExplanationOfBenefit", [])]
    assert eobs
    assert all(e["created"] > claims[e["claim"]["reference"].split("/")[1]] for e in eobs)


def test_quality_issues_are_injected_only_when_enabled():
    def broken_claims(settings):
        return sum(
            1
            for day in build_world(settings).values()
            for c in day.get("Claim", [])
            if "patient" not in c or c["total"]["value"] < 0
        )

    assert broken_claims(SMALL) > 0
    assert broken_claims(CLEAN) == 0


def test_export_day_writes_bulk_data_layout_and_is_idempotent(tmp_path):
    day = date(2026, 3, 2)
    first = export_day(day, tmp_path, SMALL)
    second = export_day(day, tmp_path, SMALL)
    folder = tmp_path / "export_date=2026-03-02"
    manifest = json.loads((folder / "manifest.json").read_text())

    assert first == second
    assert {o["type"]: o["count"] for o in manifest["output"]} == first
    for output in manifest["output"]:
        lines = (folder / output["url"]).read_text().splitlines()
        assert len(lines) == output["count"]
    assert not list(tmp_path.glob(".tmp_*"))
