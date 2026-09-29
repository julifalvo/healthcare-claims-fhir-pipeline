"""Synthetic FHIR R4 Bulk Data exports for a regional health system.

The whole simulation window is generated once from a fixed seed. `export_day` then writes only
the resources whose `meta.lastUpdated` falls on that day, like an incremental
`$export?_since=` feed. Adjudications (ExplanationOfBenefit) therefore land days or weeks after
their claims, and a small, controlled share of records is corrupted to exercise data quality.
"""

import json
import logging
import math
import random
import shutil
from collections import defaultdict
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from functools import lru_cache
from pathlib import Path

from claims_pipeline.fhir import codes as c

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class SimulationSettings:
    start: date = date(2026, 1, 1)
    end: date = date(2026, 9, 30)
    n_patients: int = 10_000
    seed: int = 7
    dq_error_rate: float = 0.012
    malformed_line_rate: float = 0.0004


DEFAULT_SETTINGS = SimulationSettings()
Resources = dict[str, list[dict]]


def _iso(ts: datetime) -> str:
    return ts.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _ref(resource_type: str, resource_id: str) -> dict:
    return {"reference": f"{resource_type}/{resource_id}"}


def _money(value: float) -> dict:
    return {"value": round(value, 2), "currency": "USD"}


def _meta(ts: datetime, version: int = 1) -> dict:
    return {"versionId": str(version), "lastUpdated": _iso(ts)}


def _coding(system: str, code: str, display: str | None = None) -> dict:
    coding = {"system": system, "code": code}
    if display:
        coding["display"] = display
    return coding


def _icd10(code: str) -> dict:
    dx = c.DIAGNOSES.get(code)
    return _coding(c.ICD10_SYSTEM, code, dx.display if dx else None)


def _adjudication(category: str, amount: float, reason: str | None = None) -> dict:
    entry = {
        "category": {"coding": [_coding(c.ADJUDICATION_SYSTEM, category)]},
        "amount": _money(amount),
    }
    if reason:
        entry["reason"] = {"coding": [_coding(c.CARC_SYSTEM, reason, c.DENIAL_REASONS[reason])]}
    return entry


def _poisson(rng: random.Random, lam: float) -> int:
    threshold, k, p = math.exp(-lam), 0, 1.0
    while True:
        p *= rng.random()
        if p <= threshold:
            return k
        k += 1


class _WorldBuilder:
    def __init__(self, settings: SimulationSettings):
        self.s = settings
        self.rng = random.Random(settings.seed)
        self.by_day: dict[date, Resources] = defaultdict(lambda: defaultdict(list))
        self.counters: dict[str, int] = defaultdict(int)
        self.window_days = (settings.end - settings.start).days + 1

    def next_id(self, prefix: str) -> str:
        self.counters[prefix] += 1
        return f"{prefix}-{self.counters[prefix]:07d}"

    def emit(self, resource: dict, ts: datetime) -> None:
        day = ts.date()
        if self.s.start <= day <= self.s.end:
            self.by_day[day][resource["resourceType"]].append(resource)

    def corrupt(self, share: float) -> bool:
        return self.rng.random() < self.s.dq_error_rate * share

    def random_ts(self, hours: tuple[int, int] = (0, 23)) -> datetime:
        day = self.s.start + timedelta(days=self.rng.randrange(self.window_days))
        return datetime.combine(day, time(self.rng.randint(*hours), self.rng.randint(0, 59)), UTC)

    def build(self) -> dict[date, Resources]:
        initial_ts = datetime.combine(self.s.start, time(0, 5), UTC)
        for org in [*c.PAYERS, *c.PROVIDERS]:
            self.emit(self.organization(org, initial_ts), initial_ts)
        for i in range(1, self.s.n_patients + 1):
            self.simulate_patient(i, initial_ts)
        return self.by_day

    def organization(self, org: c.Organization, ts: datetime) -> dict:
        segment = org.segment if isinstance(org, c.Payer) else org.kind
        return {
            "resourceType": "Organization",
            "id": org.id,
            "meta": _meta(ts),
            "active": True,
            "type": [
                {"coding": [_coding(c.ORG_TYPE_SYSTEM, org.org_type)]},
                {"coding": [_coding(c.ORG_SEGMENT_SYSTEM, segment)]},
            ],
            "name": org.name,
            "address": [{"state": org.state, "country": "US"}],
        }

    def simulate_patient(self, index: int, initial_ts: datetime) -> None:
        rng = self.rng
        age = min(95, max(0, int(rng.triangular(0, 95, 68))))
        gender = rng.choice(["female", "male"])
        state = rng.choices(list(c.STATES), weights=[0.6, 0.25, 0.15])[0]
        birth_date = self.s.start - timedelta(days=age * 365 + rng.randrange(365))
        if age >= 65:
            payer = rng.choices(["payer-medicare", "payer-harbor"], weights=[0.72, 0.28])[0]
        else:
            payer = rng.choices(
                ["payer-medicaid", "payer-northstar", "payer-summit", "payer-evergreen"],
                weights=[0.3, 0.3, 0.2, 0.2],
            )[0]
        # A list, not a set: set iteration order of str varies with PYTHONHASHSEED.
        chronic = [
            name
            for name, p in {
                "hypertension": min(0.75, 0.1 + age * 0.007),
                "diabetes": min(0.4, 0.03 + age * 0.004),
                "heart_failure": max(0.0, (age - 55) * 0.006),
                "copd": max(0.0, (age - 45) * 0.004),
                "depression": 0.09,
            }.items()
            if rng.random() < p
        ]

        patient_id = f"pat-{index:06d}"
        patient = {
            "resourceType": "Patient",
            "id": patient_id,
            "meta": _meta(initial_ts),
            "identifier": [{"system": c.MRN_SYSTEM, "value": f"MRN{index:07d}"}],
            "name": [
                {
                    "use": "official",
                    "family": rng.choice(c.FAMILY_NAMES),
                    "given": [rng.choice(c.GIVEN_NAMES[gender])],
                }
            ],
            "gender": gender,
            "birthDate": birth_date.isoformat(),
            "address": [self.address(state)],
        }
        if self.corrupt(0.3):
            patient["birthDate"] = "2099-01-01"
        self.emit(patient, initial_ts)

        if rng.random() < 0.04:
            moved_ts = self.random_ts((9, 17))
            new_state = rng.choice([s for s in c.STATES if s != state])
            self.emit(
                {
                    **patient,
                    "meta": _meta(moved_ts, version=2),
                    "address": [self.address(new_state)],
                },
                moved_ts,
            )

        ctx = {"patient_id": patient_id, "payer_id": payer, "age": age, "chronic": chronic}
        for _ in range(_poisson(rng, 0.8 + 1.1 * len(chronic))):
            reasons = [c.CHRONIC_VISIT_REASONS[x] for x in chronic] or ["Z00.00"]
            if rng.random() < 0.25:
                reasons = ["Z00.00", "M54.50"]
            self.encounter(ctx, c.AMBULATORY, rng.choice(reasons), self.random_ts((8, 17)))
        for _ in range(_poisson(rng, 0.15 + 0.08 * len(chronic))):
            dx = rng.choices(["R07.9", "N39.0", "S93.401A"], weights=[0.4, 0.35, 0.25])[0]
            self.encounter(ctx, c.EMERGENCY, dx, self.random_ts())
        inpatient_rate = (
            0.02
            + 0.3 * ("heart_failure" in chronic)
            + 0.22 * ("copd" in chronic)
            + 0.06 * (age >= 75)
        )
        for _ in range(_poisson(rng, inpatient_rate)):
            if "heart_failure" in chronic and rng.random() < 0.6:
                dx = "I50.9"
            elif "copd" in chronic and rng.random() < 0.6:
                dx = "J44.1"
            else:
                options = ["J18.9", "A41.9"] + (["S72.001A"] if age >= 65 else [])
                dx = rng.choice(options)
            self.admission(ctx, dx, self.random_ts(), allow_readmission=True)

    def address(self, state: str) -> dict:
        prefix = {"TX": (75000, 79999), "OK": (73000, 74999), "NM": (87000, 88499)}[state]
        return {
            "use": "home",
            "city": self.rng.choice(c.STATES[state]),
            "state": state,
            "postalCode": str(self.rng.randint(*prefix)),
            "country": "US",
        }

    def admission(self, ctx: dict, dx: str, start: datetime, allow_readmission: bool) -> None:
        los = self.rng.randint(2, 6) + (3 if dx == "A41.9" else 0)
        discharge = self.encounter(ctx, c.INPATIENT, dx, start, los_days=los)
        if discharge and allow_readmission:
            if self.rng.random() < c.DIAGNOSES[dx].readmission_risk:
                gap = self.rng.randint(2, 30)
            elif self.rng.random() < 0.05:
                gap = self.rng.randint(31, 75)
            else:
                return
            readmit_dx = dx if self.rng.random() < 0.7 else "J18.9"
            readmit_start = discharge + timedelta(days=gap, hours=self.rng.randint(0, 12))
            self.admission(ctx, readmit_dx, readmit_start, allow_readmission=False)

    def encounter(
        self, ctx: dict, cls: str, dx: str, start: datetime, los_days: int = 0
    ) -> datetime | None:
        rng = self.rng
        if cls == c.INPATIENT:
            end = start + timedelta(days=los_days, hours=rng.randint(-6, 6))
        elif cls == c.EMERGENCY:
            end = start + timedelta(hours=rng.randint(2, 9))
        else:
            end = start + timedelta(minutes=rng.choice([20, 30, 40, 60]))
        if end.date() > self.s.end:
            return None

        provider = rng.choice(c.PROVIDERS_BY_SETTING[cls])
        encounter_id = self.next_id("enc")
        period = {"start": _iso(start), "end": _iso(end)}
        if self.corrupt(0.2):
            period = {"start": _iso(end), "end": _iso(start)}
        encounter = {
            "resourceType": "Encounter",
            "id": encounter_id,
            "meta": _meta(end),
            "status": "finished",
            "class": _coding(c.ACT_CODE_SYSTEM, cls, c.ENCOUNTER_CLASS_DISPLAY[cls]),
            "subject": _ref("Patient", ctx["patient_id"]),
            "serviceProvider": _ref("Organization", provider.id),
            "period": period,
            "reasonCode": [{"coding": [_icd10(dx)]}],
        }
        if cls == c.INPATIENT:
            encounter["hospitalization"] = {
                "dischargeDisposition": {
                    "coding": [
                        _coding(
                            "http://terminology.hl7.org/CodeSystem/discharge-disposition",
                            "home",
                            "Home",
                        )
                    ]
                }
            }
        self.emit(encounter, end)

        diagnoses = [dx]
        if cls == c.INPATIENT:
            diagnoses += sorted({c.CHRONIC_VISIT_REASONS[x] for x in ctx["chronic"]} - {dx, "I10"})[
                :2
            ]
        for dx_code in diagnoses:
            code = dx_code
            if self.corrupt(0.5):
                code = rng.choice(["I509X", "UNKNOWN", "1234"])
            self.emit(
                {
                    "resourceType": "Condition",
                    "id": self.next_id("cond"),
                    "meta": _meta(end),
                    "clinicalStatus": {
                        "coding": [
                            _coding(
                                "http://terminology.hl7.org/CodeSystem/condition-clinical",
                                "active",
                            )
                        ]
                    },
                    "verificationStatus": {
                        "coding": [
                            _coding(
                                "http://terminology.hl7.org/CodeSystem/condition-ver-status",
                                "confirmed",
                            )
                        ]
                    },
                    "category": [
                        {
                            "coding": [
                                _coding(
                                    "http://terminology.hl7.org/CodeSystem/condition-category",
                                    "encounter-diagnosis",
                                )
                            ]
                        }
                    ],
                    "code": {"coding": [_icd10(code)]},
                    "subject": _ref("Patient", ctx["patient_id"]),
                    "encounter": _ref("Encounter", encounter_id),
                    "recordedDate": _iso(end),
                },
                end,
            )

        self.claim(ctx, cls, diagnoses, encounter_id, provider, start, end, los_days)
        return end

    def claim_lines(self, cls: str, dx: str, los_days: int) -> list[tuple[str, int]]:
        if cls != c.INPATIENT:
            return [(code, 1) for code in c.VISIT_PROCEDURES[dx]]
        lines = [("99223", 1)]
        if los_days > 2:
            lines.append(("99233", los_days - 2))
        lines += [("99239", 1), ("0120", los_days)]
        return lines + [(code, 1) for code in c.INPATIENT_EXTRA_PROCEDURES[dx]]

    def claim(
        self,
        ctx: dict,
        cls: str,
        diagnoses: list[str],
        encounter_id: str,
        provider: c.Organization,
        start: datetime,
        end: datetime,
        los_days: int,
    ) -> None:
        rng = self.rng
        payer = c.PAYERS_BY_ID[ctx["payer_id"]]
        late = rng.random() < 0.05
        lag_days = rng.randint(10, 25) if late else rng.randint(0, 4)
        created = datetime.combine(
            end.date() + timedelta(days=lag_days), time(rng.randint(17, 22), 0), UTC
        )
        price_factor = rng.uniform(0.95, 1.12)
        claim_id = self.next_id("clm")
        claim_type = "institutional" if cls in (c.INPATIENT, c.EMERGENCY) else "professional"

        items = []
        for seq, (code, qty) in enumerate(self.claim_lines(cls, diagnoses[0], los_days), 1):
            proc = c.PROCEDURES[code]
            unit_price = proc.price * price_factor
            items.append(
                {
                    "sequence": seq,
                    "productOrService": {"coding": [_coding(proc.system, code, proc.display)]},
                    "servicedDate": start.date().isoformat(),
                    "quantity": {"value": qty},
                    "unitPrice": _money(unit_price),
                    "net": _money(unit_price * qty),
                    "encounter": [_ref("Encounter", encounter_id)],
                }
            )
        billed = sum(item["net"]["value"] for item in items)

        claim = {
            "resourceType": "Claim",
            "id": claim_id,
            "meta": _meta(created),
            "status": "active",
            "type": {"coding": [_coding(c.CLAIM_TYPE_SYSTEM, claim_type)]},
            "use": "claim",
            "patient": _ref("Patient", ctx["patient_id"]),
            "billablePeriod": {"start": _iso(start), "end": _iso(end)},
            "created": _iso(created),
            "insurer": _ref("Organization", payer.id),
            "provider": _ref("Organization", provider.id),
            "priority": {
                "coding": [
                    _coding("http://terminology.hl7.org/CodeSystem/processpriority", "normal")
                ]
            },
            "diagnosis": [
                {"sequence": i, "diagnosisCodeableConcept": {"coding": [_icd10(code)]}}
                for i, code in enumerate(diagnoses, 1)
            ],
            "insurance": [{"sequence": 1, "focal": True, "coverage": {"display": payer.name}}],
            "item": items,
            "total": _money(billed),
        }
        if self.corrupt(0.4):
            del claim["patient"]
        elif self.corrupt(0.2):
            claim["total"] = _money(-billed)
        self.emit(claim, created)

        if rng.random() < 0.006:
            resent = created + timedelta(days=1)
            self.emit({**claim, "meta": _meta(resent, version=2)}, resent)

        self.adjudicate(claim, payer, cls, created, late)

    def adjudicate(
        self, claim: dict, payer: c.Payer, cls: str, created: datetime, late: bool
    ) -> None:
        rng = self.rng
        if rng.random() < 0.015:
            return  # payer never responds: the claim ages in accounts receivable
        low, high = payer.days_to_pay
        adjudicated = created + timedelta(
            days=rng.randint(low, high) + (10 if cls == c.INPATIENT else 0),
            hours=rng.randint(1, 12),
        )
        denial_p = payer.denial_rate * (1.5 if cls == c.INPATIENT else 1.0) * (2.0 if late else 1.0)
        denied = rng.random() < denial_p
        reason = None
        if denied:
            weights = c.DENIAL_WEIGHTS[cls]
            reason = (
                "29"
                if late and rng.random() < 0.6
                else rng.choices(list(weights), weights=list(weights.values()))[0]
            )
        partial_line = (
            rng.randrange(len(claim["item"])) if not denied and rng.random() < 0.07 else None
        )

        items, totals = [], {"submitted": 0.0, "eligible": 0.0, "benefit": 0.0}
        for idx, item in enumerate(claim["item"]):
            submitted = item["net"]["value"]
            line_reason = reason if denied else ("50" if idx == partial_line else None)
            if line_reason:
                eligible = paid = 0.0
            else:
                eligible = submitted * payer.allowed_ratio * rng.uniform(0.9, 1.1)
                paid = eligible * rng.uniform(0.8, 1.0)
            totals["submitted"] += submitted
            totals["eligible"] += eligible
            totals["benefit"] += paid
            items.append(
                {
                    "sequence": item["sequence"],
                    "productOrService": item["productOrService"],
                    "adjudication": [
                        _adjudication("submitted", submitted),
                        _adjudication("eligible", eligible),
                        _adjudication("benefit", paid, line_reason),
                    ],
                }
            )
        if self.corrupt(0.2):
            totals["benefit"] = totals["submitted"] * 1.5

        eob = {
            "resourceType": "ExplanationOfBenefit",
            "id": claim["id"].replace("clm-", "eob-"),
            "meta": _meta(adjudicated),
            "status": "active",
            "type": claim["type"],
            "use": "claim",
            "patient": claim.get("patient", _ref("Patient", "unknown")),
            "created": _iso(adjudicated),
            "insurer": claim["insurer"],
            "provider": claim["provider"],
            "claim": _ref("Claim", claim["id"]),
            "outcome": "partial" if partial_line is not None else "complete",
            "insurance": [{"focal": True, "coverage": {"display": payer.name}}],
            "item": items,
            "total": [
                {
                    "category": {"coding": [_coding(c.ADJUDICATION_SYSTEM, category)]},
                    "amount": _money(amount),
                }
                for category, amount in totals.items()
            ],
        }
        if totals["benefit"] > 0:
            eob["payment"] = {
                "date": (adjudicated.date() + timedelta(days=2)).isoformat(),
                "amount": _money(totals["benefit"]),
            }
        self.emit(eob, adjudicated)


@lru_cache(maxsize=4)
def build_world(settings: SimulationSettings = DEFAULT_SETTINGS) -> dict[date, Resources]:
    return _WorldBuilder(settings).build()


def resources_for_day(day: date, settings: SimulationSettings = DEFAULT_SETTINGS) -> Resources:
    return build_world(settings).get(day, {})


def export_day(
    day: date, landing_dir: Path, settings: SimulationSettings = DEFAULT_SETTINGS
) -> dict[str, int]:
    """Write one day's Bulk Data export (NDJSON per resource type + manifest), atomically."""
    resources = resources_for_day(day, settings)
    rng = random.Random(settings.seed * 1_000_003 + day.toordinal())
    target = landing_dir / f"export_date={day.isoformat()}"
    staging = landing_dir / f".tmp_export_date={day.isoformat()}"
    shutil.rmtree(staging, ignore_errors=True)
    staging.mkdir(parents=True)

    counts: dict[str, int] = {}
    for resource_type in sorted(resources):
        lines = []
        for resource in resources[resource_type]:
            line = json.dumps(resource, separators=(",", ":"))
            if rng.random() < settings.malformed_line_rate:
                line = line[: len(line) // 2]
            lines.append(line)
        (staging / f"{resource_type}.ndjson").write_text("\n".join(lines) + "\n", encoding="utf-8")
        counts[resource_type] = len(lines)

    transaction_time = datetime.combine(day, time(23, 59, 59), UTC)
    manifest = {
        "transactionTime": _iso(transaction_time),
        "request": f"[base]/$export?_since={day.isoformat()}T00:00:00Z",
        "requiresAccessToken": False,
        "output": [
            {"type": rt, "url": f"{rt}.ndjson", "count": n} for rt, n in sorted(counts.items())
        ],
        "error": [],
    }
    (staging / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    shutil.rmtree(target, ignore_errors=True)
    staging.rename(target)
    logger.info("Exported %s: %s", day, counts)
    return counts
