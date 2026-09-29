"""Reference catalogs for the synthetic health system. Organization names are fictional."""

from dataclasses import dataclass

ICD10_SYSTEM = "http://hl7.org/fhir/sid/icd-10-cm"
CPT_SYSTEM = "http://www.ama-assn.org/go/cpt"
REVENUE_SYSTEM = "https://www.nubc.org/CodeSystem/RevenueCodes"
CARC_SYSTEM = "https://x12.org/codes/claim-adjustment-reason-codes"
ACT_CODE_SYSTEM = "http://terminology.hl7.org/CodeSystem/v3-ActCode"
CLAIM_TYPE_SYSTEM = "http://terminology.hl7.org/CodeSystem/claim-type"
ORG_TYPE_SYSTEM = "http://terminology.hl7.org/CodeSystem/organization-type"
ORG_SEGMENT_SYSTEM = "https://example.org/fhir/CodeSystem/organization-segment"
ADJUDICATION_SYSTEM = "http://terminology.hl7.org/CodeSystem/adjudication"
MRN_SYSTEM = "urn:oid:2.16.840.1.113883.19.5.99999.1"

INPATIENT, EMERGENCY, AMBULATORY = "IMP", "EMER", "AMB"
ENCOUNTER_CLASS_DISPLAY = {
    INPATIENT: "inpatient encounter",
    EMERGENCY: "emergency",
    AMBULATORY: "ambulatory",
}


@dataclass(frozen=True)
class Diagnosis:
    code: str
    display: str
    category: str
    setting: str
    readmission_risk: float = 0.0


DIAGNOSES = {
    d.code: d
    for d in [
        Diagnosis("I50.9", "Heart failure, unspecified", "Cardiovascular", INPATIENT, 0.22),
        Diagnosis("J44.1", "COPD with acute exacerbation", "Respiratory", INPATIENT, 0.19),
        Diagnosis("J18.9", "Pneumonia, unspecified organism", "Respiratory", INPATIENT, 0.15),
        Diagnosis("A41.9", "Sepsis, unspecified organism", "Infectious", INPATIENT, 0.17),
        Diagnosis(
            "S72.001A",
            "Fracture of right femoral neck, initial encounter",
            "Injury",
            INPATIENT,
            0.07,
        ),
        Diagnosis("R07.9", "Chest pain, unspecified", "Symptoms", EMERGENCY),
        Diagnosis(
            "N39.0", "Urinary tract infection, site not specified", "Genitourinary", EMERGENCY
        ),
        Diagnosis(
            "S93.401A", "Sprain of right ankle ligament, initial encounter", "Injury", EMERGENCY
        ),
        Diagnosis(
            "E11.9", "Type 2 diabetes mellitus without complications", "Endocrine", AMBULATORY
        ),
        Diagnosis("I10", "Essential (primary) hypertension", "Cardiovascular", AMBULATORY),
        Diagnosis(
            "F32.9", "Major depressive disorder, single episode", "Mental health", AMBULATORY
        ),
        Diagnosis("M54.50", "Low back pain, unspecified", "Musculoskeletal", AMBULATORY),
        Diagnosis("Z00.00", "General adult medical examination", "Preventive", AMBULATORY),
    ]
}

CHRONIC_VISIT_REASONS = {
    "hypertension": "I10",
    "diabetes": "E11.9",
    "heart_failure": "I10",
    "copd": "I10",
    "depression": "F32.9",
}


@dataclass(frozen=True)
class Procedure:
    code: str
    display: str
    price: float
    system: str = CPT_SYSTEM


PROCEDURES = {
    p.code: p
    for p in [
        Procedure("99213", "Office visit, established patient, low complexity", 135.0),
        Procedure("99214", "Office visit, established patient, moderate complexity", 195.0),
        Procedure("99396", "Preventive visit, established patient, 40-64 years", 260.0),
        Procedure("90837", "Psychotherapy, 60 minutes", 185.0),
        Procedure("80053", "Comprehensive metabolic panel", 48.0),
        Procedure("93000", "Electrocardiogram, complete", 65.0),
        Procedure("81001", "Urinalysis with microscopy", 28.0),
        Procedure("71046", "Chest X-ray, 2 views", 215.0),
        Procedure("73610", "Ankle X-ray, complete", 160.0),
        Procedure("99284", "Emergency department visit, high severity", 690.0),
        Procedure("99285", "Emergency department visit, highest severity", 1150.0),
        Procedure("99223", "Initial hospital care, high complexity", 430.0),
        Procedure("99233", "Subsequent hospital care, high complexity", 215.0),
        Procedure("99239", "Hospital discharge day management, >30 minutes", 230.0),
        Procedure("27236", "Open treatment of femoral neck fracture", 3400.0),
        Procedure("0120", "Room and board, semi-private (per diem)", 2850.0, REVENUE_SYSTEM),
    ]
}

VISIT_PROCEDURES = {
    "I10": ["99213", "80053"],
    "E11.9": ["99214", "80053"],
    "F32.9": ["90837"],
    "M54.50": ["99213"],
    "Z00.00": ["99396", "80053"],
    "R07.9": ["99285", "93000", "71046", "80053"],
    "N39.0": ["99284", "81001"],
    "S93.401A": ["99284", "73610"],
}
INPATIENT_EXTRA_PROCEDURES = {
    "I50.9": ["93000", "71046"],
    "J44.1": ["71046"],
    "J18.9": ["71046", "80053"],
    "A41.9": ["80053", "81001"],
    "S72.001A": ["27236", "73610"],
}


@dataclass(frozen=True)
class Organization:
    id: str
    name: str
    kind: str
    state: str
    org_type: str


@dataclass(frozen=True)
class Payer(Organization):
    segment: str = "Commercial"
    denial_rate: float = 0.1
    allowed_ratio: float = 0.5
    days_to_pay: tuple[int, int] = (14, 35)


PAYERS = [
    Payer("payer-medicare", "Medicare", "Payer", "US", "pay", "Government", 0.06, 0.36, (12, 28)),
    Payer("payer-medicaid", "Medicaid", "Payer", "US", "pay", "Government", 0.11, 0.30, (20, 55)),
    Payer(
        "payer-northstar",
        "Northstar Health Plan",
        "Payer",
        "TX",
        "pay",
        "Commercial",
        0.08,
        0.55,
        (10, 30),
    ),
    Payer(
        "payer-summit",
        "Summit Mutual Insurance",
        "Payer",
        "TX",
        "pay",
        "Commercial",
        0.16,
        0.60,
        (15, 45),
    ),
    Payer(
        "payer-evergreen",
        "Evergreen Care Alliance",
        "Payer",
        "OK",
        "pay",
        "Commercial",
        0.10,
        0.52,
        (12, 40),
    ),
    Payer(
        "payer-harbor",
        "Harbor Health Advantage",
        "Payer",
        "NM",
        "pay",
        "Medicare Advantage",
        0.19,
        0.40,
        (20, 60),
    ),
]
PAYERS_BY_ID = {p.id: p for p in PAYERS}

PROVIDERS = [
    Organization(
        "org-cedar-hollow", "Cedar Hollow Regional Medical Center", "Hospital", "TX", "prov"
    ),
    Organization("org-lakeside-meridian", "Lakeside Meridian Hospital", "Hospital", "OK", "prov"),
    Organization("org-red-river", "Red River Valley Hospital", "Hospital", "NM", "prov"),
    Organization("org-northfield", "Northfield Family Clinic", "Clinic", "TX", "prov"),
    Organization("org-westgate", "Westgate Primary Care", "Clinic", "OK", "prov"),
    Organization("org-pineview", "Pineview Internal Medicine", "Clinic", "NM", "prov"),
    Organization("org-metro-urgent", "Metro Urgent & Emergency Care", "Emergency", "TX", "prov"),
]
PROVIDERS_BY_SETTING = {
    INPATIENT: [p for p in PROVIDERS if p.kind == "Hospital"],
    EMERGENCY: [p for p in PROVIDERS if p.kind in ("Hospital", "Emergency")],
    AMBULATORY: [p for p in PROVIDERS if p.kind == "Clinic"],
}

# X12 Claim Adjustment Reason Codes used as denial reasons (group code CO = contractual obligation).
DENIAL_REASONS = {
    "16": "Claim lacks information or has submission/billing errors",
    "197": "Precertification/authorization absent",
    "50": "Not deemed a medical necessity",
    "22": "Coordination of benefits: another payer may be primary",
    "29": "Time limit for filing has expired",
    "18": "Exact duplicate claim/service",
}
# code -> (category, recommended action, recoverable through rework or appeal)
DENIAL_ACTIONS = {
    "16": ("Billing error", "Correct claim data and resubmit", True),
    "197": ("Authorization", "Request retro-authorization and appeal", True),
    "50": ("Medical necessity", "Appeal with clinical documentation", True),
    "22": ("Coordination of benefits", "Bill the primary payer, then resubmit", True),
    "29": ("Timely filing", "Write off; review submission lag", False),
    "18": ("Duplicate", "Verify original claim status; no rebill", False),
}
DENIAL_WEIGHTS = {
    AMBULATORY: {"16": 45, "22": 20, "50": 12, "29": 8, "18": 10, "197": 5},
    EMERGENCY: {"16": 35, "22": 15, "50": 25, "29": 8, "18": 7, "197": 10},
    INPATIENT: {"16": 20, "22": 8, "50": 27, "29": 5, "18": 5, "197": 35},
}

STATES = {
    "TX": ["Austin", "Dallas", "Houston", "San Antonio", "El Paso"],
    "OK": ["Oklahoma City", "Tulsa", "Norman"],
    "NM": ["Albuquerque", "Santa Fe", "Las Cruces"],
}
GIVEN_NAMES = {
    "female": [
        "Maria",
        "Emma",
        "Olivia",
        "Sofia",
        "Grace",
        "Linda",
        "Carmen",
        "Ruth",
        "Ava",
        "Nora",
    ],
    "male": [
        "James",
        "Luis",
        "Robert",
        "Daniel",
        "Carlos",
        "Henry",
        "Samuel",
        "Jose",
        "Leo",
        "Owen",
    ],
}
FAMILY_NAMES = [
    "Garcia",
    "Smith",
    "Johnson",
    "Martinez",
    "Brown",
    "Lopez",
    "Davis",
    "Wilson",
    "Anderson",
    "Hernandez",
    "Moore",
    "Taylor",
    "Thomas",
    "Jackson",
    "White",
    "Harris",
    "Clark",
    "Lewis",
]
