# Gold data dictionary

All tables live in `lakehouse/gold/<table>` (Delta Lake). They are rebuilt atomically on every run.

## fact_claim (one row per claim)

| Column | Description |
|---|---|
| `claim_id`, `patient_id`, `provider_id`, `payer_id`, `encounter_id` | Keys to the dimensions and the encounter |
| `encounter_class` | `AMB` ambulatory, `EMER` emergency, `IMP` inpatient |
| `claim_type` | `professional` or `institutional` |
| `primary_dx_code`, `dx_chapter` | Principal ICD-10-CM diagnosis and its chapter |
| `service_date`, `submitted_date`, `submitted_month` | Date of service and date the claim was sent |
| `days_to_submit` | Days from end of service to submission (timely-filing risk) |
| `billed_amount` | Gross charges on the claim |
| `allowed_amount`, `paid_amount`, `denied_amount` | From the payer's ExplanationOfBenefit (0 while pending) |
| `claim_status` | `paid`, `partially_denied`, `denied`, or `pending` (no adjudication yet) |
| `denial_reason_code` | X12 CARC, e.g. `CO-197` (prior authorization absent) |
| `adjudicated_date`, `days_to_adjudication`, `payment_date` | Payer turnaround |
| `ar_age_days` | Pending claims only: days since submission, as of the run's end date |
| `is_clean_claim` | Paid in full on first submission |

## fact_inpatient_stay (one row per inpatient encounter)

`admit_date`, `discharge_date`, `length_of_stay_days`, `principal_dx`, `next_admit_date`,
`days_to_readmission`, `readmitted_30d`, and `is_eligible_index`. A stay is an eligible index stay
only if its discharge is at least 30 days before the as-of date, so every counted stay has a full
follow-up window.

## Dimensions

| Table | Grain | Notes |
|---|---|---|
| `dim_patient` | current patient | Age and age band as of the run, `address_versions` from SCD2 history |
| `dim_payer` | payer | `payer_segment`: Government, Commercial, Medicare Advantage |
| `dim_provider` | facility | `facility_type`: Hospital, Clinic, Emergency |
| `dim_diagnosis` | ICD-10-CM code | Description and chapter |
| `dim_denial_reason` | CARC code | Category, recommended action, `recoverable` flag |

## Marts

| Table | Grain | Key metrics |
|---|---|---|
| `mart_payer_monthly` | payer x submission month | denial rate, clean claim rate, billed/allowed/paid, net collection rate, days to adjudication |
| `mart_denials` | payer x reason x setting x month | denied claims, denied dollars, recoverable flag |
| `mart_ar_aging` | payer x aging bucket | open claims and outstanding amount (0-30, 31-60, 61-90, 90+) |
| `mart_readmissions` | discharge month x principal diagnosis | index stays, readmissions, 30-day rate, average LOS |
| `mart_data_quality` | export date x entity x rule | checked, failed, failure rate |

## Metric definitions

- **Denial rate**: fully denied claims / adjudicated claims (pending claims excluded).
- **Clean claim rate**: claims paid with no denied line / adjudicated claims.
- **Net collection rate**: paid / allowed. It measures collections against what contracts allow, not against gross charges.
- **30-day readmission rate**: eligible index stays followed by another inpatient admission of
  the same patient within 0-30 days of discharge / eligible index stays.
