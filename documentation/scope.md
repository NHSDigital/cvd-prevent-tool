# Proof of concept scope

- [Proof of concept scope](#proof-of-concept-scope)
  - [Overview](#overview)
  - [Data sources](#data-sources)
  - [Cohort definition](#cohort-definition)
    - [High risk CVD](#high-risk-cvd)
    - [CVD](#cvd)
  - [Patient Characteristics](#patient-characteristics)
  - [Additional Indicators](#additional-indicators)
    - [Treated to target Hypertension](#treated-to-target-hypertension)
    - [Not treated to target Hypertension](#not-treated-to-target-hypertension)
  - [Outcomes](#outcomes)
  - [Annex – Proposed outputs for proof of concept phase 1](#annex--proposed-outputs-for-proof-of-concept-phase-1)



## Overview
Phase 1, create a data asset linking primary, secondary and mortality data. Data asset will be used to calculate the number of current registered patients in cohort:

- CVD / high risk CVD
- Managed/ not managed to target using Blood Pressure
- Admitted for Stroke
- Admitted for Heart Attack
- Under 75 with CVD cause of death
- Stroke cause of death
- MI cause of death
- Died within 30 days from stroke hospitalisation
- Died within 30 days from MI hospitalisation

## Data sources

- Primary care data: CVDPREVENT quarterly extractions. Will use all available snapshot extractions to build a longitudinal patient record
- Secondary care: Hospital Episode Statistics (HES) inpatient (APC) data. Will filter data from 2012/13 to most recent extraction available. Earlier data will not be used as we were advised that (diagnostic) coding of the data will not be consistent to current practice.
- Mortality data: Death registration data

## Cohort definition

Patients recorded as high-risk CVD CVDPCX001 or with CVD conditions CVDPCX002

Patients will be flagged by condition, using the same definitions as used for the CVDPREVENT extraction. These will be defined only using CVDPREVENT data

Patient follow-up:

[max(Earliest diagnosis, data q acceptable date) , min(latest HES extraction date, date of death)]

### High risk CVD

- Non-diabetic hyperglycaemia TBC (NDH\_COD )
  - Do we want to include pre-diabetes: Earliest of (IGT\_COD , NDH\_COD, PRD\_COD)
- Familial hypercholesterolaemia CVDPREVENT FH measure (probable, possible, and confirmed)
- Hypertension HYP\_COD excluding later HYPRES\_COD
- Atrial fibrillation AFIB\_COD excluding later AFIBRES\_COD
- Chronic kidney disease CKD\_COD excluding later CKDRES\_COD
- Diabetes mellitus TBC: (DMTYPE1AUDIT\_COD or DMTYPE2AUDIT\_COD)

### CVD

- Stroke or transient ischaemic attack STRK\_COD or TIA\_COD
- CHD CHD\_COD
- Heart failure HF\_COD
- Abdominal aortic aneurysm AAA\_COD
- Peripheral arterial disease PAD\_COD

Using cluster definitions above is crude, CVDPREVENT will share case definitions that will then be included in the build of this asset

## Patient Characteristics

For proof of concept, we will use CVDPREVENT to define patient characteristics

- Age (groups to be defined)
- Sex
- Ethnicity
- Deprivation to be inferred using patient postcode (IMD scores)

## Additional Indicators

### Treated to target Hypertension

Definition

- Patients aged 79 years or under, in whom their latest valid blood pressure pairing capturing both a systolic and diastolic reading (measured in the preceding 12 months from extraction date) is 140/90 mmHg or less
  - Hypertension flag (HTN) and BP\_COD reading (50 ≤ systolic ≤ 140) AND (20 ≤ diastolic ≤ 90) AND Age \<80
- Patients aged 80 years or over, in whom their latest valid blood pressure pairing capturing both a systolic and diastolic reading (measured in the preceding 12 months from extraction date) is 150/90 mmHg or less
  - Hypertension flag (HTN) and BP\_COD reading (50≤systolic ≤150) AND (20 ≤ diastolic ≤ 90) AND Age ≥ 80

- Age calculated at extraction date
- Flag patient 79 years or under, 80 years or over
- On the event of multiple readings on the same date we take the readings with the highest values (max)

### Not treated to target Hypertension

Using the latest valid blood pressure pairing we define six high risk patient groups

1. Hypertension flag (HTN) and BP\_COD reading (systolic \>180) OR (diastolic \> 120) (risk group 1)
2. HTN and BP\_COD reading (systolic \>160 AND ≤180) OR (diastolic \> 100 AND ≤120) (risk group 2)
3. HTN and BP\_COD reading (systolic \>140 AND ≤160) OR (diastolic \> 90 AND ≤100) AND Age 79 years or under (risk group 3a)
4. HTN and BP\_COD reading (systolic \>150 AND ≤160) OR (diastolic \> 90 AND ≤100) AND Age 80 years or over (risk group 3b)
5. HTN and latest blood pressure reading (systolic \< 50 OR diastolic \< 20 (risk group 8)
6. HTN and no blood pressure reading in the last 12 months (risk group 9)

- Age calculated at extraction date

- On the event of multiple readings on the same date we take the readings with the lowest values

## Outcomes

Restricted to cohort patients as defined above but can occur before their first CVDPREVENT event.

- Hospital admission for Stroke (primary diagnosis): HES Primary diagnosis ICD10 I61\*, I63\* and I64\*
- Hospital admission for Heart Attack (primary diagnosis): TBD potentially I21\* and I22\*
- Mortality from CVD under 75 defined as: I00–I09, I10–I15 I20–I25 I26-128, I30-152, I60–I69, I70-I79, I80-I89, I96-I99
- Mortality from Stroke: Primary cause of death ICD10 I61\*, I63\* and I64\*
- Mortality within 30 days of hospital admission for stroke

Not in scope for Proof of Concept

- Use mortality and HES data to identify new high risk or CVD patients
- Smoking status:

  - Smoker NDASMOK\_COD or SMOKINGINT\_COD
  - No smoking status

- Vascular dementia
- Readmissions
- Combine information from HES and mortality data to improve the completeness of ethnicity (in CVDPREVENT ~82%)
- Reconcile information from all data sources to derive patient characteristics
- Build non-elective hospital admissions algorithm to allow for non-primary diagnoses for Stroke and Heart Attack.

Phase 2: similar process will then be established for the remaining proposed CVD indicators for ABCs (set out in the CVD Prevention Tool – Development Phase Indicator selection v.0.5), this will be put in place with learning taken from phase 1.

Phase 3: extend to cover all CVD indicators to align with QOF, CVDPREVENT, CVDAction and any relevant clinical recommendations.

## Annex – Proposed outputs for proof of concept phase 1

**Prevalence:**

- Number and % GP recordedhigh risk CVD and CVD conditions by week/month

(Breakdown by high risk CVD and CVD conditions)

New cases:

- Number of new patients with GP recorded high risk CVD and recorded CVD condition by week/month

(Breakdown by high risk CVD and CVD conditions)

**Interventions:**

- Number and % GP recorded hypertension patients who are currently treated to target (NICE guidance) by week/month
- Number and % GP recorded hypertension patients who are currently NOT treated to target (NICE guidance) by week/month
- Number of new patients with GP recorded hypertension who are treated to target (NICE guidance) by week/month
- Number and % GP recorded hypertension patients need to be treated to target (NICE guidance) to meet the 80% CVD ambition by week/month

**For patients NOT treated to target:**

Segment into priority groups:

1. HT and BP \>180/120\*
2. HT and BP 160-180/100-120\*
3. HT and BP 140-169/90-100\* (\< 80)
4. HT and BP 150-169/90-100\* (\>= plus)
5. HT and no BP reading in last 12 months

**Outcome indicators:**

1. Stroke: Rate of non-elective admissions per 100,000 age-sex weighted population
2. Heart attack: Rate of admissions per 100,000 age-sex weighted population
3. Mortality within 30 days of hospital admission for stroke
4. Mortality within 30 days of hospital admission for heart attack
5. Stroke mortality rates, under 75 years (age standardised) (1 year range)
6. Under 75 mortality rate from cardiovascular disease
