#  Diagnostic Codes

## Overview

This document describes the diagnostic codes used (e.g. ICD-10, SNOMED, Cluster ID codes from CVD), their purpose, how they are defined and used in the pipeline and the 
data sources that relating to them (e.g. REF SNOMED)



- [Diagnostic Codes](#diagnostic-codes)
  - [Overview](#overview)
  - [CVD Patient Cohort - CVDP Eligibility](#cvd-patient-cohort---cvdp-eligibility)
  - [CVDP Cluster ID Codes](#cvdp-cluster-id-codes)
    - [Diagnostic Flag Codes](#diagnostic-flag-codes)
    - [Resolved Codes](#resolved-codes)




## CVD Patient Cohort - CVDP Eligibility

- Cohort codes definition + explanation
- Link to other cohort page in clinical coding and definitions
- Where they are used

| **Cohort** | **Cohort Code** | **Description** |
| --- | --- | --- |
| Cohort 1 | CVDPCX001 |Patients flagged in Cohort 1 in any of the extractions:<br> high risk of developing CVD
| Cohort 2 | CVDPCX002 |Patients flagged in Cohort 2 in at any of the extractions:<br> have a CVD diagnosis 


## CVDP Cluster ID Codes

### Diagnostic Flag Codes

Diagnostic Flag Codes are defined as the Primary Diagnosis Code for a patient, these are used in our project to classify when a patient has started having a CVD.

| **Disease** | **Short Code** | **Diagnostic Code** | **Description** |
| --- | --- | --- | --- |
| Abdominal Aortic Aneurysm |AAA|	'AAA_COD'|	Used when a patient has been first diagnosed with AAA|
|Atrial Fibrillation|	AF|	'AFIB_COD'|	Used when a patient has been first diagnosed with AFIB|
|Chronic Kidney Disease|	CKD|	'CKD_COD'|	Used when a patient has been first diagnosed with CKD|
|Coronary Heart Disease|	CHD|	'CHD_COD'|	Used when a patient has been first diagnosed with CHD|
|Diabetes|	DIABETES|	'DMTYPE1AUDIT_COD' or 'DMTYPE2AUDIT_COD'|	Used when a patient has been first diagnosed with Diabetes|
|Familial Hypercholesterolaemia Screen|	FHSCREEN|	'DULIPID_COD', 'SBROOME_COD'|	Used when a patient has had a diagnostic screen for FH|
|Heart Failure|	HF|	'HF_COD'|	Used when a patient has been first diagnosed with Heart Failure|
|Hypertension|	HYP|	'HYP_COD'|	Used when a patient has been first diagnosed with Hypertension|
|Non-Diabetic Hyperglycaemia|	NDH|	'IGT_COD', 'NDH_COD', 'PRD_COD'|	Used when a patient has been first diagnosed with NDH|
|Peripheral artery disease |PAD|	'PAD_COD'|	Used when a patient has been first diagnosed with PAD|
|Stroke|	STROKE|	'STRK_COD'|	Used when a patient has first had a stroke event|
|Transient ischemic attack|	TIA|	'TIA_COD'|	Used when a patient has first had a TIA|


These diagnostic flags are used within the creation of the Diagnostic Flags Table to classify CVDP Events as their Primary Diagnosis Code

### Resolved Codes

Resolved Codes are defined when a patient has had their illness resolved, not all of the diseases that we look at have a resolved code.

| **Disease** | **Short Code** | **Resolve Code** | **Description** |
| --- | --- | --- | --- |
| Atrial Fibriliation | AF | 'AFIBRES\_COD' | Used when a patient has resolved from AFIB |
| Chronic Kidney Disease | CKD | 'CKDRES\_COD' | Used when a patient has resolved from CKD |
| Hypertension | HYP | 'HYPRES\_COD' | Used when a patient no longer has hypertension |

Resolved codes are used within the creation of the Diagnostic Flags table to identify and remove disease diagnoses that have been superseded by a resolution.

