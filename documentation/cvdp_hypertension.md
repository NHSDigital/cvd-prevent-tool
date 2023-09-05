# CVDP Hypertension

## Overview


This page details the processing required to categorise hypertensive patients according to risk, using blood pressure measurements.

- [CVDP Hypertension](#cvdp-hypertension)
  - [Overview](#overview)
  - [Detail](#detail)
- [](#)
  - [Definitions](#definitions)
  - [Hypertension Diagnosis](#hypertension-diagnosis)
  - [Hypertension Risk Groups](#hypertension-risk-groups)
  - [Treated to Target](#treated-to-target)
  - [Risk Groups](#risk-groups)
  - [Blood pressure measurements validity criteria](#blood-pressure-measurements-validity-criteria)
- [Process Overview](#process-overview)
     - [CreatePatientCohortTableStage](#createpatientcohorttablestage)
     - [PreprocessRawDataStage](#preprocessrawdatastage)
     - [CreateEventsTableStage](#createeventstablestage)
     - [CreatePatientTableStage](#createpatienttablestage)




## Detail
#

Blood pressure measurements described below are taken from the CVDP extract, which includes GP journal entries.
 These entries have a SNOMED codes, or cluster codes, that indicate what type of measurement the journal is recording, together with value1 and value 2 conditions, which in this case are used to record blood pressure measurements.
 See more detail in the systolic and diastolic BP SNOMED codes entries of the [hypertension definitions](#definitions) section below, and the example journal table extract.

## Definitions



| **Term** | **Description** | **Value (if applicable)** |
| --- | --- | --- |
| **Diagnosis code** | SNOMED cluster code indicating a hypertension diagnosis | HYP\_COD |
| **Resolved diagnosis code** | SNOMED cluster code indicating a hypertension diagnosis | HYP\_RES\_COD |
| **BP Cluster Code** | SNOMED cluster code indicating two blood pressure readings (one systolic and one diastolic) taken coincidentally | BP\_COD |
| **Systolic BP SNOMED codes** | SNOMED codes of journal records indicating a systolic blood pressure measurement | 12929001, 163030003, 18050000, 18352002, 198081000000101,251070002, 271649006,314438006, 314439003, 314440001, 314441002, 314442009,314443004,314444005,314445006, 314446007, 314447003,314448008, 314449000, 314464000, 399304008,400974009, 407554009, 407556006, 413606001,707303003, 716579001, 72313002,81010002 |
| **Diastolic BP SNOMED codes** | SNOMED codes of journal records indicating a diastolic blood pressure measurement | 1091811000000102, 163031004, 174255007, 198091000000104,23154005, 23154005, 271650006,314451001, 314452008, 314453003, 314454009, 314455005,314456006, 314457002, 314458007, 314459004, 314460009,314461008, 314462001, 314465004,400975005, 407555005, 407557002, 413605002, 42689008,42689008, 49844009, 49844009,53813002,716632005 |
| **Hypertension risk calculation date** | Date at which the hypertension risk is calculated ([risk group](#hypertension-risk-groups) assigned). Blood pressure measurements from the 12 months prior to this date are used. | Current pipeline uses each patient's latest extract.


Example of Blood Pressure Measurements from GP journals

| **PID** | **Date** | **code** | **value1\_condition** | **value2\_condition** | **Explanation** |
| --- | --- | --- | --- | --- | --- |
| 1 | 2020-01-02 | BP\_COD | 140.1 | 92.8 | value1\_condition is the systolic blood pressure measurement; value2\_condition is the diastolic blood pressure measurement |
| 1 | 2020-01-02 | **53813002** | 92.1 | null | **53813002** is a diastolic code, therefore value1\_condition is a diastolic blood pressure measurement |
| 1 | 2020-01-02 | **314439003** | 141.2 | null | **314439003** is a systolic code, therefore value1\_condition is a systolic blood pressure measurement |

![](RackMultipart20230624-1-9995cp_html_6ad4fe850bc60a0c.png) For this patient, **92.1** and **140.1** blood pressure measurements  (diastolic and systolic) are preserved.

## Hypertension Diagnosis



Identifying a hypertension diagnosis follows the same process as any relevant diagnosis for CVD, which we have detailed in our [Data Pre-Processing parent page](./data_preprocessing.md).
 The diagnosis and resolved diagnosis codes used for this process for hypertension are specified in the [definitions](#definitions) table above.

## Hypertension Risk Groups

Classification Criteria

- Hypertensive patients with a valid blood pressure reading in 12 months prior to the hypertension calculation date (see definitions above).
[See more info below regarding what counts as a 'valid' blood pressure measurement.](#blood-pressure-measurements-validity-criteria)
- **Systolic** blood pressure readings of **less than 50** and **diastolic** blood pressure readings of **less than 20** are considered invalid (see below).
- Hypertensive patients without any blood pressure reading in the last 12 months are classified as risk group **1e** and recorded under **No BP reading in last 12 months**


A patient's age, for each blood pressure measurement, is calculated at the **date of the journal entry** (in this case, each patient's most recent extract).
 This is something that we have configured so that it can be changed to a different field (for example, age calculated at the time of a patient's latest extract), if required, using the **field\_calculation\_date** parameter defined in the hypertension processing function  src/cvdp/preprocess\_cvdp\_htn::preprocess\_cvdp\_htn

## Treated to Target

The treated-to-target group indicates patients that have blood pressure measurements within a lower, "treated" range.
 These patients are not classified as being "at risk" due to their blood pressure measurements.

| **Risk Group** | **Age** | **Systolic Blood Pressure Range** | **Diastolic Blood Pressure Range** |
| --- | --- | --- | --- |
| **0.1** | \< 80 | 50 \<= BP \<= 140 | 20 \<= BP \<= 90 |
| **0.2** | \>= 80 | 50 \<= BP \<= 150 | 20 \<= BP \<= 90 |

Blood pressure measurements

Note that **both** systolic and diastolic conditions must be met for a patient to be considered treated to target.

## Risk Groups

Patient's that do not fall within treated-to-target values can be classified into several risk groups.
 These risk groups are reflective of:

- Value of systolic blood pressure measurement
- Value of diastolic blood pressure measurement
- (For certain risk groups) the age of the patient when the blood pressure measurement was taken

Blood pressure measurements

Of those patients NOT considered treated to target, their readings may fall into two risk groups (e.g. systolic = 170 and diastolic = 121).

In this case, the highest risk is assumed (in the example, the patient would be assigned risk group 1).

| **Risk Group** | **Risk Description** | **Age** | **Systolic Blood Pressure Range** | **Diastolic Blood Pressure Range** |
| --- | --- | --- | --- | --- |
| **1d** | Very high risk | Any | BP \> 180 | \> 120 |
| **1c** | High risk | Any | 160 \< BP \<= 180 | 100 \< BP \<= 120 |
| **1a** | Moderate risk | \< 80 | 140 \< BP \<= 160 | 90 \< BP \<= 100 |
| **1b** | Moderate risk | \> 80 | 150 \< BP \<= 160 | 90 \< BP \<= 100 |

These risk group values are assigned in the final stage of the hypertension preprocessing (part of the [PreprocessRawDataStage](./pipeline_stages.md#preprocess-raw-data-stage)).
 There are two additional risk group categories that are considered at the final stage of the pipeline, the [CreatePatientTableStage](./pipeline_stages.md#create-patient-table-stage).
 At this stage, only the blood pressure measurements  **12 months prior to a patient's latest extraction date** are considered for risk group assignment.
 The following risk groups are assigned only when there are no valid blood pressure measurements found within the 12 month time window.

| **Risk Group** | **Risk Description** | **Assignment Criteria** |
| --- | --- | --- |
| **-1** | Non-hypertension patients | Patient's hypertension diagnostic date (patient table) is NULL. |
| **1e** | No blood pressure measurements are identified occurring in the 12 months prior to a patient's latest extraction date.  | Patient's hypertension diagnostic date (patient table) contains a non-null date value. |

## Blood pressure measurements validity criteria

- Measurements must be recorded in the **12 months prior** to the patient's latest extract date (see [definitions](#definitions) above).
- The most recent valid measurement from the above time period is used to classify a patient's risk group.
- Measurements recorded under the [**BP Cluster Code**](#definitions) SNOMED cluster must have **non null** value1 AND value2 conditions
- Measurements recorded under the individual SNOMED codes for [Systolic](#definitions) & [Diastolic](#definitions) measurements must have a corresponding non-null Systolic/Diastolic measurement on the same day.
Value1 conditions are used for both.
- In the case of multiple Systolic & Diastolic blood pressure measurements being recorded on the same day, the lowest non-null measurements for each systolic and diastolic, on a single journal entry date, are recorded, following NICE guidelines.
- Systolic blood pressure readings of less than 50 or diastolic blood pressure readings of less than 20 are considered invalid and are not included in the blood pressure events (in the events table).
 Patients who ONLY have a pair of systolic and diastolic readings with one or both lower than these thresholds are considered to have no valid readings in the last 12 months and are categorised as **1e**.
 Otherwise, a patient's most recent _valid_ measurement is used to classify the patient into a risk group.

# Process Overview

Below is the overview of the extraction, processing, calculation and saving of patient hypertension records.

This process involves the following stages:

#### CreatePatientCohortTableStage

- The journal table records, for eligible patients ([in selected cohorts](./cohort_definitions.md)) are extracted to yield  **all** associated journal entries for those patients

#### PreprocessRawDataStage

- Blood pressure measurements (identified from the SNOMED or Cluster ID codes) are processed to yield sets of single (one value per row) and multiple (two values, systolic and diastolic, per row) for each patient.
- These results are unioned to yield a single blood pressure table.
- This table is the processed to yield the  **minimum systolic** and  **minimum diastolic** reading for each journal entry date.
- Each blood pressure measurement, where applicable, is assigned a  **hypertension risk group** based on the systolic and diastolic values, as well as the patient's age.

#### CreateEventsTableStage

- The processed blood pressure measurements are combined into the events table as  **blood pressure events** for the last 5 years

#### CreatePatientTableStage

- Hypertension risk categories (for patients with a hypertension diagnostic flag value - a non-null date of hypertension diagnosis) are assigned from the latest valid blood pressure measurement  **in the 12 months prior to a patient's latest extract date**.
- Patient's that are not diagnosed (null hypertension diagnostic date value) with hypertension are assigned a risk group value of  **-1**.
- If a patient has been diagnosed with hypertension (non-null hypertension diagnostic date) but they have  **no valid blood pressure measurement events in the last 12 months prior to their latest extract date**, they are assigned a risk group category of  **1e**.

