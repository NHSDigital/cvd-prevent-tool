# Pipeline README

# Outline

- [Quick Start Guide](#QuickStartGuide)
- [Configuration](#Configuration)
- [Assets](#Assets)
  - [Intermediate assets](#Intermediateassets)
  - [Final assets](#Finalassets)
- [Limitations](#Limitations)
  - [Global Date Ranges](#GlobalDateRanges)


#  Quick Start Guide

The Prevent Tool Pipeline is run from the main notebook.

This notebook is able to run the codebase in multiple modes (selected from the .Run Mode widget):

**Pre-Merge:**
 Running of the unit test and integration test suite, prior to GitLab merging

**Post-Merge:**
 Running of the unit test suite, integration test suite, and full pipeline run (post GitLab-to-Databricks merge)

**Pipeline Only:**
  Running of the full pipeline only (General Running)

There are also additional modes from that widget that relate to controlling the notebook:

**Selection Mode:**
  Default value when first starting the notebook. Used as a placeholder to indicate the notebook needs configuring before running one of the main run modes. 

**Reset Mode:**
 When selected (and run), returns all configurable values for main back to the default values. 


A full pipeline run undertaken as part of the **Post-Merge** or **Pipeline Only** run modes requires additional widget inputs:

**cohort\_table:**
 Specifies the use of a previous eligible\_cohort\_table table - if supplied the CreateCohortTableStage stage of the pipeline will be skipped. Default is blank (run full pipeline).

**git\_version:**
 Git commit hash from the current master branch in gitlab. Can also be set to dev\_XX where XX are the initials of the user running the pipeline - used when testing pipeline code.

**params\_path:**
 Path to the parameters notebook that controls the pipeline. Default is default. A custom path should only be used when using a non-standard parameters file.

**prepare\_pseduo\_assets:**
  Specifies whether the pseudonymisation ready assets should be created (True) or not (False). Defaults to **False**.

The pipeline run function run\_pipeline() outputs a verbose progress log of the running stages and times of the pipeline.

Once completed, assets will be available in the prevent\_tool\_collab database.



# Configuration

The pipeline functionality and running can be controlled using the pipeline parameters (found in the params folder). Below is a brief summary of the different parameter notebooks and their purpose.

**params**

The main notebook for creating the params object. This notebook checks for the parameters path (default is default) and loads the specified params\_util notebook.

**params\_util**

This notebook contains the main parameter definitions and the creation of the params dataclass.
 Input and output data fields (columns) are specified here, alongside any intermediate fields used as part of the pipeline processing.
 This notebook loads the params\_diagnostic\_codes notebook to load the relevant SNOMED and ICD10 codes that form part of the inclusion criteria.

**params\_diagnostic\_codes**

This notebook is used to specify any clinical coding variables (ICD-10, SNOMED) that are used to create the pipeline parameters.



# Assets

There are two sets of pipeline assets that are created during the pipeline run: intermediate and final.

## Intermediate assets

These assets are created during the pipeline run. A summary is given below:

### eligible\_cohort

Table of eligible patients, defined as a distinct set of (NHS Number, Date of Birth). These patients are extracted from the annual and quarterly CVDP store extracts - deduplicated to keep the latest record (on extract date) for each patient.

### eligible\_cohort\_journal

Table of eligible patients (identical to eligible\_cohort) with the associated journal entries (extracted from the journals\_table field in the CVDP annual and quarterly extracts)

### hes\_apc

Table containing CVD-related hospitalisation events (defined using the primary diagnostic code) and filtered to only contain HES records for eligible patients. Dataset is a flattened version of multiple HES years (dates specified in params, but covers the past 10 years) from HES APC and APC\_OTR.

Whilst the preprocessed HES asset contains all episode types, in the final events table only the HES episodes and spells that are **not flagged** as NO CVD are included.

### hes ae & hes op

Table containing hospitalisation events, filtered to only contain records for eligible patients. Dataset is a flattened version of multiple HES years (dates specified in params but the past 5 years)

### dars\_bird\_deaths

Table containing all deaths, filtered to only contain DARS records for eligible patients.

### cvdp\_diag\_flags

Table containing eligible patients and associated CVD diagnostic dates. Dates shown are the latest diagnostic date extracted from the patient's journal table

### demographic\_table

Table containing patient IDs and selected demographics (defined in params ). Populated by consolidating demographics collected in cvdp cohort, journal and HES datasets. This is used to populate demographic fields (namely ethnicity) in the patient\_table (see below).

## Final assets

These assets are the final assets created by the pipeline. A summary is given below:

### events\_table

A row-per-event table, containing:

- cohort events (from: eligible\_cohort)
- hospitalisation events (from: hes\_apc)
- death events (from: dars)
- blood pressure measurements and hypertension risk groups (from: eligible\_cohort\_journal)

### patient\_table

A row-per-patient table, containing:

- patient information (from: events\_table - cohort events, enhanced information taken from demographic\_table)
- summary of CVD diagnoses (from: cvdp\_diag\_flags)
- summary of CVD-related hospitalisation events (from: events\_table - hes spells)
- summary of death events (from: dars\_bird\_deaths)
- summary of latest (12 months) hypertension risk group (from: events\_table - CVD blood pressure measurements)

The data specification document of the final delivered table is available [here](./CVD_prevent_tool_product-spec_v1.4_extended.2.xlsx).


# Limitations

## Global Date Ranges

The data extracted from the cvdp\_store (CVD cohorts) is limited by the earliest and latest extractions available. Between these two dates, the cohort data is updatedly quarterly.
 As of 31 May 2023 the date ranges are as follows:

- Earliest extraction date: 2020-03
- Latest extraction date: 2023-03

For a patient to be included in the cohort, they must be alive at the time of an extraction (with a lag period of 2-3 months if they have died recently). Therefore, patients that may have been in the CVD cohort prior to the earliest extraction date, if they died before that date, will not be in the extracts.


