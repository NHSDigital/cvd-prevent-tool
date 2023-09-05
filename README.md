# DS\_234: README

**CVD Prevent Tool curated data pipeline**

**Repository owner: [NHS England Analytical Services](https://github.com/NHSDigital/data-analytics-services)**

***Email: [datascience@nhs.net](mailto:datascience@nhs.net)***

***To contact us, raise an issue on Github or via email and we will respond promptly.***

[Link or badge to folder tree structure]

- [Key features](#key-features)
- [What does the pipeline do](#what-does-the-pipeline-do)
- [Quickstart](#quickstart)
  - [Running the pipeline](#running-the-pipeline)
- [Documentation](#documentation)
  - [Further documentation](#further-documentation)
- [Licence](#licence)

This repository includes a suite of spark notebooks used to build a new data pipeline. These are used to build a new data asset to link and curate [CVDPREVENT audit data](https://www.cvdprevent.nhs.uk/home) to existing administrative data tables.

This codebase can only be run on NHS England's Data Access Environment Apache Spark *V3.2.1*. This is being shared for transparency and feedback on the algorithms used.

No sensitive data is stored within this repository.

## Key features

- The pipeline is structured using an object-oriented approach.
- The pipeline is designed to be configured, using *params* notebooks, without altering the codebase
- Outputs can be restricted for particular cohort populations or to include a subset of data sources.
- Potential to include bespoke outcomes and patient characteristics in the output tables

## What does the pipeline do

It takes information from a range of data sources, and summarises them in a number of standardised tables. The pipeline produces the following outputs:

  - Events table (row per event from each data source used)
  - Patient table (row per patient – patient only recorded if they satisfy inclusion criteria for either Cohort 1 or 2)
  - Report table (output of results check, error catching)



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

**prepare\_pseduo\_assets**
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

## Documentation

The homepage of the pipeline's documentation is [here](./documentation/pipeline_README.md). 

### Further documentation

[Configuring the pipeline](./documentation/pipeline_README.md)

[Output data specification](./documentation/CVD_prevent_tool_product-spec_v1.4_extended.2.xlsx)

***Warning - this repository is a snapshot of a repository internal to NHS England.
This means that some links may not work for external readers.***


## Licence

Unless stated otherwise, the codebase is released under the [MIT Licence](./LICENSE). This covers both the codebase and any sample code in the documentation.

Documentation is © Crown copyright and available under the terms of the [Open Government 3.0 licence](https://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/).

 
