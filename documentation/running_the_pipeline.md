# Running the pipeline

This KOP details information about running the CVD Prevent Tool Pipeline.

The pipeline should be run from the [**run_pipeline**](../run_pipeline.py) notebook. The pipeline stages, as well as the order of running and passing of data and information, is defined in [**default_pipeline**](../pipeline/default_pipeline.py).

Each code cell within **run_pipeline** should be run in order, one after the other to avoid the potential for the pipeline to break. Either this or the **run_pipeline** can be run from within another notebook using dbutils.notebook.run("run_pipeline", 0).

# Running the test

Similarly there are both unit and integration tests that should be ran from within the [**run_tests**](../run_tests.py) notebook.

## Unit tests

[**run_unit_tests**](../tests/run_unit_tests.py) is used to run all of the unit tests in the codebase. Each unit test notebook is defined, using the notebook name, in the list test_notebook_list.

The function run_unit_tests then iterates through this list, running each unit test notebook and recording whether the notebook passed or failed.

At the end of the run, a summary of notebooks ran, unit tests passed and failed, and failed notebook names will be outputted into the command window. 

## Integration tests

[**run_integration_tests**](../tests/run_integration_tests.py) is used to run the integration tests in the codebase. This notebook is similar to run_pipeline, and is controlled in the same manner (using widgets). This notebook is only usually ran as part of the branching and merging process.

## FAQs

###### How do I know the pipeline run was successful?

A successful run of the pipeline will result in the generation of all tables, which will (in the table name) contain the date the pipeline was run (e.g. running the pipeline on 6th June 2022 the tables will contain ) These tables will be outputted in the prevent_tool_collab datavase of the DAE.  The tables will consist of:

- eligible cohort table
- preprocessed data tables (e.g.hes, mortality)
- events table
- patient table

###### How can I view output errors?

When using the run_pipeline notebook you can view outputs, errors etc within the outputs of code cell 6. This should contain any information for debbugging errors as well as a output of the progress of the pipeline run.
