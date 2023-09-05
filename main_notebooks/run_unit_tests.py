# Databricks notebook source
#
# run_unit_tests
# Main notebook for running of the unit test suite
# The unit test suite runs the unit tests for the CVD Prevent Tool codebase. The unit tests are designed
# to test individual functionality of the codebase. Unit tests can be run in concurrent or parallel modes,
# described below.
# 
# Widget Descriptions and Values
# run_mode: Determins if the unit tests are run in consecutively (concurrent) or in parallel (parallel). Note 
#           that when run in parallel mode, the number of notebooks simultaneously run is set to 4. 
#           Defaults to `concurrent` mode. 
#
# Running order:
# 1. Run commands for loading of supporting libraries by running %run ../src/unit_test_lib
# 2. Run command: Load the configurable widgets
# 3. Configure widgets using widgets at the top of the notebook (no code running)
# 4. Once the widgets have been set to the desired configuration, proceed with running the command Define unit test notebooks
# 5. Run the unit test suite: Run Unit Test Suite
# 6. Once the unit tests have completed, you can either exit the notebook or run the remaining commands to close the unit test suite
#
# Notes:
# The function `run_unit_tests()` or `run_unit_tests_parallel()` iterates over each notebook and records a `PASS` or `FAIL`.
# Any notebooks that have failed will be printed out as a summary at the end of the function run.

# COMMAND ----------

# MAGIC %run ../src/unit_test_lib

# COMMAND ----------

# Load the configurable widgets
dbutils.widgets.dropdown('run_mode', 'concurrent',['concurrent','parallel'])

# COMMAND ----------

# Define unit test notebooks
test_notebook_list = [
    'add_auditing_field_stage_tests',
    'clean_dataset_tests',
    'create_cohort_table_tests',
    'create_demographic_table_tests',
    'create_demographic_table_lib_tests',
    'create_events_table_lib_tests',
    'create_events_table_tests',
    'create_logger_tests',
    'create_patient_table_lib_tests',
    'create_patient_table_tests',
    'cvdp_preprocess_cvdp_cohort_tests',
    'cvdp_preprocess_htn_tests',
    'cvdp_preprocess_cvdp_journal_tests',
    'diagnostic_flag_tests',
    'params_tests',
    'params_util_tests',
    'pipeline_results_checker_stage_tests',
    'pipeline_util_tests',
    'preprocess_cvdp_smoking_tests',
    'preprocess_dars_tests',
    'preprocess_hes_tests',
    'preprocess_pcaremeds_tests',
    'preprocess_raw_data_lib_tests',
    'preprocess_raw_data_tests',
    'process_cvdp_smoking_events_tests',
    'process_hes_events_tests',
    'pseudonymised_asset_preparation_tests',
    'test_helpers_tests',
    'util_tests',
    'write_asset_stage_tests',
]

# COMMAND ----------

# Run Unit Test Suite
# Select run mode as specified by user widget
if dbutils.widgets.get('run_mode') == 'concurrent':
    ## Run Concurrent Unit Tests
    status_unit_tests = run_unit_tests(test_notebook_list)
elif dbutils.widgets.get('run_mode') == 'parallel':
    # Run Parallel unit tests
    status_unit_tests = run_unit_tests_parallel(test_notebook_list, num_jobs = 4)
else:
    # No widget parameter generated
    raise ValueError('ERROR: run_mode widget value not populated. Ensure widget command has been run and value selected')

# COMMAND ----------

# Clear widgets
dbutils.widgets.removeAll()

# COMMAND ----------

# Return status of notebook run
dbutils.notebook.exit(status_unit_tests)

# COMMAND ----------

