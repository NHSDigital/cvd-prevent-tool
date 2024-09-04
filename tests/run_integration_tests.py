# Databricks notebook source
# run_integration_tests
# Main notebook for running of the integration test suite.
# The integration test suite is a small run (1000 records) of the full pipeline to test how pipeline stages, and
# the data passed between them, integrates and results in pass (pipeline completes) or failure (pipeline fail).
#
# Parameter Descriptions and Values
# LIMIT:       "True" if limiting the size of the cohort table. The number of rows the
#               fataframe will reduce to is given by INTEGRATION_TEST_LIMIT in params_util.
# PARAMS_PATH:  The default params is used unless a path to a non-default params is used
#               (see params/params_util).
# SAVE:        "True" if you want to save temporary tables. Default if "False"

#
# Running order:
# 1. Run commands for loading of supporting libraries by running %run ../src/util and %run ../tests/integration_tests/integration_test_util
# 2. Run comamnd: Load the configurable widgets
# 3. Configure widgets using widgets at the top of the notebook (no code running)
# 4. Once the widgets have been set to the desired configuration, proceed with running the command %run ../pipeline/default_pipeline
# 5. Run the integration tests: Run Integration Test Suite
# 6. Once integration tests have completed, you can either exit the notebook or run the remaining commands to close the integration test suite

# COMMAND ----------

# MAGIC %run ../src/util

# COMMAND ----------

# MAGIC %run ../tests/integration_tests/integration_test_util

# COMMAND ----------

# Assign test parameter values
## run_integration_test
PARAMS_PATH = 'default'
LIMIT       = "True"
SAVE        = "False"

## get_default_pipeline_stages
RUN_LOGGER  = "True"
RUN_ARCHIVE = "True"

# COMMAND ----------

# MAGIC %run ../pipeline/default_pipeline

# COMMAND ----------

DB_NAME = params.DATABASE_NAME

# COMMAND ----------

dbutils.widgets.text("params_path", PARAMS_PATH) 
PARAMS_PATH = dbutils.widgets.get("params_path")

dbutils.widgets.text("limit", LIMIT) 
LIMIT = dbutils.widgets.get("limit") == "True"

dbutils.widgets.text("save", SAVE) 
SAVE = dbutils.widgets.get("save") == "True"

# COMMAND ----------

# Run Integration Test Suite
try:
  run_integration_test(
    _param = params,
    stages = get_default_pipeline_stages(
      run_logger_stages = True,
      run_archive_stages = True,
      pipeline_dev_mode = False
    ),
    test_db = DB_NAME,
    limit = LIMIT,
    save_integration_output = SAVE
  )
  # Pass: no exception raised
  status_integration_tests = 'pass'
except Exception as e:
  # Fail: exception raised
  status_integration_tests = 'fail'
  # Return original error message
  raise Exception(f'ERROR: Integration tests failed. See error: {str(e)}')

# COMMAND ----------

dbutils.widgets.removeAll()

# COMMAND ----------

# Return status of notebook run
dbutils.notebook.exit(status_integration_tests)
