# Databricks notebook source
#
# run_integration_tests
# Main notebook for running of the integration test suite.
# The integration test suite is a small run (1000 records) of the full pipeline to test how pipeline stages, and
# the data passed between them, integrates and results in pass (pipeline completes) or failure (pipeline fail).
#
# Widget Descriptions and Values
# limiter:      "True" if limiting the size of the cohort table. The number of rows the 
#               fataframe will reduce to is given by INTEGRATION_TEST_LIMIT in params_util.
# params_path:  The deafult params is used unless a path to a non-default params is used 
#               (see params/params_util). 
# save_output: "True" if you want to save temporary tables. Default if "False"
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

# Load congifurable widgets
dbutils.widgets.text('params_path', 'default')
dbutils.widgets.dropdown('limiter', 'True', ['False','True'])
dbutils.widgets.dropdown('save_output', 'False', ['False','True'])

# COMMAND ----------

# Assign widget values from configured widgets
PARAMS_PATH = dbutils.widgets.get('params_path')
LIMIT       = dbutils.widgets.get('limiter') == "True" 
SAVE        = dbutils.widgets.get('save_output') == "True"
COHORT_OPT  = ''

# COMMAND ----------

# MAGIC %run ../pipeline/default_pipeline

# COMMAND ----------

# Run Integration Test Suite
# Note: status_integration_tests used when running from main.py
try:
  run_integration_test(
    _param = params,
    stages = get_default_pipeline_stages(
      cohort_opt = COHORT_OPT,
      prepare_pseudo_assets = True,
      is_integration = True
    ),
    test_db = "prevent_tool_collab",
    limit = LIMIT,
    save_integration_output = SAVE
  )
  # Pass: no exception raised
  status_integration_tests = 'pass'
except:
  # Fail: exception raised
  status_integration_tests = 'fail'

# COMMAND ----------

dbutils.widgets.removeAll()

# COMMAND ----------

# Return status of notebook run
dbutils.notebook.exit(status_integration_tests)
