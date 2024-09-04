# Databricks notebook source
# MAGIC %md
# MAGIC #Unit tests

# COMMAND ----------

# run_unit_tests
# The unit test suite runs the unit tests for the CVD Prevent Tool codebase. The unit tests are designed
# to test individual functionality of the codebase. Unit tests can be run in concurrent or parallel modes,
# described below.
#
# Paramteter Descriptions and Values
# TEST_MODE: Determins if the unit tests are run in consecutively (concurrent) or in parallel (parallel). Note
#            that when run in parallel mode, the number of notebooks simultaneously run is set to 4.
#            Defaults to `concurrent` mode.

# COMMAND ----------

# Test parameters
TEST_MODE = 'concurrent'

# COMMAND ----------

unit_test_result = dbutils.notebook.run(
    './tests/run_unit_tests',
    0,
    {'test_mode': TEST_MODE}
)

# COMMAND ----------

if unit_test_result == 'fail':
  raise Exception(unit_test_result)
else:
  print(unit_test_result)

# COMMAND ----------

# MAGIC %md
# MAGIC # Integration tests

# COMMAND ----------

# run_integration_tests
# The integration test suite is a small run (1000 records) of the full pipeline to test how pipeline stages, and
# the data passed between them, integrates and results in pass (pipeline completes) or failure (pipeline fail).
#
# Parameter Descriptions and Values
# LIMIT:       "True" if limiting the size of the cohort table. The number of rows the
#               fataframe will reduce to is given by INTEGRATION_TEST_LIMIT in params_util.
# PARAMS_PATH:  The default params is used unless a path to a non-default params is used
#               (see params/params_util).
# SAVE:        "True" if you want to save temporary tables. Default if "False"

# COMMAND ----------

# Assign test parameter values
## run_integration_test
PARAMS_PATH = 'default'
LIMIT       = "True"
SAVE        = "False"

# COMMAND ----------

# MAGIC %run ./params/params

# COMMAND ----------

integration_test_result = dbutils.notebook.run(
    './tests/run_integration_tests',
    0,
    {
        'params_path': PARAMS_PATH,
        'limit': LIMIT,
        'save': SAVE,
    }
)

# COMMAND ----------

if integration_test_result == 'fail':
  raise Exception(integration_test_result)
else:
  print(integration_test_result)
