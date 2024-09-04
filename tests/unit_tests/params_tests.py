# Databricks notebook source
# MAGIC %run ../../params/params_util

# COMMAND ----------

PARAMS_PATH = '../../params/test_params'

# COMMAND ----------

# MAGIC %run ../../src/test_helpers

# COMMAND ----------

# MAGIC %run ../../params/params

# COMMAND ----------

from datetime import date

from py4j.protocol import Py4JError

# COMMAND ----------

suite = FunctionTestSuite()

# COMMAND ----------

@suite.add_test
def test_params():
  assert params
  assert params.params_path == PARAMS_PATH
  assert params.DATABASE_NAME == 'test_database'
  assert params.PID_FIELD == 'test_patient_id'
  assert params.params_date == date.today()

# COMMAND ----------

@suite.add_test
def test_params_immutable_path():
  #Because of databricks limitations the immutability of the CONFIG_PATH cannot be tested within our test suite.
  try:
    dbutils.notebook.run('./params_test_immutable_path', 0)
    raise AssertionError('Immutable PARAMS_PATH test failed.')
  except Py4JError:
    assert True

# COMMAND ----------

suite.run()

# COMMAND ----------

dbutils.notebook.exit('pass')