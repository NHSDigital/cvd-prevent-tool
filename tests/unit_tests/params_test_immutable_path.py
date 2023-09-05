# Databricks notebook source
#Because of databricks limitations the immutability of the PARAMS_PATH cannot be tested within our 
#test suite. See param_tests. If this notebook fails the test passes.
#
# Unit tests: when running unit tests this notebook needs to have an exception so that failure == pass

# COMMAND ----------

PARAMS_PATH = 'default'

# COMMAND ----------

# MAGIC %run ../../params/params

# COMMAND ----------

PARAMS_PATH = '../../params/test_params'

# COMMAND ----------

# MAGIC %run ../../params/params
