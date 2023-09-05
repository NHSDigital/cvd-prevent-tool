# Databricks notebook source
# MAGIC %run ../../src/test_helpers

# COMMAND ----------

PARAMS_PATH = 'default'

# COMMAND ----------

# MAGIC %run ../../params/params

# COMMAND ----------

# MAGIC %run ../../src/cvdp/preprocess_cvdp_smoking

# COMMAND ----------

from dsp.validation.validator import compare_results
from datetime import date

import pyspark.sql.functions as F
import pyspark.sql.types as T

# COMMAND ----------

suite = FunctionTestSuite()

## The following are fake people and data created for test purposes

# COMMAND ----------

@suite.add_test
def test_extract_smoking_status():
  '''  
  Test takes journal table entries and keeps only those with SNOMED codes associated with smoking status, assigning them the relevant status flag.
  
  Functions:
    src/cvdp/preprocess_cvdp_smoking::extract_smoking_status
  '''
  
  #Smoking codes
  codes_test_current = ['C1']
  codes_test_ex = ['E1']
  codes_test_never = ['N1']
  
  #Smoking status flags
  current_test_flag = 'current'
  ex_test_flag = 'ex'
  never_test_flag = 'never'
  
  df_input = spark.createDataFrame([
    (0, 'C1'), #Keep Current status
    (1, 'E1'), #Keep Ex status
    (2, 'N1'), #Keep Never status
    (4, 'B1'), #Drop Invalid code
    (5, None) #Drop Invalid code
  ], ['idx', 'test_code'])
  
  df_expected = spark.createDataFrame([
    (0, 'C1', 'current'),
    (1, 'E1', 'ex') ,
    (2, 'N1', 'never') 
  ], ['idx', 'test_code', 'test_flag'])
  
  df_actual = extract_smoking_status(
    df = df_input,
    codes_current = codes_test_current,
    codes_ex = codes_test_ex,
    codes_never = codes_test_never,
    field_snomed_code = 'test_code',
    current_flag = current_test_flag,
    ex_flag = ex_test_flag,
    never_flag = never_test_flag,
    field_smoking_status = 'test_flag'
  )
  
  assert compare_results(df_actual, df_expected, join_columns=['idx'])

# COMMAND ----------

@suite.add_test
def test_extract_smoking_intervention():
  '''
  Test takes journal table entries and returns only those with a cluster_id related to smoking intervention
  
  Functions:
    src/cvdp/preprocess_cvdp_smoking::extract_smoking_intervention
  '''
  
  codes_test_intervention = ['I1', 'I2']
  
  df_input = spark.createDataFrame([
    (0, 'I1'), #Keep
    (1, 'I2'), #Keep
    (3, 'J1'), #Drop
    (4, None), #Drop
  ], ['idx', 'test_cluster_id'])
  
  df_expected = spark.createDataFrame([
    (0, 'I1'),
    (1, 'I2')
  ], ['idx', 'test_cluster_id'])
  
  df_actual = extract_smoking_intervention(
    df = df_input,
    field_cluster = 'test_cluster_id',
    intervention_codes = codes_test_intervention
  )
  
  assert compare_results(df_actual, df_expected, join_columns=['idx'])

# COMMAND ----------

suite.run()

# COMMAND ----------

dbutils.notebook.exit('pass')
