# Databricks notebook source
# MAGIC %run ../../src/test_helpers

# COMMAND ----------

PARAMS_PATH = 'default'

# COMMAND ----------

# MAGIC %run ../../pipeline/create_logger_stage

# COMMAND ----------

import pyspark.sql.functions as F
import pyspark.sql.types as T

from pyspark.sql import Row
from uuid import uuid4
from datetime import datetime, date

from dsp.validation.validator import compare_results

# COMMAND ----------

suite = FunctionTestSuite()

## The following are fake people and data created for test purposes

# COMMAND ----------

@suite.add_test
def test_create_logger_stage_run():
  '''
  At this point, the dictionary will have been created by the PipelineLogger
  The CreateLoggerOutputStage is tested here to make sure the four values are created and the dataframe is initialised successfully
  '''
  
  #set up log, context and params classes
  class testLog():
    def __init__(self):
      return None
    def _run():
      return None
    def _get_dict(self):
      return {'Full Pipeline': {'start_time': 123456}}
    
  @dataclass(frozen = True)
  class TestParams(ParamsBase):
    DATABASE_NAME = None
  test_params = TestParams()
  
  context = PipelineContext('12345', test_params, [])
  log = testLog()
  # end of setup
  
  stage = CreateLoggerOutputStage('')
  
  stage._run(context,log)
  
  df = stage._data_holder['log']
  
  assert df.count() > 0
  assert df.where((F.col('value').isNotNull())).count() == 4

# COMMAND ----------

@suite.add_test
def test_generate_logger_dataframe():
  '''
  At this point, the dictionary has already been generated, 
  the CreateLoggerOutputStage is tested here to make sure the dataframe is created correctly from the dictionary
  '''
  
    #set up log, context and params classes
  class testLog():
    def __init__(self):
      return None
    def _run():
      return None
    
  @dataclass(frozen = True)
  class TestParams(ParamsBase):
    DATABASE_NAME = None
  test_params = TestParams()
  
  context = PipelineContext('12345', test_params, [])
  log = testLog()
  #end setup
  
  stage = CreateLoggerOutputStage('')
  
  dictionary_input = {'test_stage': {'category_1': '123456', 'category_2': '456789', 'time_category': '0001'}, 'Full Pipeline': {'date': '2023 04 19'}}
  
  df_expected = spark.createDataFrame([
    ('test_stage', 'category_1', '123456', 'counts'),
    ('test_stage', 'category_2', '456789', 'counts'),
    ('test_stage', 'time_category', '0001', 'timing'),
    ('Full Pipeline', 'date', '2023 04 19', 'pipeline_stats')
  ], ['stage', 'criteria', 'value', 'category'])
  
  df_actual = stage._generate_logger_dataframe(dictionary_input)
  
  assert compare_results(df_actual, df_expected, join_columns = ['stage','criteria'])

# COMMAND ----------

suite.run()

# COMMAND ----------

dbutils.notebook.exit('pass')
