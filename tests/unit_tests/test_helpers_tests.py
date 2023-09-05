# Databricks notebook source
from pyspark.sql.types import StructField, StructType, IntegerType

# COMMAND ----------

# MAGIC %run ../../src/test_helpers

# COMMAND ----------

suite = FunctionTestSuite()

## The following are fake people and data created for test purposes

# COMMAND ----------

@suite.add_test
def test_temporary_table_df():
  df_input = spark.createDataFrame([(1, 2)], ['v1', 'v2'])
  with TemporaryTable(df_input) as tmp_table:
    db = tmp_table.db
    table = tmp_table.name
    assert table_exists(db, table)

  assert table_exists(db, table) is False  

# COMMAND ----------

@suite.add_test
def test_temporary_table_schema():
  input_schema = StructType([
    StructField('v1', IntegerType(), True),
  ])

  with TemporaryTable(input_schema) as tmp_table:
    db = tmp_table.db
    table = tmp_table.name
    assert table_exists(db, table)

  assert table_exists(db, table) is False  

# COMMAND ----------

@suite.add_test
def test_temporary_table_df_dont_create():
  df_input = spark.createDataFrame([(1, 2)], ['v1', 'v2'])
  with TemporaryTable(df_input, create=False) as tmp_table:
    db = tmp_table.db
    table = tmp_table.name
    assert table_exists(db, table) is False
    df_input.write.saveAsTable(f'{db}.{table}')
    assert table_exists(db, table)

  assert table_exists(db, table) is False 

# COMMAND ----------

@suite.add_test
def test_temporary_table_table_suffix():
  df_input = spark.createDataFrame([(1, 2)], ['v1', 'v2'])
  with TemporaryTable(df_input, table_suffix='_suff') as tmp_table:
    db = tmp_table.db
    table = tmp_table.name
    assert table[-5:] == '_suff'
    assert table_exists(db, table)

  assert table_exists(db, table) is False 

# COMMAND ----------

def temp_real_func():
  return 'real1'

@suite.add_test
def test_test_function_patch():
  assert temp_real_func() == 'real1'

  def mock_func():
    return 'mock1'

  with FunctionPatch('temp_real_func', mock_func):
    assert temp_real_func() == 'mock1' 

  assert temp_real_func() == 'real1'

# COMMAND ----------

suite.run()

# COMMAND ----------

dbutils.notebook.exit('pass')
