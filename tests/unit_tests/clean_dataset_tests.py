# Databricks notebook source
# MAGIC %run ../../src/clean_dataset

# COMMAND ----------

# MAGIC %run ../../src/test_helpers

# COMMAND ----------

from datetime import datetime
from uuid import uuid4

from pyspark.sql.types import StructType,StructField, StringType, IntegerType, TimestampType
from dsp.validation.validator import compare_results

# COMMAND ----------

suite = FunctionTestSuite()

## Note: The following are fake people and data created for test purposes 
## however any valid NHS Numbers are replaced with xxxxxxxxxx to avoid the posibility of any person identifiable information being shared

# COMMAND ----------

@suite.add_test
def test_clean_and_preprocess_dataset():
  df_input = spark.createDataFrame([
    ('1','xxxxxxxxxx', 'cn1', 'sv1', 'a'),
    ('2','FAKE831347', 'cn2', 'sv2', 'a'),
    ('3','xxxxxxxxxx', 'cn3', 'sv3', 'a'),
    ('4','xxxxxxxxxx', None, 'sv4', 'a'),
    ('5','xxxxxxxxxx', 'null', 'sv5', 'a'),
    ('6','xxxxxxxxxx', 'NULL', 'sv6', 'a'),
    ('7','xxxxxxxxxx', 'Null', 'sv7', 'a'),
    ('8','xxxxxxxxxx', '', None, 'a'),
    ('9','xxxxxxxxxx', 'cn5', 'sv1', ''),
    ('10','xxxxxxxxxx', 'cn6', 'sv1', None),],
    ['index', 'test_nhs_number', 'test_clean_null', 'test_static', 'test_replace_str'])

  df_expected = spark.createDataFrame([
    ('1','xxxxxxxxxx', 'cn1', 'sv1', 'a'),
    ('9','xxxxxxxxxx', 'cn5', 'sv1', None),
    ('10','xxxxxxxxxx', 'cn6', 'sv1', None),],
    ['index', 'test_nhs_number', 'test_clean_null', 'test_static', 'test_replace_str'])

  df_actual = clean_and_preprocess_dataset(df_input, ['test_nhs_number'], ['test_clean_null'],
                                           replace_empty_str_fields=['test_replace_str'])

  assert compare_results(df_actual, df_expected, join_columns=['index'])

# COMMAND ----------

@suite.add_test
def test_clean_and_preprocess_dataset_no_nhs_number_validation():
  df_input = spark.createDataFrame([
    ('1','xxxxxxxxxx', 'cn1', 'sv1', 'a'),
    ('2','FAKE831347', 'cn2', 'sv2', 'a'),
    ('3','xxxxxxxxxy', 'cn3', 'sv3', 'a'),
    ('4','xxxxxxxxxx', None, 'sv4', 'a'),
    ('5','xxxxxxxxxx', 'null', 'sv5', 'a'),
    ('6','xxxxxxxxxx', 'NULL', 'sv6', 'a'),
    ('7','xxxxxxxxxx', 'Null', 'sv7', 'a'),
    ('8','xxxxxxxxxx', 'cn4', None, 'a'),],
    ['index', 'test_nhs_number', 'test_clean_null', 'test_static', 'test_replace_str'])

  df_expected = spark.createDataFrame([
    ('1','xxxxxxxxxx', 'cn1', 'sv1', 'a'),
    ('2','FAKE831347', 'cn2', 'sv2', 'a'),
    ('3','xxxxxxxxxy', 'cn3', 'sv3', 'a'),
    ('8','xxxxxxxxxx', 'cn4', None, 'a'),],
    ['index', 'test_nhs_number', 'test_clean_null', 'test_static', 'test_replace_str'])

  df_actual = clean_and_preprocess_dataset(df_input, ['test_nhs_number'], ['test_clean_null'],
                                           replace_empty_str_fields=['test_replace_str'],
                                           validate_nhs_numbers=False)

  assert compare_results(df_actual, df_expected, join_columns=['index'])

# COMMAND ----------

@suite.add_test
def test_clean_and_preprocess_dataset_with_replace_str():
  df_input = spark.createDataFrame([
    ('1','xxxxxxxxxx', 'cn1', 'sv1', 'a'),
    ('2','FAKE831347', 'cn2', 'sv2', ''),
    ('3','xxxxxxxxxy', 'cn3', 'sv3', 'null'),
    ('4','xxxxxxxxxx', None, 'sv4', 'a'),
    ('5','xxxxxxxxxx', 'null', 'sv5', 'a'),
    ('6','xxxxxxxxxx', 'NULL', 'sv6', 'a'),
    ('7','xxxxxxxxxx', 'Null', 'sv7', 'a'),
    ('8','xxxxxxxxxx', 'cn4', None, None),],
    ['index', 'test_nhs_number', 'test_clean_null', 'test_static', 'test_replace_str'])

  df_expected = spark.createDataFrame([
    ('1','xxxxxxxxxx', 'cn1', 'sv1', 'a'),
    ('2','FAKE831347', 'cn2', 'sv2', None),
    ('3','xxxxxxxxxy', 'cn3', 'sv3', None),
    ('8','xxxxxxxxxx', 'cn4', None, None),],
    ['index', 'test_nhs_number', 'test_clean_null', 'test_static', 'test_replace_str'])

  df_actual = clean_and_preprocess_dataset(df_input, ['test_nhs_number'], ['test_clean_null'],
                                           replace_empty_str_fields=['test_replace_str'],
                                           validate_nhs_numbers=False)

  assert compare_results(df_actual, df_expected, join_columns=['index'])

# COMMAND ----------

@suite.add_test
def test_clean_and_preprocess_dataset_with_preprocessing_function():
  df_input = spark.createDataFrame([
    ('0','xxxxxxxxxx', 'cn1', 'sv1', 'a'),
    ('1','xxxxxxxxxz', 'cn1', 'sv1', 'a'),
  ], ['index', 'test_nhs_number', 'test_clean_null', 'test_static', 'test_replace_str'])

  df_expected = spark.createDataFrame([
    ('0','xxxxxxxxxx', 'cn1', 'sv1', 'a'),
  ], ['index', 'test_nhs_number', 'test_clean_null', 'test_static', 'test_replace_str'])

  def mock_preprocessing_function(df):
    return df.where(col('test_nhs_number') != '9240831348')

  df_actual = clean_and_preprocess_dataset(df_input, ['test_nhs_number'], ['test_clean_null'],
                                           preprocessing_func=mock_preprocessing_function,
                                           validate_nhs_numbers=False)

  assert compare_results(df_actual, df_expected, join_columns=['index'])

# COMMAND ----------

suite.run()

# COMMAND ----------

dbutils.notebook.exit('pass')
