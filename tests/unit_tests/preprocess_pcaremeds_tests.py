# Databricks notebook source
# MAGIC %run ../../src/test_helpers

# COMMAND ----------

# MAGIC %run ../../src/clean_dataset

# COMMAND ----------

PARAMS_PATH = 'default'

# COMMAND ----------

# MAGIC %run ../../src/pcaremeds/preprocess_pcaremeds

# COMMAND ----------

from datetime import date, datetime
from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType
from dsp.validation.validator import compare_results

# COMMAND ----------

suite = FunctionTestSuite()

## The following are fake people and data created for test purposes

# COMMAND ----------

@suite.add_test
def test_remove_duplicate_loads():
  '''
  Test of remove_duplicate_loads that removes any record that has a more up to date version (effective_to field is not null).
  
  Functions:
    src/pcaremeds/preprocess_pcaremeds::remove_duplicate_loads
  '''
  
  df_input = spark.createDataFrame([
    (1, '1111', datetime.now()),  # None_null effective_to
    (2, '2222', None)             # Null effective_to
  ], ['test_id', 'test_nhs_number', 'test_effective_to']
  )
  
  df_expected = spark.createDataFrame([
    (2, '2222')
  ], ['test_id', 'test_nhs_number']
  )
  
  df_actual = remove_duplicate_loads(
               df = df_input,
               effective_to_field = 'test_effective_to'
               )
  
  assert compare_results(df_actual, df_expected, join_columns = ['test_nhs_number'])

# COMMAND ----------

@suite.add_test
def test_remove_non_england_pharmacies():
  '''
  Test of the remove_non_england_pharmacies. 
  Test the returned table has any records with country codes not associated with England removed
  
  Functions:
    src/pcaremeds/preprocess_pcaremeds::remove_non_england_pharmacies
  '''
  
  df_input = spark.createDataFrame([
    (1, '1111', 1), #England country code
    (2, '2222', None), #Null country code
    (3, '3333', 4) #non-England country code
  ], ['test_id', 'test_nhs_number', 'test_country_code']
  )
  
  df_expected = spark.createDataFrame([
    (1, '1111')
  ], ['test_id','test_nhs_number']
  )
  
  df_actual = remove_non_england_pharmacies(
    df = df_input,
    country_code = 'test_country_code'
    )
  
  assert compare_results(df_actual, df_expected, join_columns = ['test_nhs_number'])

# COMMAND ----------

@suite.add_test
def test_remove_private_prescriptions():
  '''
  Test of the remove_private_prescriptions function.
  All non-public prescription records should be removed. Note: The field is not nullable so behaviour for none does not need to be tested.
  
  Functions:
    src/pcaremeds/preprocess_pcaremeds::remove_private_prescriptions
  '''
  
  df_input = spark.createDataFrame([
    (1, '1111', 1), #private prescription
    (2, '2222', 0) #public prescription
  ], ['test_id', 'test_nhs_number', 'test_private_indicator']
  )
  
  df_expected = spark.createDataFrame([
    (2, '2222')
    ], ['test_id', 'test_nhs_number']
  )
  
  df_actual= remove_private_prescriptions(
    df = df_input,
    private_indicator = 'test_private_indicator'
    )
  
  assert compare_results(df_actual, df_expected, join_columns = ['test_nhs_number'])

# COMMAND ----------

@suite.add_test
def test_clean_pcaremeds_nhs_number():
  '''
  Test for clean_pcaremeds_nhs_number function. Any records without an NHS number should be removed
  
  Functions:
    src/pcaremeds/preprocess_pcaremeds::clean_pcaremeds_nhs_number
  '''
  
  df_input = spark.createDataFrame([
    (1, '1111'),  # None-null NHS Number
    (2, None)     # Null NHS Number
  ], ['test_id', 'test_nhs_number']
  )
  
  df_expected = spark.createDataFrame([
    (1, '1111')
  ], ['test_id', 'test_nhs_number']
  )
  
  df_actual = clean_pcaremeds_nhs_number(
    df = df_input,
    nhs_number = 'test_nhs_number'
    )
  
  assert compare_results(df_actual, df_expected, join_columns = ['test_id'])

# COMMAND ----------

@suite.add_test
def test_create_record_id_field():
  '''
  Test for the create_record_id_field. Ensuring the prescription id and item id are successfully concatenated togetherto create a unique record id
  
  Functions:
    src/pcaremeds/preprocess_pcaremeds::create_record_id_field
  '''
  
  df_input = spark.createDataFrame([
    (1, '1111', 2222, 3)
  ], ['test_id', 'test_nhs_number', 'test_prescripton_id', 'test_item_id'])
  
  df_expected = spark.createDataFrame([
    (1, '1111', 2222, 3, '22223')
  ], ['test_id', 'test_nhs_number', 'test_prescripton_id', 'test_item_id', 'test_id_field'])
  
  df_actual = create_record_id_field(
    df = df_input,
    prescription_id = 'test_prescripton_id',
    item_id = 'test_item_id',
    id_field = 'test_id_field'
    )
  
  assert compare_results(df_actual, df_expected, join_columns = ['test_id'])

# COMMAND ----------

suite.run()

# COMMAND ----------

dbutils.notebook.exit('pass')
