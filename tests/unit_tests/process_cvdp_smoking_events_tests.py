# Databricks notebook source
# MAGIC %run ../../src/test_helpers

# COMMAND ----------

PARAMS_PATH = 'default'

# COMMAND ----------

# MAGIC %run ../../params/params

# COMMAND ----------

# MAGIC %run ../../src/cvdp/process_cvdp_smoking_events

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
def test_get_last_smoking_category_entry():
  '''
  Test takes multiple smoking status records per person and keeps only most recent of each category (using first_switch = False)
  
  Functions:
    src/cvdp/preprocess_cvdp_smoking::get_first_or_last_smoking_category_entry
  '''
  
  df_input = spark.createDataFrame([
    (0, 'N1', date(2000,1,1), 'current', date(2010,1,1)), #Keep - most recent current status
    (1, 'N1', date(2000,1,1), 'current', date(2009,1,1)), #Drop - older current status
    (2, 'N1', date(2000,1,1), 'ex', date(2009, 2, 3)),    #Drop - older ex status
    (3, 'N1', date(2000,1,1), 'ex', date(2011,1,1)),      #Keep - most recent ex status
    (4, 'N1', date(2000,1,1), 'never', date(2022, 3,5)),  #Keep, most recent never status
    (5, 'N1', date(2000,1,1), 'never', date(2022,3,4)),   #Drop - only keep one of same status on same date
    (6, 'N1', date(2000,1,1), 'never', date(2022,3,4)),   #Drop- older never status
    (7, 'N2', date(2000,1,2), 'never', date(2010,1,1)),   #Keep Latest never status
    (8, 'N2', date(2000,1,2), 'never', date(2009,1,1)),   #Drop- older never status
    (9, 'N3', date(2000,1,1), 'ex', date(2020,1,1,))      #Keep- single smoking status
  ], ['idx', 'test_nhs_number', 'test_dob', 'test_flag', 'test_journal_date'])
  
  df_expected = spark.createDataFrame([
    (0, 'N1', date(2000,1,1), 'current', date(2010,1,1)),
    (3, 'N1', date(2000,1,1), 'ex', date(2011,1,1)),
    (4, 'N1', date(2000,1,1), 'never', date(2022, 3,5)),
    (7, 'N2', date(2000,1,2), 'never', date(2010,1,1)),
    (9, 'N3', date(2000,1,1), 'ex', date(2020,1,1,))
  ], ['idx', 'test_nhs_number', 'test_dob', 'test_flag', 'test_journal_date'])
  
  df_actual = get_first_or_last_smoking_category_entry(
    df = df_input,
    field_person_id = 'test_nhs_number',
    field_person_dob = 'test_dob',
    field_date_journal = 'test_journal_date',
    field_smoking_status = 'test_flag',
    first_switch = False
  )
  
  assert compare_results(df_actual, df_expected, join_columns=['idx'])

# COMMAND ----------

@suite.add_test
def test_get_first_smoking_category_entry():
  '''
  Test takes multiple smoking status records per person and keeps only oldest of each category (using first_switch = True)
  
  Functions:
    src/cvdp/preprocess_cvdp_smoking::get_first_or_last_smoking_category_entry
  '''
  
  df_input = spark.createDataFrame([
    (0, 'N1', date(2000,1,1), 'current', date(2010,1,1)), #Drop - most recent current status
    (1, 'N1', date(2000,1,1), 'current', date(2009,1,1)), #Keep - oldest current status
    (2, 'N1', date(2000,1,1), 'ex', date(2009, 2, 3)),    #Keep - olderst ex status
    (3, 'N1', date(2000,1,1), 'ex', date(2011,1,1)),      #Drop - most recent ex status
    (4, 'N1', date(2000,1,1), 'never', date(2022, 3,5)),  #Drop, most recent never status
    (5, 'N1', date(2000,1,1), 'never', date(2022,3,4)),   #Keep- oldest never status
    (6, 'N2', date(2000,1,2), 'never', date(2010,1,1)),   #Drop Latest never status
    (7, 'N2', date(2000,1,2), 'never', date(2009,1,1)),   #Keep- oldest never status
    (8, 'N3', date(2000,1,1), 'ex', date(2020,1,1,))      #Keep- single smoking status
  ], ['idx', 'test_nhs_number', 'test_dob', 'test_flag', 'test_journal_date'])
  
  df_expected = spark.createDataFrame([
    (1, 'N1', date(2000,1,1), 'current', date(2009,1,1)), 
    (2, 'N1', date(2000,1,1), 'ex', date(2009, 2, 3)),    
    (5, 'N1', date(2000,1,1), 'never', date(2022,3,4)),
    (7, 'N2', date(2000,1,2), 'never', date(2009,1,1)),   
    (8, 'N3', date(2000,1,1), 'ex', date(2020,1,1,))
  ], ['idx', 'test_nhs_number', 'test_dob', 'test_flag', 'test_journal_date'])
  
  df_actual = get_first_or_last_smoking_category_entry(
    df = df_input,
    field_person_id = 'test_nhs_number',
    field_person_dob = 'test_dob',
    field_date_journal = 'test_journal_date',
    field_smoking_status = 'test_flag',
    first_switch = True
  )
  
  assert compare_results(df_actual, df_expected, join_columns=['idx'])

# COMMAND ----------

@suite.add_test
def test_get_start_date():
  '''
  Test takes smoking status journal table entries and returns DataFrame the start date for each status category for each person with the joiing fields (nhs number, dob and category flag)
  
  Functions:
    src/cvdp/preprocess_cvdp_smoking::get_start_date
  '''
  
  df_input = spark.createDataFrame([
    (0, 'N1', date(2000,1,1), 'current', date(2010,1,1)), #Drop - most recent current status
    (1, 'N1', date(2000,1,1), 'current', date(2009,1,1)), #Keep - oldest current status
    (2, 'N1', date(2000,1,1), 'ex', date(2009, 2, 3)),    #Keep - olderst ex status
    (3, 'N1', date(2000,1,1), 'ex', date(2011,1,1)),      #Drop - most recent ex status
    (4, 'N1', date(2000,1,1), 'never', date(2022, 3,5)),  #Drop, most recent never status
    (5, 'N1', date(2000,1,1), 'never', date(2022,3,4)),   #Keep- oldest never status
    (6, 'N2', date(2000,1,2), 'never', date(2010,1,1)),   #Drop Latest never status
    (7, 'N2', date(2000,1,2), 'never', date(2009,1,1)),   #Keep- oldest never status
    (8, 'N3', date(2000,1,1), 'ex', date(2020,1,1,))      #Keep- single smoking status
  ], ['idx', 'test_nhs_number', 'test_dob', 'test_flag', 'test_journal_date'])
  
  df_expected = spark.createDataFrame([
    ('N1', date(2000,1,1), 'current', date(2009,1,1)), 
    ('N1', date(2000,1,1), 'ex', date(2009, 2, 3)),    
    ('N1', date(2000,1,1), 'never', date(2022,3,4)),
    ('N2', date(2000,1,2), 'never', date(2009,1,1)),   
    ('N3', date(2000,1,1), 'ex', date(2020,1,1,))
  ], ['test_nhs_number', 'test_dob', 'test_flag', 'test_start_date'])
  
  df_actual = get_start_date(
    df = df_input,
    field_person_id = 'test_nhs_number',
    field_person_dob = 'test_dob',
    field_date_journal = 'test_journal_date',
    field_smoking_status = 'test_flag',
    field_start_date = 'test_start_date'
  )
  
  assert compare_results(df_actual, df_expected, join_columns=['test_nhs_number', 'test_dob', 'test_flag'])

# COMMAND ----------

@suite.add_test
def test_add_start_dates():
  '''
  Test joining start date to latest smoking status category record for each person
  
  Functions:
    src/cvdp/preprocess_cvdp_smoking::add_start_dates
  '''
  
  df_input_latest = spark.createDataFrame([
    (0, 'N1', date(2000,1,1), 'current', date(2010,1,1)),
    (1, 'N1', date(2000,1,1), 'ex', date(2011,1,1)),
    (2, 'N1', date(2000,1,1), 'never', date(2022, 3,5)),
    (3, 'N2', date(2000,1,2), 'never', date(2010,1,1)),
    (4, 'N3', date(2000,1,1), 'ex', date(2020,1,1,))
  ], ['idx', 'test_nhs_number', 'test_dob', 'test_flag', 'test_journal_date'])
  
  df_input_start = spark.createDataFrame([
    ('N1', date(2000,1,1), 'current', date(2009,1,1)), 
    ('N1', date(2000,1,1), 'ex', date(2009, 2, 3)),    
    ('N1', date(2000,1,1), 'never', date(2022,3,4)),
    ('N2', date(2000,1,2), 'never', date(2009,1,1)),   
    ('N3', date(2000,1,1), 'ex', date(2020,1,1,))
  ], ['test_nhs_number', 'test_dob', 'test_flag', 'test_start_date'])
  
  df_expected = spark.createDataFrame([
    (0, 'N1', date(2000,1,1), 'current', date(2010,1,1), date(2009,1,1)),
    (1, 'N1', date(2000,1,1), 'ex', date(2011,1,1), date(2009, 2, 3)),
    (2, 'N1', date(2000,1,1), 'never', date(2022, 3,5), date(2022,3,4)),
    (3, 'N2', date(2000,1,2), 'never', date(2010,1,1), date(2009,1,1)),
    (4, 'N3', date(2000,1,1), 'ex', date(2020,1,1,), date(2020,1,1,))
  ], ['idx', 'test_nhs_number', 'test_dob', 'test_flag', 'test_journal_date', 'test_start_date'])
  
  df_actual = add_start_dates(
    df_latest_record = df_input_latest,
    df_start_dates = df_input_start,
    field_person_id = 'test_nhs_number',
    field_person_dob = 'test_dob',
    field_smoking_status = 'test_flag'
  )
  
  assert compare_results(df_actual, df_expected, join_columns=['idx'])

# COMMAND ----------

@suite.add_test
def test_check_number_of_events_fails():
  '''
  Tests that the assertion in check_number_of_events fails when more than 3 entries per person. The test will pass if this assertion fails.
  
  Functions:
    src/cvdp/process_cvdp_smoking_events::check_number_of_events
  '''
  
  df_input = spark.createDataFrame ([
    (0, 'N1', date(2000,1,1), 'current', date(2010,1,1), date(2007,12,1)),
    (1, 'N1', date(2000,1,1), 'ex', date(2008,1,1), date(2007,12,11)),
    (2, 'N1', date(2000,1,1), 'ex', date(2008,1,1), date(2007,12,11)),
    (3, 'N1', date(2000,1,1), 'never', date(2006,1,1), date(2003,1,1))
  ], ['idx', 'test_nhs_number', 'test_dob', 'test_flag', 'test_journal_date', 'test_start_date'])
  
  try:
      check_number_of_events(
        df = df_input,
        field_person_id = 'test_nhs_number',
        field_person_dob = 'test_dob',
        field_date_journal = 'test_journal_date',
      )
      raise AssertionError('test_check_number_of_events_fails failed (data incorrectly passed assertion checks)')
  except:
      assert True

# COMMAND ----------

@suite.add_test
def test_check_number_of_events_pass():
  '''
  Tests that the assertion in check_number_of_events passes when no more than 3 entries per person. The test will pass if this assertion passes.
  
  Functions:
    src/cvdp/process_cvdp_smoking_events::check_number_of_events
  '''
  
  df_input = spark.createDataFrame ([
    (0, 'N1', date(2000,1,1), 'current', date(2010,1,1), date(2007,12,1)),
    (1, 'N1', date(2000,1,1), 'ex', date(2008,1,1), date(2007,12,11)),
    (2, 'N1', date(2000,1,1), 'never', date(2008,1,1), date(2007,12,11)),
    (3, 'N2', date(2001,1,1), 'never', date(2006,1,1), date(2003,1,1)),
    (4, 'N3', date(2010,1,2), 'curent', date(2020,1,1), date(2020,1,1)),
    (5, 'N3', date(2000,1,2), 'ex', date(2001,1,1), date(2000,1,7)),
    (6, 'N3', date(2000,1,2), 'never', date(2000,1,2), date(2000,1,2))
  ], ['idx', 'test_nhs_number', 'test_dob', 'test_flag', 'test_journal_date', 'test_start_date'])
  
  try:
      check_number_of_events(
        df = df_input,
        field_person_id = 'test_nhs_number',
        field_person_dob = 'test_dob',
        field_date_journal = 'test_journal_date',
      )
      assert True
  except:
    raise AssertionError('test_check_number_of_events_pass failed (data did not pass assertion checks)')

# COMMAND ----------

suite.run()

# COMMAND ----------

dbutils.notebook.exit('pass')
