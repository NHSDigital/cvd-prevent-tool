# Databricks notebook source
# MAGIC %run ../../src/test_helpers

# COMMAND ----------

PARAMS_PATH = 'default'

# COMMAND ----------

# MAGIC %run ../../params/params

# COMMAND ----------

# MAGIC %run ../../pipeline/create_demographic_table_lib

# COMMAND ----------

from uuid import uuid4
from datetime import datetime, date
import pyspark.sql.functions as F
import pyspark.sql.types as T

from dsp.validation.validator import compare_results

# COMMAND ----------

suite = FunctionTestSuite()

# COMMAND ----------

@suite.add_test
def test_load_ethnicity_ref_codes():
  '''test_load_ethnicity_ref_codes

  Tests ethnicity reference table loads a non-empty table
  '''

  df = load_ethnicity_ref_codes()

  assert df.first() != None


# COMMAND ----------

@suite.add_test
def test_get_ethnicity_snomed_codes():
  '''test_get_ethnicity_snomed_codes

  Tests whether get_ethnicity_snomed_codes returns a non-empty list
  '''

  snomed_code_list = get_ethnicity_snomed_codes()

  assert snomed_code_list


# COMMAND ----------

@suite.add_test
def test_filter_journal_by_code():
  '''test_filter_journal_by_code

  Tests filtering the journal table to only select snomed codes that relate to ethnicity
  '''
  input_journal_df = spark.createDataFrame([
    (0, date(1960,1,1), date(2022,1,1), '001', 'TEST_CLUSTER'),
    (0, date(1960,1,1), date(2022,1,1), '001', 'DUPE_TEST_CLUSTER'),
    (1, date(1960,1,1), date(2022,1,1), '002', 'TEST_CLUSTER'),
    (1, date(1960,1,1), date(2022,1,1), '002', 'DUPE_TEST_CLUSTER'),
    (2, date(1960,1,1), date(2022,1,1), 'XXX', 'TEST_CLUSTER'), #INVALID ETHNICITY CODE
    (2, date(1960,1,1), date(2022,1,1), 'XXX', 'DUPE_TEST_CLUSTER'), #INVALID ETHNICITY CODE
    (3, date(1960,1,1), date(2022,1,1), 'YYY', 'TEST_CLUSTER'), #INVALID SNOMED CODE
    (3, date(1960,1,1), date(2022,1,1), 'YYY', 'DUPE_TEST_CLUSTER'), #INVALID SNOMED CODE
    (4, date(1960,1,1), date(2022,1,1), None, 'TEST_CLUSTER'), #INVALID SNOMED CODE
    (4, date(1960,1,1), date(2022,1,1), None, 'DUPE_TEST_CLUSTER'), #INVALID SNOMED CODE
      ], ['pid_field','dob_field','date_field', 'snomed_code', 'CLUSTER_ID'])

  expected_journal_df = spark.createDataFrame([
    (0, date(1960,1,1), date(2022,1,1), '001'),
    (1, date(1960,1,1), date(2022,1,1), '002'),
      ], ['pid_field','dob_field','date_field', 'snomed_code'])

  actual_journal_df = filter_journal_by_code(input_journal_df, code_field='snomed_code', inclusion_codes=['001','002'], output_fields=['pid_field','dob_field','date_field','snomed_code'])
  assert compare_results(actual_journal_df, expected_journal_df, join_columns=['pid_field', 'dob_field'])


# COMMAND ----------

@suite.add_test
def test_get_journal_ethnicity():
  '''test_get_journal_ethnicity

  Tests filtering the journal table to:
  * select snomed codes that relate to ethnicity (see test_filter_journal_by_code)
  * provide the ethnicity code that the snomed code relates to
  '''
  input_journal_df = spark.createDataFrame([
    (0, date(1960,1,1), date(2022,1,1), '92561000000109', 'TEST_CLUSTER'),
    (0, date(1960,1,1), date(2022,1,1), '92561000000109', 'DUPE_TEST_CLUSTER'),
    (1, date(1960,1,1), date(2022,1,1), '92571000000102', 'TEST_CLUSTER'),
    (1, date(1960,1,1), date(2022,1,1), '92571000000102', 'DUPE_TEST_CLUSTER'),
    (2, date(1960,1,1), date(2022,1,1), '415794004', 'TEST_CLUSTER'), #INVALID ETHNICITY CODE
    (2, date(1960,1,1), date(2022,1,1), '415794004', 'DUPE_TEST_CLUSTER'), #INVALID ETHNICITY CODE
    (3, date(1960,1,1), date(2022,1,1), '91000000109', 'TEST_CLUSTER'), #INVALID SNOMED CODE
    (3, date(1960,1,1), date(2022,1,1), '91000000109', 'DUPE_TEST_CLUSTER'), #INVALID SNOMED CODE
    (4, date(1960,1,1), date(2022,1,1), None, 'TEST_CLUSTER'), #INVALID SNOMED CODE
    (4, date(1960,1,1), date(2022,1,1), None, 'DUPE_TEST_CLUSTER'), #INVALID SNOMED CODE
      ], [params.PID_FIELD,params.DOB_FIELD,params.JOURNAL_DATE_FIELD, params.CODE_FIELD, 'CLUSTER_ID'])

  expected_journal_df = spark.createDataFrame([
    (0, date(1960,1,1), date(2022,1,1), 'C', 'journal'),
    (1, date(1960,1,1), date(2022,1,1), 'A', 'journal'),
      ], [params.PID_FIELD,params.DOB_FIELD,params.RECORD_STARTDATE_FIELD, params.ETHNICITY_FIELD, params.DATASET_FIELD])

  actual_journal_df = get_journal_ethnicity(input_journal_df)
  assert compare_results(actual_journal_df, expected_journal_df, join_columns=[params.PID_FIELD, params.DOB_FIELD])


# COMMAND ----------

@suite.add_test
def test_get_hes_ethnicity() -> DataFrame:
  '''test_get_hes_ethnicity
  Tests selecting the latest known ethnicity from HES tables
  '''
  input_hes_df = spark.createDataFrame([
    (0, date(1960,1,1), 'test_hes', date(2022,1,1), 'A', 'BAR_1'),
    (1, date(1960,1,1), 'test_hes', date(2022,1,1), 'C', 'BAR_1'),
    (2, date(1960,1,1), 'test_hes', date(2022,1,1), 'Z', 'BAR_2'), #INVALID ETHNICITY CODE
    (3, date(1960,1,1), 'test_hes', date(2022,1,1), None, 'BAR_2'), #INVALID ETHNICITY CODE
      ], [params.PID_FIELD, params.DOB_FIELD, params.DATASET_FIELD, params.RECORD_STARTDATE_FIELD, params.ETHNICITY_FIELD, 'FOO'])

  expected_hes_df = spark.createDataFrame([
    (0, date(1960,1,1), 'test_hes', date(2022,1,1), 'A'),
    (1, date(1960,1,1), 'test_hes', date(2022,1,1), 'C'),
      ], [params.PID_FIELD, params.DOB_FIELD, params.DATASET_FIELD, params.RECORD_STARTDATE_FIELD, params.ETHNICITY_FIELD])

  actual_hes_df = get_hes_ethnicity(input_hes_df)
  assert compare_results(actual_hes_df, expected_hes_df, join_columns=[params.PID_FIELD, params.DOB_FIELD])

# COMMAND ----------

@suite.add_test
def test_filter_from_year() -> DataFrame:
  '''test_filter_from_year
  Tests filtering the HES AE table to anything after and including a year
  '''
  input_hes_df = spark.createDataFrame([
    (0, date(1960,1,1), 'hes_ae', date(2020,1,1), 'B'), # AFTER START YEAR
    (1, date(1960,1,1), 'hes_ae', date(2019,12,31), 'B'), # BEFORE START YEAR
    (1, date(1960,1,1), 'hes_ae', date(2020,1,2), 'B'), # AFTER START YEAR
    (2, date(1960,1,1), 'hes_ae', date(2011,1,1), None), # BEFORE START YEAR
    (2, date(1960,1,1), 'hes_ae', date(2021,1,1), 'A'), # AFTER START YEAR
    (3, date(1960,1,1), 'hes_ae', date(2010,1,1), 'A'), # BEFORE START YEAR
    (3, date(1960,1,1), 'hes_ae', date(2021,1,1), None), # AFTER START YEAR
    (4, date(1960,1,1), 'hes_ae', None, 'A'), # NULL START
      ], [params.PID_FIELD, params.DOB_FIELD, params.DATASET_FIELD, params.RECORD_STARTDATE_FIELD, params.ETHNICITY_FIELD])

  expected_hes_df = spark.createDataFrame([
    (0, date(1960,1,1), 'hes_ae', date(2020,1,1), 'B'), # AFTER START YEAR
    (1, date(1960,1,1), 'hes_ae', date(2020,1,2), 'B'), # AFTER START YEAR
    (2, date(1960,1,1), 'hes_ae', date(2021,1,1), 'A'), # AFTER START YEAR
    (3, date(1960,1,1), 'hes_ae', date(2021,1,1), None), # AFTER START YEAR
      ], [params.PID_FIELD, params.DOB_FIELD, params.DATASET_FIELD, params.RECORD_STARTDATE_FIELD, params.ETHNICITY_FIELD])

  actual_hes_df = filter_from_year(input_hes_df, start_year=2020, date_field=params.RECORD_STARTDATE_FIELD)
  assert compare_results(actual_hes_df, expected_hes_df, join_columns=[params.PID_FIELD, params.DOB_FIELD])

# COMMAND ----------

@suite.add_test
def test_select_priority_ethnicity() -> DataFrame:
  '''test_select_priority_ethnicity

  Tests to ensure that ethnicity is being selected in the correct priority order.

  This should be- most recent record.
  If there are >1 records from >1 datasets on this most recent date, a priority order is applied:
    1. Journal
    2. HES APC
    3. HES AE
    4. HES OP
  '''
  input_df = spark.createDataFrame([
    (0, date(1960,1,1), 'journal', date(2022,1,1), 'A', 'BAR_1'),
    (0, date(1960,1,1), 'hes_apc', date(2022,1,1), 'C', 'BAR_1'),
    (0, date(1960,1,1), 'hes_ae', date(2022,1,1), 'B', 'BAR_1'),
    (0, date(1960,1,1), 'hes_op', date(2022,1,1), 'C', 'BAR_1'),
    (1, date(1960,1,1), 'journal', date(2022,1,1), 'A', 'BAR_1'),
    (1, date(1960,1,1), 'hes_apc', date(2022,1,1), 'B', 'BAR_1'),
    (2, date(1960,1,1), 'journal', date(2022,1,1), 'A', 'BAR_2'),
    (2, date(1960,1,1), 'hes_op', date(2022,1,1), 'B', 'BAR_2'),
    (3, date(1960,1,1), 'hes_apc', date(2022,1,1), 'A', 'BAR_2'),
    (3, date(1960,1,1), 'hes_op', date(2022,1,1), 'B', 'BAR_2'),
    (4, date(1960,1,1), 'hes_ae', date(2022,1,1), 'B', 'BAR_2'),
    (4, date(1960,1,1), 'hes_op', date(2022,1,1), 'A', 'BAR_2'),
    (5, date(1960,1,1), 'hes_op', date(2022,1,1), 'A', 'BAR_2'),
    (6, date(1960,1,1), 'hes_op', date(2022,1,1), 'B', 'BAR_2'),
    (6, date(1960,1,1), 'hes_op', date(2019,1,1), 'A', 'BAR_2'),
    (7, date(1960,1,1), 'hes_op', date(2022,1,1), 'B', 'BAR_2'),
    (7, date(1960,1,1), 'journal', date(2019,1,1), 'A', 'BAR_2'),
    (8, date(1960,1,1), 'hes_op', date(2022,1,1), 'A', 'BAR_2'), #TWO VALUES
    (8, date(1960,1,1), 'hes_op', date(2022,1,1), 'B', 'BAR_2'), #TWO VALUES
    (8, date(1960,1,1), 'journal', date(2019,1,1), 'B', 'BAR_2'), #OLDER
      ], [params.PID_FIELD, params.DOB_FIELD, params.DATASET_FIELD, params.RECORD_STARTDATE_FIELD, params.ETHNICITY_FIELD, 'FOO'])

  expected_df = spark.createDataFrame([
    (0, date(1960,1,1), 'journal', date(2022,1,1), 'A'),
    (1, date(1960,1,1), 'journal', date(2022,1,1), 'A'),
    (2, date(1960,1,1), 'journal', date(2022,1,1), 'A'),
    (3, date(1960,1,1), 'hes_apc', date(2022,1,1), 'A'),
    (4, date(1960,1,1), 'hes_ae', date(2022,1,1), 'B'),
    (5, date(1960,1,1), 'hes_op', date(2022,1,1), 'A'),
    (6, date(1960,1,1), 'hes_op', date(2022,1,1), 'B'),
    (7, date(1960,1,1), 'hes_op', date(2022,1,1), 'B'),
      ], [params.PID_FIELD, params.DOB_FIELD, params.DATASET_FIELD, params.RECORD_STARTDATE_FIELD, params.ETHNICITY_FIELD])

  actual_df = select_priority_ethnicity(input_df)
  assert compare_results(actual_df, expected_df, join_columns=[params.PID_FIELD, params.DOB_FIELD])


# COMMAND ----------

@suite.add_test
def test_filter_latest_priority_non_null():
  '''test_filter_latest_priority_non_null
  Tests filtering the latest record that does not return a null value.
  If no non-null values exist, the latest null value is returned.
  '''

  input_df = spark.createDataFrame([
    (0, date(1960,1,1), date(2022,1,1), 'A', 'BAR_1'), # LATEST
    (0, date(1960,1,1), date(2022,1,1), 'C', 'BAR_1'), # LATEST
    (0, date(1960,1,1), date(2022,1,1), None, 'BAR_1'), # NULL
    (1, date(1960,1,1), date(2022,1,1), 'A', 'BAR_1'), # LATEST
    (1, date(1960,1,1), date(2021,1,1), None, 'BAR_1'), # OLDER, NULL
    (2, date(1960,1,1), date(2022,1,1), None, 'BAR_2'), # LATEST NULL
    (2, date(1960,1,1), date(2022,1,1), 'B', 'BAR_2'), # LATEST NON NULL
    (3, date(1960,1,1), date(2022,1,1), None, 'BAR_2'), # LATEST NULL
    (3, date(1960,1,1), date(2021,1,1), 'B', 'BAR_2'), # LATEST NON NULL
    (4, date(1960,1,1), date(2022,1,1), None, 'BAR_2'), # LATEST NULL
    (4, date(1960,1,1), date(2021,1,1), None, 'BAR_2'), # OLDER NULL
    (5, date(1960,1,1), date(2022,1,1), None, 'BAR_2'), # LATEST NULL
    (5, date(1960,1,1), date(2022,1,1), None, 'BAR_2'), # LATEST NULL
      ], [params.PID_FIELD, params.DOB_FIELD, params.RECORD_STARTDATE_FIELD, params.ETHNICITY_FIELD, 'FOO'])

  expected_df = spark.createDataFrame([
    (0, date(1960,1,1), date(2022,1,1), 'A'),
    (0, date(1960,1,1), date(2022,1,1), 'C'),
    (1, date(1960,1,1), date(2022,1,1), 'A'),
    (2, date(1960,1,1), date(2022,1,1), 'B'),
    (3, date(1960,1,1), date(2021,1,1), 'B'),
    (4, date(1960,1,1), date(2022,1,1), None),
    (5, date(1960,1,1), date(2022,1,1), None),
    (5, date(1960,1,1), date(2022,1,1), None),
      ], [params.PID_FIELD, params.DOB_FIELD, params.RECORD_STARTDATE_FIELD, params.ETHNICITY_FIELD])


  actual_df = filter_latest_priority_non_null(input_df, pid_fields=[params.PID_FIELD, params.DOB_FIELD], date_field=params.RECORD_STARTDATE_FIELD, field=params.ETHNICITY_FIELD)
  assert compare_results(actual_df, expected_df, join_columns=[params.PID_FIELD, params.DOB_FIELD, params.ETHNICITY_FIELD])

# COMMAND ----------

@suite.add_test
def test_filter_latest_non_null():
  '''test_filter_latest_non_null
  Tests filtering the latest record that does not return a null value.
  '''

  input_df = spark.createDataFrame([
    (0, date(1960,1,1), date(2022,1,1), 'A', 'BAR_1'), # LATEST
    (0, date(1960,1,1), date(2022,1,1), 'C', 'BAR_1'), # LATEST
    (0, date(1960,1,1), date(2022,1,1), None, 'BAR_1'), # NULL
    (1, date(1960,1,1), date(2022,1,1), 'A', 'BAR_1'), # LATEST
    (1, date(1960,1,1), date(2021,1,1), None, 'BAR_1'), # OLDER, NULL
    (2, date(1960,1,1), date(2022,1,1), None, 'BAR_2'), # LATEST NULL
    (2, date(1960,1,1), date(2022,1,1), 'B', 'BAR_2'), # LATEST NON NULL
    (3, date(1960,1,1), date(2022,1,1), None, 'BAR_2'), # LATEST NULL
    (3, date(1960,1,1), date(2021,1,1), 'B', 'BAR_2'), # LATEST NON NULL
    (4, date(1960,1,1), date(2022,1,1), None, 'BAR_2'), # LATEST NULL
    (4, date(1960,1,1), date(2021,1,1), None, 'BAR_2'), # OLDER NULL
    (5, date(1960,1,1), date(2022,1,1), None, 'BAR_2'), # LATEST NULL
    (5, date(1960,1,1), date(2022,1,1), None, 'BAR_2'), # LATEST NULL
      ], [params.PID_FIELD, params.DOB_FIELD, params.RECORD_STARTDATE_FIELD, params.ETHNICITY_FIELD, 'FOO'])

  expected_df = spark.createDataFrame([
    (0, date(1960,1,1), date(2022,1,1), 'A'),
    (0, date(1960,1,1), date(2022,1,1), 'C'),
    (1, date(1960,1,1), date(2022,1,1), 'A'),
    (2, date(1960,1,1), date(2022,1,1), 'B'),
    (3, date(1960,1,1), date(2021,1,1), 'B'),
      ], [params.PID_FIELD, params.DOB_FIELD, params.RECORD_STARTDATE_FIELD, params.ETHNICITY_FIELD])


  actual_df = filter_latest_non_null(input_df, pid_fields=[params.PID_FIELD, params.DOB_FIELD], date_field=params.RECORD_STARTDATE_FIELD, field=params.ETHNICITY_FIELD)
  assert compare_results(actual_df, expected_df, join_columns=[params.PID_FIELD, params.DOB_FIELD, params.ETHNICITY_FIELD])

# COMMAND ----------

@suite.add_test
def test_remove_multiple_records():
  '''test_remove_multiple_records
  Tests removing records with multiple values of a field on the same date
  '''

  input_df = spark.createDataFrame([
    (0, date(1960,1,1), date(2022,1,1), 'A'), #
    (0, date(1960,1,1), date(2022,1,1), 'C'), #
    (1, date(1960,1,1), date(2022,1,1), 'A'), #
    (1, date(1960,1,1), date(2022,1,1), 'A'), #
    (2, date(1960,1,1), date(2022,1,1), 'A'), #
    (2, date(1960,1,1), date(2021,1,1), 'B'), #
      ], ['pid', 'dob', 'record_date', 'ethnicity'])

  df_expected = spark.createDataFrame([
    (1, date(1960,1,1), date(2022,1,1), 'A'), #
    (2, date(1960,1,1), date(2022,1,1), 'A'), #
    (2, date(1960,1,1), date(2021,1,1), 'B'), #
      ], ['pid', 'dob', 'record_date', 'ethnicity'])

  df_actual = remove_multiple_records(input_df, window_fields=['pid','dob','record_date'], field='ethnicity')

  assert compare_results(df_actual, df_expected, join_columns=['pid', 'dob', 'ethnicity'])


# COMMAND ----------

@suite.add_test
def test_set_multiple_records_to_null():
  '''test_set_multiple_records_to_null
  Tests setting to null where there are records with multiple values on the same date
  '''

  input_df = spark.createDataFrame([
    (0, date(1960,1,1), date(2022,1,1), 'A'), #
    (0, date(1960,1,1), date(2022,1,1), 'C'), #
    (1, date(1960,1,1), date(2022,1,1), 'A'), #
    (1, date(1960,1,1), date(2022,1,1), 'A'), #
    (2, date(1960,1,1), date(2022,1,1), 'A'), #
    (2, date(1960,1,1), date(2021,1,1), 'B'), #
      ], ['pid', 'dob', 'record_date', 'ethnicity'])

  df_expected = spark.createDataFrame([
    (0, date(1960,1,1), date(2022,1,1), None), #
    (1, date(1960,1,1), date(2022,1,1), 'A'), #
    (2, date(1960,1,1), date(2022,1,1), 'A'), #
    (2, date(1960,1,1), date(2021,1,1), 'B'), #
      ], ['pid', 'dob', 'record_date', 'ethnicity'])

  df_actual = set_multiple_records_to_null(input_df, window_fields=['pid','dob','record_date'], field='ethnicity')

  assert compare_results(df_actual, df_expected, join_columns=['pid', 'dob', 'ethnicity'])


# COMMAND ----------

suite.run()

# COMMAND ----------

dbutils.notebook.exit('pass')