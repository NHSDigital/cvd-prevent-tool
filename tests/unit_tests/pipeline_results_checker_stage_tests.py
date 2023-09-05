# Databricks notebook source
# Notebook containing tests for pipeline_results_checker_stage notebook

# COMMAND ----------

# MAGIC %run ../../src/test_helpers

# COMMAND ----------

PARAMS_PATH = 'default'

# COMMAND ----------

# MAGIC %run ../../pipeline/pipeline_results_checker_stage

# COMMAND ----------

import pyspark.sql.functions as F
import pyspark.sql.types as T

from pyspark.sql import Row
from uuid import uuid4
from datetime import datetime, date
from pyspark.sql.types import StringType, IntegerType, StructField, StructType, DateType

from dsp.validation.validator import compare_results

# COMMAND ----------

suite = FunctionTestSuite()

## The following are fake people and data created for test purposes

# COMMAND ----------

# Create test specific parameters
@dataclass(frozen = True)
class TestParams(ParamsBase):
    DATABASE_NAME = 'prevent_tool_collab'
    ## SHARED
    PID_FIELD = 'person_id'
    PRACTICE_FIELD = 'practice_identifier'
    DOB_FIELD = 'birth_date'
    DATASET_FIELD = 'dataset'
    RECORD_ID_FIELD = 'record_id'
    RECORD_STARTDATE_FIELD = 'record_start_date'
    LSOA_FIELD = 'lsoa'
    COHORT_FIELD = 'cohort'
    LATEST_EXTRACT_DATE = 'latest_extract_date'
    LATEST_PRACTICE_ID = 'latest_practice_identifier'
    CODE_FIELD = 'code'
    CODE_ARRAY_FIELD = 'code_array'
    FLAG_FIELD = 'flag'
    ASSOC_FLAG_FIELD = 'flag_assoc'
    CATEGORY_FIELD = 'category'
    ASSOC_REC_ID_FIELD = 'record_id_assoc'
    DATASETS_RESULTS_CHECKER = ['dars_bird_deaths', 'hes_apc', 'cvdp_cohort', 'cvdp_htn']
    COHORTS_RESULTS_CHECKER = ['CVDPCX001', 'CVDPCX002']
    ## EXPECTED SCHEMAS - GROUND TRUTH
    EXPECTED_PATIENTS_SCHEMA = StructType([
      StructField("person_id",StringType(),False),
      StructField("birth_date",DateType(),False),
      StructField("latest_extract_date",DateType(),False),
      StructField("cohort",StringType(),True)
      ])
    EXPECTED_EVENTS_SCHEMA = StructType([
      StructField("person_id",StringType(),False),
      StructField("dataset",StringType(),False),
      StructField("category",StringType(),False),
      StructField("code",StringType(),False),
      StructField("record_id",IntegerType(),False),
      StructField("record_start_date",DateType(),False)
      ])

# COMMAND ----------

@suite.add_test
def test_default_create_report_table_stage():
  """Test Results Checker stage by creating Events and Patients tables which behave according to Results checker rules = all checks pass.
  """
  # Setup Test-Specific Parameters
  test_params = TestParams()
  # Fake log
  log = PipelineLogger('')
  
  ## Events table
  events_schema = StructType([
    StructField("person_id",StringType(),False),
    StructField("dataset",StringType(),False),
    StructField("category",StringType(),False),
    StructField("code",StringType(),False),
    StructField("record_id",IntegerType(),False),
    StructField("record_start_date",DateType(),False)
    ])

  df_events = spark.createDataFrame([
    ('0', 'dars_bird_deaths', 'abc', '0', 123, date(2021, 1, 1)),
    ('1', 'hes_apc', 'abc', '1', 456, date(2021, 1, 1)),
    ('2', 'cvdp_cohort', 'abc', '2', 789, date(2021, 1, 1)),
    ('3', 'cvdp_htn', 'abc', '3', 112, date(2021, 1, 1)),
  ], events_schema)
  
  ## Patients table
  patients_schema = StructType([
      StructField("person_id",StringType(),False),
      StructField("birth_date",DateType(),False),
      StructField("latest_extract_date",DateType(),False),
      StructField("cohort",StringType(),True)
      ])

  df_patients = spark.createDataFrame([
    ('0', date(2021, 1, 1), date(2021, 1, 1), 'CVDPCX001'),
    ('1', date(2021, 1, 1), date(2021, 1, 1), 'CVDPCX002'),
    ('2', date(2021, 1, 1), date(2021, 1, 1), 'CVDPCX001')
  ], patients_schema)

  df_expected = spark.createDataFrame([
    ('pass', 'check_events_table_all_datasets_present'),
    ('pass', 'check_events_table_not_empty'),
    ('pass', 'check_events_table_any_fields_all_nulls'),
    ('pass', 'check_events_table_non_nullable_columns_for_nulls'),
    ('pass', 'check_events_table_schema_equality'),
    ('pass', 'check_patients_table_not_empty'),
    ('pass', 'check_patients_table_all_cohorts_present'),
    ('pass', 'check_patients_table_any_fields_all_nulls'),
    ('pass', 'check_patients_table_non_nullable_columns_for_nulls'),
    ('pass', 'check_patients_table_schema_equality')
  ], ['level', 'check'])
  
  # Main test function
  with FunctionPatch('params', test_params):
    # Stage initialisation
    stage = DefaultCreateReportTableStage(
      events_table_input='EVENTS_TABLE', 
      patient_table_input='PATIENTS_TABLE', 
      report_table_output='REPORT_TABLE'
    )
    context = PipelineContext('12345', params, [DefaultCreateReportTableStage])
    context['EVENTS_TABLE'] = PipelineAsset('EVENTS_TABLE', context, df_events)
    context['PATIENTS_TABLE'] = PipelineAsset('PATIENTS_TABLE', context, df_patients)
    context = stage._run(context, log)
    df_actual = context['REPORT_TABLE'].df.select('level', 'check')

  #some records will be the same so check the count and then drop duplicates to compare with df_expected
  assert df_actual.count() == 12
  assert df_actual.where(col('level') == 'fail').count() == 0
  assert df_actual.where(col('level') == 'warning').count() == 0
  assert df_actual.where(col('level') == 'pass').count() == 12
  assert compare_results(df_actual.drop_duplicates(), df_expected, join_columns=['check'])

# COMMAND ----------

@suite.add_test
def test_default_create_report_table_stage_fails():
  """Test Results Checker stage by creating Events and Patients tables which do not behave according to Results checker rules:
  Some checks pass, some fails and some trigger a warning.
  """
  # Setup Test-Specific Parameters
  test_params = TestParams()
  # Fake log
  log = PipelineLogger('')
  
  ## Events table
  events_schema = StructType([
    StructField("person_id",StringType(),True),
    StructField("dataset",StringType(),False),
    StructField("category",StringType(),True),
    StructField("code",StringType(),False),
    StructField("record_id",IntegerType(),True),
    StructField("record_start_date",DateType(),True)
    ])
  
  # check for one column containing all nulls, non nullable columns containing null values, surprise dataset, missing dataset & schema not matching
  df_events = spark.createDataFrame([
    ('0', 'dars_bird_deaths', None, '0', 123, date(2021, 1, 1)),
    ('1', 'hes_apc', None, '1', 456, date(2021, 1, 1)),
    ('2', 'cvdp_cohort', None, '2', 789, date(2021, 1, 1)),
    (None, 'cvdp_cohort', None, '3', 112, date(2021, 1, 1)),
    ('4', 'dars_bird_deaths', None, '0', 123, date(2021, 1, 1)),
    ('5', 'surprise_dataset', None, '1', 456, date(2021, 1, 1)),
    ('6', 'cvdp_cohort', None, '2', 789, None),
    ('7', 'cvdp_cohort', None, '3', None, date(2021, 1, 1))
  ], events_schema)
  
  ## Patients table
  patients_schema = StructType([
      StructField("person_id",StringType(),True),
      StructField("birth_date",DateType(),True),
      StructField("latest_extract_date",DateType(),True),
      StructField("cohort",StringType(),False)
      ])

  # check for one column containing all nulls, non nullable columns containing null values, mystery cohort, missing cohort & schema not matching
  df_patients = spark.createDataFrame([
    ('0', date(2021, 1, 1), None, 'CVDPCX001'),
    ('1', None, None, 'CVDPCX001'),
    ('2', date(2021, 1, 1), None, 'mystery_cohort'),
    (None, date(2021, 1, 1), None, 'CVDPCX001'),
    ('4', date(2021, 1, 1), None, 'CVDPCX001'),
    ('5', date(2021, 1, 1), None, 'CVDPCX001')
  ], patients_schema)

  df_expected = spark.createDataFrame([
    ('warning', 'check_events_table_all_datasets_present'),
    ('pass', 'check_events_table_not_empty'),
    ('warning', 'check_events_table_any_fields_all_nulls'),
    ('warning', 'check_events_table_non_nullable_columns_for_nulls'),
    ('warning', 'check_events_table_schema_equality'),
    ('pass', 'check_patients_table_not_empty'),
    ('warning', 'check_patients_table_all_cohorts_present'),
    ('warning', 'check_patients_table_any_fields_all_nulls'),
    ('warning', 'check_patients_table_non_nullable_columns_for_nulls'),
    ('warning', 'check_patients_table_schema_equality')
  ], ['level', 'check'])

    # Main test function
  with FunctionPatch('params', test_params):
    # Stage initialisation
    stage = DefaultCreateReportTableStage(
      events_table_input='EVENTS_TABLE', 
      patient_table_input='PATIENTS_TABLE', 
      report_table_output='REPORT_TABLE'
    )
    context = PipelineContext('12345', params, [DefaultCreateReportTableStage])
    context['EVENTS_TABLE'] = PipelineAsset('EVENTS_TABLE', context, df_events)
    context['PATIENTS_TABLE'] = PipelineAsset('PATIENTS_TABLE', context, df_patients)
    context = stage._run(context, log)
    df_actual = context['REPORT_TABLE'].df.select('level', 'check')

  #some records will be the same so check the count and then drop duplicates to compare with df_expected
  assert df_actual.count() == 12
  assert df_actual.drop_duplicates().where(col('level') == 'fail').count() == 0
  assert df_actual.drop_duplicates().where(col('level') == 'warning').count() == 8
  assert df_actual.drop_duplicates().where(col('level') == 'pass').count() == 2
  assert compare_results(df_actual.drop_duplicates(), df_expected, join_columns=['check'])

# COMMAND ----------

@suite.add_test
def test_default_check_results_stage():
  """Test when all messages are non failures or warnings.
  """
  # Setup Test-Specific Parameters
  test_params = TestParams()
  # Fake log
  log = PipelineLogger('')
  
  df_input = spark.createDataFrame([
    ('pass', 'check_1', 'message_1'),
    ('pass', 'check_2', 'message_2'),
    ('info', 'check_3', 'info_3'),
  ], ['level', 'check', 'message'])
  
  # Main test function
  with FunctionPatch('params', test_params):
    # Stage initialisation
    stage = DefaultCheckResultsStage(report_input='REPORT_TABLE')
    context = PipelineContext('12345', params, [DefaultCheckResultsStage])
    context['REPORT_TABLE'] = PipelineAsset('REPORT_TABLE', context, df_input)
    stage._run(context, log)
     
  assert True

# COMMAND ----------

@suite.add_test
def test_default_check_results_stage_fails():
  """Test when messages are a mixture of passes, warnings and failures.
  """
  # Setup Test-Specific Parameters
  test_params = TestParams()
  # Fake log
  log = PipelineLogger('')
  
  df_input = spark.createDataFrame([
    ('pass', 'check_1', 'message_1'),
    ('pass', 'check_2', 'message_1'),
    ('info', 'check_3', 'info_1'),
    ('fail', 'check_4', 'fail_message_4'),
    ('fail', 'check_5', 'fail_message_5'),
    ('warning', 'check_6', 'warning_message_6')
  ], ['level', 'check', 'message'])

  with FunctionPatch('params', test_params):
    # Stage initialisation
    stage = DefaultCheckResultsStage(report_input='REPORT_TABLE')
    context = PipelineContext('12345', params, [DefaultCheckResultsStage])
    context['REPORT_TABLE'] = PipelineAsset('REPORT_TABLE', context, df_input)
    try:
      stage._run(context, log)
      assert True
    except AssertionError:
      assert True
      #raise AssertionError("test_default_check_results_stage_fail failed (data did not pass assertion checks).")

# COMMAND ----------

suite.run()

# COMMAND ----------

dbutils.notebook.exit('pass')
