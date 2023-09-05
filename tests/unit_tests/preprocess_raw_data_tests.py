# Databricks notebook source
PARAMS_PATH = 'default'

# COMMAND ----------

# MAGIC %run ../../params/params

# COMMAND ----------

# MAGIC %run ../../src/test_helpers

# COMMAND ----------

# MAGIC %run ../../pipeline/preprocess_raw_data

# COMMAND ----------

from datetime import datetime
from dsp.validation.validator import compare_results
from uuid import uuid4

# COMMAND ----------

suite = FunctionTestSuite()

## The following are fake people and data created for test purposes

# COMMAND ----------

TEST_PARAMS_ENTRY = PreprocessStageDataEntry(
    dataset_name             = 'test_ds1',
    db                       = 'test_ds1_db',
    table                    = 'test_ds1_table',
    filter_eligible_patients = filter_fields('test_pid', 'test_dob'),
    preprocessing_func       = None,
    validate_nhs_numbers     = False,
    clean_nhs_number_fields  = [],
    clean_null_fields        = [],
)

# COMMAND ----------

@suite.add_test
def test_clean_and_preprocess_raw_data_non_hes():
  
  stage = PreprocessRawDataStage(None, None, None, None, None, None, None, None, None, None, None)
    
  @dataclass(frozen = True)
  class TestParams(ParamsBase):
    DATASETS  = ['test_ds1']
    DATASET_FIELD = 'dataset'
    INPUT_COL_1 = 'index'
    INPUT_COL_2 = 'val'
    RENAME_COL_1 = 'idx'
    RENAME_COL_2 = 'output'

  test_params = TestParams()

  tmp_table_name = f'_tmp_{uuid4().hex}'
  df_input = spark.createDataFrame([(0, 'a'),], ['index', 'val'])
  df_input.createOrReplaceGlobalTempView(tmp_table_name)

  stage._param_entries = [
      PreprocessStageDataEntry(
          dataset_name             = 'test_ds1',
          db                       = 'global_temp',
          table                    = tmp_table_name,
          filter_eligible_patients = None,
          preprocessing_func       = None,
          validate_nhs_numbers     = False,
          clean_nhs_number_fields  = [],
          clean_null_fields        = [],
          rename_field_map         = {
            TestParams.INPUT_COL_1: TestParams.RENAME_COL_1,
            TestParams.INPUT_COL_2: TestParams.RENAME_COL_2,
          }
  ),
  ]

  global param_entries 
  param_entries = stage._param_entries 

  df_expected = spark.createDataFrame([(1, 'b', 'test_ds1'),], ['idx', 'output','dataset'])

  def mock_clean_and_preprocess_dataset(df, nhs_number_fields, clean_null_fields,
                                        replace_empty_str_fields = None,
                                        preprocessing_func = None, 
                                        validate_nhs_numbers = True):
    assert compare_results(df, df_input, join_columns=['index'])
    return df_expected

  with FunctionPatch('clean_and_preprocess_dataset', mock_clean_and_preprocess_dataset):
      with FunctionPatch('params',  test_params):
          stage._clean_and_preprocess_raw_data()
          assert compare_results(stage._source_data_holder['test_ds1'], df_expected, join_columns=['idx'])

# COMMAND ----------

@suite.add_test
def test_clean_and_preprocess_raw_data_hes():
  stage = PreprocessRawDataStage(None, None, None, None, None, None, None, None, None, None, None)
    
  @dataclass(frozen = True)
  class TestParams(ParamsBase):
    DATASETS  = ['test_ds2']
    HES_START_YEAR = 19 # used for reading HES tables
    HES_END_YEAR = 22 # used for reading HES tables
    DATASET_FIELD = 'dataset'
    HES_DATABASE = 'hes'
    HES_S_DATABASE = 'flat_hes_s'
    HES_AHAS_DATABASE = 'hes_ahas'
    HES_AHAS_S_DATABASE = 'hes_ahas_s'
    HES_APC_TABLE = 'hes_apc'
    INPUT_COL_1 = 'index'
    INPUT_COL_2 = 'val'
    RENAME_COL_1 = 'idx'
    RENAME_COL_2 = 'output'

  test_params = TestParams()

  tmp_table_name = f'_tmp_{uuid4().hex}'
  df_input = spark.createDataFrame([(0, 'a'),], ['index', 'val'])
  df_input.createOrReplaceGlobalTempView(tmp_table_name)

  stage._param_entries = [
      PreprocessStageDataEntry(
          dataset_name             = 'test_ds2',
          db                       = 'hes_test_ds2_db',
          table                    = 'hes_test_ds2_table',
          filter_eligible_patients = None,
          preprocessing_func       = None,
          validate_nhs_numbers     = False,
          clean_nhs_number_fields  = [],
          clean_null_fields        = [],
          rename_field_map         = {
            TestParams.INPUT_COL_1: TestParams.RENAME_COL_1,
            TestParams.INPUT_COL_2: TestParams.RENAME_COL_2,
          }
    ),
  ]

  global param_entries 
  param_entries = stage._param_entries 

  def mock_get_hes_dataframes_from_years(hes_dataset_name, start_year, end_year, db_hes, db_ahas):
    assert hes_dataset_name == 'hes_test_ds2_table'
    assert start_year == 19
    assert end_year == 22
    assert db_hes == 'hes'
    assert db_ahas == 'hes_ahas'
    return df_input

  df_expected = spark.createDataFrame([(1, 'b', 'test_ds2'),], ['idx','output','dataset'])

  def mock_clean_and_preprocess_dataset(df, nhs_number_fields, clean_null_fields,
                                        replace_empty_str_fields = None,
                                        preprocessing_func = None, 
                                        validate_nhs_numbers = True):
      assert compare_results(df, df_input, join_columns=['index'])
      return df_expected

  with FunctionPatch('get_hes_dataframes_from_years', mock_get_hes_dataframes_from_years):
      with FunctionPatch('clean_and_preprocess_dataset', mock_clean_and_preprocess_dataset):
          with FunctionPatch('params',  test_params):
              stage._clean_and_preprocess_raw_data()
              assert compare_results(stage._source_data_holder['test_ds2'], df_expected, join_columns=['idx'])

# COMMAND ----------

@suite.add_test
def test_filter_data_on_eligible_patients():
  stage = PreprocessRawDataStage(None, None, None, None, None, None, None, None, None, None, None)
    
  @dataclass(frozen = True)
  class TestParams(ParamsBase):
      DATASETS  = ['test_ds1']
      PID_FIELD = 'person_id'
      DOB_FIELD = 'birth_date'
      GLOBAL_JOIN_KEY = ['person_id','birth_date']
  test_params = TestParams()

  df_input = spark.createDataFrame([
      ('111', datetime(2021,1,1), 'foo_1', 'bar_3'),
      ('222', datetime(2022,1,1), 'foo_2', 'bar_2'),
      ('333', datetime(2023,1,1), 'foo_3', 'bar_1'),
      ], ['nhs_number', 'date_of_birth', 'FOO', 'BAR'])

  tmp_table_name = f'_tmp_{uuid4().hex}'
  df_input.createOrReplaceGlobalTempView(tmp_table_name)

  stage._param_entries = [
      PreprocessStageDataEntry(
          dataset_name             = 'test_ds1',
          db                       = 'global_temp',
          table                    = tmp_table_name,
          filter_eligible_patients = filter_fields('nhs_number', 'date_of_birth'),
          preprocessing_func       = None,
          validate_nhs_numbers     = False,
          clean_nhs_number_fields  = [],
          clean_null_fields        = [],
  )]

  df_eligible = spark.createDataFrame([
      ('111', datetime(2021,1,1), 'CXD001', datetime(2021,2,2)),
      ('222', datetime(2022,1,1), 'CXD002', datetime(2022,2,2)),
      ('222', datetime(2022,1,2), 'CXD003', datetime(2022,2,3)),
      ('A00', datetime(2023,1,1), 'CXD004', datetime(2023,2,2))
  ], [params.PID_FIELD, params.DOB_FIELD, params.COHORT_FIELD, params.JOURNAL_DATE_FIELD])

  df_expected = spark.createDataFrame([
      ('111', datetime(2021,1,1), 'foo_1', 'bar_3', 'CXD001', datetime(2021,2,2)),
      ('222', datetime(2022,1,1), 'foo_2', 'bar_2', 'CXD002', datetime(2022,2,2)),
      ], ['nhs_number', 'date_of_birth', 'FOO', 'BAR', params.COHORT_FIELD, params.JOURNAL_DATE_FIELD])

  with FunctionPatch('params',  test_params):
      stage._source_data_holder['test_ds1'] = df_input
      stage._filter_data_on_eligible_patients(df_eligible)
      assert compare_results(stage._source_data_holder['test_ds1'], df_expected, join_columns=['nhs_number'])

# COMMAND ----------

suite.run()

# COMMAND ----------

dbutils.notebook.exit('pass')
