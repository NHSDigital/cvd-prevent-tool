# Databricks notebook source
# MAGIC %run ../../params/params_util

# COMMAND ----------

# MAGIC %run ../../src/test_helpers

# COMMAND ----------

from datetime import date
from dataclasses import dataclass

# COMMAND ----------

suite = FunctionTestSuite()

# COMMAND ----------

@suite.add_test
def test_params_default():

  assert DEFAULT_PARAMS.params_date == date.today()

  # check that start_date is earlier than end_date
  assert DEFAULT_PARAMS.start_date < DEFAULT_PARAMS.end_date

  # check types of parameters are what we would expect
  assert type(DEFAULT_PARAMS.start_date) == date
  assert type(DEFAULT_PARAMS.end_date) == date
  assert type(DEFAULT_PARAMS.params_date) == date
  assert isinstance(DEFAULT_PARAMS.DATABASE_NAME, str)


# COMMAND ----------

@suite.add_test
def test_params_overwrite_from_kwargs():

  @dataclass(frozen=True)
  class TestParams(ParamsBase):

    test_value: str = 'test_val'
    test_date: date = date(2021, 6, 5)

  test_params = TestParams(test_value='fake_val')
  assert test_params.test_value == 'fake_val'
  assert test_params.test_date == date(2021, 6, 5)

# COMMAND ----------

@suite.add_test
def test_params_to_simple_types():

  @dataclass(frozen=True)
  class TestParams(ParamsBase):

    test_value: str = 'test_val'
    test_date: date = date(2021, 6, 5)

  test_params = TestParams()
  assert test_params.to_dict() == {'params_date': date.today(), 'params_path': None, 'test_value': 'test_val', 'test_date': date(2021, 6, 5)}
  assert test_params.to_json() == '{"test_date": "2021-06-05", "test_value": "test_val"}'

# COMMAND ----------

@suite.add_test
def test_params_from_simple_types():

  @dataclass(frozen=True)
  class TestParams(ParamsBase):
    test_value: str = 'blah_val'
    test_date: date = date(1867, 1, 1)

  dict_vals = {'params_date': date.today(), 'test_value': 'test_val', 'test_date': date(2021, 6, 5)}
  params_from_dict = TestParams.from_dict(dict_vals)
  assert params_from_dict.params_date == date.today()
  assert params_from_dict.test_value == 'test_val'
  assert params_from_dict.test_date == date(2021, 6, 5)

  json_val = f'{{"params_date": "{date.today().strftime("%Y-%m-%d")}", "test_date": "2021-06-05", "test_value": "test_val"}}'
  params_from_json = TestParams.from_json(json_val)
  assert params_from_json.params_date == date.today()
  assert params_from_json.test_value == 'test_val'
  assert params_from_json.test_date == date(2021, 6, 5)

# COMMAND ----------

@suite.add_test
def test_params_version_hash():

  @dataclass(frozen=True)
  class TestParams(ParamsBase):
    test_value: str = 'blah_val'
    test_date: date = date(1867, 1, 2)

  test_params = TestParams()
  assert test_params.version == '1121f3'

  alternate_test_params = TestParams(test_value='blah_vat')
  assert alternate_test_params.version != '1121f3'

# COMMAND ----------

@suite.add_test
def test_params_set_params_path():

  @dataclass(frozen=True)
  class TestParams(ParamsBase):
    test_value: str = 'blah_val'
    test_date: date = date(1867, 1, 2)

  test_params = TestParams()
  assert test_params.params_path is None
  test_params.set_params_path('new/path/params')
  assert test_params.params_path == 'new/path/params'

# COMMAND ----------

@suite.add_test
def test_params_calc_latest_hes_year():
  """test_params_calc_latest_hes_year()
  params/params_util::_calc_latest_hes_year
  
  Test of the function to calculate latest HES year from HES AHAS database. Note that
  year values in table names are created dynamically to ensure they do not fail in subsequent
  years.
  """
  # Create year suffix values
  ## Current Year (Y1Y1Y2Y2)
  test_current_suffix_y2 = int(datetime.today().strftime('%y'))
  test_current_suffix_y1 = test_current_suffix_y2 - 1
  ## Next Year (Y1Y1Y2Y2)
  test_next_suffix_y2 = test_current_suffix_y2 + 1
  test_next_suffix_y1 = test_current_suffix_y2
  ## Previous Year
  test_prev_suffix_y2 = test_current_suffix_y1
  test_prev_suffix_y1 = test_current_suffix_y1 - 1

  # Create test tables to parse
  df_input = spark.createDataFrame([(1,2)],(['v1','v2']))
  input_name_01 = f'hes_test_01_{test_prev_suffix_y1}{test_prev_suffix_y2}'
  input_name_02 = f'hes_test_01_{test_current_suffix_y1}{test_current_suffix_y2}'
  input_name_03 = f'hes_test_01_{test_next_suffix_y1}{test_next_suffix_y2}'
  input_name_04 = f'hes_test_02_{test_next_suffix_y1}{test_next_suffix_y2}'
  input_name_05 = f'other_test_02_9091'

  # Assert check that dates match expected
  try:
    ## Create global temporary tables
    df_input.createOrReplaceGlobalTempView(input_name_01)
    df_input.createOrReplaceGlobalTempView(input_name_02)
    df_input.createOrReplaceGlobalTempView(input_name_03)
    df_input.createOrReplaceGlobalTempView(input_name_04)
    df_input.createOrReplaceGlobalTempView(input_name_05)
    # Define test parameters using calculation function
    @dataclass(frozen=True)
    class TestParams(ParamsBase):
      HES_AHAS_DATABASE = 'global_temp'
      HES_TABLE_PREFIX = 'hes_'
      HES_END_YEAR: int = _calc_latest_hes_year(HES_AHAS_DATABASE,HES_TABLE_PREFIX)
      HES_5YR_START_YEAR: int = HES_END_YEAR - 5
    ## Initialise test params
    test_params = TestParams()
    ## Assert END and 5YR START values match expected
    assert (test_params.HES_END_YEAR == test_next_suffix_y2) is True
    assert (test_params.HES_5YR_START_YEAR == test_next_suffix_y2 - 5) is True
  # Drop global temp tables
  finally:
    drop_table('global_temp',input_name_01)
    drop_table('global_temp',input_name_02)
    drop_table('global_temp',input_name_03)
    drop_table('global_temp',input_name_04)
    drop_table('global_temp',input_name_05)

# COMMAND ----------

@suite.add_test
def test_params_calc_latest_hes_year_fail():
  """test_params_calc_latest_hes_year_fail()
  params/params_util::_calc_latest_hes_year
  
  Test of the function to calculate latest HES year from HES AHAS database. Note that
  year values in table names are created dynamically to ensure they do not fail in subsequent
  years. Test should fail on assertion of date ranges.
  """
  # Create test tables to parse
  df_input = spark.createDataFrame([(1,2)],(['v1','v2']))
  input_name_01 = 'hes_test_01_0102'
  input_name_02 = 'hes_test_01_0203'
  input_name_03 = 'hes_test_01_0304'
  input_name_04 = 'hes_test_02_0102'
  input_name_05 = 'other_test_02_0405'

  # Assert check that dates match expected
  try:
    ## Create global temporary tables
    df_input.createOrReplaceGlobalTempView(input_name_01)
    df_input.createOrReplaceGlobalTempView(input_name_02)
    df_input.createOrReplaceGlobalTempView(input_name_03)
    df_input.createOrReplaceGlobalTempView(input_name_04)
    df_input.createOrReplaceGlobalTempView(input_name_05)
    # Define test parameters using calculation function
    try:
      @dataclass(frozen=True)
      class TestParams(ParamsBase):
        HES_AHAS_DATABASE = 'global_temp'
        HES_TABLE_PREFIX = 'hes_'
        HES_END_YEAR: int = _calc_latest_hes_year(HES_AHAS_DATABASE,HES_TABLE_PREFIX)
        HES_5YR_START_YEAR: int = HES_END_YEAR - 5
      ## Initialise test params
      test_params = TestParams()
      ## Return assertion result (Fail)
      test_result = False
    except:
      ## Test failed (assertion in params, test passed)
      test_result = True
    ## Assert test result is True (params creation failed)
    assert test_result == True
  # Drop global temp tables
  finally:
    drop_table('global_temp',input_name_01)
    drop_table('global_temp',input_name_02)
    drop_table('global_temp',input_name_03)
    drop_table('global_temp',input_name_04)
    drop_table('global_temp',input_name_05)

# COMMAND ----------

suite.run()

# COMMAND ----------

dbutils.notebook.exit('pass')