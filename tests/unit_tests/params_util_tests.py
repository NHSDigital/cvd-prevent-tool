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

suite.run()

# COMMAND ----------

dbutils.notebook.exit('pass')
