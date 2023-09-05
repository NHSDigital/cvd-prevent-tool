# Databricks notebook source
PARAMS_PATH = 'default'

# COMMAND ----------

# MAGIC %run ../../params/params

# COMMAND ----------

# MAGIC %run ../../src/test_helpers

# COMMAND ----------

# MAGIC %run ../../pipeline/preprocess_raw_data_lib

# COMMAND ----------

from datetime import date
from dsp.validation.validator import compare_results
from typing import Any, Callable, List
from types import FunctionType
from collections import namedtuple

# COMMAND ----------

suite = FunctionTestSuite()

## The following are fake people and data created for test purposes

# COMMAND ----------

@suite.add_test
def test_data_entry_types():
    
    def test_preprocess(arg):
        print(arg)
    
    TEST_DATA_ENTRY = PreprocessStageDataEntry(
        dataset_name = 'test_dataset',
        db = 'test_db',
        table = 'test_table',
        filter_eligible_patients = filter_fields('test_pid','test_dob'),
        preprocessing_func = test_preprocess,
        validate_nhs_numbers = True,
        clean_nhs_number_fields = ['test_col_nhs'],
        clean_null_fields = ['test_col_null'],
        replace_empty_str_fields = ['test_col_empty']
    )
    
    assert type(TEST_DATA_ENTRY.dataset_name) == str
    assert type(TEST_DATA_ENTRY.db) == str
    assert type(TEST_DATA_ENTRY.table) == str
    assert type(TEST_DATA_ENTRY.filter_eligible_patients.pid_field) == str
    assert type(TEST_DATA_ENTRY.filter_eligible_patients.dob_field) == str
    assert type(TEST_DATA_ENTRY.preprocessing_func) == FunctionType
    assert type(TEST_DATA_ENTRY.clean_nhs_number_fields) == list
    assert type(TEST_DATA_ENTRY.clean_null_fields) == list
    assert type(TEST_DATA_ENTRY.replace_empty_str_fields) == list
    

# COMMAND ----------

@suite.add_test
def test_filter_fields():
    
    fields_input = filter_fields('test_pid_field','test_dob_field')
    
    assert type(fields_input.pid_field) == str
    assert type(fields_input.dob_field) == str
    assert fields_input.pid_field == 'test_pid_field'
    assert fields_input.dob_field == 'test_dob_field'
    

# COMMAND ----------

@suite.add_test
def test_filter_eligible_patients():
    
    df_patients = spark.createDataFrame([
        ('0001', date(2021,1,1),'foo_1'),
        ('0002', date(2022,1,1),'foo_2a'),
        ('0002', date(2023,1,1),'foo_2b'),
        ('0003', None, 'foo_3'),
        (None, date(2024,1,1), 'foo_4')
    ], [params.PID_FIELD, params.DOB_FIELD, 'BAR'])
    
    df_input = spark.createDataFrame([
        ('0001' ,date(2021,1,1), 'bar_1'),
        ('0002', date(2022,1,1), 'bar_2'),
        ('0003', date(2023,1,1), 'bar_3'),
        ('0004', date(2024,1,1), 'bar_4'),
        ('0005', date(2025,1,1), 'bar_5')
    ], ['pid','dob','FOO'])
    
    fields_input = filter_fields('pid','dob')
    
    df_expected = spark.createDataFrame([
        ('0001' ,date(2021,1,1), 'bar_1', 'foo_1'),
        ('0002', date(2022,1,1), 'bar_2', 'foo_2a'),
    ], ['pid','dob','FOO','BAR'])
    
    df_actual = filter_eligible_patients(patient_list = df_patients, df_filter = df_input, filter_fields = fields_input)
    
    assert compare_results(df_actual, df_expected, join_columns = ['pid'])
    

# COMMAND ----------

@suite.add_test
def test_add_dataset_field():
    
    test_dataset_name = 'test_data'
    
    df_input = spark.createDataFrame([
        (0, 'foo 0'),
        (1, 'foo 1'),
        (2, 'foo 2'),
        (3, None),
        (4, '23')
    ], ['idx','bar'])
    
    df_expected = spark.createDataFrame([
        (0, 'foo 0', 'test_data'),
        (1, 'foo 1', 'test_data'),
        (2, 'foo 2', 'test_data'),
        (3, None, 'test_data'),
        (4, '23', 'test_data')
    ], ['idx','bar',params.DATASET_FIELD])
    
    df_actual = add_dataset_field(df = df_input, dataset_name = test_dataset_name)
    
    assert compare_results(df_actual, df_expected, join_columns = ['idx'])
    

# COMMAND ----------

suite.run()

# COMMAND ----------

dbutils.notebook.exit('pass')
