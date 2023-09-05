# Databricks notebook source
PARAMS_PATH = 'default'

# COMMAND ----------

# MAGIC %run ../../params/params

# COMMAND ----------

# MAGIC %run ../../src/test_helpers

# COMMAND ----------

# MAGIC %run ../../pipeline/create_cohort_table

# COMMAND ----------

from datetime import date, datetime
from pyspark.sql.types import StructType, StructField, TimestampType, IntegerType, StringType, DateType, ShortType, LongType, DoubleType
from dsp.validation.validator import compare_results
import pyspark.sql.functions as F
from uuid import uuid4

# COMMAND ----------

suite = FunctionTestSuite()

## The following are fake people and data created for test purposes

# COMMAND ----------

@dataclass(frozen=True)
class TestParams(ParamsBase):
    params_date: date = date(2022, 2, 5)
    DATABASE_NAME: str = 'test_db'
    

# COMMAND ----------

@suite.add_test
def test_create_cohort_table_transform_cohort_schema():
    
    # TEST PARAMETERS
    @dataclass(frozen = True)
    class TestParams(ParamsBase):
        COHORT_TRANSFORM_MAPPING  = {
            'nhs_number': 'person_id',
            'date_of_birth': 'birth_date',
            'extract_date': 'date',
        }
        COHORT_STAGE_COHORT_COLUMNS = [
            'person_id', 'birth_date', 'cohort',
            'date', 'journal_date'
        ]

    test_params = TestParams()
    
    df_input = spark.createDataFrame([
        ('111', datetime(2021,1,1), 'CXD001', datetime(2021,2,1), datetime(2021,2,2), 'BAR_A'),
        ('222', datetime(2022,1,1), 'CXD002', datetime(2021,2,1), datetime(2022,2,2), 'BAR_A'),
        ('333', datetime(2023,1,1), 'CXD003', datetime(2021,2,1), datetime(2023,2,2), 'BAR_A'),
    ], ['nhs_number', 'date_of_birth', 'cohort', 'extract_date', 'journal_date', 'foo'])
    
    df_expected = spark.createDataFrame([
        ('111', datetime(2021,1,1), 'CXD001', datetime(2021,2,1), datetime(2021,2,2)),
        ('222', datetime(2022,1,1), 'CXD002', datetime(2021,2,1), datetime(2022,2,2)),
        ('333', datetime(2023,1,1), 'CXD003', datetime(2021,2,1), datetime(2023,2,2)),
    ], ['person_id', 'birth_date', 'cohort', 'date', 'journal_date'])
    
    with FunctionPatch('params',test_params):
        stage = CreatePatientCohortTableStage(None, None)
        stage._data_holder['cvdp_combined'] = df_input
        stage._transform_cohort_schema()
        df_actual = stage._data_holder['cohort_table']
        
        assert compare_results(df_actual, df_expected, join_columns = ['person_id'])
    

# COMMAND ----------

@suite.add_test
def test_create_cohort_table_transform_journal_schema():
    
    # TEST PARAMETERS
    @dataclass(frozen = True)
    class TestParams(ParamsBase):
        JOURNAL_TRANSFORM_MAPPING  = {
            'nhs_number': 'person_id',
            'date_of_birth': 'birth_date',
            'journal_date': 'date',
            'journal_array': 'code_array'
        }
        COHORT_STAGE_JOURNAL_COLUMNS = [
            'person_id', 'birth_date', 'cohort',
            'date', 'code_array'
        ]

    test_params = TestParams()
    
    df_input = spark.createDataFrame([
        ('111', datetime(2021,1,1), 'CXD001', 'BAR_A', datetime(2021,2,1), ['A','B']),
        ('222', datetime(2022,1,1), 'CXD002', 'BAR_B', datetime(2021,2,1), ['C','D']),
        ('333', datetime(2023,1,1), 'CXD003', 'BAR_C', datetime(2021,2,1), ['E',None]),
    ], ['nhs_number', 'date_of_birth', 'cohort', 'foo', 'journal_date', 'journal_array'])
    
    df_expected = spark.createDataFrame([
        ('111', datetime(2021,1,1), 'CXD001', datetime(2021,2,1), ['A','B']),
        ('222', datetime(2022,1,1), 'CXD002', datetime(2021,2,1), ['C','D']),
        ('333', datetime(2023,1,1), 'CXD003', datetime(2021,2,1), ['E',None]),
    ], ['person_id', 'birth_date', 'cohort', 'date', 'code_array'])
    
    with FunctionPatch('params',test_params):
        stage = CreatePatientCohortTableStage(None, None)
        stage._data_holder['cvdp_journal'] = df_input
        stage._transform_journal_schema()
        df_actual = stage._data_holder['journal_table']
        
        assert compare_results(df_actual, df_expected, join_columns = ['person_id'])
    

# COMMAND ----------

@suite.add_test
def test_limit_cohort():
    
    # TEST PARAMETERS
    @dataclass(frozen = True)
    class TestParams(ParamsBase):
        COHORT_TRANSFORM_MAPPING  = {
            'nhs_number': 'person_id',
            'date_of_birth': 'birth_date',
            'extract_date': 'date',
        }
        COHORT_STAGE_COHORT_COLUMNS = [
            'person_id', 'birth_date', 'cohort',
            'date', 'journal_date'
        ]

    test_params = TestParams()
    
    df_input = spark.createDataFrame([
        ('1',  datetime(2021,1,1), 'CXD001', datetime(2021,2,1), datetime(2021,2,2), 'BAR_A'),
        ('2',  datetime(2022,1,1), 'CXD002', datetime(2021,2,1), datetime(2022,2,2), 'BAR_A'),
        ('3',  datetime(2023,1,1), 'CXD003', datetime(2021,2,1), datetime(2023,2,2), 'BAR_A'),
        ('4',  datetime(2021,2,1), 'CXD001', datetime(2021,2,1), datetime(2021,2,2), 'BAR_A'),
        ('5',  datetime(2022,2,1), 'CXD002', datetime(2021,2,1), datetime(2022,2,2), 'BAR_A'),
        ('6',  datetime(2023,2,1), 'CXD003', datetime(2021,2,1), datetime(2023,2,2), 'BAR_A'),
        ('7',  datetime(2021,3,1), 'CXD001', datetime(2021,2,1), datetime(2021,2,2), 'BAR_A'),
        ('8',  datetime(2022,3,1), 'CXD002', datetime(2021,2,1), datetime(2022,2,2), 'BAR_A'),
        ('9',  datetime(2023,3,1), 'CXD003', datetime(2021,2,1), datetime(2023,2,2), 'BAR_A'),
        ('10', datetime(2023,3,1), 'CXD003', datetime(2021,2,1), datetime(2023,2,2), 'BAR_A'),
    ], ['nhs_number', 'date_of_birth', 'cohort', 'extract_date', 'journal_date', 'foo'])
    
    # check sample is correct length
    with FunctionPatch('params',test_params):
        stage = CreatePatientCohortTableStage(None, None)
        stage._data_holder['cvdp_combined'] = df_input
        stage._limit_cohort(lim_int=5)
        df_actual = stage._data_holder['cvdp_combined']
        length = df_actual.count()
        assert (length <= 5) and (length > 0)
   
    
        
        
        
    

# COMMAND ----------

@suite.add_test
def test_limit_cohort_distinct_rows():
    
    # TEST PARAMETERS
    @dataclass(frozen = True)
    class TestParams(ParamsBase):
        COHORT_TRANSFORM_MAPPING  = {
            'nhs_number': 'person_id',
            'date_of_birth': 'birth_date',
            'extract_date': 'date',
        }
        COHORT_STAGE_COHORT_COLUMNS = [
            'person_id', 'birth_date', 'cohort',
            'date', 'journal_date'
        ]

    test_params = TestParams()
    
    df_input = spark.createDataFrame([
        ('1', datetime(2021,1,1), 'CXD001', datetime(2021,2,1), datetime(2021,2,2), 'BAR_A'),
        ('2', datetime(2022,1,1), 'CXD002', datetime(2021,2,1), datetime(2022,2,2), 'BAR_A'),
        ('3', datetime(2023,1,1), 'CXD003', datetime(2021,2,1), datetime(2023,2,2), 'BAR_A'),
        ('4', datetime(2021,2,1), 'CXD001', datetime(2021,2,1), datetime(2021,2,2), 'BAR_A'),
        ('5', datetime(2022,2,1), 'CXD002', datetime(2021,2,1), datetime(2022,2,2), 'BAR_A'),
        ('6', datetime(2023,2,1), 'CXD003', datetime(2021,2,1), datetime(2023,2,2), 'BAR_A'),
        ('7', datetime(2021,3,1), 'CXD001', datetime(2021,2,1), datetime(2021,2,2), 'BAR_A'),
        ('8', datetime(2022,3,1), 'CXD002', datetime(2021,2,1), datetime(2022,2,2), 'BAR_A'),
        ('9', datetime(2023,3,1), 'CXD003', datetime(2021,2,1), datetime(2023,2,2), 'BAR_A'),
    ], ['nhs_number', 'date_of_birth', 'cohort', 'extract_date', 'journal_date', 'foo'])
    
    # check sample is correct length and does not duplicate rows
    with FunctionPatch('params',test_params):
        stage = CreatePatientCohortTableStage(None, None)
        stage._data_holder['cvdp_combined'] = df_input
        stage._limit_cohort(lim_int=9)
        df_actual = stage._data_holder['cvdp_combined']
        df_actual_distinct = stage._data_holder['cvdp_combined'].distinct()
        length = df_actual.count()
        length_distinct = df_actual_distinct.count()
        assert (length <= 9) and (length > 0)
        assert (length_distinct <= 9) and (length_distinct > 0)

# COMMAND ----------

@suite.add_test
def test_limit_cohort_limit_too_high():
    
    # TEST PARAMETERS
    @dataclass(frozen = True)
    class TestParams(ParamsBase):
        COHORT_TRANSFORM_MAPPING  = {
            'nhs_number': 'person_id',
            'date_of_birth': 'birth_date',
            'extract_date': 'date',
        }
        COHORT_STAGE_COHORT_COLUMNS = [
            'person_id', 'birth_date', 'cohort',
            'date', 'journal_date'
        ]

    test_params = TestParams()
    
    df_input = spark.createDataFrame([
        ('1', datetime(2021,1,1), 'CXD001', datetime(2021,2,1), datetime(2021,2,2), 'BAR_A'),
        ('2', datetime(2022,1,1), 'CXD002', datetime(2021,2,1), datetime(2022,2,2), 'BAR_A'),
        ('3', datetime(2023,1,1), 'CXD003', datetime(2021,2,1), datetime(2023,2,2), 'BAR_A'),
        ('4', datetime(2021,2,1), 'CXD001', datetime(2021,2,1), datetime(2021,2,2), 'BAR_A'),
        ('5', datetime(2022,2,1), 'CXD002', datetime(2021,2,1), datetime(2022,2,2), 'BAR_A'),
        ('6', datetime(2023,2,1), 'CXD003', datetime(2021,2,1), datetime(2023,2,2), 'BAR_A'),
        ('7', datetime(2021,3,1), 'CXD001', datetime(2021,2,1), datetime(2021,2,2), 'BAR_A'),
        ('8', datetime(2022,3,1), 'CXD002', datetime(2021,2,1), datetime(2022,2,2), 'BAR_A'),
        ('9', datetime(2023,3,1), 'CXD003', datetime(2021,2,1), datetime(2023,2,2), 'BAR_A'),
    ], ['nhs_number', 'date_of_birth', 'cohort', 'extract_date', 'journal_date', 'foo'])
    
    # check sample does not error if lim_int is greater than the number of rows 
    with FunctionPatch('params',test_params):
        stage = CreatePatientCohortTableStage(None, None)
        stage._data_holder['cvdp_combined'] = df_input
        stage._limit_cohort(lim_int=10)
        df_actual = stage._data_holder['cvdp_combined']
        df_actual_distinct = stage._data_holder['cvdp_combined'].distinct()
        length = df_actual.count()
        assert (length <= 9) and (length > 0)
        


# COMMAND ----------

@suite.add_test
def test_limit_cohort_limit_random():
    
    # TEST PARAMETERS
    @dataclass(frozen = True)
    class TestParams(ParamsBase):
        COHORT_TRANSFORM_MAPPING  = {
            'nhs_number': 'person_id',
            'date_of_birth': 'birth_date',
            'extract_date': 'date',
        }
        COHORT_STAGE_COHORT_COLUMNS = [
            'person_id', 'birth_date', 'cohort',
            'date', 'journal_date'
        ]

    test_params = TestParams()
    
    df_input = spark.createDataFrame([
        ('1',  datetime(2021,1,1), 'CXD001', datetime(2021,2,1), datetime(2021,2,2), 'BAR_A'),
        ('2',  datetime(2022,1,1), 'CXD002', datetime(2021,2,1), datetime(2022,2,2), 'BAR_A'),
        ('3',  datetime(2023,1,1), 'CXD003', datetime(2021,2,1), datetime(2023,2,2), 'BAR_A'),
        ('4',  datetime(2021,2,1), 'CXD001', datetime(2021,2,1), datetime(2021,2,2), 'BAR_A'),
        ('5',  datetime(2022,2,1), 'CXD002', datetime(2021,2,1), datetime(2022,2,2), 'BAR_A'),
        ('6',  datetime(2023,2,1), 'CXD003', datetime(2021,2,1), datetime(2023,2,2), 'BAR_A'),
        ('7',  datetime(2021,3,1), 'CXD001', datetime(2021,2,1), datetime(2021,2,2), 'BAR_A'),
        ('8',  datetime(2022,3,1), 'CXD002', datetime(2021,2,1), datetime(2022,2,2), 'BAR_A'),
        ('9',  datetime(2023,3,1), 'CXD003', datetime(2021,2,1), datetime(2023,2,2), 'BAR_A'),
        ('11', datetime(2021,1,1), 'CXD001', datetime(2021,2,1), datetime(2021,2,2), 'BAR_A'),
        ('12', datetime(2022,1,1), 'CXD002', datetime(2021,2,1), datetime(2022,2,2), 'BAR_A'),
        ('13', datetime(2023,1,1), 'CXD003', datetime(2021,2,1), datetime(2023,2,2), 'BAR_A'),
        ('14', datetime(2021,2,1), 'CXD001', datetime(2021,2,1), datetime(2021,2,2), 'BAR_A'),
        ('15', datetime(2022,2,1), 'CXD002', datetime(2021,2,1), datetime(2022,2,2), 'BAR_A'),
        ('16', datetime(2023,2,1), 'CXD003', datetime(2021,2,1), datetime(2023,2,2), 'BAR_A'),
        ('17', datetime(2021,3,1), 'CXD001', datetime(2021,2,1), datetime(2021,2,2), 'BAR_A'),
        ('18', datetime(2022,3,1), 'CXD002', datetime(2021,2,1), datetime(2022,2,2), 'BAR_A'),
        ('19', datetime(2023,3,1), 'CXD003', datetime(2021,2,1), datetime(2023,2,2), 'BAR_A'),
    ], ['nhs_number', 'date_of_birth', 'cohort', 'extract_date', 'journal_date', 'foo'])
    
    with FunctionPatch('params',test_params):
        stage = CreatePatientCohortTableStage(None, None)
        stage._data_holder['cvdp_combined'] = df_input
        stage._limit_cohort(lim_int=15)
        df_actual_1 = stage._data_holder['cvdp_combined']
        
    with FunctionPatch('params',test_params):
        stage = CreatePatientCohortTableStage(None, None)
        stage._data_holder['cvdp_combined'] = df_input
        stage._limit_cohort(lim_int=15)
        df_actual_2 = stage._data_holder['cvdp_combined']
        
    assert compare_results(df_actual_1, df_actual_2, join_columns = ['nhs_number']) == False
    
    

# COMMAND ----------

suite.run()

# COMMAND ----------

dbutils.notebook.exit('pass')
