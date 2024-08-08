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
        stage = CreatePatientCohortTableStage(None, None, None)
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
        stage = CreatePatientCohortTableStage(None, None, None)
        stage._data_holder['cvdp_journal'] = df_input
        stage._transform_journal_schema()
        df_actual = stage._data_holder['journal_table']

        assert compare_results(df_actual, df_expected, join_columns = ['person_id'])

# COMMAND ----------

@suite.add_test
def test_create_cohort_table_check_hash_collisions_cohort_pass():
    """test_create_cohort_table_check_hash_collisions_cohort_pass
    pipeline/create_cohort_table::_check_hash_collisions_cohort

    Test of unique identifier check for the cohort table. Test passes if only unique
    values are found.
    """

    # Setup Test Parameters
    @dataclass(frozen = True)
    class TestParams(ParamsBase):
        CVDP_COHORT_PRIMARY_KEY = 'test_key_cohort'
    test_params = TestParams()

    # Input Data
    df_input = spark.createDataFrame([
        (1,'a1'),
        (2,'a2'),
        (3,'a3')
    ], ['idx','test_key_cohort'])

    # Stage Test
    with FunctionPatch('params',test_params):
        stage = CreatePatientCohortTableStage(None, None, None)
        stage._data_holder['cohort_table'] = df_input
        try:
            stage._check_unique_identifiers_cohort()
            result = True
        except:
            result = False

    # Assertion
    assert result == True

# COMMAND ----------

@suite.add_test
def test_create_cohort_table_check_hash_collisions_cohort_fail():
    """test_create_cohort_table_check_hash_collisions_cohort_fail
    pipeline/create_cohort_table::_check_hash_collisions_cohort

    Test of unique identifier check for the cohort table. Test passes if non-unique values
    are found.
    """

    # Setup Test Parameters
    @dataclass(frozen = True)
    class TestParams(ParamsBase):
        CVDP_COHORT_PRIMARY_KEY = 'test_key_cohort'
    test_params = TestParams()

    # Input Data
    df_input = spark.createDataFrame([
        (1,'a1'),
        (2,'a2'),
        (3,'a2')
    ], ['idx','test_key_cohort'])

    # Stage Test
    with FunctionPatch('params',test_params):
        stage = CreatePatientCohortTableStage(None, None, None)
        stage._data_holder['cohort_table'] = df_input
        try:
            stage._check_unique_identifiers_cohort()
            result = False
        except:
            result = True

    # Assertion
    assert result == True


# COMMAND ----------

@suite.add_test
def test_create_cohort_table_check_hash_collisions_journal_pass():
    """test_create_cohort_table_check_hash_collisions_journal_pass
    pipeline/create_cohort_table::_check_hash_collisions_journal

    Test of unique identifier check for the journal table. Test passes if only unique
    values are found.
    """

    # Setup Test Parameters
    @dataclass(frozen = True)
    class TestParams(ParamsBase):
        CVDP_JOURNAL_PRIMARY_KEY = 'test_key_journal'
    test_params = TestParams()

    # Input Data
    df_input = spark.createDataFrame([
        (1,'a1'),
        (2,'a2'),
        (3,'a3')
    ], ['idx','test_key_journal'])

    # Stage Test
    with FunctionPatch('params',test_params):
        stage = CreatePatientCohortTableStage(None, None, None)
        stage._data_holder['journal_table'] = df_input
        try:
            stage._check_unique_identifiers_journal()
            result = True
        except:
            result = False

    # Assertion
    assert result == True

# COMMAND ----------

@suite.add_test
def test_create_cohort_table_check_hash_collisions_journal_fail():
    """test_create_cohort_table_check_hash_collisions_journal_fail
    pipeline/create_cohort_table::_check_hash_collisions_journal

    Test of unique identifier check for the journal table. Test passes if non-unique values
    are found.
    """

    # Setup Test Parameters
    @dataclass(frozen = True)
    class TestParams(ParamsBase):
        CVDP_JOURNAL_PRIMARY_KEY = 'test_key_journal'
    test_params = TestParams()

    # Input Data
    df_input = spark.createDataFrame([
        (1,'a1'),
        (2,'a2'),
        (3,'a2')
    ], ['idx','test_key_journal'])

    # Stage Test
    with FunctionPatch('params',test_params):
        stage = CreatePatientCohortTableStage(None, None, None)
        stage._data_holder['journal_table'] = df_input
        try:
            stage._check_unique_identifiers_journal()
            result = False
        except:
            result = True

    # Assertion
    assert result == True

# COMMAND ----------

suite.run()

# COMMAND ----------

dbutils.notebook.exit('pass')
