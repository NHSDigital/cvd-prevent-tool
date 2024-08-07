# Databricks notebook source
PARAMS_PATH = 'default'

# COMMAND ----------

# MAGIC %run ../../params/params

# COMMAND ----------

# MAGIC %run ../../src/test_helpers

# COMMAND ----------

# MAGIC %run ../../pipeline/extract_cvdp_stage

# COMMAND ----------

import pyspark.sql.types as T

from datetime import date
from dsp.validation.validator import compare_results

# COMMAND ----------

suite = FunctionTestSuite()

# COMMAND ----------

@suite.add_test
def test_check_hash_collisions_pass():
    '''test_check_hash_collisions_pass
    extract_cvdp_stage::ExtractCVDPDataStage._check_hash_collisions()
    Tests hash id checking of hash id column in combined CVDP asset. Test will
    pass if no error is raised (all values unique).
    '''
    # Setup Test Parameters
    @dataclass(frozen = True)
    class TestParams(ParamsBase):
        CVDP_PRIMARY_KEY = 'hash_id'
    test_params = TestParams()

    # Setup Test Data
    ## Input
    df_input = spark.createDataFrame([
        (1,'a1'),
        (2,'a2'),
        (3,'a3')
    ], ['idx', 'hash_id'])

    # Stage Test
    with FunctionPatch('params',test_params):
        stage = ExtractCVDPDataStage(None)
        stage._data_holder['cohort_target'] = df_input
        try:
            stage._check_hash_collisions()
            return True
        except:
            raise Exception('test_check_hash_collisions_pass failed')


# COMMAND ----------

@suite.add_test
def test_check_hash_collisions_fail():
    '''test_check_hash_collisions_fail
    extract_cvdp_stage::ExtractCVDPDataStage._check_hash_collisions()
    Tests hash id checking of hash id column in combined CVDP asset. Test will
    pass if an error is raised (presence of non-unique values).
    '''
    # Setup Test Parameters
    @dataclass(frozen = True)
    class TestParams(ParamsBase):
        CVDP_PRIMARY_KEY = 'hash_id'
    test_params = TestParams()

    # Setup Test Data
    ## Input
    df_input = spark.createDataFrame([
        (1,'a1'),
        (2,'a2'),
        (3,'a2')
    ], ['idx', 'hash_id'])

    # Stage Test
    with FunctionPatch('params',test_params):
        stage = ExtractCVDPDataStage(None)
        stage._data_holder['cohort_target'] = df_input
        try:
            stage._check_hash_collisions()
            raise Exception('test_check_hash_collisions_fail failed')
        except:
            return True


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
        stage = ExtractCVDPDataStage(None)
        stage._data_holder['cohort_target'] = df_input
        stage._limit_cohort(lim_int=5)
        df_actual = stage._data_holder['cohort_target']
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
        stage = ExtractCVDPDataStage(None)
        stage._data_holder['cohort_target'] = df_input
        stage._limit_cohort(lim_int=9)
        df_actual = stage._data_holder['cohort_target']
        df_actual_distinct = stage._data_holder['cohort_target'].distinct()
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
        stage = ExtractCVDPDataStage(None)
        stage._data_holder['cohort_target'] = df_input
        stage._limit_cohort(lim_int=10)
        df_actual = stage._data_holder['cohort_target']
        df_actual_distinct = stage._data_holder['cohort_target'].distinct()
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
        stage = ExtractCVDPDataStage(None)
        stage._data_holder['cohort_target'] = df_input
        stage._limit_cohort(lim_int=15)
        df_actual_1 = stage._data_holder['cohort_target']

    with FunctionPatch('params',test_params):
        stage = ExtractCVDPDataStage(None)
        stage._data_holder['cohort_target'] = df_input
        stage._limit_cohort(lim_int=15)
        df_actual_2 = stage._data_holder['cohort_target']

    assert compare_results(df_actual_1, df_actual_2, join_columns = ['nhs_number']) == False

# COMMAND ----------

suite.run()

# COMMAND ----------

dbutils.notebook.exit('pass')