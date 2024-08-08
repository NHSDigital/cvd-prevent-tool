# Databricks notebook source
PARAMS_PATH = 'default'

# COMMAND ----------

# MAGIC %run ../../params/params

# COMMAND ----------

# MAGIC %run ../../src/test_helpers

# COMMAND ----------

# MAGIC %run ../../pipeline/extract_hes_stage

# COMMAND ----------

import pyspark.sql.types as T

from datetime import date
from dsp.validation.validator import compare_results

# COMMAND ----------

suite = FunctionTestSuite()

## The following are fake people and data created for test purposes

# COMMAND ----------

@suite.add_test
def test_check_unique_identifiers_pass_hes_apc():
    """test_check_unique_identifiers_pass_hes_apc
    pipeline/extract_hes_stage::_check_unique_identifiers

    Test of unique identifier check, specific for HES APC. Test passes if only unique values
    are found.
    """

    # Setup Test Parameters
    @dataclass(frozen = True)
    class TestParams(ParamsBase):
        HES_S_APC_LINK_KEY = 'test_key_apc'
    test_params = TestParams()

    # Input Data
    df_input = spark.createDataFrame([
        (1,'a1'),
        (2,'a2'),
        (3,'a3')
    ], ['idx', 'test_key_apc'])

    # Stage Test
    with FunctionPatch('params',test_params):
        stage = ExtractHESDataStage(None,None,None,None)
        stage._data_holder['hes_apc'] = df_input
        try:
            stage._check_unique_identifiers()
            result = True
        except Exception as e:
            result = False

    # Assertion
    assert result == True

# COMMAND ----------

@suite.add_test
def test_check_unique_identifiers_fail_hes_apc():
    """test_check_unique_identifiers_fail_hes_apc
    pipeline/extract_hes_stage::_check_unique_identifiers

    Test of unique identifier check, specific for HES APC. Test passes if non-unique values
    are found.
    """

    # Setup Test Parameters
    @dataclass(frozen = True)
    class TestParams(ParamsBase):
        HES_S_APC_LINK_KEY = 'test_key_apc'
    test_params = TestParams()

    # Input Data
    df_input = spark.createDataFrame([
        (1,'a1'),
        (2,'a2'),
        (3,'a2')
    ], ['idx', 'test_key_apc'])

    # Stage Test
    with FunctionPatch('params',test_params):
        stage = ExtractHESDataStage(None,None,None,None)
        stage._data_holder['hes_apc'] = df_input
        try:
            stage._check_unique_identifiers()
            result = False
        except:
            result = True

    # Assertion
    assert result == True

# COMMAND ----------

@suite.add_test
def test_check_unique_identifiers_pass_hes_ae():
    """test_check_unique_identifiers_pass_hes_ae
    pipeline/extract_hes_stage::_check_unique_identifiers

    Test of unique identifier check, specific for HES AE. Test passes if only unique values
    are found.
    """

    # Setup Test Parameters
    @dataclass(frozen = True)
    class TestParams(ParamsBase):
        HES_S_AE_LINK_KEY = 'test_key_ae'
    test_params = TestParams()

    # Input Data
    df_input = spark.createDataFrame([
        (1,'a1'),
        (2,'a2'),
        (3,'a3')
    ], ['idx', 'test_key_ae'])

    # Stage Test
    with FunctionPatch('params',test_params):
        stage = ExtractHESDataStage(None,None,None,None)
        stage._data_holder['hes_ae'] = df_input
        try:
            stage._check_unique_identifiers()
            result = True
        except Exception as e:
            result = False

    # Assertion
    result == True

# COMMAND ----------

@suite.add_test
def test_check_unique_identifiers_fail_hes_ae():
    """test_check_unique_identifiers_fail_hes_ae
    pipeline/extract_hes_stage::_check_unique_identifiers

    Test of unique identifier check, specific for HES AE. Test passes if non-unique values
    are found.
    """

    # Setup Test Parameters
    @dataclass(frozen = True)
    class TestParams(ParamsBase):
        HES_S_AE_LINK_KEY = 'test_key_ae'
    test_params = TestParams()

    # Input Data
    df_input = spark.createDataFrame([
        (1,'a1'),
        (2,'a2'),
        (3,'a2')
    ], ['idx', 'test_key_ae'])

    # Stage Test
    with FunctionPatch('params',test_params):
        stage = ExtractHESDataStage(None,None,None,None)
        stage._data_holder['hes_ae'] = df_input
        try:
            stage._check_unique_identifiers()
            result = False
        except:
            result = True

    # Assertion
    assert result == True

# COMMAND ----------

@suite.add_test
def test_check_unique_identifiers_pass_hes_op():
    """test_check_unique_identifiers_pass_hes_op
    pipeline/extract_hes_stage::_check_unique_identifiers

    Test of unique identifier check, specific for HES OP. Test passes if only unique values
    are found.
    """

    # Setup Test Parameters
    @dataclass(frozen = True)
    class TestParams(ParamsBase):
        HES_S_OP_LINK_KEY = 'test_key_op'
    test_params = TestParams()

    # Input Data
    df_input = spark.createDataFrame([
        (1,'a1'),
        (2,'a2'),
        (3,'a3')
    ], ['idx', 'test_key_op'])

    # Stage Test
    with FunctionPatch('params',test_params):
        stage = ExtractHESDataStage(None,None,None,None)
        stage._data_holder['hes_op'] = df_input
        try:
            stage._check_unique_identifiers()
            result =  True
        except Exception as e:
            result = False

    # Assertion
    assert result == True

# COMMAND ----------

@suite.add_test
def test_check_unique_identifiers_fail_hes_op():
    """test_check_unique_identifiers_fail_hes_op
    pipeline/extract_hes_stage::_check_unique_identifiers

    Test of unique identifier check, specific for HES OP. Test passes if non-unique values
    are found.
    """

    # Setup Test Parameters
    @dataclass(frozen = True)
    class TestParams(ParamsBase):
        HES_S_OP_LINK_KEY = 'test_key_op'
    test_params = TestParams()

    # Input Data
    df_input = spark.createDataFrame([
        (1,'a1'),
        (2,'a2'),
        (3,'a2')
    ], ['idx', 'test_key_op'])

    # Stage Test
    with FunctionPatch('params',test_params):
        stage = ExtractHESDataStage(None,None,None,None)
        stage._data_holder['hes_op'] = df_input
        try:
            stage._check_unique_identifiers()
            result = False
        except:
            result = True

    # Assertion
    assert result == True

# COMMAND ----------

@suite.add_test
def test_check_unique_identifiers_pass_all():
    """test_check_unique_identifiers_pass_all
    pipeline/extract_hes_stage::_check_unique_identifiers

    Test of unique identifier check, for HES APC, AE and OP. Test passes if unique values (only) are
    found in all HES datasets.
    """

    # Setup Test Parameters
    @dataclass(frozen = True)
    class TestParams(ParamsBase):
        HES_S_APC_LINK_KEY = 'test_key_apc'
        HES_S_AE_LINK_KEY = 'test_key_ae'
        HES_S_OP_LINK_KEY = 'test_key_op'
    test_params = TestParams()

    # Input Data
    df_input_apc = spark.createDataFrame([
        (1,'a1'),
        (2,'a2'),
        (3,'a3')
    ], ['idx', 'test_key_apc'])

    df_input_ae = spark.createDataFrame([
        (1,'a1'),
        (2,'a2'),
        (3,'a3')
    ], ['idx', 'test_key_ae'])

    df_input_op = spark.createDataFrame([
        (1,'a1'),
        (2,'a2'),
        (3,'a3')
    ], ['idx', 'test_key_op'])

    # Stage Test
    with FunctionPatch('params',test_params):
        stage = ExtractHESDataStage(None,None,None,None)
        stage._data_holder['hes_apc'] = df_input_apc
        stage._data_holder['hes_ae'] = df_input_ae
        stage._data_holder['hes_op'] = df_input_op
        try:
            stage._check_unique_identifiers()
            result = True
        except Exception as e:
            result = False

    # Assertion
    assert result == True

# COMMAND ----------

@suite.add_test
def test_check_unique_identifiers_fail_one():
    """test_check_unique_identifiers_fail_one
    pipeline/extract_hes_stage::_check_unique_identifiers

    Test of unique identifier check, for HES APC, AE and OP where one table contains
    non-unique values. Test passes if non-unique values are found in one table and
    exception is raised.
    """

    # Setup Test Parameters
    @dataclass(frozen = True)
    class TestParams(ParamsBase):
        HES_S_APC_LINK_KEY = 'test_key_apc'
        HES_S_AE_LINK_KEY = 'test_key_ae'
        HES_S_OP_LINK_KEY = 'test_key_op'
    test_params = TestParams()

    # Input Data
    df_input_apc = spark.createDataFrame([
        (1,'a1'),
        (2,'a2'),
        (3,'a3')
    ], ['idx', 'test_key_apc'])

    df_input_ae = spark.createDataFrame([
        (1,'a1'),
        (2,'a2'),
        (3,'a3')
    ], ['idx', 'test_key_ae'])

    df_input_op = spark.createDataFrame([
        (1,'a1'),
        (2,'a2'),
        (3,'a2')
    ], ['idx', 'test_key_op'])

    # Stage Test
    with FunctionPatch('params',test_params):
        stage = ExtractHESDataStage(None,None,None,None)
        stage._data_holder['hes_apc'] = df_input_apc
        stage._data_holder['hes_ae'] = df_input_ae
        stage._data_holder['hes_op'] = df_input_op
        try:
            stage._check_unique_identifiers()
            result = False
        except Exception as e:
            result = True

    # Assertion
    assert result == True

# COMMAND ----------

@suite.add_test
def test_filter_hes_data():
    """test_filter_hes_data
    pipeline/extract_hes_stage::_filter_hes_data()
    Test of the HES filtering method using the extracted CVDP data. Test passes if
    only filtered records (NHS Number and Date of Birth) are present in the dataframe
    output.
    """
    # Setup Test Parameters
    @dataclass(frozen = True)
    class TestParams(ParamsBase):
        CVDP_PID_FIELD      = "cvd_pid"
        CVDP_DOB_FIELD      = "cvd_dob"
        HES_S_APC_PID_FIELD = "apc_pid"
        HES_S_APC_DOB_FIELD = "apc_dob"
        HES_S_AE_PID_FIELD  = "ae_pid"
        HES_S_AE_DOB_FIELD  = "ae_dob"
        HES_S_OP_PID_FIELD  = "op_pid"
        HES_S_OP_DOB_FIELD  = "op_dob"
    test_params = TestParams()

    # Input Data
    ## HES APC
    df_input_apc = spark.createDataFrame([
        (1,"A01",date(2020,1,1),"FOO_1"),
        (2,"B01",date(2020,1,2),"FOO_2"),
        (3,"C02",date(2020,1,3),"FOO_3")
    ], ["idx","apc_pid","apc_dob","bar_apc"])
    ## HES AE
    df_input_ae = spark.createDataFrame([
        (1,"A01",date(2020,1,1),"FOO_4"),
        (2,"B01",date(2020,1,2),"FOO_5"),
        (3,"D01",date(2020,1,5),"FOO_6")
    ], ["idx","ae_pid","ae_dob","bar_ae"])
    ## HES OP
    df_input_op = spark.createDataFrame([
        (1,"B01",date(2020,1,1),"FOO_7"),
        (2,"D01",date(2020,1,4),"FOO_8"),
        (3,"E01",date(2020,1,5),"FOO_9")
    ], ["idx","op_pid","op_dob","bar_op"])
    ## Cohort Table
    df_input_cohort = spark.createDataFrame([
        (1,"A01",date(2020,1,1)),
        (2,"B01",date(2020,1,2)),
        (3,"C01",date(2020,1,3)),
        (4,"D01",date(2020,1,4)),
    ], ["idx","cvd_pid","cvd_dob"])

    # Expected Data
    ## HES APC
    df_expected_apc = spark.createDataFrame([
        (1,"A01",date(2020,1,1),"FOO_1"),
        (2,"B01",date(2020,1,2),"FOO_2"),
    ], ["idx","apc_pid","apc_dob","bar_apc"])
    ## HES AE
    df_expected_ae = spark.createDataFrame([
        (1,"A01",date(2020,1,1),"FOO_4"),
        (2,"B01",date(2020,1,2),"FOO_5"),
    ], ["idx","ae_pid","ae_dob","bar_ae"])
    ## HES OP
    df_expected_op = spark.createDataFrame([
        (2,"D01",date(2020,1,4),"FOO_8")
    ], ["idx","op_pid","op_dob","bar_op"])

    # Stage Test
    with FunctionPatch("params",test_params):
        stage = ExtractHESDataStage(None,None,None,None)
        stage._data_holder["hes_apc"]   = df_input_apc
        stage._data_holder["hes_ae"]    = df_input_ae
        stage._data_holder["hes_op"]    = df_input_op
        stage._data_holder["cvdp"]      = df_input_cohort
        stage._filter_hes_data()
        df_actual_apc = stage._data_holder["hes_apc"]
        df_actual_ae = stage._data_holder["hes_ae"]
        df_actual_op = stage._data_holder["hes_op"]

    # Assertion
    assert compare_results(df_actual_apc, df_expected_apc, join_columns = ['idx'])
    assert compare_results(df_actual_ae, df_expected_ae, join_columns = ['idx'])
    assert compare_results(df_actual_op, df_expected_op, join_columns = ['idx'])

# COMMAND ----------

suite.run()

# COMMAND ----------

dbutils.notebook.exit('pass')
