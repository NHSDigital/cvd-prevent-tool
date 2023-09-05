# Databricks notebook source
PARAMS_PATH = 'default'

# COMMAND ----------

# MAGIC %run ../../params/params

# COMMAND ----------

# MAGIC %run ../../src/test_helpers

# COMMAND ----------

# MAGIC %run ../../src/cvdp/preprocess_cvdp_journal

# COMMAND ----------

from datetime import date
from dsp.validation.validator import compare_results
import pyspark.sql.functions as F
import pyspark.sql.types as T

# COMMAND ----------

suite = FunctionTestSuite()

## The following are fake people and data created for test purposes

# COMMAND ----------

@suite.add_test
def test_cvdp_explode_journal_columns():
    
    test_fields_select = ['idx','PID','DOB']
    test_field_explode = 'EXP'
    test_field_explode_select = ['COND_01','COND_03']

    input_schema = T.StructType([
        T.StructField('idx', T.IntegerType(), False),
        T.StructField('PID', T.StringType(), False),
        T.StructField('DOB', T.DateType(), False),
        T.StructField('EXP', T.ArrayType(
            T.StructType([
                T.StructField('COND_01', T.StringType(), True),
                T.StructField('COND_02', T.StringType(), True),
                T.StructField('COND_03', T.StringType(), True)
            ])), False),
    ])

    expected_schema = T.StructType([
        T.StructField('idx', T.IntegerType(), False),
        T.StructField('PID', T.StringType(), False),
        T.StructField('DOB', T.DateType(), False),
        T.StructField('COND_01', T.StringType(), True),
        T.StructField('COND_03', T.StringType(), True),
    ])

    df_input = spark.createDataFrame([
        (0, 'A', date(2000,1,1), [{'COND_01':'A01', 'COND_02':'A02', 'COND_03':'A03'}]),
        (1, 'B', date(2000,1,2), [{'COND_01':None, 'COND_02':'B02', 'COND_03':'B03'}]),
        (2, 'C', date(2000,1,3), [{'COND_01':'C01', 'COND_02':None, 'COND_03':'C03'}]),
        (3, 'D', date(2000,1,4), [{'COND_01':'D01', 'COND_02':'D02', 'COND_03':None}]),
        (4, 'E', date(2000,1,5), [{'COND_01':None, 'COND_02':None, 'COND_03':None}]),
    ], input_schema)


    df_expected = spark.createDataFrame([
        (0, 'A', date(2000,1,1), 'A01', 'A03'),
        (1, 'B', date(2000,1,2), None, 'B03'),
        (2, 'C', date(2000,1,3), 'C01', 'C03'),
        (3, 'D', date(2000,1,4), 'D01', None),
        (4, 'E', date(2000,1,5), None, None),
    ], expected_schema)

    df_actual = cvdp_explode_journal_columns(
        df = df_input, 
        fields_select = test_fields_select,
        field_explode = test_field_explode,
        field_explode_select = test_field_explode_select
    )

    assert compare_results(df_actual, df_expected, join_columns = ['idx'])


# COMMAND ----------

@suite.add_test
def test_cvdp_create_journal_array():
    
    df_input = spark.createDataFrame([
        (0, 'A', 'cat', 'meow'),
        (1, 'B', 'dog', 'woof'),
        (2, 'C', 'frog', None)
    ], ['idx','group','animal','sound'])
    
    
    df_expected = spark.createDataFrame([
        (0, 'A', ['cat', 'meow']),
        (1, 'B', ['dog', 'woof']),
        (2, 'C', ['frog', None])
    ], ['idx','group','comb'])
    
    df_actual = cvdp_create_journal_array(
        df = df_input, 
        field_array_code = 'comb',
        field_array_values = ['animal','sound']
    )

    assert compare_results(df_actual, df_expected, join_columns = ['idx'])


# COMMAND ----------

@suite.add_test
def test_cvdp_join_ref_codes():
    
    df_input = spark.createDataFrame([
        (0,'001'),
        (1,'002'),
        (2,'004'),
        (3,'004'),
        (4,'005'),
    ], ['idx', 'code'])

    df_ref = spark.createDataFrame([
        ('001','A'),
        ('002','B'),
        ('003','C'),
        ('004','D')
    ], ['ref_code','cluster'])

    df_expected = spark.createDataFrame([
        (0,'001','A'),
        (1,'002','B'),
        (2,'004','D'),
        (3,'004','D'),
        (4,'005',None),
    ], ['idx', 'code', 'cluster'])

    def mock_load_ref_codes():
        return df_ref

    with FunctionPatch('load_ref_codes', mock_load_ref_codes):
        df_actual = join_ref_data(
            df              = df_input,
            join_field      = 'code',
            ref_join_field  = 'ref_code'
        )
        
        assert compare_results(df_actual, df_expected, join_columns = ['idx'])
        

# COMMAND ----------

@suite.add_test
def test_preprocess_cvdp_journal():
    '''test_preprocess_cvdp_journal
    
    Test for preprocess_cvdp_journal end-to-end, using a dataset containing multiple extracts per patient,
    with some journal date duplications (deduplicated to keep the most recent, latest extraction date, record).
    '''
    
    input_schema = T.StructType([
        T.StructField('idx', T.IntegerType(), False),
        T.StructField('PID', T.StringType(), False),
        T.StructField('DOB', T.DateType(), False),
        T.StructField('EXTRACT', T.DateType(), False),
        T.StructField('EXP', T.ArrayType(
            T.StructType([
                T.StructField('CODE', T.StringType(), True),
                T.StructField('JOURNAL_DATE', T.DateType(), True),
                T.StructField('COND_01', T.StringType(), True),
                T.StructField('COND_02', T.StringType(), True),
            ])), False),
    ])

    expected_schema = T.StructType([
        T.StructField('idx', T.IntegerType(), False),
        T.StructField('PID', T.StringType(), False),
        T.StructField('DOB', T.DateType(), False),
        T.StructField('EXTRACT', T.DateType(), False),
        T.StructField('CODE', T.StringType(), True),
        T.StructField('JOURNAL_DATE', T.DateType(), True),
        T.StructField('COND_ARRAY', T.ArrayType(T.StringType()), True),
        T.StructField('CLUSTER', T.StringType(), True),
    ])

    df_input = spark.createDataFrame([
        # Single Entry
        (0, 'A', date(2000,1,1), date(2010,1,1), [{'CODE':'001', 'JOURNAL_DATE': date(2000,1,1), 'COND_01': 'C1', 'COND_02': 'C2'}]),
        # Single Extract, Multiple Journal
        (1, 'B', date(2000,1,2), date(2010,1,1), [{'CODE':'001', 'JOURNAL_DATE': date(2000,1,1), 'COND_01': 'C1', 'COND_02': 'C2'}]),
        (2, 'B', date(2000,1,2), date(2010,1,1), [{'CODE':'001', 'JOURNAL_DATE': date(2001,1,2), 'COND_01': 'C1', 'COND_02': 'C2'}]),
        # Multiple Extract, Single Journal
        (3, 'C', date(2000,1,3), date(2010,1,1), [{'CODE':'001', 'JOURNAL_DATE': date(2000,1,1), 'COND_01': 'C1', 'COND_02': 'C2'}]),
        (4, 'C', date(2000,1,3), date(2010,1,2), [{'CODE':'001', 'JOURNAL_DATE': date(2000,1,1), 'COND_01': 'C1', 'COND_02': 'C2'}]),
        # Multiple Extracts, Multiple Journals
        (5, 'D', date(2000,1,4), date(2010,1,1), [{'CODE':'001', 'JOURNAL_DATE': date(2001,1,1), 'COND_01': 'C1', 'COND_02': 'C2'}]),
        (6, 'D', date(2000,1,4), date(2010,1,1), [{'CODE':'002', 'JOURNAL_DATE': date(2001,1,1), 'COND_01': 'C1', 'COND_02': 'C2'}]),
        (7, 'D', date(2000,1,4), date(2010,1,2), [{'CODE':'001', 'JOURNAL_DATE': date(2001,1,1), 'COND_01': 'C11', 'COND_02': 'C22'}]),
        (8, 'D', date(2000,1,4), date(2010,1,2), [{'CODE':'001', 'JOURNAL_DATE': date(2001,1,2), 'COND_01': 'C1', 'COND_02': 'C2'}]),
        (9, 'D', date(2000,1,4), date(2010,1,1), [{'CODE':'002', 'JOURNAL_DATE': date(2001,1,1), 'COND_01': 'C1', 'COND_02': 'C2'}]),
        (10, 'D', date(2000,1,4), date(2010,1,1), [{'CODE':'003', 'JOURNAL_DATE': date(2001,1,1), 'COND_01': 'C1', 'COND_02': 'C2'}]),
        # Single Extract, Single Journal, Different COND values
        (11, 'E', date(2000,1,5), date(2010,1,1), [{'CODE':'003', 'JOURNAL_DATE': date(2001,1,1), 'COND_01': 'C1', 'COND_02': 'C2'}]),
        (12, 'E', date(2000,1,5), date(2010,1,1), [{'CODE':'003', 'JOURNAL_DATE': date(2001,1,1), 'COND_01': 'C2', 'COND_02': 'C3'}]),
        # Single Extract, Single Journal, First Value == None
        (13, 'F', date(2000,1,5), date(2010,1,1), [{'CODE':'003', 'JOURNAL_DATE': date(2001,1,1), 'COND_01': None, 'COND_02': 'C2'}]),
        # Single Extract, Single Journal, Second Value == None
        (14, 'G', date(2000,1,5), date(2010,1,1), [{'CODE':'003', 'JOURNAL_DATE': date(2001,1,1), 'COND_01': 'C2', 'COND_02': None}]),
        # Single Extract, Single Journal, Both Values == None
        (15, 'H', date(2000,1,5), date(2010,1,1), [{'CODE':'003', 'JOURNAL_DATE': date(2001,1,1), 'COND_01': None, 'COND_02': None}]),
    ], input_schema)

    df_ref = spark.createDataFrame([
        ('001','A'),
        ('002','B'),
        ('003','C'),
        ('004','D'),
    ], ['REF_CODE','CLUSTER'])

    df_expected = spark.createDataFrame([
        # Single Entry
        (0, 'A', date(2000,1,1), date(2010,1,1), '001', date(2000,1,1), ['C1','C2'], 'A'),
        # Single Extract, Multiple Journal
        (1, 'B', date(2000,1,2), date(2010,1,1), '001', date(2000,1,1), ['C1','C2'], 'A'),
        (2, 'B', date(2000,1,2), date(2010,1,1), '001', date(2001,1,2), ['C1','C2'], 'A'),
        # Multiple Extract, Single Journal
        (4, 'C', date(2000,1,3), date(2010,1,2), '001', date(2000,1,1), ['C1','C2'], 'A'),
        # Multiple Extracts, Multiple Journals
        (6, 'D', date(2000,1,4), date(2010,1,1), '002', date(2001,1,1), ['C1','C2'], 'B'),
        (7, 'D', date(2000,1,4), date(2010,1,2), '001', date(2001,1,1), ['C11','C22'], 'A'),
        (8, 'D', date(2000,1,4), date(2010,1,2), '001', date(2001,1,2), ['C1','C2'], 'A'),
        (9, 'D', date(2000,1,4), date(2010,1,1), '002', date(2001,1,1), ['C1','C2'], 'B'),
        (10, 'D', date(2000,1,4), date(2010,1,1), '003', date(2001,1,1), ['C1','C2'], 'C'),
        # Single Extract, Single Journal, Different COND values
        (11, 'E', date(2000,1,5), date(2010,1,1), '003', date(2001,1,1), ['C1','C2'], 'C'),
        (12, 'E', date(2000,1,5), date(2010,1,1), '003', date(2001,1,1), ['C2','C3'], 'C'),
        # Single Extract, Single Journal, First Value == None
        (13, 'F', date(2000,1,5), date(2010,1,1), '003', date(2001,1,1), [None,'C2'], 'C'),
        # Single Extract, Single Journal, Second Value == None
        (14, 'G', date(2000,1,5), date(2010,1,1), '003', date(2001,1,1), ['C2',None], 'C'),
        # Single Extract, Single Journal, Both Values == None
        (15, 'H', date(2000,1,5), date(2010,1,1), '003', date(2001,1,1), [None,None], 'C'),
    ], expected_schema)

    def mock_load_ref_codes():
        return df_ref

    with FunctionPatch('load_ref_codes', mock_load_ref_codes):
        df_actual = preprocess_cvdp_journal(
            df = df_input,
            add_ref_data = True,
            field_extract_date = 'EXTRACT',
            fields_select = ['idx','PID','DOB','EXTRACT'],
            field_explode = 'EXP',
            field_explode_select = ['CODE','JOURNAL_DATE','COND_01','COND_02'],
            field_array_code = 'COND_ARRAY',
            field_array_values = ['COND_01','COND_02'],
            join_field = 'CODE',
            ref_join_field = 'REF_CODE',
            fields_window_partition = ['PID','DOB','CODE','JOURNAL_DATE']
        )

    assert compare_results(df_actual, df_expected, join_columns = ['idx'])

# COMMAND ----------

suite.run()

# COMMAND ----------

dbutils.notebook.exit('pass')
