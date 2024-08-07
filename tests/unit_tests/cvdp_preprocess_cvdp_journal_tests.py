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

    def mock_process_ref_codes():
        return df_ref

    with FunctionPatch('process_ref_codes', mock_process_ref_codes):
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
    Test also ensures that the primary key is generated correctly.
    '''

    input_schema = T.StructType([
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
        T.StructField('PK_HASH_COHORT', T.StringType(), False)
    ])

    expected_schema = T.StructType([
        T.StructField('PID', T.StringType(), False),
        T.StructField('DOB', T.DateType(), False),
        T.StructField('EXTRACT', T.DateType(), False),
        T.StructField('CODE', T.StringType(), True),
        T.StructField('JOURNAL_DATE', T.DateType(), True),
        T.StructField('COND_ARRAY', T.ArrayType(T.StringType()), True),
        T.StructField('CLUSTER', T.StringType(), True),
        T.StructField('PK_HASH_COHORT', T.StringType(), False),
        T.StructField('PK_HASH_JOURNAL', T.StringType(), False),
    ])

    df_input = spark.createDataFrame([
        # | PID | DOB | EXTRACT | EXP | PK_HASH_COHORT |
        # Single Entry
        ('A',date(2000,1,1),date(2010,1,1),[{'CODE':'001','JOURNAL_DATE':date(2000,1,1),'COND_01':'C1','COND_02':'C2'}],'A1'),
        # Single Extract, Multiple Journal
        ('B',date(2000,1,2),date(2010,1,1),[{'CODE':'001','JOURNAL_DATE':date(2000,1,1),'COND_01':'C1','COND_02':'C2'}],'B1'),
        ('B',date(2000,1,2),date(2010,1,1),[{'CODE':'001','JOURNAL_DATE':date(2001,1,2),'COND_01':'C1','COND_02':'C2'}],'B1'),
        # Multiple Extract, Single Journal
        ('C',date(2000,1,3),date(2010,1,1),[{'CODE':'001','JOURNAL_DATE':date(2000,1,1),'COND_01':'C1','COND_02':'C2'}],'C1'),
        ('C',date(2000,1,3),date(2010,1,2),[{'CODE':'001','JOURNAL_DATE':date(2000,1,1),'COND_01':'C1','COND_02':'C2'}],'C2'),
        # Multiple Extracts, Multiple Journals
        ('D',date(2000,1,4),date(2010,1,1),[{'CODE':'001','JOURNAL_DATE':date(2001,1,1),'COND_01':'C1','COND_02':'C2'}],'D1'),
        ('D',date(2000,1,4),date(2010,1,1),[{'CODE':'002','JOURNAL_DATE':date(2001,1,1),'COND_01':'C1','COND_02':'C2'}],'D1'),
        ('D',date(2000,1,4),date(2010,1,2),[{'CODE':'001','JOURNAL_DATE':date(2001,1,1),'COND_01':'C11','COND_02':'C22'}],'D2'),
        ('D',date(2000,1,4),date(2010,1,2),[{'CODE':'001','JOURNAL_DATE':date(2001,1,2),'COND_01':'C1','COND_02':'C2'}],'D2'),
        ('D',date(2000,1,4),date(2010,1,1),[{'CODE':'002','JOURNAL_DATE':date(2001,1,1),'COND_01':'C1','COND_02':'C2'}],'D1'),
        ('D',date(2000,1,4),date(2010,1,1),[{'CODE':'003','JOURNAL_DATE':date(2001,1,1),'COND_01':'C1','COND_02':'C2'}],'D1'),
        # Single Extract, Single Journal, Different COND values
        ('E',date(2000,1,5),date(2010,1,1),[{'CODE':'003','JOURNAL_DATE':date(2001,1,1),'COND_01':'C1','COND_02':'C2'}],'E1'),
        ('E',date(2000,1,5),date(2010,1,1),[{'CODE':'003','JOURNAL_DATE':date(2001,1,1),'COND_01':'C2','COND_02':'C3'}],'E1'),
        # Single Extract, Single Journal, First Value == None
        ('F',date(2000,1,5),date(2010,1,1),[{'CODE':'003','JOURNAL_DATE':date(2001,1,1),'COND_01':None,'COND_02':'C2'}],'F1'),
        # Single Extract, Single Journal, Second Value == None
        ('G',date(2000,1,5),date(2010,1,1),[{'CODE':'003','JOURNAL_DATE':date(2001,1,1),'COND_01':'C2','COND_02':None}],'G1'),
        # Single Extract, Single Journal, Both Values == None
        ('H',date(2000,1,5),date(2010,1,1),[{'CODE':'003','JOURNAL_DATE':date(2001,1,1),'COND_01':None,'COND_02':None}],'H1'),
    ], input_schema)

    df_ref = spark.createDataFrame([
        ('001','A'),
        ('002','B'),
        ('003','C'),
        ('004','D'),
    ], ['REF_CODE','CLUSTER'])

    df_expected = spark.createDataFrame([
        # | PID | DOB | EXTRACT | CODE | JOURNAL_DATE | COND_ARRAY | CLUSTER | PK_HASH_COHORT | PK_HASH_JOURNAL |
        # Single Entry
        ('A',date(2000,1,1),date(2010,1,1),'001',date(2000,1,1),['C1','C2'],'A','A1','e9f6a7e9089f66b8a3bb7bf9bcb837259a6d47ce5371c523a4427ffaec5a00a3'),
        # Single Extract, Multiple Journal
        ('B',date(2000,1,2),date(2010,1,1),'001',date(2000,1,1),['C1','C2'],'A','B1','0d923240c13e79d5c4f81f93698a40965df5175369de3bfdb7e6832ae4db2cf5'),
        ('B',date(2000,1,2),date(2010,1,1),'001',date(2001,1,2),['C1','C2'],'A','B1','d55f753e92cefa5b1fbea90fd9cc90499186d0a14cb1fd92b9aec2077998a39a'),
        # Multiple Extract, Single Journal
        ('C',date(2000,1,3),date(2010,1,2),'001',date(2000,1,1),['C1','C2'],'A','C2','e34016fe9a28fa13e63627652193fc820d1e5204485aca5069fa5a69491eab36'),
        # Multiple Extracts, Multiple Journals
        ('D',date(2000,1,4),date(2010,1,1),'001',date(2001,1,1),['C1','C2'],'A','D1','a850c0fbdfc1ae9d20d514d2424147d1f13f20dbff184ef5fc5f7bc742626d56'),
        ('D',date(2000,1,4),date(2010,1,1),'002',date(2001,1,1),['C1','C2'],'B','D1','75a7af153b79ca9311336cb59fac998e9df7b437d824bf7b99a9ed3bca3c4623'),
        ('D',date(2000,1,4),date(2010,1,2),'001',date(2001,1,1),['C11','C22'],'A','D2','e34274a34af22488b25da1ca4d6570facb57bf0254202d24f84c2b6418716bfb'),
        ('D',date(2000,1,4),date(2010,1,2),'001',date(2001,1,2),['C1','C2'],'A','D2','6915eaea2f9c1897dc9a7ce63d793e1e946152583c98616c06020eb9d30cc12b'),
        ('D',date(2000,1,4),date(2010,1,1),'003',date(2001,1,1),['C1','C2'],'C','D1','9521ca922814224a0d6a29fcdde47b5b7394988ccb72faced9c5a69d8cd5bbbd'),
        # Single Extract, Single Journal, Different COND values
        ('E',date(2000,1,5),date(2010,1,1),'003',date(2001,1,1),['C1','C2'],'C','E1','c92062a60a266fe5ce4bd089ff56aee628d3355fe64336163ff68fcb3cb654a4'),
        ('E',date(2000,1,5),date(2010,1,1),'003',date(2001,1,1),['C2','C3'],'C','E1','4d82d93804e53947a5aaf1fdcb3843f9cce964c8c4a42d130fc19847aa9b864c'),
        # Single Extract, Single Journal, First Value == None
        ('F',date(2000,1,5),date(2010,1,1),'003',date(2001,1,1),[None,'C2'],'C','F1','4c06c961603524e03a59154a3300f427b96a68a745fac699920670a81c949849'),
        # Single Extract, Single Journal, Second Value == None
        ('G',date(2000,1,5),date(2010,1,1),'003',date(2001,1,1),['C2',None],'C','G1','368e9b7463ad5ecd31289f730cc14c7282548d297724382897bf6cfc2a323ee4'),
        # Single Extract, Single Journal, Both Values == None
        ('H',date(2000,1,5),date(2010,1,1),'003',date(2001,1,1),[None,None],'C','H1','8637e3076900eb7ea9bb7e73ea7a6f4775c774eec29dc485d5b49fc326fde318'),
    ], expected_schema)

    def mock_process_ref_codes():
        return df_ref

    with FunctionPatch('process_ref_codes', mock_process_ref_codes):
        df_actual = preprocess_cvdp_journal(
            df = df_input,
            add_ref_data = True,
            field_extract_date = 'EXTRACT',
            fields_select = ['PID','DOB','EXTRACT','PK_HASH_COHORT'],
            field_explode = 'EXP',
            field_explode_select = ['CODE','JOURNAL_DATE','COND_01','COND_02'],
            field_array_code = 'COND_ARRAY',
            field_array_values = ['COND_01','COND_02'],
            join_field = 'CODE',
            ref_join_field = 'REF_CODE',
            fields_window_partition = ['PID','DOB','CODE','COND_ARRAY','JOURNAL_DATE'],
            col_hashed_key = 'PK_HASH_JOURNAL',
            fields_to_hash = ['EXTRACT','CODE','JOURNAL_DATE','COND_ARRAY','CLUSTER','PK_HASH_COHORT'],
        )

    assert compare_results(df_actual, df_expected, join_columns = ['PK_HASH_JOURNAL'])

# COMMAND ----------

@suite.add_test
def test_process_ref_codes():
  df_ref = spark.createDataFrame([
        (0, '001', 'A', 1),
        (1, '002', 'B', 0),
        (2, '002', 'C', 1),
        (3, '004', 'D', 1),
        (4, '005', 'D', 0),
    ], ['id_ref', 'ConceptId', 'Cluster_ID', 'active'])
  
  df_expected = spark.createDataFrame([
        (0, '001', 'A'),
        (2, '002', 'C'),
        (3, '004', 'D'),
    ], ['id_ref', 'ConceptId', 'Cluster_ID'])
  
  def mock_read_ref_codes(db, table):
      return df_ref

  with FunctionPatch('read_table', mock_read_ref_codes):
    df_actual = process_ref_codes(active_in_refset = 'active', output_fields = ['id_ref', 'ConceptId', 'Cluster_ID'])
  
  assert compare_results(df_actual, df_expected, join_columns = ['id_ref'])

# COMMAND ----------

suite.run()

# COMMAND ----------

dbutils.notebook.exit('pass')