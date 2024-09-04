# Databricks notebook source
# MAGIC %run ../../src/test_helpers

# COMMAND ----------

PARAMS_PATH = 'default'

# COMMAND ----------

# MAGIC %run ../../params/params

# COMMAND ----------

# MAGIC %run ../../src/cvdp/preprocess_cvdp_htn

# COMMAND ----------

from dsp.validation.validator import compare_results
from datetime import date
from uuid import uuid4
from datetime import datetime

import pyspark.sql.functions as F
import pyspark.sql.types as T

# COMMAND ----------

suite = FunctionTestSuite()

## The following are fake people and data created for test purposes

# COMMAND ----------

@suite.add_test
def test_split_and_process_htn_entries_single():
    '''
    preprocess_cvdp_htn::split_and_process_htn_entries_single()
    Test takes a cvdp journal-style dataframe and returns only those that match the single-bp-reading criteria
    of seperate systolic and diastolic readings in differing records.
    '''

    # BP SNOMED CODES
    codes_snomed_test_systolic = ['S1']
    codes_snomed_test_diastolic = ['D1']

    # TEST DATA SCHEMAS
    schema_input = T.StructType([
        T.StructField('idx', T.IntegerType(), False),
        T.StructField('code', T.StringType(), True),
        T.StructField('value',T.ArrayType(
            T.StringType(), True
        ))
    ])

    schema_expected = T.StructType([
        T.StructField('idx', T.IntegerType(), False),
        T.StructField('code', T.StringType(), True),
        T.StructField('value',T.ArrayType(
            T.StringType(), True
        )),
        T.StructField('bp_sys', T.StringType(), True),
        T.StructField('bp_dia', T.StringType(), True),
    ])

    # TEST DATAFRAMES
    df_input = spark.createDataFrame([
        (0, 'S1', ['001', None]),   # (KEEP) Code = Valid, Value = Valid
        (1, 'D1', ['002', None]),   # (KEEP) Code = Valid, Value = Valid
        (2, 'S1', [None, None]),    # (DROP) Code = Valid, Value = Invalid
        (3, 'S1', ['001','002']),   # (DROP) Code = Valid, Value = Invalid
        (4, 'M1', ['001', None]),   # (DROP) Code = Invalid, Value = Valid
        (5, None, ['001', None]),   # (DROP) Code = Invalid, Value = Valid
        (6, 'S1', [None, '001']),   # (DROP) Code = Valid, Value = Invalid
    ], schema_input)


    df_expected = spark.createDataFrame([
        (0, 'S1', ['001', None], '001', None),
        (1, 'D1', ['002', None], None, '002'),
    ], schema_expected)

    # ACTUAL DATAFRAME
    df_actual = split_and_process_htn_entries_single(
        df = df_input,
        codes_bp_diastolic = codes_snomed_test_diastolic,
        codes_bp_systolic = codes_snomed_test_systolic,
        field_values_array = 'value',
        field_snomed_code = 'code',
        col_systolic = 'bp_sys',
        col_diastolic = 'bp_dia',
    )

    assert compare_results(df_actual, df_expected, join_columns = ['idx'])

# COMMAND ----------

@suite.add_test
def test_split_and_process_htn_entries_multi():
    '''
    preprocess_cvdp_htn::split_and_process_htn_entries_multi()
    Test takes a cvdp journal-style dataframe and returns only those that match the multi-bp-reading criteria
    of joint systolic and diastolic readings in single records.
    '''

    # BP CLUSTER CODES
    codes_cluster_test = ['bp']

    # TEST DATA SCHEMAS
    schema_input = T.StructType([
        T.StructField('idx', T.IntegerType(), False),
        T.StructField('code', T.StringType(), True),
        T.StructField('value',T.ArrayType(
            T.StringType(), True
        ))
    ])

    schema_expected = T.StructType([
        T.StructField('idx', T.IntegerType(), False),
        T.StructField('code', T.StringType(), True),
        T.StructField('value',T.ArrayType(
            T.StringType(), True
        )),
        T.StructField('bp_sys', T.StringType(), True),
        T.StructField('bp_dia', T.StringType(), True),
    ])

    # TEST DATAFRAMES
    df_input = spark.createDataFrame([
        (0, 'bp', ['001','002']),  # (KEEP) Code = Valid; Readings = Valid
        (1, 'bp', [None,'002']),   # (DROP) Code = Valid; Readings = Incomplete
        (2, 'bp', ['001',None]),   # (DROP) Code = Valid; Readings = Incomplete
        (3, 'bp', [None,None]),    # (DROP) Code = Valid; Readings = Null
        (4, 'S1', ['001','002']),  # (DROP) Code = Invalid; Readings = Valid
        (5, 'S1', [None,None]),    # (DROP) Code = Invalid; Readings = Null
        (6, None, ['001','002']),  # (DROP) Code = Null; Readings = Valid
        (7, None, [None,None]),    # (DROP) Code = Null; Readings = Null
        ], schema_input)


    df_expected = spark.createDataFrame([
        (0, 'bp', ['001','002'],'001','002'),
        #(3, 'bp', [None,None],None,None),
    ], schema_expected)

    # ACTUAL DATAFRAME
    df_actual = split_and_process_htn_entries_multi(
        df = df_input,
        codes_bp_clusters = codes_cluster_test,
        field_values_array = 'value',
        field_cluster_id = 'code',
        col_systolic = 'bp_sys',
        col_diastolic = 'bp_dia',
    )

    # ASSERTION
    assert compare_results(df_actual, df_expected, join_columns = ['idx'])

# COMMAND ----------

@suite.add_test
def test_union_bp_frames():
    '''
    preprocess_cvdp_htn::union_bp_frames()
    Testing the union of two dataframes with identical columns
    '''

    df_input_1 = spark.createDataFrame([
        (0,'A','1'),
    ], ['idx','key','value'])

    df_input_2 = spark.createDataFrame([
        (1,'B','2'),
    ], ['idx','key','value'])


    df_expected = spark.createDataFrame([
        (0,'A','1'),
        (1,'B','2'),
    ], ['idx','key','value'])

    df_actual = union_bp_frames(
        df_single = df_input_1,
        df_multi = df_input_2
    )

    assert compare_results(df_actual, df_expected, join_columns = ['idx'])


# COMMAND ----------

@suite.add_test
def test_filter_bp_frames():
    '''
    preprocess_cvdp_htn::filter_bp_frames()
    Test takes a dataframe of test single and multiple blood pressure readings, filters to remove any records that
    contain invalid ages (below minimum, above maximum) and invalid diastolic or systolic BP readings (below minimum)
    '''

    # Test variables
    test_min_age = 5
    test_max_age = 50
    test_bp_sys_min = 80
    test_bp_dia_min = 60

    # Test schemas
    schema_input = T.StructType([
        T.StructField('idx',T.IntegerType(),False),
        T.StructField('pid',T.StringType(),True),
        T.StructField('dob',T.DateType(),True),
        T.StructField('event_date',T.DateType(),True),
        T.StructField('bp_sys',T.LongType(),True),
        T.StructField('bp_dia',T.LongType(),True)
    ])

    schema_expected = T.StructType([
        T.StructField('idx',T.IntegerType(),False),
        T.StructField('pid',T.StringType(),True),
        T.StructField('dob',T.DateType(),True),
        T.StructField('event_date',T.DateType(),True),
        T.StructField('bp_sys',T.LongType(),True),
        T.StructField('bp_dia',T.LongType(),True),
        T.StructField('age',T.LongType(),False),
    ])

    # Test dataframes
    df_input = spark.createDataFrame([
        (0,'001',date(2000,1,1),date(2020,1,1),100,80),     # Valid         (all)
        (1,'002',date(2000,1,1),date(2002,1,1),100,80),     # Invalid age   (min)
        (2,'003',date(1960,1,1),date(2020,1,1),100,80),     # Invalid age   (max)
        (3,'004',date(2000,1,1),date(2020,1,1),75,80),      # Invalid bp    (systolic)
        (4,'005',date(2000,1,1),date(2020,1,1),100,45),     # Invalid bp    (diastolic)
        (5,'006',date(2000,1,1),date(2020,1,1),75,45),      # Invalid bp    (both)
        (5,'007',date(2000,1,1),None,100,80),               # Invalid age   (None)
        (6,'008',date(2000,1,1),date(2020,1,1),None,80),    # Valid allowing for split measuremnts
        (7,'009',date(2000,1,1),date(2020,1,1),100,None),   # Valid allowing for split measuremnts
        (8,'010',date(2000,1,1),date(2020,1,1),None,None),  # Invalid bp    (None - both)
    ],schema_input)

    df_expected = spark.createDataFrame([
        (0,'001',date(2000,1,1),date(2020,1,1),100,80,20),
        (6,'008',date(2000,1,1),date(2020,1,1),None,80,20),
        (7,'009',date(2000,1,1),date(2020,1,1),100,None,20),
    ],schema_expected)

    df_actual = filter_bp_frames(
        df = df_input,
        age_min_value = test_min_age,
        age_max_value = test_max_age,
        bp_min_diastolic = test_bp_dia_min,
        bp_min_systolic = test_bp_sys_min,
        col_age_at_event = 'age',
        field_bp_diastolic = 'bp_dia',
        field_bp_systolic = 'bp_sys',
        field_calculation_date = 'event_date',
        field_person_dob = 'dob',
    )

    assert compare_results(df_actual, df_expected, join_columns = ['idx'])


# COMMAND ----------

@suite.add_test
def test_calculate_min_bp_readings():
    '''
    preprocess_cvdp_htn::calculate_min_bp_readings()
    Test takes the processed blood pressure measurement dataset (extracted, valid values) and calculates
    the minimum systolic and minimum diastolic for each journal date, for each extract date.
    '''

    df_input = spark.createDataFrame([
        # Single SYS and DIA
        ('N1','001',None,date(2000,1,1),date(2010,1,1),date(2022,1,1)),
        ('N1',None,'002',date(2000,1,1),date(2010,1,1),date(2022,1,1)),
        # Multiple SYS and DIA - Same Journal Date
        ('N2','001',None,date(2000,1,1),date(2010,1,1),date(2022,1,1)),
        ('N2',None,'002',date(2000,1,1),date(2010,1,1),date(2022,1,1)),
        ('N2','011',None,date(2000,1,1),date(2010,1,1),date(2022,1,1)),
        ('N2',None,'012',date(2000,1,1),date(2010,1,1),date(2022,1,1)),
        # Multiple SYS and DIA - Different Extract Dates, Same Journal Date
        ('N3','001',None,date(2000,1,1),date(2010,1,1),date(2022,1,1)),
        ('N3',None,'002',date(2000,1,1),date(2010,1,1),date(2022,1,1)),
        ('N3','011',None,date(2000,1,1),date(2010,1,1),date(2022,1,1)),
        ('N3',None,'012',date(2000,1,1),date(2010,1,1),date(2022,1,1)),
        ('N3','101',None,date(2000,1,1),date(2010,1,1),date(2023,1,1)),
        ('N3',None,'102',date(2000,1,1),date(2010,1,1),date(2023,1,1)),
        ('N3','111',None,date(2000,1,1),date(2010,1,1),date(2023,1,1)),
        ('N3',None,'112',date(2000,1,1),date(2010,1,1),date(2023,1,1)),
        # Multiple SYS and DIA - Different Extract Dates, Different Journal Dates
        ('N4','001',None,date(2000,1,1),date(2010,1,1),date(2022,1,1)),
        ('N4',None,'002',date(2000,1,1),date(2010,1,1),date(2022,1,1)),
        ('N4','011',None,date(2000,1,1),date(2010,1,1),date(2022,1,1)),
        ('N4',None,'012',date(2000,1,1),date(2010,1,1),date(2022,1,1)),
        ('N4','101',None,date(2000,1,1),date(2010,1,1),date(2023,1,1)),
        ('N4',None,'102',date(2000,1,1),date(2010,1,1),date(2023,1,1)),
        ('N4','111',None,date(2000,1,1),date(2010,1,1),date(2023,1,1)),
        ('N4',None,'112',date(2000,1,1),date(2010,1,1),date(2023,1,1)),
        ('N4','001',None,date(2000,1,1),date(2011,1,1),date(2022,1,1)),
        ('N4',None,'002',date(2000,1,1),date(2011,1,1),date(2022,1,1)),
        ('N4','011',None,date(2000,1,1),date(2011,1,1),date(2022,1,1)),
        ('N4',None,'012',date(2000,1,1),date(2011,1,1),date(2022,1,1)),
        ('N4','101',None,date(2000,1,1),date(2011,1,1),date(2023,1,1)),
        ('N4',None,'102',date(2000,1,1),date(2011,1,1),date(2023,1,1)),
        ('N4','111',None,date(2000,1,1),date(2011,1,1),date(2023,1,1)),
        ('N4',None,'112',date(2000,1,1),date(2011,1,1),date(2023,1,1)),
        # Combination of single and multiple readings
        ('N5','001',None,date(2000,1,1),date(2010,1,1),date(2022,1,1)),
        ('N5',None,'202',date(2000,1,1),date(2010,1,1),date(2022,1,1)),
        ('N5','101','002',date(2000,1,1),date(2010,1,1),date(2022,1,1)),
        # Single SYS and DIA - duplicated entry
        ('N6','001',None,date(2000,1,1),date(2010,1,1),date(2022,1,1)),
        ('N6','001',None,date(2000,1,1),date(2010,1,1),date(2022,1,1)),
        ('N6',None,'002',date(2000,1,1),date(2010,1,1),date(2022,1,1)),
        # No SYS and DIA - duplicated entry
        ('N7','001',None,date(2000,1,1),date(2010,1,1),date(2022,1,1)),
        ('N7','001',None,date(2000,1,1),date(2010,1,1),date(2022,1,1)),
    ], ['pid','sys','dia','dob','journal','extract'])


    df_expected = spark.createDataFrame([
        ('N1','001','002',date(2000,1,1),date(2010,1,1),date(2022,1,1)),
        ('N2','001','002',date(2000,1,1),date(2010,1,1),date(2022,1,1)),
        ('N3','001','002',date(2000,1,1),date(2010,1,1),date(2022,1,1)),
        ('N3','101','102',date(2000,1,1),date(2010,1,1),date(2023,1,1)),
        ('N4','001','002',date(2000,1,1),date(2010,1,1),date(2022,1,1)),
        ('N4','101','102',date(2000,1,1),date(2010,1,1),date(2023,1,1)),
        ('N4','001','002',date(2000,1,1),date(2011,1,1),date(2022,1,1)),
        ('N4','101','102',date(2000,1,1),date(2011,1,1),date(2023,1,1)),
        ('N5','001','002',date(2000,1,1),date(2010,1,1),date(2022,1,1)),
        ('N6','001','002',date(2000,1,1),date(2010,1,1),date(2022,1,1)),
    ], ['pid','sys','dia','dob','journal','extract'])

    df_actual = calculate_min_bp_readings(
        df = df_input,
        field_person_id = 'pid',
        field_person_dob = 'dob',
        field_date_extract = 'extract',
        field_date_journal = 'journal',
        field_bp_systolic = 'sys',
        field_bp_diastolic = 'dia',
    )

    assert compare_results(df_actual, df_expected, join_columns = ['pid','dob','journal','extract'])


# COMMAND ----------

@suite.add_test
def test_keep_latest_journal_updates():
    '''
    preprocess_cvdp_htn::keep_latest_journal_updates()
    Test takes the processed and minimised blood pressure measurements and keeps the BP record with the latest extract
    date, where multiple minimums are present on the same journal date entry.
    '''

    df_input = spark.createDataFrame([
        ('N1','BP','001','002',date(2000,1,1),date(2010,1,1),date(2022,1,1)), # (KEEP) Single Record
        ('N2','BP','001','002',date(2000,1,2),date(2010,1,1),date(2023,1,1)), # (KEEP) Latest Record
        ('N2','BP','011','012',date(2000,1,2),date(2010,1,1),date(2022,1,1)), # (DROP) Earlier Record
        ('N3','BP','001','002',date(2000,1,3),date(2010,1,1),date(2023,1,1)), # (KEEP) Latest Record
        ('N3','BP','011','012',date(2000,1,3),date(2010,1,1),date(2022,1,1)), # (DROP) Earlier Record
        ('N3','BP','021','022',date(2000,1,3),date(2010,1,1),date(2021,1,1)), # (DROP) Earlier Record
    ], ['pid','code','sys','dia','dob','journal','extract'])


    df_expected = spark.createDataFrame([
        ('N1','BP','001','002',date(2000,1,1),date(2010,1,1),date(2022,1,1)),
        ('N2','BP','001','002',date(2000,1,2),date(2010,1,1),date(2023,1,1)),
        ('N3','BP','001','002',date(2000,1,3),date(2010,1,1),date(2023,1,1)),
    ], ['pid','code','sys','dia','dob','journal','extract'])

    df_actual = keep_latest_journal_updates(
        df = df_input,
        field_person_id = 'pid',
        field_person_dob = 'dob',
        field_date_extract = 'extract',
        field_date_journal = 'journal'
    )

    assert compare_results(df_actual, df_expected, join_columns = ['pid','dob'])

# COMMAND ----------

@suite.add_test
def test_classify_htn_risk():
    '''
    preprocess_cvdp_htn::classify_htn_risk_group()
    Test takes the processed blood pressure dataframe (extracted, minimised and deduplicated) and classifies each blood pressure
    measurement with a corresponding hypertension risk group.
    '''

    # Test variables
    test_htn_risk_group_col = 'htn_risk_group'
    test_age_col = 'age'
    test_bp_systolic_col = 'bp_sys'
    test_bp_diastolic_col = 'bp_dia'

    # Test schema
    schema_input = T.StructType([
        T.StructField('idx',T.IntegerType(),False),
        T.StructField('pid',T.StringType(),False),
        T.StructField('age',T.IntegerType(),False),
        T.StructField('bp_sys',T.IntegerType(),True),
        T.StructField('bp_dia',T.IntegerType(),True)
    ])

    schema_expected = T.StructType([
        T.StructField('idx',T.IntegerType(),False),
        T.StructField('pid',T.StringType(),False),
        T.StructField('age',T.IntegerType(),False),
        T.StructField('bp_sys',T.IntegerType(),True),
        T.StructField('bp_dia',T.IntegerType(),True),
        T.StructField('htn_risk_group',T.StringType(),True)
    ])

    # Test dataframes
    df_input = spark.createDataFrame([
        (0,'1',80,149,80),
        (1,'2',80,151,89),
        (2,'3',79,149,80),
        (3,'4',79,139,80),
        (4,'5',79,139,95),
        (5,'6',80,181,120),
        (6,'7',80,181,121),
        (7,'8',80,161,121),
        (8,'9',80,161,120),
        (9,'10',79,141,121),
        (10,'11',79,141,120),
        (11,'12',79,141,101),
        (12,'13',79,141,100),
        (13,'14',79,141,90),
        (14,'15',79,None,None),
        (15,'16',79,49,20),
        (16,'17',79,0,90),
        (17,'18',79,0,0),
        (18,'19',79,100,0),
        (19,'20',79,100,None),
        (20,'21',79,None,100),
    ], schema_input)


    df_expected = spark.createDataFrame([
        (0,'1',80,149,80,'0.2'),
        (1,'2',80,151,89,'1b'),
        (2,'3',79,149,80,'1a'),
        (3,'4',79,139,80,'0.1'),
        (4,'5',79,139,95,'1a'),
        (5,'6',80,181,120,'1d'),
        (6,'7',80,181,121,'1d'),
        (7,'8',80,161,121,'1d'),
        (8,'9',80,161,120,'1c'),
        (9,'10',79,141,121,'1d'),
        (10,'11',79,141,120,'1c'),
        (11,'12',79,141,101,'1c'),
        (12,'13',79,141,100,'1a'),
        (13,'14',79,141,90,'1a'),
        (14,'15',79,None,None,None),
        (15,'16',79,49,20,None),
        (16,'17',79,0,90,None),
        (17,'18',79,0,0,None),
        (18,'19',79,100,0,None),
        (19,'20',79,100,None,None),
        (20,'21',79,None,100,None),
    ], schema_expected)

    df_actual = classify_htn_risk_group(
        df = df_input,
        col_risk_group = test_htn_risk_group_col,
        field_age_at_event = test_age_col,
        field_bp_diastolic = test_bp_diastolic_col,
        field_bp_systolic = test_bp_systolic_col,
    )

    assert compare_results(df_actual, df_expected, join_columns = ['idx'])


# COMMAND ----------

@suite.add_test
def test_format_processed_htn_output():
    '''
    preprocess_cvdp_htn::format_processed_htn_output()
    Test takes the classified bp measurement dataframe and returns a formatted (selected columns) and filtered (based on a starting
    date from which to keep bp records) dataframe.
    '''

    df_input = spark.createDataFrame([
        ('N1','BP','001','002',date(2000,1,1),date(2010,1,1),date(2022,1,1)),
        ('N2','BP','001','002',date(2000,1,2),date(2010,1,1),date(2023,1,1)),
        ('N2','BP','001','002',date(2000,1,2),date(2009,1,1),date(2023,1,1)),
        ('N3','BP','001','002',date(2000,1,3),date(2009,1,1),date(2023,1,1)),
    ], ['pid','code','sys','dia','dob','journal','extract'])


    df_expected = spark.createDataFrame([
        ('N1','001','002',date(2000,1,1),date(2010,1,1),date(2022,1,1),'ea271c4ee1cef55ed31a3ff4a2a17578dab518e28fc8380fa6eb36728949137b'),
        ('N2','001','002',date(2000,1,2),date(2010,1,1),date(2023,1,1),'4ec8190b497e9003d1d75792de5efd2fa210ea34c4c75e5a1072917fb699e70c'),
    ], ['pid','sys','dia','dob','journal','extract','htn_pk'])

    df_actual = format_processed_htn_output(
        df = df_input,
        fields_select = ['pid','dob','sys','dia','journal','extract'],
        field_bp_date = 'journal',
        filter_start_date = date(2010,1,1),
        col_primary_key = 'htn_pk',
        fields_hashable = ['pid','dob','journal']
    )

    assert compare_results(df_actual, df_expected, join_columns = ['htn_pk'])

# COMMAND ----------

@suite.add_test
def test_preprocess_cvdp_htn_updated_records_hash():
  '''
  preprocess_cvdp_htn::preprocess_cvdp_htn()
  Test takes two extracts (representing when the CVDP audit data is updated) and runs the processing function to produce the 
  final CVDP hypertension records. These records should have matching record identifiers as they are from the same journal
  entry, over different extract updates (representing an updated extract).
  '''

  # Test SNOMED and Cluster Codes
  codes_cluster_test = ['bp']
  codes_snomed_test_systolic = ['S1']
  codes_snomed_test_diastolic = ['D1']

  ## Test Schemas
  # Input
  schema_input = T.StructType([
      T.StructField('pid',T.StringType(),True),
      T.StructField('dob',T.DateType(),True),
      T.StructField('journal_date',T.DateType(),True),
      T.StructField('extract_date',T.DateType(),True),
      T.StructField('code', T.StringType(), True),
      T.StructField('value',T.ArrayType(
          T.StringType(), True
      ))
  ])
  # Expected
  schema_expected = T.StructType([
      T.StructField('pid',T.StringType(),False),
      T.StructField('dob',T.DateType(),False),
      T.StructField('age',T.LongType(),False),
      T.StructField('extract_date',T.DateType(),False),
      T.StructField('bp_sys',T.StringType(),False),
      T.StructField('bp_dia',T.StringType(),False),
      T.StructField('htn_risk_group',T.StringType(),False),
      T.StructField('journal_date',T.DateType(),False),
      T.StructField('htn_pk',T.StringType(),False)
  ])

  ## Input Test Data
  # First Extract
  df_input_01 = spark.createDataFrame([
    ('001', date(2000,1,1), date(2010,1,1), date(2020,1,1), 'S1', ['60', None]),
    ('001', date(2000,1,1), date(2010,1,1), date(2020,1,1), 'D1', ['120', None]),
  ], schema_input)
  # Second Extract
  df_input_02 = spark.createDataFrame([
    ('001', date(2000,1,1), date(2010,1,1), date(2020,1,1), 'S1', ['60', None]),
    ('001', date(2000,1,1), date(2010,1,1), date(2020,1,1), 'D1', ['120', None]),
    ('001', date(2000,1,1), date(2010,1,1), date(2021,1,1), 'bp', ['60', '170']),
    ('001', date(2000,1,1), date(2010,1,1), date(2021,1,1), 'S1', ['80', None]),
  ], schema_input)

  ## Expected Test Data
  # First Extract
  df_expected_01 = spark.createDataFrame([
    ('001',date(2000,1,1),10,date(2020,1,1),'60','120','1c',date(2010,1,1),'897a16e019618de4432d19c73f31020690351c82be2b53ce0d5fdd0b6f0672c2'),
  ], schema_expected)
  # Second Extract
  df_expected_02 = spark.createDataFrame([
    ('001',date(2000,1,1),10,date(2021,1,1),'60','170','1d',date(2010,1,1),'897a16e019618de4432d19c73f31020690351c82be2b53ce0d5fdd0b6f0672c2'),
  ], schema_expected)

  ## Actual Data
  # First Extract
  df_actual_01 = preprocess_cvdp_htn(
    df = df_input_01,
    age_min_value = 0,
    age_max_value = 120,
    bp_min_diastolic = 0,
    bp_min_systolic = 0,
    codes_bp_clusters = codes_cluster_test,
    codes_bp_diastolic = codes_snomed_test_diastolic,
    codes_bp_systolic = codes_snomed_test_systolic,
    col_age_at_event = 'age',
    col_diastolic = 'bp_dia',
    col_risk_group = 'htn_risk_group',
    col_systolic = 'bp_sys',
    field_calculation_date = 'journal_date',
    field_cluster_id = 'code',
    field_values_array = 'value',
    field_date_extract = 'extract_date',
    field_date_journal = 'journal_date',
    field_person_dob = 'dob',
    field_person_id = 'pid',
    field_snomed_code = 'code',
    field_bp_date = 'journal_date',
    filter_start_date = date(2000,1,1),
    fields_final_output = ['pid','dob','age','extract_date','bp_sys','bp_dia','htn_risk_group','journal_date'],
    col_primary_key = 'htn_pk',
    fields_hashable = ['pid','dob','journal_date']
  )
  # Second Extract
  df_actual_02 = preprocess_cvdp_htn(
    df = df_input_02,
    age_min_value = 0,
    age_max_value = 120,
    bp_min_diastolic = 0,
    bp_min_systolic = 0,
    codes_bp_clusters = codes_cluster_test,
    codes_bp_diastolic = codes_snomed_test_diastolic,
    codes_bp_systolic = codes_snomed_test_systolic,
    col_age_at_event = 'age',
    col_diastolic = 'bp_dia',
    col_risk_group = 'htn_risk_group',
    col_systolic = 'bp_sys',
    field_calculation_date = 'journal_date',
    field_cluster_id = 'code',
    field_values_array = 'value',
    field_date_extract = 'extract_date',
    field_date_journal = 'journal_date',
    field_person_dob = 'dob',
    field_person_id = 'pid',
    field_snomed_code = 'code',
    field_bp_date = 'journal_date',
    filter_start_date = date(2000,1,1),
    fields_final_output = ['pid','dob','age','extract_date','bp_sys','bp_dia','htn_risk_group','journal_date'],
    col_primary_key = 'htn_pk',
    fields_hashable = ['pid','dob','journal_date']
  )

  ## Assertion Tests
  # First Extract
  assert compare_results(df_actual_01, df_expected_01, join_columns = ['htn_pk'])
  assert compare_results(df_actual_02, df_expected_02, join_columns = ['htn_pk'])

# COMMAND ----------

suite.run()

# COMMAND ----------

dbutils.notebook.exit('pass')
