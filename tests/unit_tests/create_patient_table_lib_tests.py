# Databricks notebook source
# MAGIC %run ../../src/test_helpers

# COMMAND ----------

PARAMS_PATH = 'default'

# COMMAND ----------

# MAGIC %run ../../params/params

# COMMAND ----------

# MAGIC %run ../../pipeline/create_patient_table_lib

# COMMAND ----------

from uuid import uuid4
from datetime import datetime, date
import pyspark.sql.functions as F
import pyspark.sql.types as T

from dsp.validation.validator import compare_results

# COMMAND ----------

suite = FunctionTestSuite()

# COMMAND ----------

@suite.add_test
def test_format_base_patient_table_schema():

  input_schema = T.StructType([
        T.StructField('idx', T.StringType(), False),
        T.StructField('pid', T.StringType(), False),
        T.StructField('info', T.StringType(), False),
        T.StructField('ids', T.ArrayType(T.StringType()), True)
    ])

  expected_schema = T.StructType([
        T.StructField('idx', T.StringType(), False),
        T.StructField('pid', T.StringType(), False),
        T.StructField('flag', T.StringType(), False),
        T.StructField('ids', T.StringType(), True)
    ])

  df_input = spark.createDataFrame([
    (0, 'A', '1', ['A']),
    (1, 'B', '2', ['B']),
    (2, 'C', '3', [None]),
  ], input_schema)

  df_expected = spark.createDataFrame([
    (0, 'A', '1', 'A'),
    (1, 'B', '2', 'B'),
    (2, 'C', '3', '')
  ], expected_schema)

  df_actual = format_base_patient_table_schema(
    df = df_input,
    mapping_cols = {'info':'flag'},
    field_practice_identifier = 'ids'
  )

  assert compare_results(df_actual, df_expected, join_columns=['idx'])


# COMMAND ----------

@suite.add_test
def test_extract_patient_events():
  '''test_extract_patient_events

  Test of extract_patient_events() - filters to only keep cohort events
  '''

  # Input data schema - events table
  input_schema = T.StructType([
      T.StructField('person_id', T.StringType(), False),
      T.StructField('birth_date', T.DateType(), False),
      T.StructField('dataset', T.StringType(), False),
      T.StructField('category', T.StringType(), False),
      T.StructField('code', T.StringType(), False),
      T.StructField('record_date', T.DateType(), False),
      T.StructField('code_array', T.ArrayType(T.StringType()), False)
  ])

  # Expected data schema - patient table
  expected_schema = T.StructType([
      T.StructField('person_id', T.StringType(), False),
      T.StructField('birth_date', T.DateType(), False),
      T.StructField('dataset', T.StringType(), False),
      T.StructField('category', T.StringType(), False),
      T.StructField('code', T.StringType(), False),
      T.StructField('record_date', T.DateType(), False),
      T.StructField('code_array', T.ArrayType(T.StringType()), False)
  ])

  # Input data - events table
  df_input = spark.createDataFrame([
      ('001',date(2001,1,1),'test_cohort','test_extract','CVD001',date(2010,1,1), ['P0001']), # (001) Keep
      ('002',date(2002,1,1),'test_cohort','test_extract','CVD001',date(2010,1,1), ['P0000']), # (002) Keep
      ('002',date(2002,1,1),'test_cohort','test_extract','CVD002',date(2011,1,1), ['P0002']), # (002) Keep
      ('003',date(2003,1,1),'test_cohort','test_extract','CVD001',date(2010,1,1), ['P0000']), # (003) Keep
      ('003',date(2003,1,1),'test_other','test_other','A3',date(2011,1,1), ['A','B']),        # (003) Remove
      ('003',date(2003,1,1),'test_cohort','test_extract','CVD001',date(2012,1,1), ['P0003']), # (003) Keep
      ('004',date(2004,1,1),'test_cohort','test_extract','CVD001',date(2010,1,1), ['P0000']), # (004) Keep
      ('004',date(2004,1,1),'test_other','test_other','A4',date(2012,1,1), ['A','B','C']),    # (004) Remove
      ('004',date(2004,1,1),'test_cohort','test_extract','CVD001',date(2011,1,1), ['P0001']), # (004) Keep
      ('005',date(2005,1,1),'test_other','test_other','A5',date(2010,1,1), ['A','B']),        # (005) Remove
      ('005',date(2005,1,1),'test_other','test_other','B5',date(2011,1,1), ['C','D','E']),    # (005) Remove
      ('005',date(2005,1,1),'test_other','test_other','C5',date(2012,1,1), ['F']),            # (005) Remove
  ], input_schema)

  # Expected data - patient table with cohort events (and latest) only
  df_expected = spark.createDataFrame([
    ('001',date(2001,1,1),'test_cohort','test_extract','CVD001',date(2010,1,1), ['P0001']),
    ('002',date(2002,1,1),'test_cohort','test_extract','CVD001',date(2010,1,1), ['P0000']),
    ('002',date(2002,1,1),'test_cohort','test_extract','CVD002',date(2011,1,1), ['P0002']),
    ('003',date(2003,1,1),'test_cohort','test_extract','CVD001',date(2010,1,1), ['P0000']),
    ('003',date(2003,1,1),'test_cohort','test_extract','CVD001',date(2012,1,1), ['P0003']),
    ('004',date(2004,1,1),'test_cohort','test_extract','CVD001',date(2010,1,1), ['P0000']),
    ('004',date(2004,1,1),'test_cohort','test_extract','CVD001',date(2011,1,1), ['P0001']),
  ], expected_schema)

  df_actual = extract_patient_events(
    df = df_input,
    value_dataset = 'test_cohort',
    value_category = 'test_extract',
    field_dataset = 'dataset',
    field_category = 'category'
  )

  assert compare_results(df_actual, df_expected, join_columns=['person_id','birth_date','record_date'])

# COMMAND ----------

@suite.add_test
def test_add_stroke_mi_information_keepAll():
  """test_add_stroke_mi_information_keepAll()

  Testing of the add_stroke_mi_information() function, where all patient's are kept (dead, alive) and
  date of death filtering is set to inclusive (less than or equal to date of death).

  Variables:
    keep_nulls == True
    keep_inclusive == True

  Associated:
    pipeline/create_patient_table_lib::add_stroke_mi_information()
  """

  # Schema Definitions
  ## Input - Patient
  input_schema_patient = T.StructType([
    T.StructField('person_id', T.StringType(), False),
    T.StructField('birth_date', T.DateType(), False),
    T.StructField('date_of_death', T.DateType(), True)
  ])
  ## Input - Events
  input_schema_events = T.StructType([
    T.StructField('person_id', T.StringType(), False),
    T.StructField('birth_date', T.DateType(), False),
    T.StructField('record_start_date', T.DateType(), False),
    T.StructField('flag', T.StringType(), False),
    T.StructField('flag_assoc', T.ArrayType(T.StringType()), True)
  ])
  ## Expected
  expected_schema = T.StructType([
    T.StructField('person_id', T.StringType(), False),
    T.StructField('birth_date', T.DateType(), False),
    T.StructField('stroke_count', T.LongType(), False),
    T.StructField('max_stroke_date', T.DateType(), True),
    T.StructField('mi_count', T.LongType(), False),
    T.StructField('max_mi_date', T.DateType(), True),
    T.StructField('date_of_death', T.DateType(), True),
  ])

  # Input DataFrames
  ## Patient
  df_patient_input = spark.createDataFrame([
    # person_id | birth_date | date_of_death
    (1,date(2001,1,1),date(2021,1,1)),  # Expected: 1 Stroke
    (2,date(2002,2,2),date(2022,1,1)),  # Expected: 2 MI
    (3,date(2003,3,3),date(2023,1,1)),  # Expected: 2 Stroke, 1 MI
    (4,date(2004,4,4),date(2024,1,1)),  # Expected: No Stroke, No MI
    (5,date(2005,5,5),None),            # Expected: 1 Stroke, 1 MI
    (6,date(2006,6,6),None),            # Expected: No Stroke, No MI
    (7,date(2007,7,7),None),            # Expected: No Stroke, No MI
  ], input_schema_patient)

  ## Events
  df_events_input = spark.createDataFrame([
    # person_id | birth_date | record_start_date | flag | flag_assoc
    # Patient 1 (Has Died, Valid Records)
    (1,date(2001,1,1),date(2011,1,1),'stroke',None),                    # Keep - Before Death
    # Patient 2 (Has Died, Valid Records)
    (2,date(2002,2,2),date(2012,1,1),'mi',None),                        # Keep - Before Death
    (2,date(2002,2,2),date(2022,1,1),'mi',None),                        # Keep - On Death
    # Patient 3 (Has Died, Valid and Invalid Records)
    (3,date(2003,3,3),date(2013,1,1),'mi',None),                        # Keep - Before Death
    (3,date(2003,3,3),date(2023,1,1),'stroke',None),                    # Keep - On Death
    (3,date(2003,3,3),date(2023,1,1),'multiple',['stroke','non_cvd']),  # Keep - On Death
    (3,date(2003,3,3),date(2023,1,2),'stroke',None),                    # Remove - After Death
    (3,date(2003,3,3),date(2023,1,2),'multiple',['mi','stroke']),       # Remove - After Death
    # Patient 4 (Has Died, Invalid Records)
    (4,date(2004,4,4),date(2024,1,2),'mi',None),                        # Remove - After Death
    # Patient 5 (Alive, Valid and Invalid Records)
    (5,date(2005,5,5),date(2015,1,1),'multiple',['mi','stroke']),       # Keep
    (5,date(2005,5,5),date(2015,1,1),'multiple',['no_cvd','hf']),       # Remove - No valid flags
    # Patient 6 (Alive, Invalid Records)
    (6,date(2006,6,6),date(2016,1,1),'hf',None),                        # Remove - No valid flags
    # Patient 7 (Alive, No Records)
  ], input_schema_events)

  # Expected DataFrame
  df_expected = spark.createDataFrame([
    # person_id | birth_date | stroke_count | max_stroke_date | mi_count | max_mi_date | date_of_death
    (1,date(2001,1,1),1,date(2011,1,1),0,None,date(2021,1,1)),            # Expected: 1 Stroke
    (2,date(2002,2,2),0,None,2,date(2022,1,1),date(2022,1,1)),            # Expected: 2 MI
    (3,date(2003,3,3),2,date(2023,1,1),1,date(2013,1,1),date(2023,1,1)),  # Expected: 2 Stroke, 1 MI
    (4,date(2004,4,4),0,None,0,None,date(2024,1,1)),                      # Expected: No Stroke, No MI
    (5,date(2005,5,5),1,date(2015,1,1),1,date(2015,1,1),None),            # Expected: 1 Stroke, 1 MI
    (6,date(2006,6,6),0,None,0,None,None),                                # Expected: No Stroke, No MI
    (7,date(2007,7,7),0,None,0,None,None),                                # Expected: No Stroke, No MI
  ], expected_schema)

  # Actual DataFrame
  df_actual = add_stroke_mi_information(
    patient_table = df_patient_input,
    events_table = df_events_input,
    field_death_date = 'date_of_death',
    field_event_start = 'record_start_date',
    field_flag = 'flag',
    field_flag_assoc = 'flag_assoc',
    field_flag_count = 'count',
    field_flag_mi_count = 'mi_count',
    field_flag_stroke_count = 'stroke_count',
    field_max_mi_date = 'max_mi_date',
    field_max_stroke_date = 'max_stroke_date',
    keep_inclusive = True,
    keep_nulls = True,
    key_join = ['person_id','birth_date'],
    value_flag_mi = 'mi',
    value_flag_stroke = 'stroke',
  )

  # Result comparison
  assert compare_results(df_actual, df_expected, join_columns=['person_id', 'birth_date'])


# COMMAND ----------

@suite.add_test
def test_add_stroke_mi_information_keepBeforeDeathOnly():
  """test_add_stroke_mi_information_keepBeforeDeathOnly()

  Testing of the add_stroke_mi_information() function, where all patient's are kept (dead, alive) and
  date of death filtering is set to exclusive (less than the date of death).

  Variables:
    keep_nulls == True
    keep_inclusive == False

  Associated:
    pipeline/create_patient_table_lib::add_stroke_mi_information()
  """

  # Schema Definitions
  ## Input - Patient
  input_schema_patient = T.StructType([
    T.StructField('person_id', T.StringType(), False),
    T.StructField('birth_date', T.DateType(), False),
    T.StructField('date_of_death', T.DateType(), True)
  ])
  ## Input - Events
  input_schema_events = T.StructType([
    T.StructField('person_id', T.StringType(), False),
    T.StructField('birth_date', T.DateType(), False),
    T.StructField('record_start_date', T.DateType(), False),
    T.StructField('flag', T.StringType(), False),
    T.StructField('flag_assoc', T.ArrayType(T.StringType()), True)
  ])
  ## Expected
  expected_schema = T.StructType([
    T.StructField('person_id', T.StringType(), False),
    T.StructField('birth_date', T.DateType(), False),
    T.StructField('stroke_count', T.LongType(), False),
    T.StructField('max_stroke_date', T.DateType(), True),
    T.StructField('mi_count', T.LongType(), False),
    T.StructField('max_mi_date', T.DateType(), True),
    T.StructField('date_of_death', T.DateType(), True),
  ])

  # Input DataFrames
  ## Patient
  df_patient_input = spark.createDataFrame([
    # person_id | birth_date | date_of_death
    (1,date(2001,1,1),date(2021,1,1)),  # Expected: 1 Stroke
    (2,date(2002,2,2),date(2022,1,1)),  # Expected: 1 MI
    (3,date(2003,3,3),date(2023,1,1)),  # Expected: 0 Stroke, 1 MI
    (4,date(2004,4,4),date(2024,1,1)),  # Expected: No Stroke, No MI
    (5,date(2005,5,5),None),            # Expected: 1 Stroke, 1 MI
    (6,date(2006,6,6),None),            # Expected: No Stroke, No MI
    (7,date(2007,7,7),None),            # Expected: No Stroke, No MI
  ], input_schema_patient)

  ## Events
  df_events_input = spark.createDataFrame([
    # person_id | birth_date | record_start_date | flag | flag_assoc
    # Patient 1 (Has Died, Valid Records)
    (1,date(2001,1,1),date(2011,1,1),'stroke',None),                    # Keep - Before Death
    # Patient 2 (Has Died, Valid Records)
    (2,date(2002,2,2),date(2012,1,1),'mi',None),                        # Keep - Before Death
    (2,date(2002,2,2),date(2022,1,1),'mi',None),                        # Remove - On Death
    # Patient 3 (Has Died, Valid and Invalid Records)
    (3,date(2003,3,3),date(2013,1,1),'mi',None),                        # Keep - Before Death
    (3,date(2003,3,3),date(2023,1,1),'stroke',None),                    # Remove - On Death
    (3,date(2003,3,3),date(2023,1,1),'multiple',['stroke','non_cvd']),  # Remove - On Death
    (3,date(2003,3,3),date(2023,1,2),'stroke',None),                    # Remove - After Death
    (3,date(2003,3,3),date(2023,1,2),'multiple',['mi','stroke']),       # Remove - After Death
    # Patient 4 (Has Died, Invalid Records)
    (4,date(2004,4,4),date(2024,1,2),'mi',None),                        # Remove - After Death
    # Patient 5 (Alive, Valid and Invalid Records)
    (5,date(2005,5,5),date(2015,1,1),'multiple',['mi','stroke']),       # Keep
    (5,date(2005,5,5),date(2015,1,1),'multiple',['no_cvd','hf']),       # Remove - No valid flags
    # Patient 6 (Alive, Invalid Records)
    (6,date(2006,6,6),date(2016,1,1),'hf',None),                        # Remove - No valid flags
    # Patient 7 (Alive, No Records)
  ], input_schema_events)

  # Expected DataFrame
  df_expected = spark.createDataFrame([
    # person_id | birth_date | stroke_count | max_stroke_date | mi_count | max_mi_date | date_of_death
    (1,date(2001,1,1),1,date(2011,1,1),0,None,date(2021,1,1)),  # Expected: 1 Stroke
    (2,date(2002,2,2),0,None,1,date(2012,1,1),date(2022,1,1)),  # Expected: 1 MI
    (3,date(2003,3,3),0,None,1,date(2013,1,1),date(2023,1,1)),  # Expected: 0 Stroke, 1 MI
    (4,date(2004,4,4),0,None,0,None,date(2024,1,1)),            # Expected: No Stroke, No MI
    (5,date(2005,5,5),1,date(2015,1,1),1,date(2015,1,1),None),  # Expected: 1 Stroke, 1 MI
    (6,date(2006,6,6),0,None,0,None,None),                      # Expected: No Stroke, No MI
    (7,date(2007,7,7),0,None,0,None,None),                      # Expected: No Stroke, No MI
  ], expected_schema)

  # Actual DataFrame
  df_actual = add_stroke_mi_information(
    patient_table = df_patient_input,
    events_table = df_events_input,
    field_death_date = 'date_of_death',
    field_event_start = 'record_start_date',
    field_flag = 'flag',
    field_flag_assoc = 'flag_assoc',
    field_flag_count = 'count',
    field_flag_mi_count = 'mi_count',
    field_flag_stroke_count = 'stroke_count',
    field_max_mi_date = 'max_mi_date',
    field_max_stroke_date = 'max_stroke_date',
    keep_inclusive = False,
    keep_nulls = True,
    key_join = ['person_id','birth_date'],
    value_flag_mi = 'mi',
    value_flag_stroke = 'stroke',
  )

  # Result comparison
  assert compare_results(df_actual, df_expected, join_columns=['person_id', 'birth_date'])


# COMMAND ----------

@suite.add_test
def test_add_stroke_mi_information_keepBeforeDeathAndDeadOnly():
  """test_add_stroke_mi_information_keepBeforeDeathAndDeadOnly()

  Testing of the add_stroke_mi_information() function, where only dead patient's records are kept
  and date of death filtering is set to exclusive (less than the date of death).

  Variables:
    keep_nulls == False
    keep_inclusive == False

  Associated:
    pipeline/create_patient_table_lib::add_stroke_mi_information()
  """

  # Schema Definitions
  ## Input - Patient
  input_schema_patient = T.StructType([
    T.StructField('person_id', T.StringType(), False),
    T.StructField('birth_date', T.DateType(), False),
    T.StructField('date_of_death', T.DateType(), True)
  ])
  ## Input - Events
  input_schema_events = T.StructType([
    T.StructField('person_id', T.StringType(), False),
    T.StructField('birth_date', T.DateType(), False),
    T.StructField('record_start_date', T.DateType(), False),
    T.StructField('flag', T.StringType(), False),
    T.StructField('flag_assoc', T.ArrayType(T.StringType()), True)
  ])
  ## Expected
  expected_schema = T.StructType([
    T.StructField('person_id', T.StringType(), False),
    T.StructField('birth_date', T.DateType(), False),
    T.StructField('stroke_count', T.LongType(), False),
    T.StructField('max_stroke_date', T.DateType(), True),
    T.StructField('mi_count', T.LongType(), False),
    T.StructField('max_mi_date', T.DateType(), True),
    T.StructField('date_of_death', T.DateType(), True),
  ])

  # Input DataFrames
  ## Patient
  df_patient_input = spark.createDataFrame([
    # person_id | birth_date | date_of_death
    (1,date(2001,1,1),date(2021,1,1)),  # Expected: 1 Stroke
    (2,date(2002,2,2),date(2022,1,1)),  # Expected: 1 MI
    (3,date(2003,3,3),date(2023,1,1)),  # Expected: 0 Stroke, 1 MI
    (4,date(2004,4,4),date(2024,1,1)),  # Expected: No Stroke, No MI
    (5,date(2005,5,5),None),            # Expected: No Stroke, No MI
    (6,date(2006,6,6),None),            # Expected: No Stroke, No MI
    (7,date(2007,7,7),None),            # Expected: No Stroke, No MI
  ], input_schema_patient)

  ## Events
  df_events_input = spark.createDataFrame([
    # person_id | birth_date | record_start_date | flag | flag_assoc
    # Patient 1 (Has Died, Valid Records)
    (1,date(2001,1,1),date(2011,1,1),'stroke',None),                    # Keep - Before Death
    # Patient 2 (Has Died, Valid Records)
    (2,date(2002,2,2),date(2012,1,1),'mi',None),                        # Keep - Before Death
    (2,date(2002,2,2),date(2022,1,1),'mi',None),                        # Remove - On Death
    # Patient 3 (Has Died, Valid and Invalid Records)
    (3,date(2003,3,3),date(2013,1,1),'mi',None),                        # Keep - Before Death
    (3,date(2003,3,3),date(2023,1,1),'stroke',None),                    # Remove - On Death
    (3,date(2003,3,3),date(2023,1,1),'multiple',['stroke','non_cvd']),  # Remove - On Death
    (3,date(2003,3,3),date(2023,1,2),'stroke',None),                    # Remove - After Death
    (3,date(2003,3,3),date(2023,1,2),'multiple',['mi','stroke']),       # Remove - After Death
    # Patient 4 (Has Died, Invalid Records)
    (4,date(2004,4,4),date(2024,1,2),'mi',None),                        # Remove - After Death
    # Patient 5 (Alive, Valid and Invalid Records)
    (5,date(2005,5,5),date(2015,1,1),'multiple',['mi','stroke']),       # Remove - Alive
    (5,date(2005,5,5),date(2015,1,1),'multiple',['no_cvd','hf']),       # Remove - Alive
    # Patient 6 (Alive, Invalid Records)
    (6,date(2006,6,6),date(2016,1,1),'hf',None),                        # Remove - Alive
    # Patient 7 (Alive, No Records)
  ], input_schema_events)

  # Expected DataFrame
  df_expected = spark.createDataFrame([
    # person_id | birth_date | stroke_count | max_stroke_date | mi_count | max_mi_date | date_of_death
    (1,date(2001,1,1),1,date(2011,1,1),0,None,date(2021,1,1)),  # Expected: 1 Stroke
    (2,date(2002,2,2),0,None,1,date(2012,1,1),date(2022,1,1)),  # Expected: 1 MI
    (3,date(2003,3,3),0,None,1,date(2013,1,1),date(2023,1,1)),  # Expected: 0 Stroke, 1 MI
    (4,date(2004,4,4),0,None,0,None,date(2024,1,1)),            # Expected: No Stroke, No MI
    (5,date(2005,5,5),0,None,0,None,None),                      # Expected: No Stroke, No MI
    (6,date(2006,6,6),0,None,0,None,None),                      # Expected: No Stroke, No MI
    (7,date(2007,7,7),0,None,0,None,None),                      # Expected: No Stroke, No MI
  ], expected_schema)

  # Actual DataFrame
  df_actual = add_stroke_mi_information(
    patient_table = df_patient_input,
    events_table = df_events_input,
    field_death_date = 'date_of_death',
    field_event_start = 'record_start_date',
    field_flag = 'flag',
    field_flag_assoc = 'flag_assoc',
    field_flag_count = 'count',
    field_flag_mi_count = 'mi_count',
    field_flag_stroke_count = 'stroke_count',
    field_max_mi_date = 'max_mi_date',
    field_max_stroke_date = 'max_stroke_date',
    keep_inclusive = False,
    keep_nulls = False,
    key_join = ['person_id','birth_date'],
    value_flag_mi = 'mi',
    value_flag_stroke = 'stroke',
  )

  # Result comparison
  assert compare_results(df_actual, df_expected, join_columns=['person_id', 'birth_date'])

# COMMAND ----------

@suite.add_test
def test_add_under_75_flag():
  df_input = spark.createDataFrame([
    (1, date(2000,1,1),date(2022,12,20), 'STROKE'),      # should have a flag - died before 75
    (2, date(1900,1,1),date(2022,12,20), 'HEARTATTACK'), # should not have a flag - died after 75
    (3, date(2000,1,1),None,             None),          # not dead
    (4, date(1990,1,1),date(2022,12,20), None),          # should not have a flag because no STROKE or MI
  ]
  ,['person_id', 'birth_date', 'date_of_death', 'death_flag'])

  df_expected = spark.createDataFrame([
    (1, date(2000,1,1),date(2022,12,20), 'DIED_UNDER_75', 'STROKE'),
    (2, date(1900,1,1),date(2022,12,20), None           , 'HEARTATTACK'),
    (3, date(2000,1,1),None            , None           , None),
    (4, date(1990,1,1),date(2022,12,20), None           , None),
  ]
  ,['person_id', 'birth_date', 'date_of_death', 'death_age_flag', 'death_flag'])

  df_actual = add_under_age_flag(df_input, 75, 'birth_date', 'date_of_death', 'death_age_flag', 'DIED_UNDER_75', 'death_flag', True)

  assert compare_results(df_actual, df_expected, join_columns=['person_id', 'birth_date'])

# COMMAND ----------

@suite.add_test
def test_add_30_days_flag():
  df_hes_input = spark.createDataFrame([

    #PERSON 1 HAS 2 HEART ATTACKS HOSPITALISATIONS, 1 MONTH APART
    (1,'episode',date(2000,1,1), date(2022,2,1), 'HEARTATTACK'),
    (1,'episode',date(2000,1,1), date(2022,1,1) , 'HEARTATTACK'),

    #PERSON 2 HAS NO HOSPITALISATIONS + A DUPLICATE
    (2,'episode',date(2020,1,1), None           , None),
    (2,'episode',date(2020,1,1), None           , None),

    #PERSON 3 WAS HOPITALISED BUT WITH NO FLAG
    (3,'episode',date(1900,1,1), date(2020,8,1) , None),

    #PERSON 4 IS HOSPITALISED 3 TIMES, ALL OVER A MONTH APART
    (4,'episode',date(1990,1,1), date(2001,1,1) , None),
    (4,'episode',date(1990,1,1), date(2002,2,2) , 'HEARTATTACK'),
    (4,'episode',date(1990,1,1), date(2003,3,3) , 'STROKE'),

    #PERSON 5 HAD A HEART ATTACK BEFORE DEATH WITHIN 30 DAYS, AND A STROKE AFTER DEATH
    (5,'episode',date(1970,1,1), date(2019,12,2), 'NO_CVD'),#31 days before
    (5,'episode',date(1970,1,1), date(2019,12,3), 'HEARTATTACK'), #30 days before, had a heart attack before death
    (5,'episode',date(1970,1,1), date(2024,1,1), 'STROKE'), #had a stroke after death

    #PERSON 6 HAD 2 HEART ATTACK HOSPITALISATIONS WITHIN 30 BEFORE DEATH, AND A STROKE AFTER DEATH
    (6,'episode',date(1970,1,1), date(2019,12,9), 'HEARTATTACK'),
    (6,'episode',date(1970,1,1), date(2020,1,5), 'HEARTATTACK'),
    (6,'episode',date(1970,1,1), date(2024,1,1), 'STROKE'),

    #PERSON 7 HAD A HEARTATTACK AND A STROKE BEFORE DEATH, WITHIN 30 DAYS
    (7,'episode',date(1970,1,1), date(2020,1,5), 'HEARTATTACK'),
    (7,'episode',date(1970,1,1), date(2020,1,5), 'STROKE'),

    #PERSON 8 IS HOSPITALISED WITH NO DEATH
    (8,'episode',date(1970,1,1), date(2020,1,5), 'HEARTATTACK'),

    #PERSON 9 DOES NOT HAVE ANY RECORD

    #PERSON 11 HAD A STROKE THEN A HEARTATTACK BEFORE DEATH, WITHIN 30 DAYS
    (10,'episode',date(1970,1,1), date(2020,1,4), 'STROKE'),
    (10,'episode',date(1970,1,1), date(2020,1,5), 'HEARTATTACK'),

  ],
    ['person_id','category', 'birth_date', 'record_start_date', 'flag']
  )

  df_input = spark.createDataFrame([

    (1,date(2000,1,1), date(2022,10,1)),
    (2,date(2020,1,1), None),
    (3,date(1900,1,1), date(2022,8,10)),
    (4,date(2000,7,1), date(2022,8,1)),
    (5,date(1970,1,1), date(2020,1,2)),
    (6,date(1970,1,1), date(2020,1,10)),
    (7,date(1970,1,1), date(2020,1,6)),
    (8,date(1970,1,1), None),
    (9,date(1970,1,1), None),
    (10,date(1970,1,1), date(2020,1,6)),

  ], ['person_id', 'birth_date', 'date_of_death'])



  df_expected = spark.createDataFrame([

    #PERSON 1 DIED AFTER 2 MONTHS AFTER HOSPITALISATION, NO FLAG NEEDED
    (1,date(2000,1,1), date(2022,10,1), None), # no flag needed, died 2 months later

    ##PERSON 2 HAS NO HOSPITALISATIONS AND NO DEATHS
    (2,date(2020,1,1), None, None), #nulls in columns, no flag

    #PERSON 3 HAS NO HOSPITALISATION FLAG, DIES OF A HEART ATTACK, THEREFORE NO DEATH FLAG NEEDED
    (3,date(1900,1,1), date(2022,8,10), None),

    #NO HOSP FLAG NEEDED AS DEATH WAS TOO LONG AFTER EPISODES
    (4,date(2000,7,1), date(2022,8,1), None),

    # JUST 1 HEARTATTACK FLAG
    (5,date(1970,1,1),date(2020,1,2),['HEARTATTACK']),

    # JUST 1 HEARTATTACK FLAG AS DUPLICATE DROPPED
    (6,date(1970,1,1),date(2020,1,10),['HEARTATTACK']),

    #PERSON 7 HAD A HEARTATTACK AND A STROKE BEFORE DEATH, WITHIN 30 DAYS
    (7,date(1970,1,1),date(2020,1,6), ['HEARTATTACK', 'STROKE']),

    #PERSON 8 IS HOSPITALISED WITH NO DEATH
    (8,date(1970,1,1),None,None),

    #PERSON 9 HAS A NON EPISODE CATEGORY, NO DEATH
    (9,date(1970,1,1),None,None),

    #PERSON 10 HAD A STROKE THEN A HEARTATTACKBEFORE DEATH, WITHIN 30 DAYS, BUT ARRAY SHOULD BE ORDERED SAME AS 7
    (10,date(1970,1,1),date(2020,1,6), ['HEARTATTACK', 'STROKE']),

  ], ['person_id', 'birth_date', 'date_of_death', 'died_within_30_days_hospitalisation_flags'])

  df_actual = add_30_days_flag(df_input, df_hes_input)

  assert compare_results(df_actual, df_expected, join_columns=['person_id', 'birth_date'])

# COMMAND ----------

@suite.add_test
def test_assign_htn_risk_groups():
  '''
  create_patient_table_lib::assign_htn_risk_groups
  Test takes a patient table (with diagnostic flags) and an events table (pre-filtered to hypertension only events)
  and assigns the risk group from the most recent, valid blood pressure reading (within 12 months of the extract date)
  '''
  df_input_patient = spark.createDataFrame([
    # HTN Diag = T; Single Valid BP Reading
    ('001',date(2000,1,1),date(2020,1,1),date(2010,1,1)),
    # HTN Diag = T; Multiple Valid BP Readings
    ('002',date(2000,1,2),date(2020,1,1),date(2010,1,1)),
    # HTN Diag = T; Multiple Valid BP Readings; Earlier Extract Date
    ('003',date(2000,1,3),date(2010,1,1),date(2010,1,1)),
    # HTN Diag = T; No Valid BP Readings
    ('004',date(2000,1,4),date(2020,1,1),date(2010,1,1)),
    # HTN Diag = F; No Valid BP Readings
    ('005',date(2000,1,5),date(2020,1,1),None),
    # HTN Diag = F; Single Valid BP Reading
    ('006',date(2000,1,6),date(2020,1,1),None),
  ],['pid','dob','extract_date','htn_diag_flag'])

  df_input_events = spark.createDataFrame([
    # HTN Diag = T; Single Valid BP Reading
    ('001',date(2000,1,1),date(2019,6,1),'120/80','A'),
    # HTN Diag = T; Multiple Valid BP Readings
    ('002',date(2000,1,2),date(2017,6,1),'90/60','A'),
    ('002',date(2000,1,2),date(2019,6,1),'100/70','A'),
    ('002',date(2000,1,2),date(2019,8,1),'130/90','C'),
    # HTN Diag = T; Multiple Valid BP Readings; Earlier Extract Date
    ('003',date(2000,1,3),date(2009,1,1),'100/100','F'),
    ('003',date(2000,1,3),date(2011,1,1),'90/90','A'),
    ('003',date(2000,1,3),date(2006,1,1),'80/80','C'),
    # HTN Diag = T; No Valid BP Readings
    ('004',date(2000,1,4),date(2000,1,1),'100/100','B'),
    # HTN Diag = F; No Valid BP Readings
    # HTN Diag = F; Single Valid BP Reading
    ('006',date(2000,1,6),date(2019,6,1),'60/60','A'),
  ],['pid','dob','record_start_date','code','flag'])

  df_expected = spark.createDataFrame([
    # HTN Diag = T; Single Valid BP Reading
    ('001',date(2000,1,1),date(2020,1,1),date(2010,1,1),'A'),
    # HTN Diag = T; Multiple Valid BP Readings
    ('002',date(2000,1,2),date(2020,1,1),date(2010,1,1),'C'),
    # HTN Diag = T; Multiple Valid BP Readings; Earlier Extract Date
    ('003',date(2000,1,3),date(2010,1,1),date(2010,1,1),'F'),
    # HTN Diag = T; No Valid BP Readings
    ('004',date(2000,1,4),date(2020,1,1),date(2010,1,1),'1e'),
    # HTN Diag = F; No Valid BP Readings
    ('005',date(2000,1,5),date(2020,1,1),None,'-1'),
    # HTN Diag = F; Single Valid BP Reading
    ('006',date(2000,1,6),date(2020,1,1),None,'-1'),
  ],['pid','dob','extract_date','htn_diag_flag','htn_risk_group'])

  df_actual = assign_htn_risk_groups(
    patient_table = df_input_patient,
    events_table = df_input_events,
    col_htn_risk_group = 'htn_risk_group',
    field_htn_diag_date = 'htn_diag_flag',
    field_htn_events_flag = 'flag',
    field_htn_event_date = 'record_start_date',
    fields_join_cols = ['pid','dob'],
    field_patient_dob = 'dob',
    field_patient_extract_date = 'extract_date',
    field_patient_pid = 'pid',
  )

  assert compare_results(df_actual, df_expected, join_columns=['pid', 'dob'])

# COMMAND ----------

suite.run()

# COMMAND ----------

dbutils.notebook.exit('pass')