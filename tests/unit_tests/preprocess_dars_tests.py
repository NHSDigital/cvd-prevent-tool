# Databricks notebook source
# MAGIC %run ../../src/test_helpers

# COMMAND ----------

# MAGIC %run ../../src/clean_dataset

# COMMAND ----------

PARAMS_PATH = 'default'

# COMMAND ----------

# MAGIC %run ../../src/dars/preprocess_dars

# COMMAND ----------

from datetime import date
from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType
from dsp.validation.validator import compare_results

# COMMAND ----------

suite = FunctionTestSuite()

## The following are fake people and data created for test purposes

# COMMAND ----------

@suite.add_test

def test_remove_dars_cancelled():
  
  df_input = spark.createDataFrame([
    (1, '1111', 'True', 'N'),
    (2, '2222', 'False', 'N'), #not current
    (3, '3333', 'True', 'Y'), #cancelled
    (4, '4444', None, 'N'), #none current flag
    (5, '5555', 'True', None), #none cancelled flag
    (6, '6666', None, None) #none current and cancelled flags
  ], ['test_id', 'test_nhs_number', 'test_current_field', 'test_cancelled_field']
  )
  
  df_expected = spark.createDataFrame([
    (1, '1111'),
  ], ['test_id', 'test_nhs_number']
  )
  
  df_actual = remove_cancelled_dars_records(
    df = df_input,
    current_field = 'test_current_field',
    cancelled_field = 'test_cancelled_field'
    )
  
  assert compare_results(df_actual, df_expected, join_columns=['test_nhs_number'])
  

# COMMAND ----------

@suite.add_test

def test_filter_dars_region():
  
  df_input = spark.createDataFrame([
    (1, '1111', 'W0', None), #not in england
    (2, '2222', None, 'E01010044'), #residence null location england
    (3, '3333', None, 'W01000327'), #residence null location not england
    (4, '4444', 'E123', None), # in England 
  ], ['test_id', 'test_nhs_number', 'test_residence', 'test_location'])
  
  df_expected = spark.createDataFrame([
    (2, '2222', None, 'E01010044'),
    (4, '4444', 'E123', None)
  ], ['test_id', 'test_nhs_number', 'test_residence', 'test_location'])
  
  df_actual = filter_dars_region(
    df = df_input,
    residence_field = 'test_residence',
    location_field = 'test_location',
    region_prefix = 'E'
    )
  
  assert compare_results(df_actual, df_expected, join_columns=['test_id'])
  

# COMMAND ----------

@suite.add_test

def test_clean_dars_nhs_number():
  
  df_input = spark.createDataFrame([
    (1, None, None, ), #no NHS number
    (2, None, '2222'), #secondary NHS number
    (3, '3333', None)
  ], ['test_id', 'test_primary_nhs_number', 'test_secondary_nhs_number']
  )
  
  df_expected = spark.createDataFrame([
    (2, '2222'),
    (3, '3333')
  ], ['test_id', 'final_nhs_number'])
  
  df_actual = clean_dars_nhs_number(
    df = df_input,
    nhs_number_out_field = 'final_nhs_number',
    nhs_number_origin_field_1 = 'test_primary_nhs_number',
    nhs_number_origin_field_2 = 'test_secondary_nhs_number'
    )
  
  assert compare_results(df_actual, df_expected, join_columns=['test_id'])
  

# COMMAND ----------

@suite.add_test

def test_ensure_dars_date_from_str():
  
  df_input = spark.createDataFrame([
    (1, '1111', '19600105', '20200102'), 
    (2, '2222', '19600105', '20210102'), 
    (3, '3333', '19600105', '20220102')
  ], ['test_id', 'test_nhs_number', 'test_dob', 'test_dod']
  )
  
  df_expected = spark.createDataFrame([
    (1, '1111', date(1960,1,5), date(2020,1,2)),
    (2, '2222', date(1960,1,5), date(2021,1,2)),
    (3, '3333', date(1960,1,5), date(2022,1,2)),
  ],
    schema = (
      StructType([
        StructField("test_id",LongType(),True),
        StructField("test_nhs_number",StringType(),True),
        StructField("test_dob",DateType(),True),
        StructField("test_dod",DateType(),True)
      ])
    )
  )
  df_actual = convert_dars_dates(
    df = df_input, 
    date_fields = ['test_dob', 'test_dod']
    )
  
  assert compare_results(df_actual, df_expected, join_columns=['test_id'])
  

# COMMAND ----------

@suite.add_test

def test_filter_dars_dates():
  test_start_date = date(1960,1,5)
  test_end_date = date(2022,1,5)
  
  df_input = spark.createDataFrame([
    (1, '1111', '19590105', '20200102'), 
    (2, '2222', '19600105', '20220202'), # dod too late
    (3, '3333', '19600105', '19600104'), # dod < dob
    (4, '4444', '19600105', '20200102'),
    (5, '5555', '19400105', '19590105'), #dod too early
  ], ['test_id', 'test_nhs_number', 'test_dob', 'test_dod']
  )
  
  df_expected = spark.createDataFrame([
    (1, '1111', date(1959,1,5), date(2020,1,2)),
    (4, '4444', date(1960,1,5), date(2020,1,2)),
  ],
    schema = (
      StructType([
        StructField("test_id",LongType(),True),
        StructField("test_nhs_number",StringType(),True),
        StructField("test_dob",DateType(),True),
        StructField("test_dod",DateType(),True)
      ])
    )
  )
  df_actual = filter_dars_dates(
    df = df_input,
    start_date = test_start_date,
    end_date = test_end_date,
    dob_field = 'test_dob',
    dod_field = 'test_dod'
    )
  
  assert compare_results(df_actual, df_expected, join_columns=['test_id'])
  

# COMMAND ----------

@suite.add_test

def test_deduplication_dars():
  df_input = spark.createDataFrame([
    (1, '1111', date(1960,2,1), date(2021,10,5), 'J132,H364', '123'), 
    (2, '2222', date(1960,2,1), date(2021,10,5), 'J132,H364', '123'), # earliest
    (3, '2222', date(1960,2,1), date(2021,10,6), 'J132,H363', '123'), 
    (4, '3333', date(1960,2,1), date(2021,10,5), 'J132,H364', '23'), # earliest
    (5, '3333', date(1960,2,1), date(2021,10,6), 'J132,H364', '123'),
  ], ['test_id', 'test_nhs_number', 'test_dob', 'test_dod', 'test_code_array', 'test_underlying_code']
  )
  
  df_expected = spark.createDataFrame([
    (1, '1111', date(1960,2,1), date(2021,10,5), 'J132,H364', '123'), 
    (2, '2222', date(1960,2,1), date(2021,10,5), 'J132,H364', '123'), 
    (4, '3333', date(1960,2,1), date(2021,10,5), 'J132,H364', '23'), 
  ], ['test_id', 'test_nhs_number', 'test_dob', 'test_dod', 'test_code_array', 'test_underlying_code']
  )

  df_actual = dedup_dars(
      df=df_input,
      partition_field='test_nhs_number',
      order_field='test_dod'
      )

  assert compare_results(df_actual, df_expected, join_columns=['test_id'])

# COMMAND ----------

@suite.add_test

def test_preprocess_outcomes_flag():

  test_cause_of_death_map = {
    'STROKE' : ['A12', 'A13', 'A14'],
    'HEARTATTACK' : ['A08', 'A09', 'A10'],
    'CVD_OTHER': ['A11']
  }
  
  df_input = spark.createDataFrame([
    (1, '1111', 'X,Y', 'X'), # no relevant codes
    (2, '2222', 'X,Y', 'A11'), # CVD underlying 
    (3, '3333', 'X,A11', 'A11'), # CVD underlying and code array
    (4, '4444', 'A11,X', 'Y'), #CVD not underlying but in code array
    (5, '5555', 'X,Y', 'A12'), #STROKE underlying
    (6, '6666', 'A12,Y', 'A11'), #STROKE in code array, CVD underlying
    (7, '7777', 'X,A11', 'A12'), #STROKE underlying, CVD in code array
    (8, '8888', 'X,A12', 'A13'), #STROKE underlying, other STROKE in code array
    (9, '9999', 'X,A12', 'A12'), #STROKE underlying & in code array
    (10, '0010', 'X,A12', 'Y'), #STROKE in code array, not underlying
    (11, '0011', 'A08,X,A11', 'X'), #MI & CVD in code array, not underlying
    (12, '0012', 'A08,X,Y', 'A12'), #MI in code array, STROKE underlying
  ], ['test_id', 'test_nhs_number', 'test_code_array', 'test_underlying_code']
  )
  
  df_expected = spark.createDataFrame([
    (1, '1111', 'X,Y', 'X', 'NON_CVD', 0, 0, 0), # no relevant codes
    (2, '2222', 'X,Y', 'A11', 'CVD_OTHER', 0, 0, 0), # CVD underlying 
    (3, '3333', 'X,A11', 'A11', 'CVD_OTHER', 0, 0, 0), # CVD underlying and code array
    (4, '4444', 'A11,X', 'Y', 'NON_CVD', 1, 0, 0), #CVD not underlying but in code array
    (5, '5555', 'X,Y', 'A12', 'STROKE', 0, 0, 0), #STROKE underlying
    (6, '6666', 'A12,Y', 'A11', 'CVD_OTHER', 0, 1, 0), #STROKE in code array, CVD underlying
    (7, '7777', 'X,A11', 'A12', 'STROKE', 1, 0, 0), #STROKE underlying, CVD in code array
    (8, '8888', 'X,A12', 'A13', 'STROKE', 0, 0, 0), #STROKE underlying, other STROKE in code array
    (9, '9999', 'X,A12', 'A12', 'STROKE', 0, 0, 0), #STROKE underlying & in code array
    (10, '0010', 'X,A12', 'Y', 'NON_CVD', 0, 1, 0), #STROKE in code array, not underlying
    (11, '0011', 'A08,X,A11', 'X', 'NON_CVD', 1, 0, 1), #MI & CVD in code array, not underlying
    (12, '0012', 'A08,X,Y', 'A12', 'STROKE', 0, 0, 1), #MI in code array, STROKE underlying
  ], schema = (
      StructType([
        StructField('test_id',LongType(),True),
        StructField('test_nhs_number',StringType(),True),
        StructField('test_code_array',StringType(),True),
        StructField('test_underlying_code',StringType(),True),
        StructField('test_flag',StringType(),True),
        StructField('test_flag_assoc_cvd_other',IntegerType(),True),
        StructField('test_flag_assoc_stroke',IntegerType(),True),
        StructField('test_flag_assoc_heartattack',IntegerType(),True),
      ]))
  )
  
  df_actual = create_cod_flags(
    df=df_input, 
    flag_field='test_flag',
    code_map=test_cause_of_death_map, 
    underlying_code_field='test_underlying_code', 
    comorbs_field='test_code_array',
    assoc_cod_flags=True
  )
  
  assert compare_results(df_actual, df_expected, join_columns=['test_id'])


# COMMAND ----------

suite.run()

# COMMAND ----------

dbutils.notebook.exit('pass')
