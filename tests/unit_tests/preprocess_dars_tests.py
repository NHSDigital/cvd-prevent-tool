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
  """test_deduplication_dars() [src/dars/preprocess_dars::dedup_dars()]
  Tests the deduplication of the DARS DEATHS dataset as part of dars preprocessing. Multiple records should
  be deduplicated to keep the following records:
    1) Earliest date of death
    2) Latest date of registration of death
  If there are multiple records still present, then the record kept should:
    3) First (ascending) primary key record
  """
  
  # Setup Test Data
  ## Input Data
  df_input = spark.createDataFrame([
    # Single Record
    (0, '1111', date(1960,1,1), date(2021,10,5), date(2021,12,31), 'J132,H364', '123'), # Keep
    # Two Records: Earlier Death Date
    (1, '2222', date(1960,2,1), date(2021,10,5), date(2021,12,31), 'J132,H364', '123'), # Keep
    (2, '2222', date(1960,2,1), date(2021,10,6), date(2021,12,31), 'J132,H363', '456'), 
    # Two Records: Latest Registration Date
    (3, '3333', date(1960,3,1), date(2021,10,5), date(2021,12,30), 'J132,H364', '123'),
    (4, '3333', date(1960,3,1), date(2021,10,5), date(2021,12,31), 'J132,H364', '456'), # Keep
    # Three Records: Earlier Death Date, Latest Registration Date
    (5, '4444', date(1960,4,1), date(2021,10,5), date(2021,12,30), 'J132,H364', '123'),
    (6, '4444', date(1960,4,1), date(2021,10,5), date(2021,12,31), 'J132,H364', '456'), # Keep
    (7, '4444', date(1960,4,1), date(2021,10,6), date(2021,12,31), 'J132,H364', '789'),
    # Four Records: Multiple Identical, Ordering Differences, Ascending Primary Key
    (8, '5555', date(1960,4,1), date(2021,10,6), date(2021,12,31), 'J132,H364', '123'),
    (9, '5555', date(1960,4,1), date(2021,10,5), date(2021,12,30), 'J132,H364', '123'),
    (10, '5555', date(1960,4,1), date(2021,10,5), date(2021,12,31), 'J132,H364', '123'), # Keep
    (11, '5555', date(1960,4,1), date(2021,10,5), date(2021,12,31), 'J132,H364', '123'),
    # Three Records: All identical, Record ID ascending
    (12, '6666', date(1960,6,1), date(2021,10,6), date(2021,12,31), 'J123,K123', '123'), # Keep
    (13, '6666', date(1960,6,1), date(2021,10,6), date(2021,12,31), 'J124,K124', '123'),
    (14, '6666', date(1960,6,1), date(2021,10,6), date(2021,12,31), 'J125,K125', '123'),
    # Three Records: Identical information with differing registration dates, Ascending Primary Key
    (15, '7777', date(1960,7,1), date(2021,10,7), date(2021,12,30), 'J123,K123', '123'),
    (16, '7777', date(1960,7,1), date(2021,10,7), date(2021,12,31), 'J123,K123', '123'), # Keep
    (17, '7777', date(1960,7,1), date(2021,10,7), date(2021,12,31), 'J124,K124', '123'),
  ], ['test_id', 'test_nhs_number', 'test_dob', 'test_dod', 'test_reg_date', 'test_code_array', 'test_underlying_code']
  )
  ## Expected Data
  df_expected = spark.createDataFrame([
    (0, '1111', date(1960,1,1), date(2021,10,5), date(2021,12,31), 'J132,H364', '123'), # Keep
    (1, '2222', date(1960,2,1), date(2021,10,5), date(2021,12,31), 'J132,H364', '123'), # Keep
    (4, '3333', date(1960,3,1), date(2021,10,5), date(2021,12,31), 'J132,H364', '456'), # Keep
    (6, '4444', date(1960,4,1), date(2021,10,5), date(2021,12,31), 'J132,H364', '456'), # Keep
    (10, '5555', date(1960,4,1), date(2021,10,5), date(2021,12,31), 'J132,H364', '123'), # Keep
    (12, '6666', date(1960,6,1), date(2021,10,6), date(2021,12,31), 'J123,K123', '123'), # Keep
    (16, '7777', date(1960,7,1), date(2021,10,7), date(2021,12,31), 'J123,K123', '123'), # Keep
  ], ['test_id', 'test_nhs_number', 'test_dob', 'test_dod', 'test_reg_date', 'test_code_array', 'test_underlying_code']
  )

  df_actual = dedup_dars(
      df = df_input,
      partition_fields = ['test_nhs_number', 'test_dob'],
      death_field = 'test_dod',
      reg_date_field = 'test_reg_date',
      id_field = 'test_id',
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

@suite.add_test
def test_add_dars_record_id():
  '''
  test_add_dars_record_id [src/dars/preprocess_dars::add_dars_record_id()]

  Tests that the preexisting dars record ID column is renamed to associated record ID 
  and a unique record id is created from the hash of the patient NHS number and dob.
  '''

  df_input = spark.createDataFrame([
    (1, '1', '1111', date(2000,1,1)),
    (2, '2', '2222', date(2000,1,1)),
    (3, '3', '2222', date(1990,1,1)),
    (4, '4', '4444', date(1980,1,2)),
  ], ['idx', 'test_dars_id', 'test_pid', 'test_dob'])

  df_expected = spark.createDataFrame([
    (1, '14ae61127ad5681c91c04d16fcd076aab3621de09e062344c89d0358d2ab6de7', '1111', date(2000,1,1), '1'),
    (2, '1f6bc20556ab3863e8f0d67ef116e7ac1ee9ac30c2833cb03d3ccb63f4371cef', '2222', date(2000,1,1), '2'),
    (3, '10a347a4e33b64216e590fc6a2b2d2fa4720a91f4a2fd41bccd62276f6a24231', '2222', date(1990,1,1), '3'),
    (4, 'b59f93f27dc1581f409359ca5fd6c93c9cb8cf93ab6cbe3c5b888d5de70b3f44', '4444', date(1980,1,2), '4'),
  ], ['idx', 'record_id', 'test_pid', 'test_dob', 'test_assoc_id'])

  df_actual = add_dars_record_id(
        df = df_input,
        field_record_id='record_id',
        field_dars_id='test_dars_id',
        field_assoc_record_id='test_assoc_id',
        field_pid='test_pid',
        field_dob='test_dob'
  )

  assert compare_results(df_actual, df_expected, join_columns=['idx'])


# COMMAND ----------

@suite.add_test
def test_check_unique_identifiers_dars_pass():
    '''test_check_unique_identifiers_dars_pass [src/dars/preprocess_dars::check_unique_identifiers_dars()]
    Tests hash id checking of hash id column in preprocessed dars. Test will
    pass if no error is raised (all values unique).
    '''

    df_input = spark.createDataFrame([
        (1,'a1'),
        (2,'a2'),
        (3,'a3')
    ], ['idx', 'record_id'])


    try:
        check_unique_identifiers_dars(
        df= df_input,
        record_id = 'record_id')
        return True
    except:
        raise Exception('test_check_unique_identifiers_dars_pass failed')

# COMMAND ----------

@suite.add_test
def test_check_unique_identifiers_dars_fail():
    '''test_check_unique_identifiers_dars_fail [src/dars/preprocess_dars::check_unique_identifiers_dars()]
    Tests hash id checking of hash id column in preprocessed dars. Test will
    pass if an error is raised ((presence of non-unique values).
    '''

    df_input = spark.createDataFrame([
        (1,'a1'),
        (2,'a2'),
        (3,'a2')
    ], ['idx', 'record_id'])


    try:
        check_unique_identifiers_dars(
        df= df_input,
        record_id = 'record_id')
        raise Exception('test_check_unique_identifiers_dars_pass failed')
    except:
        return True

# COMMAND ----------

suite.run()

# COMMAND ----------

dbutils.notebook.exit('pass')