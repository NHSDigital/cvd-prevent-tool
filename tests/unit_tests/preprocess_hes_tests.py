# Databricks notebook source
# MAGIC %run ../../src/test_helpers

# COMMAND ----------

# MAGIC %run ../../src/clean_dataset

# COMMAND ----------

PARAMS_PATH = 'default'

# COMMAND ----------

# MAGIC %run ../../params/params

# COMMAND ----------

# MAGIC %run ../../src/hes/preprocess_hes

# COMMAND ----------

from uuid import uuid4
from datetime import datetime
import pyspark.sql.types as T

from dsp.validation.validator import compare_results

# COMMAND ----------

suite = FunctionTestSuite()

# COMMAND ----------

@suite.add_test
def test_clean_code_field_level_3():

  df_input = spark.createDataFrame([
    (0, '111', '11111', 'I16'),
    (1, '222', '22222', 'I161'),
    (2, '333', '33333', 'I1'),
    (3, '444', '44444', None)
  ], ['idx', 'EPIKEY',  'NEWNHSNO', 'DIAG_3_01'])

  df_expected = spark.createDataFrame([
    (0, '111', '11111', 'I16'),
    (1, '222', '22222', 'I16'),
    (2, '333', '33333', 'I1'),
  ], ['idx', 'EPIKEY',  'NEWNHSNO', 'DIAG_3_01'])

  df_actual = clean_code_field(df = df_input, code_field = 'DIAG_3_01')
  assert compare_results(df_actual, df_expected, join_columns = ['idx'])

# COMMAND ----------

@suite.add_test
def test_clean_code_field_level_3_ae():

  df_input = spark.createDataFrame([
    (0, '111', '11111', 'I16'),
    (1, '222', '22222', 'I161'),
    (2, '333', '33333', 'I1'),
    (3, '444', '44444', None)
  ], ['idx', 'EPIKEY',  'NEWNHSNO', 'DIAG3_01'])

  df_expected = spark.createDataFrame([
    (0, '111', '11111', 'I16'),
    (1, '222', '22222', 'I16'),
    (2, '333', '33333', 'I1'),
  ], ['idx', 'EPIKEY',  'NEWNHSNO', 'DIAG3_01'])

  df_actual = clean_code_field(df = df_input, code_field = 'DIAG3_01', prefix='')
  assert compare_results(df_actual, df_expected, join_columns = ['idx'])

# COMMAND ----------

@suite.add_test
def test_clean_code_field_level_4():

  df_input = spark.createDataFrame([
    (0, '111', '11111', 'I16'),
    (1, '222', '22222', 'I161'),
    (2, '333', '33333', 'I1'),
    (3, '444', '44444', None),
    (4, '555', '55555', 'I1613'),
    (5, '666', '66666', 'R69X')
  ], ['idx', 'EPIKEY',  'NEWNHSNO', 'DIAG_4_01'])

  df_expected = spark.createDataFrame([
    (0, '111', '11111', 'I16', 'I16'),
    (1, '222', '22222', 'I16', 'I161'),
    (2, '333', '33333', 'I1', 'I1'),
    (4, '555', '55555', 'I16', 'I161')
  ], ['idx', 'EPIKEY',  'NEWNHSNO', 'DIAG_4_01', 'DIAG_4_01_temp'])

  df_actual = clean_code_field(df = df_input, code_field = 'DIAG_4_01')
  assert compare_results(df_actual, df_expected, join_columns = ['idx'])

# COMMAND ----------

@suite.add_test
def test_select_hes_primary_coded_events():

  HES_ICD10_CODES_MAP = {'HEARTATTACK': ['I21','I22']}

  df_input = spark.createDataFrame([
    (0, '111', '11111', 'I21'),
    (1, '222', '22222', 'I22'),
    (2, '333', '33333', 'I222'),
    (3, '444', '44444', 'J22'),
    (4, '555', '55555', 'FOO'),
    (5, '666', '66666', None)
  ], ['idx', 'EPIKEY',  'NEWNHSNO', 'DIAG_3_01'])

  df_expected = spark.createDataFrame([
    (0, '111', '11111', 'I21'),
    (1, '222', '22222', 'I22'),
  ], ['idx', 'EPIKEY',  'NEWNHSNO', 'DIAG_3_01'])

  df_actual = select_hes_primary_coded_events(df = df_input, code_field = 'DIAG_3_01')
  assert compare_results(df_actual, df_expected, join_columns = ['idx'])

# COMMAND ----------

@suite.add_test
def test_create_array_from_code_list():

  df_input = spark.createDataFrame([
    (0, '111', '11111', 'I21', 'I21,J32 , I23, FOO'),
    (1, '222', '22222', 'I22', 'I22,FOO'),
    (2, '333', '33333', 'I22', 'I22, I22, FOO'),
    (3, '444', '44444', 'J22', 'J22,J23, J24'),
    (4, '555', '55555', 'FOO', 'FOO'),
    (5, '666', '66666', None, None)
  ], ['idx', 'EPIKEY',  'NEWNHSNO', 'DIAG_3_01', 'DIAG_3_CONCAT'])

  df_expected = spark.createDataFrame([
    (0, '111', '11111', 'I21', ['I21','J32','I23','FOO']),
    (1, '222', '22222', 'I22', ['I22','FOO']),
    (2, '333', '33333', 'I22', ['I22','I22','FOO']),
    (3, '444', '44444', 'J22', ['J22','J23','J24']),
    (4, '555', '55555', 'FOO', ['FOO']),
    (5, '666', '66666', None, None)
  ], ['idx', 'EPIKEY',  'NEWNHSNO', 'DIAG_3_01', 'DIAG_3_CONCAT'])

  df_actual = create_array_from_code_list(df = df_input, code_list_field = 'DIAG_3_CONCAT')
  assert compare_results(df_actual, df_expected, join_columns = ['idx'])

# COMMAND ----------

@suite.add_test
def test_create_list_from_codes():

  input_schema = T.StructType([
    T.StructField('idx', T.IntegerType(), True),
    T.StructField('EPIKEY', T.StringType(), True),
    T.StructField('NEWNHSNO', T.StringType(), True),
    T.StructField('DIAG3_01', T.StringType(), True),
    T.StructField('DIAG3_02', T.StringType(), True),
    T.StructField('DIAG3_03', T.StringType(), True),
    T.StructField('DIAG3_04', T.StringType(), True),
    T.StructField('DIAG3_05', T.StringType(), True),
    T.StructField('DIAG3_06', T.StringType(), True),
    T.StructField('DIAG3_07', T.StringType(), True),
    T.StructField('DIAG3_08', T.StringType(), True),
    T.StructField('DIAG3_09', T.StringType(), True),
    T.StructField('DIAG3_10', T.StringType(), True),
    T.StructField('DIAG3_11', T.StringType(), True),
    T.StructField('DIAG3_12', T.StringType(), True),
  ])

  expected_schema = T.StructType([
    T.StructField('idx', T.IntegerType(), True),
    T.StructField('EPIKEY', T.StringType(), True),
    T.StructField('NEWNHSNO', T.StringType(), True),
    T.StructField('DIAG3_CONCAT', T.StringType(), True)
  ])

  df_input = spark.createDataFrame([
    (0, '111', '11111', 'I21', 'J32', 'I23', 'FOO', None, None, None, None, None, None, None, None),
    (1, '222', '22222', 'I22', 'FOO', None, None, None, None, None, None, None, None, None, None),
    (2, '333', '33333', 'I22', 'I22', 'FOO', None, None, None, None, None, None, None, None, None),
    (3, '444', '44444', 'J22', 'J23', 'J24', None, None, None, None, None, None, None, None, None),
    (4, '555', '55555', 'FOO', None, None, None, None, None, None, None, None, None, None, None),
    (5, '666', '66666', None, None, None, None, None, None, None, None, None, None, None, None)
  ], input_schema)

  df_expected = spark.createDataFrame([
    (0, '111', '11111', 'I21,J32,I23,FOO'),
    (1, '222', '22222', 'I22,FOO'),
    (2, '333', '33333', 'I22,I22,FOO'),
    (3, '444', '44444', 'J22,J23,J24'),
    (4, '555', '55555', 'FOO'),
    (5, '666', '66666', '')
  ], expected_schema)

  df_actual = create_list_from_codes(df = df_input, code_col_prefix = 'DIAG3_', list_col_name='DIAG3_CONCAT')

  # Remove all the code columns for ease of comparison
  df_actual = df_actual.drop(*[f'DIAG3_{str(i).zfill(2)}' for i in range(1, 13)])
  assert compare_results(df_actual, df_expected, join_columns = ['idx'])

# COMMAND ----------

@suite.add_test
def test_create_outcomes_flag():

  df_input = spark.createDataFrame([
    (0, '111', '11111', 'I61', []),
    (1, '222', '22222', 'I63', []),
    (2, '333', '33333', 'I64', []),
    (3, '444', '44444', 'I21', []),
    (4, '555', '55555', 'I22', []),
    (5, '666', '66666', 'FOO', []),
    (6, '777', '77777', None , []),
    (7, '888', '88888', None , ['I61']),
    (8, '999', '99999', None , []),
  ], ['idx', 'epikey',  'newnhsno','DIAG_4_01','DIAG_4_CONCAT'])

  df_expected = spark.createDataFrame([
    (0, '111', '11111', 'I61', [], 'STROKE'),
    (1, '222', '22222', 'I63', [], 'STROKE'),
    (2, '333', '33333', 'I64', [], 'STROKE'),
    (3, '444', '44444', 'I21', [], 'HEARTATTACK'),
    (4, '555', '55555', 'I22', [], 'HEARTATTACK'),
    (5, '666', '66666', 'FOO', [], 'NO_CVD'),
    (6, '777', '77777', None , [], 'NO_CVD'),
    (7, '888', '88888', None , ['I61'], 'CVD_NON_PRIMARY'),
    (8, '999', '99999', None , [], 'NO_CVD')
  ], ['idx', 'epikey',  'newnhsno', 'DIAG_4_01','DIAG_4_CONCAT', 'flag'])

  df_actual = create_outcomes_flag(df = df_input, code_field = 'DIAG_4_01')

  assert compare_results(df_actual, df_expected, join_columns = ['idx'])

# COMMAND ----------

@suite.add_test
def test_removing_temp_ICD_cols():

  df_input = spark.createDataFrame([
    (0, '111', '11111', 'I16', 'I16'),
    (1, '222', '22222', 'I16', 'I161'),
    (2, '333', '33333', 'I1', 'I1'),
    (3, '444', '44444', None, None),
    (4, '555', '55555', 'I16', 'I161')
  ], ['idx', 'EPIKEY',  'NEWNHSNO', 'DIAG_4_01', 'DIAG_4_01_temp'])

  df_expected = spark.createDataFrame([
    (0, '111', '11111', 'I16'),
    (1, '222', '22222', 'I161'),
    (2, '333', '33333', 'I1'),
    (3, '444', '44444', None),
    (4, '555', '55555', 'I161')
  ], ['idx', 'EPIKEY',  'NEWNHSNO', 'DIAG_4_01'])

  df_actual = clean_temp_icd_length_4_cols(df_input, "DIAG_4_01")
  assert compare_results(df_actual, df_expected, join_columns = ['idx'])


# COMMAND ----------

@suite.add_test
def test_removing_temp_ICD_cols_with_no_temp_cols():

  df_input = spark.createDataFrame([
    (0, '111', '11111', 'I16'),
    (1, '222', '22222', 'I161'),
    (2, '333', '33333', 'I1'),
    (3, '444', '44444', None),
    (4, '555', '55555', 'I16')
  ], ['idx', 'EPIKEY',  'NEWNHSNO', 'DIAG_3_01'])

  df_expected = spark.createDataFrame([
    (0, '111', '11111', 'I16'),
    (1, '222', '22222', 'I161'),
    (2, '333', '33333', 'I1'),
    (3, '444', '44444', None),
    (4, '555', '55555', 'I16')
  ], ['idx', 'EPIKEY',  'NEWNHSNO', 'DIAG_3_01'])

  df_actual = clean_temp_icd_length_4_cols(df_input, "DIAG_3_01")
  assert compare_results(df_actual, df_expected, join_columns = ['idx'])


# COMMAND ----------

@suite.add_test
def test_filter_array_of_diag_codes_one_code():
  df_input = spark.createDataFrame([
    (0, ['U2']),
    (1, ['U1']),
    (2, ['U1', 'U2', 'U3']),
    (3, ['U2b']),
    (4, ['U26']),
    (5, ['U1', 'U2b', 'U3']),
    (6, ['U1', 'U24', 'U3']),
    (7, ['U1', 'UU2', 'U3']),
    (8, ['U1', '5U2', 'U3']),
    (9, ['U2', None]),
    (10, ['U2', '']),
    (11, [None]),
    (12, ['']),
  ], ['idx', 'arr_col'])

  df_expected = spark.createDataFrame([
    (0, ['U2']),
    (2, ['U1', 'U2', 'U3']),
    (4, ['U26']),
    (6, ['U1', 'U24', 'U3']),
    (9, ['U2', None]),
    (10, ['U2', '']),
  ], ['idx', 'arr_col'])

  code_list = ['U2']

  df_actual = filter_array_of_diag_codes(df_input, 'arr_col', code_list)

  assert compare_results(df_actual, df_expected, join_columns=['idx'])

# COMMAND ----------

@suite.add_test
def test_filter_array_of_diag_codes_multiple_codes():
  df_input = spark.createDataFrame([
    (0, ['U2']),
    (1, ['U1']),
    (2, ['U1', 'U2', 'U3']),
    (3, ['U2b']),
    (4, ['U26']),
    (5, ['U1', 'U2b', 'U3']),
    (6, ['U1', 'U24', 'U3']),
    (7, ['U1', 'UU2', 'U3']),
    (8, ['U1', '5U2', 'U3']),
    (9, ['U2', None]),
    (10, ['U2', '']),
    (11, [None]),
    (12, ['']),
    (13, ['U4']),
    (14, ['U1', 'U4', 'U3']),
    (15, ['U1', 'U3', 'U44']),
    (16, ['U4', None]),
    (17, ['U4', '']),
    (18, ['U4', 'U2']),
    (19, ['U4', 'U1', 'U2']),
  ], ['idx', 'arr_col'])

  df_expected = spark.createDataFrame([
    (0, ['U2']),
    (2, ['U1', 'U2', 'U3']),
    (4, ['U26']),
    (6, ['U1', 'U24', 'U3']),
    (9, ['U2', None]),
    (10, ['U2', '']),
    (13, ['U4']),
    (14, ['U1', 'U4', 'U3']),
    (15, ['U1', 'U3', 'U44']),
    (16, ['U4', None]),
    (17, ['U4', '']),
    (18, ['U4', 'U2']),
    (19, ['U4', 'U1', 'U2']),
  ], ['idx', 'arr_col'])

  code_list = ['U2', 'U4']

  df_actual = filter_array_of_diag_codes(df_input, 'arr_col', code_list)

  assert compare_results(df_actual, df_expected, join_columns=['idx'])


# COMMAND ----------

@suite.add_test
def test_calculate_spell_dates():

  df_input = spark.createDataFrame([
    (0,'000','10000',date(2000,1,1),date(2000,2,1)),
    (1,'000','10000',date(2001,1,1),date(2001,2,1)),
    (2,'000','10000',date(2002,1,1),date(2002,2,1)),
    (3,'000','10000',date(1800,1,1),date(2003,2,1)),
    (4,'111','20000',date(2022,1,1),date(2023,1,1)),
    (5,'111','20000',None,date(2023,1,1)),
    (6,'111','20000',date(2022,2,1),date(2023,1,1)),
    (7,'222','30000',date(2021,1,1),date(1801,1,1)),
    (8,'222','30000',date(2022,1,1),date(2023,1,1)),
    (9,'222','30000',date(1801,1,1),None),
    (10,'333','40000',date(2022,1,1),date(2023,1,1)),
    (11,'333','40000',None,None),
    (12,'333','40000',date(2022,1,1),date(2024,1,1)),
    (13,'444','50000',date(2022,1,1),date(2022,1,1)),
  ],['idx','pid','spellid','admitdate','disdate'])

  df_expected = spark.createDataFrame([
    (0,'000','10000',date(2000,1,1),date(2000,2,1), date(2000,1,1), date(2003,2,1)),
    (1,'000','10000',date(2001,1,1),date(2001,2,1), date(2000,1,1), date(2003,2,1)),
    (2,'000','10000',date(2002,1,1),date(2002,2,1), date(2000,1,1), date(2003,2,1)),
    (3,'000','10000',date(9999,1,1),date(2003,2,1), date(2000,1,1), date(2003,2,1)),
    (4,'111','20000',date(2022,1,1),date(2023,1,1), date(2022,1,1), date(2023,1,1)),
    (5,'111','20000',date(9999,1,1),date(2023,1,1), date(2022,1,1), date(2023,1,1)),
    (6,'111','20000',date(2022,2,1),date(2023,1,1), date(2022,1,1), date(2023,1,1)),
    (7,'222','30000',date(2021,1,1),date(1801,1,1), date(2021,1,1), date(2023,1,1)),
    (8,'222','30000',date(2022,1,1),date(2023,1,1), date(2021,1,1), date(2023,1,1)),
    (9,'222','30000',date(9999,1,1),None, date(2021,1,1), date(2023,1,1)),
    (10,'333','40000',date(2022,1,1),date(2023,1,1), date(2022,1,1), date(2024,1,1)),
    (11,'333','40000',date(9999,1,1),None, date(2022,1,1), date(2024,1,1)),
    (12,'333','40000',date(2022,1,1),date(2024,1,1), date(2022,1,1), date(2024,1,1)),
    (13,'444','50000',date(2022,1,1),date(2022,1,1), date(2022,1,1),date(2022,1,1)),
  ],['idx','pid','spellid','admitdate','disdate','spellstart','spellend'])

  df_actual = calculate_spell_dates(df_input, start_date_col = 'admitdate', end_date_col = 'disdate',
                                    spell_id_col = 'spellid',
                                    date_filter_list = ['1800-01-01','1801-01-01','9999-01-01'],
                                    date_replace_str = '9999-01-01', spell_startdate_col = 'spellstart',
                                    spell_enddate_col = 'spellend')

  assert compare_results(df_actual,df_expected, join_columns = ['idx'])


# COMMAND ----------

@suite.add_test
def test_clean_hes_spell_dates():

  input_clean_fields = ['date_1','date_2']

  date_remove_list = ['1800-01-01','1801-01-01','9999-01-01']

  df_input = spark.createDataFrame([
    (0,'000',date(2000,1,1),date(2000,2,1)),
    (1,'111',date(2000,1,1),date(1801,1,1)),
    (2,'222',date(1800,1,1),date(2000,2,1)),
    (3,'333',date(9999,1,1),date(2000,2,1)),
    (4,'444',date(1801,1,1),date(9999,1,1)),
    (6,'666',None,None),
    (7,'777',date(2000,1,1),None),
    (8,'888',None,date(2000,2,1)),
  ],['idx','pid','date_1','date_2'])

  df_expected = spark.createDataFrame([
    (0,'000',date(2000,1,1),date(2000,2,1)),
    (1,'111',date(2000,1,1),None),
    (2,'222',None,date(2000,2,1)),
    (3,'333',None,date(2000,2,1)),
    (4,'444',None,None),
    (6,'666',None,None),
    (7,'777',date(2000,1,1),None),
    (8,'888',None,date(2000,2,1)),
  ],['idx','pid','date_1','date_2'])

  df_actual = clean_hes_spell_dates(df = df_input,
                                    date_filter_list = date_remove_list,
                                    clean_fields_list = input_clean_fields)

  assert compare_results(df_actual,df_expected, join_columns = ['idx'])


# COMMAND ----------

@suite.add_test
def test_clean_hes_spell_id():

  id_filter_values = '-1'

  df_input = spark.createDataFrame([
    (0, '001', '123456789'),
    (1, '002', '-1'),
    (2, '003', None)
  ], ['idx','pid','spell_id'])

  df_expected = spark.createDataFrame([
    (0, '001', '123456789'),
    (1, '002', None),
    (2, '003', None)
  ], ['idx','pid','spell_id'])

  df_actual = clean_hes_spell_id(df = df_input,
                                 spell_id_col = 'spell_id',
                                 spell_id_filter_values = id_filter_values)

  assert compare_results(df_actual,df_expected, join_columns = ['idx'])

# COMMAND ----------

suite.run()

# COMMAND ----------

dbutils.notebook.exit('pass')