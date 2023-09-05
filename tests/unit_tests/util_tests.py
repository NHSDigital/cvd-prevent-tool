# Databricks notebook source
# MAGIC %run ../../src/util

# COMMAND ----------

# MAGIC %run ../../src/test_helpers

# COMMAND ----------

from uuid import uuid4
from datetime import date, datetime

from pyspark.sql.functions import col, lit, when
from pyspark.sql.types import StructType, StructField, TimestampType, IntegerType, StringType, DateType, ShortType, LongType, DoubleType, ArrayType

from dsp.validation.validator import compare_results

from pyspark.sql.functions import lit, rank
from pyspark.sql.window import Window

# COMMAND ----------

suite = FunctionTestSuite()

## The following are fake people and data created for test purposes

# COMMAND ----------

@suite.add_test
def test_table_exists():
  df_input = spark.createDataFrame([(1, 2,)], ['v1', 'v2'])
  input_name = f'_tmp_{uuid4().hex}'
  df_input.createOrReplaceGlobalTempView(input_name)

  assert table_exists('global_temp', input_name)

  assert table_exists('global_temp', f'_tmp_{uuid4().hex}') is False

# COMMAND ----------

@suite.add_test
def test_drop_table():
  df_input = spark.createDataFrame([(1, 2,)], ['v1', 'v2'])
  input_name = f'_tmp_{uuid4().hex}'
  df_input.createOrReplaceGlobalTempView(input_name)

  assert table_exists('global_temp', input_name)

  drop_table('global_temp', input_name)

  assert table_exists('global_temp', input_name) is False

# COMMAND ----------

@suite.add_test
def test_create_table():
  input_name = f'_tmp_{uuid4().hex}'
  owner_db = 'prevent_tool_collab'
  asset_name = f'{owner_db}.{input_name}'
  df_input = spark.createDataFrame([(1, 2,)], ['v1', 'v2'])

  try:
    create_table(df_input, owner_db, input_name, overwrite=True)
    assert table_exists(owner_db, input_name)

    assert spark.sql(f'SHOW GRANT ON {asset_name}').where(col('ActionType') == lit('OWN')).where(col('Principal') == lit(owner_db)).count() == 1

  finally:
    drop_table(owner_db, input_name)

# COMMAND ----------

@suite.add_test
def test_check_type():
  input_schema = StructType([
      StructField('dob', DateType(), True),
      StructField('timestamp', TimestampType(), True),
      StructField('age', ShortType(), True),
      StructField('code', IntegerType(), True),
      StructField('name', StringType(), True),
      StructField('meta', StructType([
        StructField('id', IntegerType(), True),
        StructField('date', DateType(), True),
      ]), True),
  ])

  df_input = spark.createDataFrame([
    (date(1980, 1, 1), datetime(2020, 1, 1, 13, 45, 34), 6, 465, 'Carl', (8, date(2021, 1, 1))),
  ], input_schema)

  assert check_type(df_input, 'dob', DateType) is None
  assert check_type(df_input, 'timestamp', TimestampType) is None
  assert check_type(df_input, 'age', ShortType) is None
  assert check_type(df_input, 'code', IntegerType) is None
  assert check_type(df_input, 'name', StringType) is None
  assert check_type(df_input, 'dob', [DateType, TimestampType]) is None
  assert check_type(df_input, 'timestamp', [DateType, TimestampType]) is None
  assert check_type(df_input, 'age', [ShortType, IntegerType]) is None
  assert check_type(df_input, 'code', [ShortType, IntegerType, DateType]) is None
  assert check_type(df_input, 'name', [StringType]) is None
  assert check_type(df_input, 'meta', [StructType]) is None
  assert check_type(df_input, 'meta.id', [IntegerType]) is None
  assert check_type(df_input, 'meta.date', [DateType]) is None

  try:
    check_type(df_input, 'dob', TimestampType)
    assert False
  except TypeError:
    pass
  try:
    check_type(df_input, 'timestamp', DateType)
    assert False
  except TypeError:
    pass
  try:
    check_type(df_input, 'code', [ShortType, StringType])
    assert False
  except TypeError:
    pass
  try:
    check_type(df_input, 'name', [TimestampType, DateType])
    assert False
  except TypeError:
    pass
  try:
    check_type(df_input, 'meta', [TimestampType, DateType])
    assert False
  except TypeError:
    pass
  try:
    check_type(df_input, 'meta.id', [TimestampType, DateType])
    assert False
  except TypeError:
    pass
  try:
    check_type(df_input, 'meta.date', [StringType])
    assert False
  except TypeError:
    pass

  assert is_of_type(df_input, 'dob', DateType) is True
  assert is_of_type(df_input, 'timestamp', TimestampType) is True
  assert is_of_type(df_input, 'age', ShortType) is True
  assert is_of_type(df_input, 'code', IntegerType) is True
  assert is_of_type(df_input, 'name', StringType) is True
  assert is_of_type(df_input, 'dob', [DateType, TimestampType]) is True
  assert is_of_type(df_input, 'timestamp', [DateType, TimestampType]) is True
  assert is_of_type(df_input, 'age', [ShortType, IntegerType]) is True
  assert is_of_type(df_input, 'code', [ShortType, IntegerType, DateType]) is True
  assert is_of_type(df_input, 'name', [StringType]) is True
  assert is_of_type(df_input, 'meta', [StructType]) is True
  assert is_of_type(df_input, 'meta.id', [IntegerType]) is True
  assert is_of_type(df_input, 'meta.date', [DateType]) is True

  assert is_of_type(df_input, 'dob', TimestampType) is False
  assert is_of_type(df_input, 'timestamp', DateType) is False
  assert is_of_type(df_input, 'code', [ShortType, StringType]) is False
  assert is_of_type(df_input, 'name', [TimestampType, DateType]) is False
  assert is_of_type(df_input, 'meta', [TimestampType, DateType]) is False
  assert is_of_type(df_input, 'meta.id', [TimestampType, DateType]) is False
  assert is_of_type(df_input, 'meta.date', [TimestampType]) is False

  try:
    is_of_type(df_input, 'dob', TimestampType())
    assert False
  except AttributeError:
    pass

# COMMAND ----------

@suite.add_test
def test_rolling_average():
  df_input = spark.createDataFrame([
    (date(2020, 2, 1), '1'),
    (date(2020, 2, 2), '2'),
    (date(2020, 2, 2), '3'),
    (date(2020, 2, 3), '4'), 
    (date(2020, 2, 3), '5'), 
    (date(2020, 2, 3), '6'), 
    (date(2020, 2, 4), '7'), 
    (date(2020, 2, 4), '8'), 
    (date(2020, 2, 4), '9'), 
    (date(2020, 2, 4), '10'),
    (date(2020, 2, 5), '11'), 
    (date(2020, 2, 5), '12'), 
    (date(2020, 2, 5), '13'), 
    (date(2020, 2, 5), '14'),
    (date(2020, 2, 5), '15'),
    (date(2020, 2, 6), '16'),
    (date(2020, 2, 6), '17'),
    (date(2020, 2, 6), '18'),
    (date(2020, 2, 6), '19'),
    (date(2020, 2, 6), '20'),
    (date(2020, 2, 6), '21'),
    (date(2020, 2, 7), '22'),
    (date(2020, 2, 7), '23'),
    (date(2020, 2, 7), '24'),
    (date(2020, 2, 7), '25'),
    (date(2020, 2, 7), '26'),
    (date(2020, 2, 7), '27'),
    (date(2020, 2, 7), '28'),
    (date(2020, 2, 8), '29'),
    (date(2020, 2, 8), '30'),
    (date(2020, 2, 8), '31'),
    (date(2020, 2, 8), '32'),
    (date(2020, 2, 8), '33'),
    (date(2020, 2, 8), '34'),
    (date(2020, 2, 8), '35'),
    (date(2020, 2, 8), '36'),
    (date(2020, 2, 9), '37'),
    (date(2020, 2, 9), '38'),
    (date(2020, 2, 9), '39'),
    (date(2020, 2, 9), '40'),
    (date(2020, 2, 9), '41'),
    (date(2020, 2, 9), '42'),
    (date(2020, 2, 9), '43'),
    (date(2020, 2, 9), '44'),
    (date(2020, 2, 9), '45'),
  ], ['date', 'event_id'])
  
  expected_schema = StructType([
        #StructField('index', StringType(), True),
        StructField('date', DateType(), True),
        StructField('count', LongType(), True),
        StructField('test_rolling_average', DoubleType(), True),
  ])
  
  df_expected = spark.createDataFrame([
    (date(2020, 2, 1), 1, 1.0),
    (date(2020, 2, 2), 2, 1.5),
    (date(2020, 2, 3), 3, 2.0),
    (date(2020, 2, 4), 4, 2.5),
    (date(2020, 2, 5), 5, 3.0),
    (date(2020, 2, 6), 6, 3.5),
    (date(2020, 2, 7), 7, 4.0),
    (date(2020, 2, 8), 8, 5.0),
    (date(2020, 2, 9), 9, 6.0),
  ], expected_schema)
  
  df_actual = rolling_average(df_input, 'date', 'test_rolling_average')
  
  assert compare_results(df_actual, df_expected, join_columns=['date'])

# COMMAND ----------

@suite.add_test
def test_filter_months():
  schema = StructType([
    StructField('test_nhs_number', StringType(), True),
    StructField('test_extract_date', DateType(), True),
    StructField('test_journal_date', DateType(), True),
  ])
  
  month_window = 12 # previous 12 months  
  df_input = spark.createDataFrame([
    ('1', date(2022, 1, 1), date(2021, 1, 1)),
    ('2', date(2022, 1, 1), date(2021, 1, 2)),
    ('3', date(2022, 1, 1), date(2020, 12, 31))
  ], schema)
  
  df_expected = spark.createDataFrame([
    ('2', date(2022, 1, 1), date(2021, 1, 2)),
  ], schema)
  
  df_actual = filter_months(df_input, 'test_extract_date', 'test_journal_date', 12, before=True)
  
  assert compare_results(df_actual, df_expected, join_columns=['test_nhs_number'])

# COMMAND ----------

@suite.add_test
def test_is_empty():
  
  schema = StructType([
    StructField('index', IntegerType(), True),
    StructField('val', StringType(), True),
  ])
  
  df_not_empty = spark.createDataFrame([
    (0, 'a'),
    (1, 'b'),
  ], schema)
  
  df_empty = spark.createDataFrame([], schema)
  
  assert is_empty(df_not_empty) is False
  assert is_empty(df_empty) is True  

# COMMAND ----------

@suite.add_test
def test_create_array_field():
  
  input_schema = StructType([
    StructField('test_nhs_number', StringType(), False),
    StructField('test_code_1', DoubleType(), True),
    StructField('test_code_2', DoubleType(), True),
  ])
  output_schema = StructType([
    StructField('test_nhs_number', StringType(), False),
    StructField('test_code_array', ArrayType(DoubleType(),True))
  ])
  
  df_input = spark.createDataFrame([
    ('1234', 127.5, 120.0),
    ('1235', 127.5, None),
    ('1236', None, 120.0),
    ('1237', None, None),
  ], input_schema)
  
  df_expected = spark.createDataFrame([
    ('1234', [127.5, 120.0]),
    ('1235', [127.5, None]),
    ('1236', [None, 120.0]),
    ('1237', [None, None])
  ], output_schema)
  
  df_actual = create_array_field(df_input, array_field_name='test_code_array', array_value_fields=['test_code_1','test_code_2'])
  
  assert compare_results(df_actual, df_expected, join_columns=['test_nhs_number'])

# COMMAND ----------

@suite.add_test
def test_create_array_field_multitype():
  
  input_schema = StructType([
    StructField('test_nhs_number', StringType(), False),
    StructField('test_code_1', StringType(), True),
    StructField('test_code_2', DoubleType(), True),
  ])
  
  df_input = spark.createDataFrame([
    ('1234', '127.5', 120.0),
    ('1235', '127.5', None),
    ('1236', None, 120.0),
    ('1237', None, None),
  ], input_schema)
    
  try: 
    create_array_field(df_input, array_field_name='test_code_array', array_value_fields=['test_code_1','test_code_2'])
    assert False
    raise AssertionError('Array function return TRUE - type error not raised - test failed')
  except TypeError:
    assert True
  

# COMMAND ----------

@suite.add_test
def test_read_csv_string_to_df():
  header = 'index,name,code'
  csv_data = header + '\n0,Carl,45643\n1,Paul,345345\n2,Paul,\n3,,345345\n4,,\n5,Paul,2395876329875698315769'

  df_expected = spark.createDataFrame([
    ('0', 'Carl', '45643'),
    ('1', 'Paul', '345345'),
    ('2', 'Paul', None),
    ('3', None, '345345'),
    ('4', None, None),
    ('5', 'Paul', '2395876329875698315769'),
  ], ['index', 'name', 'code'])

  df_actual = read_csv_string_to_df(csv_data)

  assert compare_results(df_actual, df_expected, join_columns=['index'])

# COMMAND ----------

@suite.add_test
def test_replace_value_none_single():
  
  date_col = 'date'
  date_replace = '9999-01-01'
  
  df_input = spark.createDataFrame([
    (0, '000', date(2022,1,1)),
    (1, '111', date(9999,1,1)),
    (2, '222', date(1800,1,1)),
    (3, '333', None)
  ], ['idx','pid','date'])
  
  df_expected = spark.createDataFrame([
    (0, '000', date(2022,1,1)),
    (1, '111', None),
    (2, '222', date(1800,1,1)),
    (3, '333', None)
  ], ['idx','pid','date'])
  
  df_actual = replace_value_none(df = df_input, col_names = date_col,
                                 filter_values = date_replace)
  
  assert compare_results(df_actual, df_expected, join_columns=['idx'])
  

# COMMAND ----------

@suite.add_test
def test_replace_value_none_multi():
  
  date_cols = ['date_1','date_2']
  date_replace_list = ['9999-01-01','1800-01-01']
  
  df_input = spark.createDataFrame([
    (0, '000', date(2022,1,1), date(2022,2,1)),
    (1, '111', date(9999,1,1), date(2023,2,1)),
    (2, '222', date(2023,1,1), date(1800,1,1)),
    (3, '333', date(9999,1,1), date(9999,1,1)),
    (4, '444', None, date(2024,2,1)),
    (5, '555', date(2024,2,1), None),
    (6, '666', None, None),
  ], ['idx','pid','date_1', 'date_2'])
  
  df_expected = spark.createDataFrame([
    (0, '000', date(2022,1,1), date(2022,2,1)),
    (1, '111', None, date(2023,2,1)),
    (2, '222', date(2023,1,1), None),
    (3, '333', None, None),
    (4, '444', None, date(2024,2,1)),
    (5, '555', date(2024,2,1), None),
    (6, '666', None, None),
  ], ['idx','pid','date_1', 'date_2'])
  
  df_actual = replace_value_none(df = df_input, col_names = date_cols,
                                 filter_values = date_replace_list)
  
  assert compare_results(df_actual, df_expected, join_columns=['idx'])

# COMMAND ----------

@suite.add_test
def test_ensure_short_type():
  input_schema = StructType([
    StructField('index', StringType(), True),
    StructField('i_int', IntegerType(), True),
    StructField('i_str', StringType(), True),
    StructField('i_long', LongType(), True),
    StructField('i_short', ShortType(), True),
  ])

  df_input = spark.createDataFrame([
    ('0', 0, '0', 0, 0),
    ('1', 1, '1', 1, 1),
    ('2', -1, '-1', -1, -1),
    ('3', 30, '30', 30, 30),
    ('4', None, None, None, None),
  ], input_schema)

  expected_schema = StructType([
    StructField('index', StringType(), True),
    StructField('i_int', ShortType(), True),
    StructField('i_str', ShortType(), True),
    StructField('i_long', ShortType(), True),
    StructField('i_short', ShortType(), True),
  ])

  df_expected = spark.createDataFrame([
    ('0', 0, 0, 0, 0),
    ('1', 1, 1, 1, 1),
    ('2', -1, -1, -1, -1),
    ('3', 30, 30, 30, 30),
    ('4', None, None, None, None),
  ], expected_schema)

  df = df_input
  df = ensure_short_type(df, 'i_int')
  df = ensure_short_type(df, 'i_str')
  df = ensure_short_type(df, 'i_long')
  df = ensure_short_type(df, 'i_short')

  df_actual = df

  assert compare_results(df_actual, df_expected, join_columns=['index'])
  

# COMMAND ----------

@suite.add_test
def test_ensure_dob():
  df_input = spark.createDataFrame([
    ('0', date(2000, 1, 1), date(2020, 1, 1)),
    ('1', date(1000, 1, 1), date(2020, 1, 1)),
    ('2', date(1890, 1, 1), date(2020, 2, 1)),
  ], ['index', 'dob', 'ts'])

  df_expected = spark.createDataFrame([
    ('0', date(2000, 1, 1), date(2020, 1, 1), date(2000, 1, 1)),
    ('1', date(1000, 1, 1), date(2020, 1, 1), None),
    ('2', date(1890, 1, 1), date(2020, 2, 1), None),
  ], ['index', 'dob', 'ts', 'res'])

  df_actual = ensure_dob(df_input, 'res', 'dob', 'ts', max_age_year = 130)

  assert compare_results(df_actual, df_expected, join_columns=['index'])

# COMMAND ----------

@suite.add_test
def test_ensure_dob_from_str():
  df_input = spark.createDataFrame([
    ('0', '2000-01-01T12:30:56.4440000Z', date(2020, 1, 1)),
    ('1', '1000-01-01T12:30:56.4440000Z', date(2020, 1, 1)),
    ('2', '1890-01-01T12:30:56.4440000Z', date(2020, 2, 1)),
    ('3', '2000-01-01 12:30:56', date(2020, 1, 1)),
  ], ['index', 'dob', 'ts'])

  df_expected = spark.createDataFrame([
    ('0', '2000-01-01T12:30:56.4440000Z', date(2020, 1, 1), date(2000, 1, 1)),
    ('1', '1000-01-01T12:30:56.4440000Z', date(2020, 1, 1), None),
    ('2', '1890-01-01T12:30:56.4440000Z', date(2020, 2, 1), None),
    ('3', '2000-01-01 12:30:56', date(2020, 1, 1), None),
  ], ['index', 'dob', 'ts', 'res'])

  df_actual = ensure_dob_from_str(df_input, 'res', 'dob', 'ts', str_format="yyyy-MM-dd'T'HH:mm:ss.SSS", end_trim=5, max_age_year = 130)

  assert compare_results(df_actual, df_expected, join_columns=['index'])

# COMMAND ----------

@suite.add_test
def test_ensure_age():
  input_schema = StructType([
    StructField('index', StringType(), True),
    StructField('age_int', IntegerType(), True),
    StructField('age_str', StringType(), True),
    StructField('age_short', ShortType(), True),
    StructField('age_long', LongType(), True),
  ])

  df_input = spark.createDataFrame([
    ('0', 56, '56', 56, 56),
    ('1', 0, '0', 0, 0),
    ('2', 1, '1', 1, 1),
    ('3', None, None, None, None),
    ('4', -1, '-1', -1, -1),
  ], input_schema)

  expected_schema = StructType([
    StructField('index', StringType(), True),
    StructField('age_int', ShortType(), True),
    StructField('age_str', ShortType(), True),
    StructField('age_short', ShortType(), True),
    StructField('age_long', ShortType(), True),
  ])

  df_expected = spark.createDataFrame([
    ('0', 56, 56, 56, 56),
    ('1', 0, 0, 0, 0),
    ('2', 1, 1, 1, 1),
    ('3', None, None, None, None),
    ('4', None, None, None, None),
  ], expected_schema)

  df = df_input
  df = ensure_age(df, 'age_int', 'age_int')
  df = ensure_age(df, 'age_str', 'age_str')
  df = ensure_age(df, 'age_short', 'age_short')
  df = ensure_age(df, 'age_long', 'age_long')
  df_actual = df

  assert compare_results(df_actual, df_expected, join_columns=['index'])

# COMMAND ----------

@suite.add_test
def test_add_age_from_dob_at_date():
  df_input = spark.createDataFrame([
    ('0', date(1980, 1, 1), date(2020, 1, 1)),
    ('1', date(1980, 1, 2), date(2019, 1, 1)),
    ('2', date(2020, 1, 2), date(2020, 1, 1)),
    ('3', None, date(2020, 1, 1)),
    ('4', date(2020, 1, 2), None),
    ('5', date(1020, 1, 2), date(2020, 1, 1)),
  ], ['index', 'dob', 'record_date'])

  expected_schema = StructType([
      StructField('index', StringType(), True),
      StructField('dob', DateType(), True),
      StructField('record_date', DateType(), True),
      StructField('_age', ShortType(), True),
  ])

  df_expected = spark.createDataFrame([
    ('0', date(1980, 1, 1), date(2020, 1, 1), 40),
    ('1', date(1980, 1, 2), date(2019, 1, 1), 38),
    ('2', date(2020, 1, 2), date(2020, 1, 1), None),
    ('3', None, date(2020, 1, 1), None),
    ('4', date(2020, 1, 2), None, None),
    ('5', date(1020, 1, 2), date(2020, 1, 1), None),
  ], expected_schema)

  df_actual = add_age_from_dob_at_date(df_input, '_age', 'dob', 'record_date')

  assert compare_results(df_actual, df_expected, join_columns=['index'])

# COMMAND ----------

@suite.add_test
def test_add_age_from_dob_at_date_bad_dob():
  ## CHECKING FOR TYPE: SHOULD BE DATE NOT STR
  df_input = spark.createDataFrame([
    ('0', '1980-01-01', date(2020, 1, 1)),
  ], ['index', 'dob', 'record_date'])

  try:
    add_age_from_dob_at_date(df_input, '_age', 'dob', 'record_date')
    assert False
  except TypeError:
    pass

# COMMAND ----------

@suite.add_test
def test_add_age_from_dob_at_date_bad_at_date():
  ## CHECKING FOR TYPE: SHOULD BE DATE NOT STR
  df_input = spark.createDataFrame([
    ('0', date(2020, 1, 1), '1980-01-01'),
  ], ['index', 'dob', 'record_date'])

  try:
    add_age_from_dob_at_date(df_input, '_age', 'dob', 'record_date')
    assert False
  except TypeError:
    pass

# COMMAND ----------

@suite.add_test
def test_split_str_to_array():
  
  input_schema = StructType([
    StructField('idx', IntegerType(), False),
    StructField('codes', StringType(), True),
  ])

  df_input = spark.createDataFrame([
    (0, 'A,B,C,D,E,F,G'),
    (1, 'A'),
    (2, 'A,B, ,C'),
    (3, 'A,B,,C'),
    (4, None)
  ], input_schema)

  expected_schema = StructType([
    StructField('idx', IntegerType(), False),
    StructField('codes', ArrayType(StringType()), True),
  ])

  df_expected = spark.createDataFrame([
    (0, ['A','B','C','D','E','F','G']),
    (1, ['A']),
    (2, ['A','B',' ','C']),
    (3, ['A','B','','C']),
    (4, None)
  ], expected_schema)

  df_actual = split_str_to_array(
    df = df_input,
    col_name = 'codes',
    delimiter = ','
  )

  assert compare_results(df_actual, df_expected, join_columns=['idx'])

# COMMAND ----------

@suite.add_test
def test_union_multiple_dfs():
  df_schema = StructType([
    StructField('idx', IntegerType(), False),
    StructField('codes', ArrayType(StringType()), True),
    StructField('dataset', StringType(), True),
  ])
  
  df1 = spark.createDataFrame([
    (0, ['A'],'df1'),
    (1, ['A'], 'df1'),
    (3, None, 'df1')
  ], df_schema)
  
  df2 = spark.createDataFrame([
    (0, ['C', 'A'],'df2'),
    (1, ['B'], 'df2'),
  ], df_schema)
  
  df3 = spark.createDataFrame([
    (0, None, 'df3'),
    (4, None, 'df3')
  ], df_schema)
    
  df4= spark.createDataFrame([
    (0, None, None),
    (5, None, None)
  ], df_schema)
    
  dfs_input = [df1, df2, df3, df4]
  
  df_expected = spark.createDataFrame([
    (0, ['A'],'df1'),
    (1, ['A'], 'df1'),
    (3, None, 'df1'),
    (0, ['C', 'A'],'df2'),
    (1, ['B'], 'df2'),
    (0, None, 'df3'),
    (4, None, 'df3'),
    (0, None, None),
    (5, None, None)
  ], df_schema)
  
  df_actual = union_multiple_dfs(dfs_input)
  
  assert compare_results(df_actual, df_expected, join_columns=['idx', 'dataset'])
  

# COMMAND ----------

@suite.add_test
def test_remove_elements_after_date_default():
  """test_remove_elements_after_date_default
  Testing of the function with default values (False) for `keep_nulls` and `keep_inclusive`
  """
  # Input DataFrames
  ## Dataframe to filter
  df_input_filter = spark.createDataFrame([
    (0,date(2000,1,1),'A'),
    (1,date(2001,1,1),'B'),
    (2,None,'C'),
    (3,date(2003,1,1),'D'),
    (4,date(2004,1,1),'E')
  ],['idx','f_date','f_val'])
  ## Dataframe providing dates to filter on
  df_input_dates = spark.createDataFrame([
    (0,date(2000,1,2)),
    (1,date(2001,1,1)),
    (2,date(2002,1,1)),
    (4,date(2003,1,1)),
  ],['idx','d_date'])
  
  # Expected DataFrame
  df_expected = spark.createDataFrame([
    (0,date(2000,1,1),'A',date(2000,1,2)),
  ],['idx','f_date','f_val','d_date'])
  
  # Actual DataFrame
  df_actual = remove_elements_after_date(
    df_to_filter=df_input_filter,
    df_dates=df_input_dates,
    date_to_compare='f_date',
    max_date='d_date',
    link=['idx'],
    keep_nulls=False,
    keep_inclusive=False
  )
  
  # Compare results
  assert compare_results(df_actual, df_expected, join_columns = ['idx'])
  

# COMMAND ----------

@suite.add_test
def test_remove_elements_after_date_keep_nulls():
  """test_remove_elements_after_date_keep_nulls
  Testing of the function with default values (False) for `keep_nulls` and `keep_inclusive`
  """
  # Input DataFrames
  ## Dataframe to filter
  df_input_filter = spark.createDataFrame([
    (0,date(2000,1,1),'A'),
    (1,date(2001,1,1),'B'),
    (2,None,'C'),
    (3,date(2003,1,1),'D'),
    (4,date(2004,1,1),'E')
  ],['idx','f_date','f_val'])
  ## Dataframe providing dates to filter on
  df_input_dates = spark.createDataFrame([
    (0,date(2000,1,2)),
    (1,date(2001,1,1)),
    (2,date(2002,1,1)),
    (4,date(2003,1,1)),
  ],['idx','d_date'])

  # Expected DataFrame
  df_expected = spark.createDataFrame([
    (0,date(2000,1,1),'A',date(2000,1,2)),
    (3,date(2003,1,1),'D',None),
  ],['idx','f_date','f_val','d_date'])

  # Actual DataFrame
  df_actual = remove_elements_after_date(
    df_to_filter=df_input_filter,
    df_dates=df_input_dates,
    date_to_compare='f_date',
    max_date='d_date',
    link=['idx'],
    keep_nulls=True,
    keep_inclusive=False
  )

  # Compare results
  assert compare_results(df_actual, df_expected, join_columns = ['idx'])

# COMMAND ----------

@suite.add_test
def test_remove_elements_after_date_keep_inclusive():
  """test_remove_elements_after_date_keep_inclusive
  Testing of the function with `keep_nulls == False` and `keep_inclusive == True`
  """
  # Input DataFrames
  ## Dataframe to filter
  df_input_filter = spark.createDataFrame([
    (0,date(2000,1,1),'A'),
    (1,date(2001,1,1),'B'),
    (2,None,'C'),
    (3,date(2003,1,1),'D'),
    (4,date(2004,1,1),'E')
  ],['idx','f_date','f_val'])
  ## Dataframe providing dates to filter on
  df_input_dates = spark.createDataFrame([
    (0,date(2000,1,2)),
    (1,date(2001,1,1)),
    (2,date(2002,1,1)),
    (4,date(2003,1,1)),
  ],['idx','d_date'])

  # Expected DataFrame
  df_expected = spark.createDataFrame([
    (0,date(2000,1,1),'A',date(2000,1,2)),
    (1,date(2001,1,1),'B',date(2001,1,1)),
  ],['idx','f_date','f_val','d_date'])

  # Actual DataFrame
  df_actual = remove_elements_after_date(
    df_to_filter=df_input_filter,
    df_dates=df_input_dates,
    date_to_compare='f_date',
    max_date='d_date',
    link=['idx'],
    keep_nulls=False,
    keep_inclusive=True
  )

  # Compare results
  assert compare_results(df_actual, df_expected, join_columns = ['idx'])

# COMMAND ----------

@suite.add_test
def test_remove_elements_after_date_keep_null_keep_inclusive():
  """test_remove_elements_after_date_keep_null_keep_inclusive
  Testing of the function with `keep_nulls == True` and `keep_inclusive == True`
  """
  # Input DataFrames
  ## Dataframe to filter
  df_input_filter = spark.createDataFrame([
    (0,date(2000,1,1),'A'),
    (1,date(2001,1,1),'B'),
    (2,None,'C'),
    (3,date(2003,1,1),'D'),
    (4,date(2004,1,1),'E')
  ],['idx','f_date','f_val'])
  ## Dataframe providing dates to filter on
  df_input_dates = spark.createDataFrame([
    (0,date(2000,1,2)),
    (1,date(2001,1,1)),
    (2,date(2002,1,1)),
    (4,date(2003,1,1)),
  ],['idx','d_date'])
  
  # Expected DataFrame
  df_expected = spark.createDataFrame([
    (0,date(2000,1,1),'A',date(2000,1,2)),
    (1,date(2001,1,1),'B',date(2001,1,1)),
    (3,date(2003,1,1),'D',None)
  ],['idx','f_date','f_val','d_date'])
  
  # Actual DataFrame
  df_actual = remove_elements_after_date(
    df_to_filter=df_input_filter,
    df_dates=df_input_dates,
    date_to_compare='f_date',
    max_date='d_date',
    link=['idx'],
    keep_nulls=True,
    keep_inclusive=True
  )
  
  # Compare results
  assert compare_results(df_actual, df_expected, join_columns = ['idx'])
  

# COMMAND ----------

suite.run()

# COMMAND ----------

dbutils.notebook.exit('pass')
