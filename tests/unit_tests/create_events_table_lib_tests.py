# Databricks notebook source
# MAGIC %run ../../src/test_helpers

# COMMAND ----------

PARAMS_PATH = 'default'

# COMMAND ----------

# MAGIC %run ../../params/params

# COMMAND ----------

# MAGIC %run ../../pipeline/create_events_table_lib

# COMMAND ----------

from uuid import uuid4
from datetime import datetime, date
import pyspark.sql.functions as F
import pyspark.sql.types as T

from dsp.validation.validator import compare_results

# COMMAND ----------

suite = FunctionTestSuite()

## The following are fake people and data created for test purposes

## The following are fake people and data created for test purposes

# COMMAND ----------

@suite.add_test
def test_ensure_england_lsoa():

    df_input = spark.createDataFrame([
        (0, 'E00001'),
        (1, 'X00001'),
        (2, None)
    ], ['idx','lsoa'])

    df_expected = spark.createDataFrame([
        (0, 'E00001'),
    ], ['idx','lsoa'])

    df_actual = ensure_england_lsoa(df_input, 'lsoa')
    assert compare_results(df_actual, df_expected, join_columns = ['idx'])


# COMMAND ----------

@suite.add_test
def test_ensure_int_type():

    input_schema = T.StructType([
    T.StructField('index', T.StringType(), True),
    T.StructField('i_int', T.IntegerType(), True),
    T.StructField('i_str', T.StringType(), True),
    T.StructField('i_long', T.LongType(), True),
    T.StructField('i_short', T.ShortType(), True),
    ])

    df_input = spark.createDataFrame([
        ('0', 0, '0', 0, 0),
        ('1', 1, '1', 1, 1),
        ('2', -1, '-1', -1, -1),
        ('3', 30, '30', 30, 30),
        ('4', None, None, None, None),
    ], input_schema)

    expected_schema = T.StructType([
        T.StructField('index', T.StringType(), True),
        T.StructField('i_int', T.IntegerType(), True),
        T.StructField('i_str', T.IntegerType(), True),
        T.StructField('i_long', T.IntegerType(), True),
        T.StructField('i_short', T.IntegerType(), True),
    ])

    df_expected = spark.createDataFrame([
        ('0', 0, 0, 0, 0),
        ('1', 1, 1, 1, 1),
        ('2', -1, -1, -1, -1),
        ('3', 30, 30, 30, 30),
        ('4', None, None, None, None),
    ], expected_schema)

    df = df_input
    df = ensure_int_type(df, 'i_int')
    df = ensure_int_type(df, 'i_str')
    df = ensure_int_type(df, 'i_long')
    df = ensure_int_type(df, 'i_short')

    df_actual = df
    assert compare_results(df_actual, df_expected, join_columns=['index'])


# COMMAND ----------

@suite.add_test
def test_run_field_mapping_str():

    df_input = spark.createDataFrame([
        (0, 'A', '1'),
        (1, 'B', '2'),
        (2, 'C', '3'),
        (3, 'D', '4'),
    ], ['idx','pid','num'])

    df_expected = spark.createDataFrame([
        (0, 'A', '1'),
        (1, 'B', '2'),
        (2, 'C', '3'),
        (3, 'D', '4'),
    ], ['idx','pid','number'])

    df_actual = _run_field_mapping(df_input, 'number', 'num')
    assert compare_results(df_actual, df_expected, join_columns = ['idx'])

# COMMAND ----------

@suite.add_test
def test_run_field_mapping_func():

    def make_lower_case(df: DataFrame, col_name: str):
        return df.withColumn(col_name, F.lower(F.col(col_name)))

    df_input = spark.createDataFrame([
        (0, 'A', '1'),
        (1, 'B', '2'),
        (2, 'C', '3'),
        (3, 'D', '4'),
        (4, 'e', '5'),
        (5, '', '6'),
        (6, None, '7'),
        (7, '1', '8'),
    ], ['idx','pid','num'])

    df_expected = spark.createDataFrame([
        (0, 'a', '1'),
        (1, 'b', '2'),
        (2, 'c', '3'),
        (3, 'd', '4'),
        (4, 'e', '5'),
        (5, '', '6'),
        (6, None, '7'),
        (7, '1', '8'),
    ], ['idx','pid_lower','num'])

    df_actual = _run_field_mapping(df_input, 'pid_lower', ('pid', make_lower_case))
    assert compare_results(df_actual, df_expected, join_columns = ['idx'])

# COMMAND ----------

@suite.add_test
def test_run_field_mapping_lit():

    df_input = spark.createDataFrame([
        (0, 'A', '1'),
        (1, 'B', '2'),
        (2, 'C', '3'),
        (3, 'D', '4'),
    ], ['idx','pid','num'])

    df_expected = spark.createDataFrame([
        (0, 'A', '1', 'foo'),
        (1, 'B', '2', 'foo'),
        (2, 'C', '3', 'foo'),
        (3, 'D', '4', 'foo'),
    ], ['idx','pid','num','data'])

    df_actual = _run_field_mapping(df_input, 'data', F.lit('foo'))
    assert compare_results(df_actual, df_expected, join_columns = ['idx'])


# COMMAND ----------

@suite.add_test
def test_format_source_data_to_combined_schema():

    df_input = spark.createDataFrame([
        ('0', date(2020, 3, 1), '0', 'I64', date(1980, 1, 1)),
        ('1', date(2020, 3, 2), '1', 'I24', date(1980, 1, 2)),
        ('2', date(2020, 3, 3), '2', None, date(1980, 1, 3)),
    ], ['PID', 'REC', 'EID', 'CODE', 'DOB'])

    output_fields = [params.PID_FIELD, params.RECORD_STARTDATE_FIELD, params.RECORD_ID_FIELD, params.CODE_FIELD, params.DOB_FIELD]

    expected_schema = T.StructType([
        T.StructField(params.PID_FIELD, T.StringType(), True),
        T.StructField(params.RECORD_STARTDATE_FIELD, T.DateType(), True),
        T.StructField(params.RECORD_ID_FIELD, T.StringType(), True),
        T.StructField(params.CODE_FIELD, T.StringType(), True),
        T.StructField(params.DOB_FIELD, T.DateType(), True),
    ])
    df_expected = spark.createDataFrame([
        ('0', date(2020, 3, 1), '0', 'I64', date(1980, 1, 1)),
        ('1', date(2020, 3, 2), '1', 'I24', date(1980, 1, 2)),
        ('2', date(2020, 3, 3), '2', None, date(1980, 1, 3)),
    ], expected_schema)

    event_entry = EventsStageDataEntry(
        dataset_name = 'test_df',
        context_key = None,
        processing_func = None,
        mapping = EventsStageColumnMapping(
            pid_field = 'PID',
            dob_field = 'DOB',
            age_field = None,
            sex_field = None,
            dataset_field = None,
            category_field = None,
            record_id_field = 'EID',
            record_start_date_field = 'REC',
            record_end_date_field = None,
            lsoa_field = None,
            ethnicity_field = None,
            code_field = 'CODE',
            flag_field = None,
            code_array_field = None,
            flag_array_field = None,
            assoc_record_id_field = None,
        ))

    df_actual = format_source_data_to_combined_schema(event_entry, df_input, output_fields)

    assert compare_results(df_actual, df_expected, join_columns=[params.PID_FIELD])

# COMMAND ----------

suite.run()

# COMMAND ----------

dbutils.notebook.exit('pass')
