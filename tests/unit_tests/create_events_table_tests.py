# Databricks notebook source
# MAGIC %run ../../src/test_helpers

# COMMAND ----------

PARAMS_PATH = 'default'

# COMMAND ----------

# MAGIC %run ../../pipeline/create_events_table

# COMMAND ----------

import pyspark.sql.functions as F
import pyspark.sql.types as T

from uuid import uuid4
from datetime import datetime, date

from dsp.validation.validator import compare_results

# COMMAND ----------

suite = FunctionTestSuite()

# COMMAND ----------

TEST_EVENT_DATA_ENTRY = EventsStageDataEntry(
    dataset_name = 'test_df',
    context_key = 'test_key',
    processing_func = process_hes_events_episodes,
    mapping = EventsStageColumnMapping(
        pid_field = params.PID_FIELD,
        dob_field = (params.DOB_FIELD, partial(ensure_dob, dob_col = params.DOB_FIELD, ts_col = params.RECORD_STARTDATE_FIELD, max_age_year = 130)),
        age_field = (params.AGE_FIELD, partial(add_age_from_dob_at_date, dob_col = params.DOB_FIELD, at_date_col = params.RECORD_STARTDATE_FIELD, max_age_year = 130)),
        sex_field = params.SEX_FIELD,
        dataset_field = params.DATASET_FIELD,
        category_field = F.lit('episode'),
        record_id_field = params.RECORD_ID_FIELD,
        record_start_date_field = params.RECORD_STARTDATE_FIELD,
        record_end_date_field = params.RECORD_ENDDATE_FIELD,
        lsoa_field = params.LSOA_FIELD,
        ethnicity_field = params.ETHNICITY_FIELD,
        code_field = params.CODE_FIELD,
        flag_field = params.HES_FLAG_FIELD,
        code_array_field = params.CODE_ARRAY_FIELD,
        flag_array_field = None,
        assoc_record_id_field = None,
    ))

# COMMAND ----------

@suite.add_test
def test_transform_data_to_schema_and_combine():

    # CUSTOM FUNCTION TEST DEFINITION
    def make_lower_case(df: DataFrame):
        return df.withColumn('code', F.lower(F.col('code')))

    def filter_unplanned(df: DataFrame):
        return df.filter(F.col('unplanned_flag') == 1)

    # TEST PARAMETERS
    @dataclass(frozen = True)
    class TestParams(ParamsBase):
        DATASETS  = ['test_ds1', 'hes_apc_test_ds2']
        DATABASE_NAME = None
        PID_FIELD = 'person_id'
        DOB_FIELD = 'birth_date'
        AGE_FIELD = 'age'
        SEX_FIELD = 'sex'
        DATASET_FIELD = 'dataset'
        CATEGORY_FIELD = 'category'
        RECORD_ID_FIELD = 'record_id'
        RECORD_STARTDATE_FIELD = 'record_start_date'
        RECORD_ENDDATE_FIELD = 'record_end_date'
        LSOA_FIELD = 'lsoa'
        ETHNICITY_FIELD = 'ethnicity'
        CODE_FIELD = 'code'
        FLAG_FIELD = 'flag'
        CODE_ARRAY_FIELD = 'code_array'
        ASSOC_FLAG_FIELD = 'flag_assoc'
        ASSOC_REC_ID_FIELD = 'record_id_assoc'
        EVENTS_OUTPUT_FIELDS = [
          'person_id','birth_date','age','sex','dataset','category',
          'record_id','record_start_date','record_end_date','lsoa',
          'code','flag','code_array','flag_assoc','record_id_assoc']
    test_params = TestParams()

    input_schema_ds1 = T.StructType([
      T.StructField('pid', T.StringType(), False),
      T.StructField('rec_id', T.StringType(), False),
      T.StructField('rec_start', T.DateType(), True),
      T.StructField('rec_end', T.DateType(), True),
      T.StructField('code', T.StringType(), False),
      T.StructField('flag', T.StringType(), False),
      T.StructField('dob', T.DateType(), False),
      T.StructField('dataset', T.StringType(), False),
      T.StructField('code_array', T.ArrayType(T.StringType()), True),
      T.StructField('flag_array', T.ArrayType(T.StringType()), True),
      T.StructField('assoc_ids', T.ArrayType(T.StringType()), True),
    ])

    input_schema_ds2 = T.StructType([
      T.StructField('pid', T.StringType(), False),
      T.StructField('rec_id', T.StringType(), False),
      T.StructField('rec_start', T.DateType(), True),
      T.StructField('rec_end', T.DateType(), True),
      T.StructField('code', T.StringType(), False),
      T.StructField('flag', T.StringType(), False),
      T.StructField('date_of_frog', T.DateType(), False),
      T.StructField('dataset', T.StringType(), False),
      T.StructField('code_array', T.ArrayType(T.StringType()), True),
      T.StructField('flag_array', T.ArrayType(T.StringType()), True),
      T.StructField('assoc_ids', T.ArrayType(T.StringType()), True),
    ])

    # DATA INITIALISATION
    df_input_ds1 = spark.createDataFrame([
        ('001', '10001', date(2001,1,1), date(2001,2,2), 'A', 'F1', date(1901,1,1), 'test_ds1', [None], [None], [None]),
        ('002', '20001', date(2002,1,1), date(2002,2,2), 'B', 'F2', date(1902,1,1), 'test_ds1', ['A'], [None], [None]),
        ('002', '20002', date(2002,1,1), date(2002,2,2), 'C', 'F3', date(1902,1,1), 'test_ds1', [None], ['B'], [None]),
        ('003', '30001', date(2003,1,1), date(2003,2,2), 'D', 'F4', date(1903,1,1), 'test_ds1', ['A'], ['B'], [None]),
        ('003', '30002', date(2003,1,1), date(2003,2,2), 'E', 'F5', date(1903,1,1), 'test_ds1', [None], ['A'], ['B']),
        ('003', '30003', date(2003,1,1), date(2003,2,2), 'F', 'F6', date(1903,1,1), 'test_ds1', ['A'], ['B'], ['C']),
        ('004', '40001', date(2000,1,1), date(2000,2,2), 'G', 'F7', date(1904,1,1), 'test_ds1', ['A','B'], [None], ['C','D']),
    ], input_schema_ds1)

    df_input_ds2 = spark.createDataFrame([
        ('001', '10011', date(2001,1,1), date(2001,2,2), 'AA', 'FA', date(1901,1,1), 'hes_apc_test_ds2', [None], [None], [None]),
        ('002', '20011', date(2002,1,1), date(2002,2,2), 'BB', 'FB', date(1902,1,1), 'hes_apc_test_ds2', ['frog'], [None], [None]),
        ('002', '20012', date(2002,1,1), date(2002,2,2), 'CC', 'F2', date(1902,1,1), 'hes_apc_test_ds2', [None], ['frog'], [None]),
        ('003', '30011', date(2003,1,1), date(2003,2,2), 'DD', 'FC', date(1903,1,1), 'hes_apc_test_ds2', [None], [None], ['frog']),
        ('003', '30012', date(2003,1,1), date(2003,2,2), 'EE', 'FD', date(1903,1,1), 'hes_apc_test_ds2', [None], ['frog'], [None]),
        ('003', '30013', date(2003,1,1), None, 'FF', 'FE', date(1903,1,1), 'hes_apc_test_ds2', ['frog'], [None], ['frog']),
        ('004', '40011', date(2000,1,1), date(2000,2,2), 'GG', 'FE', date(1904,1,1), 'hes_apc_test_ds2', ['frog'], ['frog'], ['frog']),
    ], input_schema_ds2)

    expected_schema = T.StructType([
      T.StructField('person_id', T.StringType(), False),
      T.StructField('birth_date', T.DateType(), False),
      T.StructField('age', T.IntegerType(), True),
      T.StructField('sex', T.StringType(), True),
      T.StructField('dataset', T.StringType(), False),
      T.StructField('category', T.StringType(), False),
      T.StructField('record_id', T.StringType(), False),
      T.StructField('record_start_date', T.DateType(), False),
      T.StructField('record_end_date', T.DateType(), True),
      T.StructField('lsoa', T.StringType(), True),
      T.StructField('ethnicity', T.StringType(), True),
      T.StructField('code', T.StringType(), True),
      T.StructField('flag', T.StringType(), True),
      T.StructField('code_array', T.ArrayType(T.StringType()), True),
      T.StructField('flag_assoc', T.ArrayType(T.StringType()), True),
      T.StructField('record_id_assoc', T.ArrayType(T.StringType()), True),
    ])

    df_expected = spark.createDataFrame([
        ('001',date(1901,1,1),100,'sex','test_ds1','tests','10001',date(2001,1,1), date(2001,2,2),'froggyland','A','A','F1', [None], [None], [None]),
        ('002',date(1902,1,1),100,'sex','test_ds1','tests','20001',date(2002,1,1), date(2002,2,2),'froggyland','A','B','F2', ['A'], [None], [None]),
        ('002',date(1902,1,1),100,'sex','test_ds1','tests','20002',date(2002,1,1), date(2002,2,2),'froggyland','A','C','F3', [None], ['B'], [None]),
        ('003',date(1903,1,1),100,'sex','test_ds1','tests','30001',date(2003,1,1), date(2003,2,2),'froggyland','A','D','F4', ['A'], ['B'], [None]),
        ('003',date(1903,1,1),100,'sex','test_ds1','tests','30002',date(2003,1,1), date(2003,2,2),'froggyland','A','E','F5', [None], ['A'], ['B']),
        ('003',date(1903,1,1),100,'sex','test_ds1','tests','30003',date(2003,1,1), date(2003,2,2),'froggyland','A','F','F6', ['A'], ['B'], ['C']),
        ('004',date(1904,1,1),96,'sex','test_ds1','tests','40001',date(2000,1,1), date(2000,2,2),'froggyland','A','G','F7', ['A','B'], [None], ['C','D']),
        ('001',date(1901,1,1),100,None,'hes_apc_test_ds2','episodes','10011',date(2001,1,1), date(2001,2,2),None,'B','aa','FA',[None], [None], [None]),
        ('002',date(1902,1,1),100,None,'hes_apc_test_ds2','episodes','20011',date(2002,1,1), date(2002,2,2),None,'B','bb','FB',['frog'], [None], [None]),
        ('002',date(1902,1,1),100,None,'hes_apc_test_ds2','episodes','20012',date(2002,1,1), date(2002,2,2),None,'B','cc','F2',[None], ['frog'], [None]),
        ('003',date(1903,1,1),100,None,'hes_apc_test_ds2','episodes','30011',date(2003,1,1), date(2003,2,2),None,'B','dd','FC',[None], [None], ['frog']),
        ('003',date(1903,1,1),100,None,'hes_apc_test_ds2','episodes','30012',date(2003,1,1), date(2003,2,2),None,'B','ee','FD',[None], ['frog'], [None]),
        ('003',date(1903,1,1),100,None,'hes_apc_test_ds2','episodes','30013',date(2003,1,1), None,None,'B','ff','FE',['frog'], [None], ['frog']),
        ('004',date(1904,1,1),96,None,'hes_apc_test_ds2','episodes','40011',date(2000,1,1), date(2000,2,2),None,'B','gg','FE',['frog'], ['frog'], ['frog']),
    ], expected_schema)

    # DATA AND STAGE SETUP
    ds1_temp_table_name = f'_temp_{uuid4().hex}'
    ds2_temp_table_name = f'_temp_{uuid4().hex}'
    df_input_ds1.createOrReplaceGlobalTempView(ds1_temp_table_name)
    df_input_ds2.createOrReplaceGlobalTempView(ds2_temp_table_name)
    ds1_key = f'global_temp.{ds1_temp_table_name}'
    ds2_key = f'global_temp.{ds2_temp_table_name}'

    # STAGE INITIALISATION
    stage = CreateEventsTableStage(cvdp_cohort_input = None,
                                   cvdp_htn_input = None,
                                   dars_input = 'DS1',
                                   hes_apc_input = 'DS2',
                                   events_table_output = 'OUTPUT')
    context = PipelineContext('12345', test_params, [CreateEventsTableStage])
    context['DS1'] = PipelineAsset('DS1', context, df_input_ds1)
    context['DS2'] = PipelineAsset('DS2', context, df_input_ds2)

    with FunctionPatch('params',test_params):
      stage._data_entries = [
          ## TEST DF 1
          EventsStageDataEntry(
              dataset_name = 'test_ds1',
              context_key = 'DS1',
              processing_func = None,
              mapping = EventsStageColumnMapping(
                  pid_field = 'pid',
                  dob_field = (params.DOB_FIELD, partial(ensure_dob, dob_col = 'dob', ts_col = 'rec_start', max_age_year = 130)),
                  age_field = (params.AGE_FIELD, partial(add_age_from_dob_at_date, dob_col = params.DOB_FIELD, at_date_col = 'rec_start', max_age_year = 130)),
                  sex_field = F.lit('sex'),
                  dataset_field = 'dataset',
                  category_field = F.lit('tests'),
                  record_id_field = 'rec_id',
                  record_start_date_field = 'rec_start',
                  record_end_date_field = 'rec_end',
                  lsoa_field = F.lit('froggyland'),
                  ethnicity_field = F.lit('A'),
                  code_field = 'code',
                  flag_field = 'flag',
                  code_array_field = 'code_array',
                  flag_array_field = 'flag_array',
                  assoc_record_id_field = 'assoc_ids',
                  )),
          ## TEST DF 2
          EventsStageDataEntry(
              dataset_name = 'hes_apc_test_ds2',
              context_key = 'DS2',
              processing_func = make_lower_case,
              mapping = EventsStageColumnMapping(
                  pid_field = 'pid',
                  dob_field = (params.DOB_FIELD, partial(ensure_dob, dob_col = 'date_of_frog', ts_col = 'rec_start', max_age_year = 130)),
                  age_field = (params.AGE_FIELD, partial(add_age_from_dob_at_date, dob_col = params.DOB_FIELD, at_date_col = 'rec_start', max_age_year = 130)),
                  sex_field = None,
                  dataset_field = 'dataset',
                  category_field = F.lit('episodes'),
                  record_id_field = 'rec_id',
                  record_start_date_field = 'rec_start',
                  record_end_date_field = 'rec_end',
                  lsoa_field = None,
                  ethnicity_field = F.lit('B'),
                  code_field = 'code',
                  flag_field = 'flag',
                  code_array_field = 'code_array',
                  flag_array_field = 'flag_array',
                  assoc_record_id_field = 'assoc_ids',
                  )),
      ]

      stage._transform_data_to_schema_and_combine(context)
      df_actual = stage._events_data

      assert compare_results(df_actual, df_expected, join_columns = ['person_id','birth_date','record_id'])

# COMMAND ----------

@suite.add_test
def test_format_events_table():

    stage = CreateEventsTableStage(None, None, None, None, None)
    stage._output_fields = ['pid','date','code']

    df_events = spark.createDataFrame([
        ('001','A',date(2000,1,1),'foo')
    ], ['pid','code','date','bar'])

    df_expected = spark.createDataFrame([
        ('001',date(2000,1,1),'A')
    ], ['pid','date','code'])

    stage._events_data = df_events
    stage._format_events_table()
    df_actual = stage._events_data
    assert compare_results(df_actual, df_expected, join_columns = ['pid'])



# COMMAND ----------

@suite.add_test
def test_check_events_table_non_null():

    stage = CreateEventsTableStage(None,None,None,None,None)

    df_input = spark.createDataFrame([
        ('001', date(2001,1,1), 'foo_1'),
        ('002', date(2002,1,1), 'foo_2'),
        ('003', date(2003,1,1), 'foo_3'),
        ('004', date(2004,1,1), 'foo_4'),
    ], [params.PID_FIELD,params.DOB_FIELD,'BAR'])

    stage._events_data = df_input
    stage._check_events_table()

    assert stage._data_check_rec['pid'] == 0
    assert stage._data_check_rec['dob'] == 0


# COMMAND ----------

@suite.add_test
def test_check_events_table_pid_null():

    stage = CreateEventsTableStage(None,None,None,None,None)

    df_input = spark.createDataFrame([
        ('001', date(2001,1,1), 'foo_1'),
        (None, date(2002,1,1), 'foo_2'),
    ], [params.PID_FIELD,params.DOB_FIELD,'BAR'])

    stage._events_data = df_input
    stage._check_events_table()

    assert stage._data_check_rec['pid'] == 1
    assert stage._data_check_rec['dob'] == 0


# COMMAND ----------

@suite.add_test
def test_check_events_table_dob_null():

    stage = CreateEventsTableStage(None,None,None,None,None)

    df_input = spark.createDataFrame([
        ('001', date(2001,1,1), 'foo_1'),
        ('002', None, 'foo_2'),
    ], [params.PID_FIELD,params.DOB_FIELD,'BAR'])

    stage._events_data = df_input
    stage._check_events_table()

    assert stage._data_check_rec['pid'] == 0
    assert stage._data_check_rec['dob'] == 1


# COMMAND ----------

@suite.add_test
def test_check_events_table_both_null():

    stage = CreateEventsTableStage(None,None,None,None,None)

    df_input = spark.createDataFrame([
        ('001', date(2001,1,1), 'foo_1'),
        ('002', None, 'foo_2'),
        (None, date(2003,1,1), 'foo_3')
    ], [params.PID_FIELD,params.DOB_FIELD,'BAR'])

    stage._events_data = df_input
    stage._check_events_table()

    assert stage._data_check_rec['pid'] == 1
    assert stage._data_check_rec['dob'] == 1

# COMMAND ----------

@suite.add_test
def test_check_unique_identifiers_events_pass():
    '''test_check_unique_identifiers_events_pass
    create_events_table::CreateEventsTableStage._check_unique_identifiers_events()
    Tests checking of unqiue values in the record identifier field in the events table.
    Test will pass if no error is raised (all values unique).
    '''
    # Setup Test Parameters
    @dataclass(frozen = True)
    class TestParams(ParamsBase):
        RECORD_ID_FIELD = 'test_record_id'
    test_params = TestParams()

    # Setup Test Data
    ## Input
    df_input = spark.createDataFrame([
        (1,'a1'),
        (2,'a2'),
        (3,'a3')
    ], ['idx', 'test_record_id'])

    # Stage Test
    with FunctionPatch('params',test_params):
        stage = CreateEventsTableStage(
            cvdp_cohort_input = None,
            cvdp_htn_input = None,
            dars_input = None,
            hes_apc_input = None,
            events_table_output = None
            )
        stage._events_data = df_input
        try:
            stage._check_unique_identifiers_events()
            result = True
        except:
            result = False
            raise Exception('test_check_unique_identifiers_events_pass failed')
    assert result == True

# COMMAND ----------

@suite.add_test
def test_check_unique_identifiers_events_fail():
    '''test_check_unique_identifiers_events_fail
    create_events_table::CreateEventsTableStage._check_unique_identifiers_events()
    Tests checking of unqiue values in the record identifier field in the events table.
    Test will pass if an error is raised (presence of non-unique values).
    '''
    # Setup Test Parameters
    @dataclass(frozen = True)
    class TestParams(ParamsBase):
        RECORD_ID_FIELD = 'test_record_id'
    test_params = TestParams()

    # Setup Test Data
    ## Input
    df_input = spark.createDataFrame([
        (1,'a1'),
        (2,'a2'),
        (3,'a2')
    ], ['idx', 'test_record_id'])

    # Stage Test
    with FunctionPatch('params',test_params):
        stage = CreateEventsTableStage(
            cvdp_cohort_input = None,
            cvdp_htn_input = None,
            dars_input = None,
            hes_apc_input = None,
            events_table_output = None
            )
        stage._events_data = df_input
        try:
            stage._check_unique_identifiers_events()
            result = False
            raise Exception('test_check_unique_identifiers_events_fail failed')
        except:
            result = True
    assert result == True

# COMMAND ----------

suite.run()

# COMMAND ----------

dbutils.notebook.exit('pass')