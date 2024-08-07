# Databricks notebook source
# MAGIC %run ../../src/test_helpers

# COMMAND ----------

PARAMS_PATH = 'default'

# COMMAND ----------

# MAGIC %run ../../pipeline/create_demographic_table

# COMMAND ----------

import pyspark.sql.functions as F
import pyspark.sql.types as T

from uuid import uuid4
from datetime import datetime, date

from dsp.validation.validator import compare_results

# COMMAND ----------

suite = FunctionTestSuite()

# COMMAND ----------

@suite.add_test
def test_load_apc_from_events():
    '''test_load_data_assets
    '''

    # TEST PARAMETERS
    @dataclass(frozen = True)
    class TestParams(ParamsBase):
        DATASETS                    = ['test_ds1','test_ds2']
        DATABASE_NAME               = None
        PID_FIELD                   = 'person_id'
        COHORT_FIELD                = 'cohort'
        DOB_FIELD                   = 'birth_date'
        ETHNICITY_FIELD             = 'ethnicity'
        CODE_FIELD                  = 'code'
        DATASET_FIELD               = 'dataset'
        CATEGORY_FIELD              = 'category'
        RECORD_STARTDATE_FIELD      = 'record_date'
        CODE_ARRAY_FIELD            = 'code_array'
        EVENTS_CVDP_COHORT_DATASET  = 'test_cvdp'
        EVENTS_CVDP_COHORT_CATEGORY = 'test_category_cvdp'
        HES_APC_TABLE               = 'test_apc'
        EVENTS_HES_APC_EPISODE_CATEGORY = 'test_category_hes'
        DEMOGRAPHIC_OUTPUT_FIELDS   = [
          'person_id', 'birth_date', 'dataset', 'record_date','ethnicity'
        ]

    test_params = TestParams()

    # INPUT DATA - EVENTS TABLE TEST SCHEMA
    input_schema = T.StructType([
        T.StructField('idx', T.IntegerType(), False),
        T.StructField('person_id', T.StringType(), False),
        T.StructField('birth_date', T.DateType(), False),
        T.StructField('dataset', T.StringType(), False),
        T.StructField('category', T.StringType(), False),
        T.StructField('code', T.StringType(), False),
        T.StructField('record_date', T.DateType(), False),
        T.StructField('code_array', T.ArrayType(T.StringType()), False),
        T.StructField('ethnicity', T.StringType(), False)
    ])

    # EXPECTED DATA - HES APC TABLE TEST SCHEMA
    expected_schema = T.StructType([
        T.StructField('idx', T.IntegerType(), False),
        T.StructField('person_id', T.StringType(), False),
        T.StructField('birth_date', T.DateType(), False),
        T.StructField('dataset', T.StringType(), False),
        T.StructField('category', T.StringType(), False),
        T.StructField('code', T.StringType(), False),
        T.StructField('record_date', T.DateType(), False),
        T.StructField('code_array', T.ArrayType(T.StringType()), False),
        T.StructField('ethnicity', T.StringType(), False),
    ])

    # INPUT DATA - EVENTS TABLE TEST DATAFRAME
    df_events_input = spark.createDataFrame([
        (1, '001',date(2000,1,1),'test_apc','test_category_hes','HEARTATTACK',date(2022,1,1), ['P0001'], 'A'), # Keep
        (2, '001',date(2000,1,1),'test_apc','test_category_hes2','HEARTATTACK',date(2022,1,1), ['P0001'], 'A'), # Remove
        (3, '001',date(2000,1,1),'test_cvdp','test_category_cvdp','HEARTATTACK',date(2022,2,1), ['P0001'], 'B'), # Remove
        (4, '001',date(2000,1,1),'test_cvdp','test_category_cvdp2','HEARTATTACK',date(2022,2,1), ['P0001'], 'B'), # Remove
        (5, '002',date(2000,1,2),'test_cvdp','test_category_cvdp','HEARTATTACK',date(2021,1,2), ['P0002'], 'A'), # Remove
        (6, '002',date(2000,1,2),'test_cvdp','test_category_cvdp','HEARTATTACK',date(2022,1,2), ['P0001'], 'B'), # Remove
        (7, '003',date(2000,1,2),'test_cvdp','test_category_hes','HEARTATTACK',date(2020,1,2), ['P0001'], 'B'), # Remove
        (8, '003',date(2000,1,2),'test_apc','test_category_hes','HEARTATTACK',date(2020,1,2), ['P0001'], 'B'), # Keep
        (9, '003',date(2000,1,2),'test_apc','test_category_hes','HEARTATTACK',date(2022,1,2), ['P0001'], 'B'), # Keep
    ], input_schema)

    # EMPTY SCHEMA FOR BLANK TABLES
    empty_schema = T.StructType([
        T.StructField('idx', T.IntegerType(), False),
    ])

    # BLANK TABLES - NOT USED IN THIS FUNCTION
    df_journal = spark.createDataFrame([], empty_schema)
    df_hes_ae = spark.createDataFrame([], empty_schema)
    df_hes_op = spark.createDataFrame([], empty_schema)

    # EXPECTED DATA - HES APC TABLE TEST DATAFRAME
    df_expected_apc = spark.createDataFrame([
        (1, '001',date(2000,1,1),'test_apc','test_category_hes','HEARTATTACK',date(2022,1,1), ['P0001'], 'A'),
        (8, '003',date(2000,1,2),'test_apc','test_category_hes','HEARTATTACK',date(2020,1,2), ['P0001'], 'B'),
        (9, '003',date(2000,1,2),'test_apc','test_category_hes','HEARTATTACK',date(2022,1,2), ['P0001'], 'B'), # Keep
    ], expected_schema)

    # DATA AND STAGE SETUP
    input_tmp_table_name = f'_temp_{uuid4().hex}'
    df_events_input.createOrReplaceGlobalTempView(input_tmp_table_name)


    with FunctionPatch('params',test_params):
        # STAGE INITIALISATION
        stage = CreateDemographicTableStage(events_table_input = 'events', cvdp_cohort_journal_input = 'journal',
                                            hes_op_input = 'hes_op', hes_ae_input = 'hes_ae',demographic_table_output = None
                                           )

        context = PipelineContext('12345', params, [CreateDemographicTableStage])
        context['events'] = PipelineAsset('events', context, df_events_input)
        context['journal'] = PipelineAsset('journal', context, df_journal)
        context['hes_ae'] = PipelineAsset('hes_ae', context, df_hes_ae)
        context['hes_op'] = PipelineAsset('hes_op', context, df_hes_op)
        stage._load_data_assets(context)
        df_actual_apc = stage._data_holder['hes_apc']

    assert compare_results(df_actual_apc, df_expected_apc, join_columns=['idx'])

# COMMAND ----------

@suite.add_test
def test_filter_hes_ae():
    '''test_filter_hes_ae

    Test of sub-method _restrict_hes_ae_year.
    '''
    # TEST PARAMETERS
    @dataclass(frozen = True)
    class TestParams(ParamsBase):
        DATABASE_NAME               = None
        HES_APC_TABLE                   = 'test_apc'
        EVENTS_HES_APC_EPISODE_CATEGORY = 'test_category_hes'
        HES_5YR_START_YEAR              = 2020
        RECORD_STARTDATE_FIELD          = 'test_start_date'

    test_params = TestParams()

     # INPUT DATA - HES AE TEST SCHEMA
    hes_schema = T.StructType([
        T.StructField('person_id', T.IntegerType(), False),
        T.StructField('birth_date', T.DateType(), False),
        T.StructField('dataset', T.StringType(), False),
        T.StructField('test_start_date', T.DateType(), True),
        T.StructField('ethnicity', T.StringType(), True),
    ])

    # INPUT DATA - HES AE TEST DATAFRAME
    df_hes_ae_input = spark.createDataFrame([
    (0, date(1960,1,1), 'hes_ae', date(2020,1,1), 'B'), # AFTER START YEAR
    (1, date(1960,1,1), 'hes_ae', date(2019,12,31), 'B'), # BEFORE START YEAR
    (1, date(1960,1,1), 'hes_ae', date(2020,1,2), 'B'), # AFTER START YEAR
    (2, date(1960,1,1), 'hes_ae', date(2011,1,1), None), # BEFORE START YEAR
    (2, date(1960,1,1), 'hes_ae', date(2021,1,1), 'A'), # AFTER START YEAR
    (3, date(1960,1,1), 'hes_ae', date(2010,1,1), 'A'), # BEFORE START YEAR
    (3, date(1960,1,1), 'hes_ae', date(2021,1,1), None), # AFTER START YEAR
    (4, date(1960,1,1), 'hes_ae', None, 'A'), # NULL START
      ], hes_schema)

    # EMPTY SCHEMA FOR BLANK TABLES
    empty_schema = T.StructType([
        T.StructField('idx', T.IntegerType(), False),
    ])

    # BLANK TABLES - NOT USED IN THIS FUNCTION
    df_journal = spark.createDataFrame([], empty_schema)
    df_hes_op = spark.createDataFrame([], empty_schema)
    df_events = spark.createDataFrame([
        ('001',date(2000,1,1),'test_apc','test_category_hes')
    ], ['person_id','birth_date','dataset','category'])

    # EXPECTED DATA - BASE TABLE TEST DATAFRAME
    df_expected_hes_ae = spark.createDataFrame([
    (0, date(1960,1,1), 'hes_ae', date(2020,1,1), 'B'), # AFTER START YEAR
    (1, date(1960,1,1), 'hes_ae', date(2020,1,2), 'B'), # AFTER START YEAR
    (2, date(1960,1,1), 'hes_ae', date(2021,1,1), 'A'), # AFTER START YEAR
    (3, date(1960,1,1), 'hes_ae', date(2021,1,1), None), # AFTER START YEAR
    ], hes_schema)

    # DATA AND STAGE SETUP
    input_tmp_table_name = f'_temp_{uuid4().hex}'
    df_hes_ae_input.createOrReplaceGlobalTempView(input_tmp_table_name)


    with FunctionPatch('params',test_params):
        # STAGE INITIALISATION
        stage = CreateDemographicTableStage(events_table_input = 'events', cvdp_cohort_journal_input = 'journal',
                                            hes_op_input = 'hes_op', hes_ae_input = 'hes_ae',demographic_table_output = None
                                           )

        context = PipelineContext('12345', params, [CreateDemographicTableStage])
        context['events'] = PipelineAsset('events', context, df_events)
        context['journal'] = PipelineAsset('journal', context, df_journal)
        context['hes_ae'] = PipelineAsset('hes_ae', context, df_hes_ae_input)
        context['hes_op'] = PipelineAsset('hes_op', context, df_hes_op)
        stage._load_data_assets(context)
        stage._restrict_hes_ae_year()
        df_actual_hes_ae = stage._data_holder['hes_ae']

    assert compare_results(df_actual_hes_ae, df_expected_hes_ae, join_columns=['person_id','birth_date'])

# COMMAND ----------

@suite.add_test
def test_create_base_table_cvdp():
    '''test_create_base_table

    Test of sub-method _create_base_table in the CreateDemographicTableStage class. The extended functionality of
    extract_patient_events is captured in create_patient_table_lib_tests::test_extract_patient_events.add().
    Test returns a single-row-per-patient, pulling cohort events (ONLY) from the events table.
    This output is stored as 'demographic_data' in the data_holder object.
    '''

    # TEST PARAMETERS
    @dataclass(frozen = True)
    class TestParams(ParamsBase):
        DATASETS                    = ['test_ds1','test_ds2']
        DATABASE_NAME               = None
        PID_FIELD                   = 'person_id'
        COHORT_FIELD                = 'cohort'
        DOB_FIELD                   = 'birth_date'
        ETHNICITY_FIELD             = 'ethnicity'
        CODE_FIELD                  = 'code'
        DATASET_FIELD               = 'dataset'
        CATEGORY_FIELD              = 'category'
        RECORD_STARTDATE_FIELD      = 'record_date'
        CODE_ARRAY_FIELD            = 'code_array'
        EVENTS_CVDP_COHORT_DATASET  = 'test_cvdp'
        EVENTS_CVDP_COHORT_CATEGORY = 'test_category_cvdp'
        HES_APC_TABLE               = 'test_apc'
        EVENTS_HES_APC_EPISODE_CATEGORY = 'test_category_hes'
        DEMOGRAPHIC_OUTPUT_FIELDS   = [
            'person_id', 'birth_date', 'dataset', 'record_date','ethnicity'
        ]

    test_params = TestParams()

    # INPUT DATA - EVENTS TABLE TEST SCHEMA
    input_schema = T.StructType([
        T.StructField('person_id', T.StringType(), False),
        T.StructField('birth_date', T.DateType(), False),
        T.StructField('dataset', T.StringType(), False),
        T.StructField('category', T.StringType(), False),
        T.StructField('code', T.StringType(), False),
        T.StructField('record_date', T.DateType(), False),
        T.StructField('code_array', T.ArrayType(T.StringType()), False),
        T.StructField('ethnicity', T.StringType(), True)
    ])

    # EXPECTED DATA - BASE TABLE TEST SCHEMA
    expected_schema = T.StructType([
        T.StructField('person_id', T.StringType(), False),
        T.StructField('birth_date', T.DateType(), False),
        T.StructField('dataset', T.StringType(), False),
        T.StructField('record_date', T.DateType(), False),
        T.StructField('ethnicity', T.StringType(), True),
    ])

    # INPUT DATA - EVENTS TABLE TEST DATAFRAME
    df_events_input = spark.createDataFrame([
        ('001',date(2000,1,1),'test_apc','test_category_hes','HEARTATTACK',date(2022,1,1), ['P0001'], 'A'), # Remove
        ('001',date(2000,1,1),'test_apc','test_category_hes2','HEARTATTACK',date(2022,1,1), ['P0001'], 'A'), # Remove
        ('001',date(2000,1,1),'test_cvdp','test_category_cvdp','HEARTATTACK',date(2022,2,1), ['P0001'], 'B'), # Keep
        ('001',date(2000,1,1),'test_cvdp','test_category_cvdp2','HEARTATTACK',date(2022,2,1), ['P0001'], 'B'), # Remove
        ('002',date(2000,1,2),'test_cvdp','test_category_cvdp','HEARTATTACK',date(2021,1,2), ['P0002'], 'A'), # Keep
        ('002',date(2000,1,2),'test_cvdp','test_category_cvdp','HEARTATTACK',date(2022,1,2), ['P0001'], None), # Keep
        ('003',date(2000,1,2),'test_cvdp','test_category_hes','HEARTATTACK',date(2020,1,2), ['P0001'], 'B'), # Remove
    ], input_schema)

    # EMPTY SCHEMA FOR BLANK TABLES
    empty_schema = T.StructType([
        T.StructField('idx', T.IntegerType(), False),
    ])

    # BLANK TABLES - NOT USED IN THIS FUNCTION
    df_journal = spark.createDataFrame([], empty_schema)
    df_hes_ae = spark.createDataFrame([], empty_schema)
    df_hes_op = spark.createDataFrame([], empty_schema)

    # EXPECTED DATA - BASE TABLE TEST DATAFRAME
    df_expected_base = spark.createDataFrame([
        ('001',date(2000,1,1),'test_cvdp', date(2022,2,1), 'B'),
        ('002',date(2000,1,2),'test_cvdp',date(2022,1,2), None),
        ('002',date(2000,1,2),'test_cvdp',date(2021,1,2), 'A'),
    ], expected_schema)

    # DATA AND STAGE SETUP
    input_tmp_table_name = f'_temp_{uuid4().hex}'
    df_events_input.createOrReplaceGlobalTempView(input_tmp_table_name)


    with FunctionPatch('params',test_params):
        # STAGE INITIALISATION
        stage = CreateDemographicTableStage(events_table_input = 'events', cvdp_cohort_journal_input = 'journal',
                                            hes_op_input = 'hes_op', hes_ae_input = 'hes_ae',demographic_table_output = None
                                           )

        context = PipelineContext('12345', params, [CreateDemographicTableStage])
        context['events'] = PipelineAsset('events', context, df_events_input)
        context['journal'] = PipelineAsset('journal', context, df_journal)
        context['hes_ae'] = PipelineAsset('hes_ae', context, df_hes_ae)
        context['hes_op'] = PipelineAsset('hes_op', context, df_hes_op)
        stage._load_data_assets(context)
        stage._create_base_table()
        df_actual_base = stage._data_holder['demographic_data']

    assert compare_results(df_actual_base, df_expected_base, join_columns=['person_id','birth_date', 'ethnicity'])

# COMMAND ----------

@suite.add_test
def test_find_known_ethnicity():
    '''test_find_known_ethnicity

    Test of sub-method _create_base_table in the CreateDemographicTableStage class. The extended functionality of
    extract_patient_events is captured in create_patient_table_lib_tests::test_extract_patient_events.add().
    Test returns a single-row-per-patient, pulling cohort events (ONLY) from the events table.
    '''

    # TEST PARAMETERS
    @dataclass(frozen = True)
    class TestParams(ParamsBase):
        DATASETS                    = ['test_ds1','test_ds2']
        DATABASE_NAME               = None
        PID_FIELD                   = 'person_id'
        COHORT_FIELD                = 'cohort'
        DOB_FIELD                   = 'birth_date'
        ETHNICITY_FIELD             = 'ethnicity'
        CODE_FIELD                  = 'code'
        DATASET_FIELD               = 'dataset'
        CATEGORY_FIELD              = 'category'
        RECORD_STARTDATE_FIELD      = 'record_date'
        CODE_ARRAY_FIELD            = 'code_array'
        EVENTS_CVDP_COHORT_DATASET  = 'test_cvdp'
        EVENTS_CVDP_COHORT_CATEGORY = 'test_category_cvdp'
        ETHNICITY_UNKNOWN_CODES     = ['z','Z','',None]
        HES_APC_TABLE               = 'test_apc'
        EVENTS_HES_APC_EPISODE_CATEGORY = 'test_category_hes'
        DEMOGRAPHIC_OUTPUT_FIELDS   = [
            'person_id', 'birth_date', 'dataset', 'record_date','ethnicity'
        ]

    test_params = TestParams()

    # INPUT DATA - EVENTS TABLE TEST SCHEMA
    input_schema = T.StructType([
        T.StructField('person_id', T.StringType(), False),
        T.StructField('birth_date', T.DateType(), False),
        T.StructField('dataset', T.StringType(), False),
        T.StructField('category', T.StringType(), False),
        T.StructField('code', T.StringType(), False),
        T.StructField('record_date', T.DateType(), False),
        T.StructField('code_array', T.ArrayType(T.StringType()), False),
        T.StructField('ethnicity', T.StringType(), True)
    ])

    # EXPECTED DATA - KNOWN ETHNICITY TABLE TEST SCHEMA
    expected_schema = T.StructType([
        T.StructField('person_id', T.StringType(), False),
        T.StructField('birth_date', T.DateType(), False),
        T.StructField('dataset', T.StringType(), False),
        T.StructField('record_date', T.DateType(), False),
        T.StructField('ethnicity', T.StringType(), True),
    ])

    # INPUT DATA - EVENTS TABLE TEST DATAFRAME
    df_input = spark.createDataFrame([
        ('001',date(2000,1,1),'test_cvdp','test_category_cvdp','HEARTATTACK',date(2022,1,1), ['P0001'], 'A'), # Keep
        ('001',date(2000,1,1),'test_cvdp','test_category_cvdp','HEARTATTACK',date(2022,1,1), ['P0001'], ''), # Remove
        ('001',date(2000,1,1),'test_cvdp','test_category_cvdp','HEARTATTACK',date(2022,2,1), ['P0001'], None),# Remove
        ('001',date(2000,1,1),'test_cvdp','test_category_cvdp','HEARTATTACK',date(2022,2,1), ['P0001'], 'B'), # Keep
        ('002',date(2000,1,2),'test_cvdp','test_category_cvdp','HEARTATTACK',date(2022,1,2), ['P0002'], 'Z'), # Remove
        ('002',date(2000,1,2),'test_cvdp','test_category_cvdp','HEARTATTACK',date(2022,1,2), ['P0001'], 'z'), # Remove
        ('003',date(2000,1,2),'test_cvdp','test_category_cvdp','HEARTATTACK',date(2022,1,2), ['P0001'], 'B'), # Keep
        ('003',date(2000,1,2),'test_cvdp','test_category_cvdp','HEARTATTACK',date(2020,1,2), ['P0001'], 'A'), # Keep
        ('004',date(2000,1,2),'test_cvdp','test_category_cvdp','HEARTATTACK',date(2022,1,2), ['P0001'], 'z'), # Keep
        ('004',date(2000,1,2),'test_cvdp','test_category_cvdp','HEARTATTACK',date(2020,1,2), ['P0001'], 'A'), # Keep
        ('004',date(2000,1,2),'test_apc','test_category_hes','HEARTATTACK',date(2020,1,2), ['P0001'], 'A'), # Remove
    ], input_schema)

    # EMPTY SCHEMA FOR BLANK TABLES
    empty_schema = T.StructType([
        T.StructField('idx', T.IntegerType(), False),
    ])

    # BLANK TABLES - NOT USED IN THIS FUNCTION
    df_journal = spark.createDataFrame([], empty_schema)
    df_hes_ae = spark.createDataFrame([], empty_schema)
    df_hes_op = spark.createDataFrame([], empty_schema)

    # EXPECTED DATA - KNOWN ETHNICITY TABLE TEST DATAFRAME
    df_expected_known_ethnicity = spark.createDataFrame([
        ('001',date(2000,1,1),'test_cvdp', date(2022,2,1), 'B'),
        ('003',date(2000,1,2),'test_cvdp',date(2022,1,2), 'B'),
        ('004',date(2000,1,2),'test_cvdp',date(2020,1,2), 'A'),
    ], expected_schema)

    # DATA AND STAGE SETUP
    input_tmp_table_name = f'_temp_{uuid4().hex}'
    df_input.createOrReplaceGlobalTempView(input_tmp_table_name)


    with FunctionPatch('params',test_params):
        # STAGE INITIALISATION
        stage = CreateDemographicTableStage(events_table_input = 'events', cvdp_cohort_journal_input = 'journal',
                                            hes_op_input = 'hes_op', hes_ae_input = 'hes_ae',demographic_table_output = None
                                           )

        context = PipelineContext('12345', params, [CreateDemographicTableStage])
        context['events'] = PipelineAsset('events', context, df_input)
        context['journal'] = PipelineAsset('journal', context, df_journal)
        context['hes_ae'] = PipelineAsset('hes_ae', context, df_hes_ae)
        context['hes_op'] = PipelineAsset('hes_op', context, df_hes_op)
        stage._load_data_assets(context)
        stage._create_base_table()
        stage._find_known_ethnicity()
        df_actual_known_ethnicity = stage._data_holder['demographic_known']

    assert compare_results(df_actual_known_ethnicity, df_expected_known_ethnicity, join_columns=['person_id','birth_date', 'ethnicity'])

# COMMAND ----------

@suite.add_test
def test_find_unknown_ethnicity():
    '''test_find_known_ethnicity

    Test of sub-method _create_base_table in the CreateDemographicTableStage class. The extended functionality of
    extract_patient_events is captured in create_patient_table_lib_tests::test_extract_patient_events.add().
    Test returns a single-row-per-patient, pulling cohort events (ONLY) from the events table.
    '''

    # TEST PARAMETERS
    @dataclass(frozen = True)
    class TestParams(ParamsBase):
        DATASETS                    = ['test_ds1','test_ds2']
        DATABASE_NAME               = None
        PID_FIELD                   = 'person_id'
        COHORT_FIELD                = 'cohort'
        DOB_FIELD                   = 'birth_date'
        ETHNICITY_FIELD             = 'ethnicity'
        CODE_FIELD                  = 'code'
        DATASET_FIELD               = 'dataset'
        CATEGORY_FIELD              = 'category'
        RECORD_STARTDATE_FIELD      = 'record_date'
        CODE_ARRAY_FIELD            = 'code_array'
        EVENTS_CVDP_COHORT_DATASET  = 'test_cvdp'
        EVENTS_CVDP_COHORT_CATEGORY = 'test_category_cvdp'
        ETHNICITY_UNKNOWN_CODES     = ['z','Z','',None]
        HES_APC_TABLE               = 'test_apc'
        EVENTS_HES_APC_EPISODE_CATEGORY = 'test_category_hes'
        DEMOGRAPHIC_OUTPUT_FIELDS   = [
            'person_id', 'birth_date', 'dataset', 'record_date','ethnicity'
        ]

    test_params = TestParams()

    # INPUT DATA - EVENTS TABLE TEST SCHEMA
    input_schema = T.StructType([
        T.StructField('person_id', T.StringType(), False),
        T.StructField('birth_date', T.DateType(), False),
        T.StructField('dataset', T.StringType(), False),
        T.StructField('category', T.StringType(), False),
        T.StructField('code', T.StringType(), False),
        T.StructField('record_date', T.DateType(), False),
        T.StructField('code_array', T.ArrayType(T.StringType()), False),
        T.StructField('ethnicity', T.StringType(), True)
    ])

    # EXPECTED DATA - PATIENT TABLE TEST SCHEMA
    expected_schema = T.StructType([
        T.StructField('person_id', T.StringType(), False),
        T.StructField('birth_date', T.DateType(), False)
    ])

    # INPUT DATA - EVENTS TABLE TEST DATAFRAME
    df_input = spark.createDataFrame([
        ('001',date(2000,1,1),'test_cvdp','test_category_cvdp','HEARTATTACK',date(2022,1,1), ['P0001'], 'A'), # Known- remove
        ('001',date(2000,1,1),'test_cvdp','test_category_cvdp','HEARTATTACK',date(2022,1,1), ['P0001'], ''), # Unknown- remove
        ('001',date(2000,1,1),'test_cvdp','test_category_cvdp','HEARTATTACK',date(2022,2,1), ['P0001'], None),# Unknown- remove
        ('001',date(2000,1,1),'test_cvdp','test_category_cvdp','HEARTATTACK',date(2022,2,1), ['P0001'], 'B'), # Known- remove
        ('002',date(2000,1,2),'test_cvdp','test_category_cvdp','HEARTATTACK',date(2022,1,2), ['P0002'], 'Z'), # Unknown- keep
        ('002',date(2000,1,2),'test_cvdp','test_category_cvdp','HEARTATTACK',date(2022,1,2), ['P0001'], 'z'), # Unknown- keep
        ('003',date(2000,1,2),'test_cvdp','test_category_cvdp','HEARTATTACK',date(2022,1,2), ['P0001'], 'B'), # Known- remove
        ('003',date(2000,1,2),'test_cvdp','test_category_cvdp','HEARTATTACK',date(2020,1,2), ['P0001'], 'A'), # Known- remove
        ('004',date(2000,1,2),'test_cvdp','test_category_cvdp','HEARTATTACK',date(2022,1,2), ['P0001'], 'z'), # Unknown- remove
        ('004',date(2000,1,2),'test_cvdp','test_category_cvdp','HEARTATTACK',date(2020,1,2), ['P0001'], 'A'), # Known- remove
        ('004',date(2000,1,2),'test_apc','test_category_hes','HEARTATTACK',date(2020,1,2), ['P0001'], 'A'), # Remove
    ], input_schema)

    # EMPTY SCHEMA FOR BLANK TABLES
    empty_schema = T.StructType([
        T.StructField('idx', T.IntegerType(), False),
    ])

    #BLANK TABLES - NOT USED IN THIS FUNCTION
    df_journal = spark.createDataFrame([], empty_schema)
    df_hes_ae = spark.createDataFrame([], empty_schema)
    df_hes_op = spark.createDataFrame([], empty_schema)

    # EXPECTED DATA - PATIENT TABLE TEST DATAFRAME
    df_expected_unknown_ethnicity = spark.createDataFrame([
        ('002',date(2000,1,2))
    ], expected_schema)

    # DATA AND STAGE SETUP
    input_tmp_table_name = f'_temp_{uuid4().hex}'
    df_input.createOrReplaceGlobalTempView(input_tmp_table_name)


    with FunctionPatch('params',test_params):
        # STAGE INITIALISATION
        stage = CreateDemographicTableStage(events_table_input = 'events', cvdp_cohort_journal_input = 'journal',
                                            hes_op_input = 'hes_op', hes_ae_input = 'hes_ae',demographic_table_output = None
                                           )

        context = PipelineContext('12345', params, [CreateDemographicTableStage])
        context['events'] = PipelineAsset('events', context, df_input)
        context['journal'] = PipelineAsset('journal', context, df_journal)
        context['hes_ae'] = PipelineAsset('hes_ae', context, df_hes_ae)
        context['hes_op'] = PipelineAsset('hes_op', context, df_hes_op)
        stage._load_data_assets(context)
        stage._create_base_table()
        stage._find_known_ethnicity()
        df_actual_unknown_ethnicity = stage._data_holder['demographic_unknown']

    assert compare_results(df_actual_unknown_ethnicity, df_expected_unknown_ethnicity, join_columns=['person_id','birth_date'])

# COMMAND ----------

@suite.add_test
def test_enhance_ethnicity():
      # TEST PARAMETERS
    @dataclass(frozen = True)
    class TestParams(ParamsBase):
        DATASETS                    = ['test_ds1','test_ds2']
        DATABASE_NAME               = None
        PID_FIELD                   = 'person_id'
        COHORT_FIELD                = 'cohort'
        DOB_FIELD                   = 'birth_date'
        ETHNICITY_FIELD             = 'ethnicity'
        CODE_FIELD                  = 'code'
        DATASET_FIELD               = 'dataset'
        CATEGORY_FIELD              = 'category'
        RECORD_STARTDATE_FIELD      = 'record_start_date'
        CODE_ARRAY_FIELD            = 'code_array'
        EVENTS_CVDP_COHORT_DATASET  = 'test_cvdp'
        EVENTS_CVDP_COHORT_CATEGORY = 'test_category_cvdp'
        ETHNICITY_UNKNOWN_CODES     = ['z','Z','',None]
        HES_APC_TABLE               = 'test_apc'
        EVENTS_HES_APC_EPISODE_CATEGORY = 'test_category_hes'
        DEMOGRAPHIC_OUTPUT_FIELDS   = [
            'person_id', 'birth_date', 'dataset', 'record_start_date','ethnicity'
        ]
        JOURNAL_DATE_FIELD          = 'journal_date'
        REF_CODE_FIELD              = 'ConceptId'
        REF_ETHNICITY_CODE_FIELD    = 'PrimaryCode'
        GLOBAL_JOIN_KEY             = ['person_id','birth_date']

    test_params = TestParams()

    expected_schema = T.StructType([
        T.StructField('person_id', T.IntegerType(), False),
        T.StructField('birth_date', T.DateType(), False),
        T.StructField('dataset', T.StringType(), True),
        T.StructField('record_start_date', T.DateType(), True),
        T.StructField('ethnicity', T.StringType(), True)
    ])

    id_schema = T.StructType([
        T.StructField('person_id', T.IntegerType(), False),
        T.StructField('birth_date', T.DateType(), False)
    ])

    df_ids = spark.createDataFrame([
        (0,date(1960,1,1)),
        (1,date(1960,1,1)),
        (2,date(1960,1,1)),
        (4,date(1960,1,1)),
        (7,date(1960,1,1)),
        (9,date(1960,1,1)),
        (10,date(1960,1,1)),
    ], id_schema)

    journal_schema = T.StructType([
        T.StructField('person_id', T.IntegerType(), False),
        T.StructField('birth_date', T.DateType(), False),
        T.StructField('journal_date', T.DateType(), False),
        T.StructField('code', T.StringType(), True),
        T.StructField('CLUSTER_ID', T.StringType(), True)
    ])

    hes_schema = T.StructType([
        T.StructField('person_id', T.IntegerType(), False),
        T.StructField('birth_date', T.DateType(), False),
        T.StructField('dataset', T.StringType(), False),
        T.StructField('record_start_date', T.DateType(), False),
        T.StructField('ethnicity', T.StringType(), True),
        T.StructField('FOO', T.StringType(), True)
    ])

    df_journal = spark.createDataFrame([
    (0, date(1960,1,1), date(2022,1,1), '92571000000102', 'TEST_CLUSTER'), # PRIORITY ALL SAME DATE -> A
    (0, date(1960,1,1), date(2022,1,1), '92571000000102', 'DUPE_TEST_CLUSTER'), # PRIORITY ALL SAME DATE -> A (DUPE)
    (1, date(1960,1,1), date(2022,1,1), '92571000000102', 'TEST_CLUSTER'), # PRIORITY DUE TO RECENCY -> A
    (1, date(1960,1,1), date(2022,1,1), '92571000000102', 'DUPE_TEST_CLUSTER'), # PRIORITY DUE TO RECENCY -> A (DUPE)
    (2, date(1960,1,1), date(2022,1,1), '415794004', 'TEST_CLUSTER'), #INVALID ETHNICITY CODE
    (2, date(1960,1,1), date(2022,1,1), '415794004', 'DUPE_TEST_CLUSTER'), #INVALID ETHNICITY CODE
    (3, date(1960,1,1), date(2022,1,1), '91000000109', 'TEST_CLUSTER'), #INVALID SNOMED CODE
    (3, date(1960,1,1), date(2022,1,1), '91000000109', 'DUPE_TEST_CLUSTER'), #INVALID SNOMED CODE
    (7, date(1960,1,1), date(2019,1,1), '92571000000102', 'TEST_CLUSTER'), # NOT PRIORITY (MORE RECENT RECORD IN OTHER SOURCE) -> A
    (7, date(1960,1,1), date(2019,1,1), '92571000000102', 'DUPE_TEST_CLUSTER'), # NOT PRIORITY (MORE RECENT RECORD IN OTHER SOURCE) -> A (DUPE)
    (8, date(1960,1,1), date(2019,1,1), '92561000000109', 'TEST_CLUSTER'), # NOT PRIORITY (MORE RECENT RECORD IN OTHER SOURCE) -> C
    (8, date(1960,1,1), date(2019,1,1), '92561000000109', 'DUPE_TEST_CLUSTER'), # NOT PRIORITY (MORE RECENT RECORD IN OTHER SOURCE) -> C
    ], journal_schema)
    
    df_hes_ae = spark.createDataFrame([
    (0, date(1960,1,1), 'hes_ae', date(2022,1,1), 'B', 'BAR_1'), # NOT PRIORITY (RECORD ON SAME DATE FROM HIGHER PRIORITY SOURCE)
    (4, date(1960,1,1), 'hes_ae', date(2022,1,1), 'B', 'BAR_1'), # PRIORITY (RECORD ON SAME DATE FROM LOWER PRIORITY SOURCE)
    (7, date(1960,1,1), 'hes_ae', date(2022,1,1), 'Z', 'BAR_2'), #INVALID ETHNICITY CODE
    (8, date(1960,1,1), 'hes_ae', date(2022,1,1), None, 'BAR_2'), #INVALID ETHNICITY CODE
    (9, date(1960,1,1), 'hes_ae', date(2021,1,1), 'A', 'BAR_2'), # NOT PRIORITY (MORE RECENT RECORD IN OTHER SOURCE)
      ], hes_schema)

    df_hes_op = spark.createDataFrame([
    (0, date(1960,1,1), 'hes_op', date(2022,1,1), 'C', 'BAR_1'), # NOT PRIORITY (RECORD ON SAME DATE FROM HIGHER PRIORITY SOURCE)
    (1, date(1960,1,1), 'hes_op', date(2021,1,1), '', 'BAR_2'), #INVALID ETHNICITY CODE
    (2, date(1960,1,1), 'hes_op', date(2022,1,1), 'B', 'BAR_1'), # PRIORITY (MOST RECENT VALID RECORD FROM ANY SOURCE)
    (3, date(1960,1,1), 'hes_op', date(2022,1,1), None, 'BAR_2'), #INVALID ETHNICITY CODE
    (3, date(1960,1,1), 'hes_op', date(2022,1,1), 'B', 'BAR_2'), # NOT PRIORITY (RECORD ON SAME DATE FROM HIGHER PRIORITY SOURCE)
    (4, date(1960,1,1), 'hes_op', date(2022,1,1), 'A', 'BAR_2'), # NOT PRIORITY (RECORD ON SAME DATE FROM HIGHER PRIORITY SOURCE)
    (5, date(1960,1,1), 'hes_op', date(2022,1,1), 'A', 'BAR_2'), # PRIORITY (MOST RECENT VALID RECORD FROM ANY SOURCE) -> NOT IN IDS
    (6, date(1960,1,1), 'hes_op', date(2022,1,1), 'B', 'BAR_2'), # PRIORITY (MOST RECENT VALID RECORD FROM ANY SOURCE) -> NOT IN IDS
    (6, date(1960,1,1), 'hes_op', date(2019,1,1), 'A', 'BAR_2'), # NOT PRIORITY (MORE RECENT VALID RECORD FROM SAME SOURCE) -> NOT IN IDS
    (7, date(1960,1,1), 'hes_op', date(2022,1,1), 'B', 'BAR_2'), # PRIORITY (MOST RECENT VALID RECORD FROM ANY SOURCE)
    (8, date(1960,1,1), 'hes_op', date(2022,1,1), 'A', 'BAR_2'), # INVALID (MULTIPLE ETHNICITIES ON SAME DATE FROM SAME SOURCE) -> NOT IN IDS
    (8, date(1960,1,1), 'hes_op', date(2022,1,1), 'B', 'BAR_2'), # INVALID (MULTIPLE ETHNICITIES ON SAME DATE FROM SAME SOURCE) -> NOT IN IDS
    (9, date(1960,1,1), 'hes_op', date(2022,1,1), 'A', 'BAR_2'), # NOT PRIORITY (RECORD ON SAME DATE FROM HIGHER PRIORITY SOURCE)
      ], hes_schema)

    df_hes_apc = spark.createDataFrame([
    (0, date(1960,1,1), 'hes_apc', date(2022,1,1), 'C', 'BAR_1'), # NOT PRIORITY (RECORD ON SAME DATE FROM HIGHER PRIORITY SOURCE)
    (1, date(1960,1,1), 'hes_apc', date(2021,1,1), 'B', 'BAR_1'), # NOT PRIORITY (MORE RECENT RECORD IN OTHER SOURCE)
    (2, date(1960,1,1), 'hes_apc', date(2022,1,1), 'Z', 'BAR_2'), #INVALID ETHNICITY CODE
    (3, date(1960,1,1), 'hes_apc', date(2022,1,1), 'A', 'BAR_2'), # PRIORITY (RECORD ON SAME DATE FROM LOWER PRIORITY SOURCE) -> NOT IN IDS
    (4, date(1960,1,1), 'hes_apc', date(2022,1,1), None, 'BAR_2'), #INVALID ETHNICITY CODE,
    (9, date(1960,1,1), 'hes_apc', date(2022,1,1), 'A', 'BAR_2'), # MOST RECENT PRIORITY
      ], hes_schema)

    # EXPECTED DATA - ENHANCED ETHNICITY TABLE TEST DATAFRAME
    df_expected_enhanced = spark.createDataFrame([
        (0, date(1960,1,1), 'journal', date(2022,1,1), 'A'),
        (1, date(1960,1,1), 'journal', date(2022,1,1), 'A'),
        (2, date(1960,1,1), 'hes_op', date(2022,1,1), 'B'),
        (4, date(1960,1,1), 'hes_ae', date(2022,1,1), 'B'),
        (7, date(1960,1,1), 'hes_op', date(2022,1,1), 'B'),
        (9, date(1960,1,1), 'hes_apc', date(2022,1,1), 'A'),
        (10, date(1960,1,1), None, None, None),
    ], expected_schema)

    with FunctionPatch('params',test_params):
        # STAGE INITIALISATION
        stage = CreateDemographicTableStage(events_table_input = 'events', cvdp_cohort_journal_input = 'journal',
                                            hes_op_input = 'hes_op', hes_ae_input = 'hes_ae',demographic_table_output = None
                                           )

        context = PipelineContext('12345', params, [CreateDemographicTableStage])
        stage._data_holder['demographic_unknown'] = df_ids
        stage._data_holder['journal'] = df_journal
        stage._data_holder['hes_apc'] = df_hes_apc
        stage._data_holder['hes_ae'] = df_hes_ae
        stage._data_holder['hes_op'] = df_hes_op
        stage._enhance_ethnicity()
        df_actual_enhanced = stage._data_holder['demographic_enhanced']

    assert compare_results(df_actual_enhanced, df_expected_enhanced, join_columns=['person_id','birth_date'])

# COMMAND ----------

@suite.add_test
def test_consolidate_ethnicites():
      # TEST PARAMETERS
    @dataclass(frozen = True)
    class TestParams(ParamsBase):
        DATASETS                    = ['test_ds1','test_ds2']
        DATABASE_NAME               = None
        PID_FIELD                   = 'person_id'
        COHORT_FIELD                = 'cohort'
        DOB_FIELD                   = 'birth_date'
        ETHNICITY_FIELD             = 'ethnicity'
        CODE_FIELD                  = 'code'
        DATASET_FIELD               = 'dataset'
        CATEGORY_FIELD              = 'category'
        RECORD_STARTDATE_FIELD      = 'record_start_date'
        CODE_ARRAY_FIELD            = 'code_array'
        EVENTS_CVDP_COHORT_DATASET  = 'test_cvdp'
        EVENTS_CVDP_COHORT_CATEGORY = 'test_category_cvdp'
        ETHNICITY_UNKNOWN_CODES     = ['z','Z','',None]
        HES_APC_TABLE               = 'test_apc'
        EVENTS_HES_APC_EPISODE_CATEGORY = 'test_category_hes'
        DEMOGRAPHIC_OUTPUT_FIELDS   = [
          'person_id', 'birth_date', 'dataset', 'record_start_date','ethnicity'
        ]
        JOURNAL_DATE_FIELD          = 'journal_date'
        REF_CODE_FIELD              = 'ConceptId'
        REF_ETHNICITY_CODE_FIELD    = 'PrimaryCode'

    test_params = TestParams()

    # SCHEMA FOR ALL
    data_schema = T.StructType([
        T.StructField('person_id', T.IntegerType(), False),
        T.StructField('birth_date', T.DateType(), False),
        T.StructField('dataset', T.StringType(), False),
        T.StructField('record_start_date', T.DateType(), False),
        T.StructField('ethnicity', T.StringType(), True)
    ])

    expected_schema = T.StructType([
        T.StructField('person_id', T.IntegerType(), False),
        T.StructField('birth_date', T.DateType(), False),
        T.StructField('dataset', T.StringType(), False),
        T.StructField('record_start_date', T.DateType(), False),
        T.StructField('ethnicity', T.StringType(), True),
        T.StructField('enhanced_ethnicity_flag', T.IntegerType(), True),
    ])

    df_enhanced = spark.createDataFrame([
        (0, date(1960,1,1), 'journal', date(2022,1,1), 'A'),
        (1, date(1960,1,1), 'journal', date(2022,1,1), 'A'),
        (2, date(1960,1,1), 'hes_op', date(2022,1,1), 'B'),
        (4, date(1960,1,1), 'hes_ae', date(2022,1,1), 'B'),
        (7, date(1960,1,1), 'hes_op', date(2022,1,1), 'B'),
    ], data_schema)

    df_known = spark.createDataFrame([
        (3, date(1960,1,1), 'cvdp_cohort', date(2022,1,1), 'A'),
        (5, date(1960,1,1), 'cvdp_cohort', date(2022,1,1), 'A'),
        (6, date(1960,1,1), 'cvdp_cohort', date(2022,1,1), 'B'),
        (8, date(1960,1,1), 'cvdp_cohort', date(2022,1,1), 'B')
    ], data_schema)

    # EXPECTED DATA - DEMOGRAPHIC TABLE TEST DATAFRAME
    df_expected_demographic = spark.createDataFrame([
        (0, date(1960,1,1), 'journal', date(2022,1,1), 'A' , 1),
        (1, date(1960,1,1), 'journal', date(2022,1,1), 'A', 1),
        (2, date(1960,1,1), 'hes_op', date(2022,1,1), 'B', 1),
        (3, date(1960,1,1), 'cvdp_cohort', date(2022,1,1), 'A', 0),
        (4, date(1960,1,1), 'hes_ae', date(2022,1,1), 'B', 1),
        (5, date(1960,1,1), 'cvdp_cohort', date(2022,1,1), 'A', 0),
        (6, date(1960,1,1), 'cvdp_cohort', date(2022,1,1), 'B', 0),
        (7, date(1960,1,1), 'hes_op', date(2022,1,1), 'B', 1),
        (8, date(1960,1,1), 'cvdp_cohort', date(2022,1,1), 'B', 0)
    ], expected_schema)

    with FunctionPatch('params',test_params):
        # STAGE INITIALISATION
        stage = CreateDemographicTableStage(events_table_input = 'events', cvdp_cohort_journal_input = 'journal',
                                            hes_op_input = 'hes_op', hes_ae_input = 'hes_ae',demographic_table_output = None
                                           )

        context = PipelineContext('12345', params, [CreateDemographicTableStage])
        stage._data_holder['demographic_known'] = df_known
        stage._data_holder['demographic_enhanced'] = df_enhanced
        stage._consolidate_ethnicities()
        df_actual_demographic = stage._data_holder['demographic_data']

    assert compare_results(df_actual_demographic, df_expected_demographic, join_columns=['person_id','birth_date'])

# COMMAND ----------

suite.run()

# COMMAND ----------

dbutils.notebook.exit('pass')