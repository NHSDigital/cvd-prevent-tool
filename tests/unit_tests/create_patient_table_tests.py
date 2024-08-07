# Databricks notebook source
# MAGIC %run ../../src/test_helpers

# COMMAND ----------

PARAMS_PATH = 'default'

# COMMAND ----------

# MAGIC %run ../../pipeline/create_patient_table

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
def test_create_base_table():
    '''test_create_base_table

    Test of sub-method _create_base_table in the CreatePatientTableStage class. The extended functionality of
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
        CODE_FIELD                  = 'code'
        DATASET_FIELD               = 'dataset'
        CATEGORY_FIELD              = 'category'
        RECORD_STARTDATE_FIELD      = 'record_date'
        CODE_ARRAY_FIELD            = 'code_array'
        EVENTS_CVDP_COHORT_DATASET  = 'test_ds1'
        EVENTS_CVDP_COHORT_CATEGORY = 'ds1_extract'
        PATIENT_TABLE_BASE_FIELDS   = [
            'person_id','birth_date','code','record_date','code_array','test_demographic'
            ]
        PATIENT_TABLE_BASE_MAPPING  = {
            'code': 'cohort',
            'record_date': 'latest_extract_date',
            'code_array': 'latest_practice_id'
        }
        DEMOGRAPHIC_FIELDS_TO_ENHANCE = ['test_demographic']
        SWITCH_PATIENT_ENHANCE_DEMOGRAPHICS = True

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
        T.StructField('test_demographic', T.StringType(), True)
    ])

    # EXPECTED DATA - PATIENT TABLE TEST SCHEMA
    expected_schema = T.StructType([
        T.StructField('person_id', T.StringType(), False),
        T.StructField('birth_date', T.DateType(), False),
        T.StructField('cohort', T.StringType(), False),
        T.StructField('latest_extract_date', T.DateType(), False),
        T.StructField('latest_practice_id', T.StringType(), False)
    ])

    # INPUT DATA - EVENTS TABLE TEST DATAFRAME
    df_input = spark.createDataFrame([
        ('001',date(2000,1,1),'test_ds1','ds1_extract','CVD001',date(2022,1,1), ['P0001'], None), # Keep
        ('001',date(2000,1,1),'test_ds2','ds2_event','HEARTATTACK',date(2022,1,1), ['A'], None),  # Remove
        ('002',date(2000,1,2),'test_ds1','ds1_extract','CVD002',date(2022,1,2), ['P0002'], 'B'), # Keep
        ('002',date(2000,1,2),'test_ds2','ds2_event','STROKE',date(2022,1,2), ['C'], 'B'),       # Remove
        ('002',date(2000,1,2),'test_ds2','ds2_event','HEARTATTACK',date(2022,1,2), ['D'], 'C'),  # Remove
        ('003',date(2000,1,3),'test_ds2','ds2_event','MI',date(2022,1,3), ['E'], 'C'),           # Remove
    ], input_schema)

    blank_schema = T.StructType([
        T.StructField('idx', T.IntegerType(), False),
    ])

    #BLANK TABLES - NOT USED IN THIS FUNCTION
    df_indicators = spark.createDataFrame([], blank_schema)
    df_demographics = spark.createDataFrame([], blank_schema)

    # EXPECTED DATA - PATIENT TABLE TEST DATAFRAME
    df_expected = spark.createDataFrame([
        ('001',date(2000,1,1),'CVD001',date(2022,1,1), 'P0001'),
        ('002',date(2000,1,2),'CVD002',date(2022,1,2), 'P0002'),
    ], expected_schema)

    # DATA AND STAGE SETUP
    input_tmp_table_name = f'_temp_{uuid4().hex}'
    df_input.createOrReplaceGlobalTempView(input_tmp_table_name)

    with FunctionPatch('params',test_params):
        # STAGE INITIALISATION
        stage = CreatePatientTableStage(events_table_input = 'EVENTS',
                                        indicators_table_input = 'diag_flags',
                                        demographic_table_input = 'demographics',
                                        patient_table_output = None)

        context = PipelineContext('12345', params, [CreatePatientTableStage])
        context['EVENTS'] = PipelineAsset('EVENTS', context, df_input)
        context['diag_flags'] = PipelineAsset('diag_flags', context, df_indicators)
        context['demographics'] = PipelineAsset('demographics', context, df_demographics)
        stage._load_data_assets(context)
        stage._create_base_table()
        df_actual = stage._data_holder['patient']

    assert compare_results(df_actual, df_expected, join_columns=['person_id','birth_date'])

# COMMAND ----------

@suite.add_test
def test_add_demographic_data():
    '''test_add_demographic_data

    Test of sub-method _add_demographic_data in the CreatePatientTableStage class.
    Test returns the patient table with additional enhanced demographic columns
    '''
    # TEST PARAMETERS
    @dataclass(frozen = True)
    class TestParams(ParamsBase):
        DATASETS                    = ['test_ds1','test_ds2']
        DATABASE_NAME               = None
        PID_FIELD                   = 'person_id'
        COHORT_FIELD                = 'cohort'
        DOB_FIELD                   = 'birth_date'
        CODE_FIELD                  = 'code'
        DATASET_FIELD               = 'dataset'
        CATEGORY_FIELD              = 'category'
        RECORD_STARTDATE_FIELD      = 'record_date'
        CODE_ARRAY_FIELD            = 'code_array'
        EVENTS_CVDP_COHORT_DATASET  = 'test_ds1'
        EVENTS_CVDP_COHORT_CATEGORY = 'ds1_extract'
        PATIENT_TABLE_BASE_FIELDS   = [
            'person_id','birth_date','code','record_date','code_array','test_demographic'
            ]
        PATIENT_TABLE_BASE_MAPPING  = {
            'code': 'cohort',
            'record_date': 'latest_extract_date',
            'code_array': 'latest_practice_id'
        }
        DEMOGRAPHIC_FIELDS_TO_ENHANCE = ['test_demographic']
        SWITCH_PATIENT_ENHANCE_DEMOGRAPHICS = True

    test_params = TestParams()

    # EXPECTED DATA - PATIENT TABLE TEST SCHEMA
    patient_schema = T.StructType([
        T.StructField('person_id', T.StringType(), False),
        T.StructField('birth_date', T.DateType(), False),
        T.StructField('cohort', T.StringType(), False),
        T.StructField('latest_extract_date', T.DateType(), False),
        T.StructField('latest_practice_id', T.StringType(), False)
    ])

    demographics_schema = T.StructType([
        T.StructField('person_id', T.StringType(), False),
        T.StructField('birth_date', T.DateType(), False),
        T.StructField('dataset', T.StringType(), False),
        T.StructField('record_date', T.DateType(), False),
        T.StructField('test_demographic', T.StringType(), False)
    ])

    expected_schema = T.StructType([
        T.StructField('person_id', T.StringType(), False),
        T.StructField('birth_date', T.DateType(), False),
        T.StructField('cohort', T.StringType(), False),
        T.StructField('latest_extract_date', T.DateType(), False),
        T.StructField('latest_practice_id', T.StringType(), False),
        T.StructField('test_demographic', T.StringType(), True)
    ])
    empty_schema = T.StructType([
        T.StructField('idx', T.IntegerType(), False),
    ])

    # INPUT PATIENT TABLE
    df_patient = spark.createDataFrame([
        ('001',date(2000,1,1),'CVD001',date(2022,1,1), 'P0001'),
        ('002',date(2000,1,2),'CVD002',date(2022,1,2), 'P0002'),
    ], patient_schema)

    #BLANK TABLES - NOT USED IN THIS FUNCTION
    df_indicators = spark.createDataFrame([], empty_schema)
    df_events = spark.createDataFrame([], empty_schema)

    df_demographics = spark.createDataFrame([
        ('001',date(2000,1,1),'test_ds1',date(2022,1,1), 'A'),
    ], demographics_schema)

    # EXPECTED DATA - PATIENT TABLE TEST DATAFRAME
    df_expected = spark.createDataFrame([
        ('001',date(2000,1,1),'CVD001',date(2022,1,1), 'P0001', 'A'),
        ('002',date(2000,1,2),'CVD002',date(2022,1,2), 'P0002', None),
    ], expected_schema)

    # DATA AND STAGE SETUP
    input_tmp_table_name = f'_temp_{uuid4().hex}'
    df_patient.createOrReplaceGlobalTempView(input_tmp_table_name)

    with FunctionPatch('params',test_params):
        # STAGE INITIALISATION
        stage = CreatePatientTableStage(events_table_input = 'EVENTS',
                                        indicators_table_input = 'diag_flags',
                                        demographic_table_input = 'demographics',
                                        patient_table_output = None)

        context = PipelineContext('12345', params, [CreatePatientTableStage])
        context['EVENTS'] = PipelineAsset('EVENTS', context, df_events)
        context['diag_flags'] = PipelineAsset('diag_flags', context, df_indicators)
        context['demographics'] = PipelineAsset('demographics', context, df_demographics)
        stage._data_holder['demographics'] = df_demographics
        stage._data_holder['patient'] = df_patient
        stage._add_demographic_data()
        df_actual = stage._data_holder['patient']

    assert compare_results(df_actual, df_expected, join_columns=['person_id','birth_date'])

# COMMAND ----------

@suite.add_test
def test_add_deaths_data():

    @dataclass(frozen = True)
    class TestParams(ParamsBase):
        EVENTS_DARS_DATASET = 'test_deaths'
        EVENTS_DARS_CATEGORY = 'death'
        DATASET_FIELD = 'dataset'
        CATEGORY_FIELD = 'category'
        PID_FIELD = 'person_id'
        DOB_FIELD = 'birth_date'
        RECORD_STARTDATE_FIELD = 'record_date'
        DATE_OF_DEATH = 'death_date'
        FLAG_FIELD = 'flag'
        DEATH_FLAG = 'death_flag'
        GLOBAL_JOIN_KEY = ['person_id','birth_date']

    test_params = TestParams()

    input_events_schema = T.StructType([
        T.StructField('person_id', T.StringType(), False),
        T.StructField('birth_date', T.DateType(), False),
        T.StructField('dataset', T.StringType(), False),
        T.StructField('category', T.StringType(), False),
        T.StructField('flag', T.StringType(), True),
        T.StructField('record_date', T.DateType(), False),
    ])

    input_patient_schema = T.StructType([
        T.StructField('person_id', T.StringType(), False),
        T.StructField('birth_date', T.DateType(), False),
        T.StructField('cohort', T.StringType(), False)
    ])

    expected_patient_schema = T.StructType([
        T.StructField('person_id', T.StringType(), False),
        T.StructField('birth_date', T.DateType(), False),
        T.StructField('cohort', T.StringType(), False),
        T.StructField('death_date', T.DateType(), True),
        T.StructField('death_flag', T.StringType(), True)
    ])

    df_input_events = spark.createDataFrame([
        ('001',date(2000,1,1),'test_deaths','death','CVD',date(2020,1,1)),
        ('001',date(2000,1,1),'test_hosp','spell','A1',date(2021,1,1)),
        ('001',date(2000,1,1),'test_hosp','spell','A2',date(2021,1,2)),
        ('002',date(2001,1,1),'test_deaths','death',None,date(2020,1,1)),
        ('003',date(2002,1,1),'test_deaths','death','HEARTATTACK',date(2020,1,1)),
        ('004',date(2003,1,1),'test_deaths','death','STROKE',date(2020,1,1)),
        ('005',date(2004,1,1),'test_deaths','death',None,date(2020,1,1)),
        ('005',date(2004,1,1),'test_hosp','spell','B1',date(2021,1,1)),
        ('005',date(2004,1,1),'test_hosp','spell','B2',date(2021,1,2)),
        ('006',date(2005,1,1),'test_hosp','spell','C1',date(2021,1,2)),
    ], input_events_schema)

    df_input_patient = spark.createDataFrame([
        ('001',date(2000,1,1),'C01'),
        ('002',date(2001,1,1),'C02'),
        ('003',date(2002,1,1),'C01'),
        ('004',date(2003,1,1),'C02'),
        ('005',date(2004,1,1),'C01'),
        ('006',date(2005,1,1),'C02'),
        ('007',date(2006,1,1),'C01'),
    ], input_patient_schema)

    df_expected = spark.createDataFrame([
        ('001',date(2000,1,1),'C01',date(2020,1,1),'CVD'),
        ('002',date(2001,1,1),'C02',date(2020,1,1),None),
        ('003',date(2002,1,1),'C01',date(2020,1,1),'HEARTATTACK'),
        ('004',date(2003,1,1),'C02',date(2020,1,1),'STROKE'),
        ('005',date(2004,1,1),'C01',date(2020,1,1),None),
        ('006',date(2005,1,1),'C02',None,None),
        ('007',date(2006,1,1),'C01',None,None),
    ], expected_patient_schema)

    with FunctionPatch('params',test_params):
        stage = CreatePatientTableStage(events_table_input = None,
                                        indicators_table_input = None,
                                        demographic_table_input = None,
                                        patient_table_output = None)
        stage._data_holder['patient'] = df_input_patient
        stage._data_holder['events'] = df_input_events
        stage._add_deaths_data()
        df_actual =  stage._data_holder['patient']

    assert compare_results(df_actual, df_expected, join_columns=['person_id','birth_date'])


# COMMAND ----------

@suite.add_test
def test_add_hospitalisation_data():
    """test_add_hospitalisation_data()

    Testing of the patient table stage method _add_hospitalisation_data(). Events table contains HES APC Spell events,
    including stroke and heartattack, for patients in the patient table. The mode of create_patient_table_lib::add_stroke_mi_information()
    is set to keep events for dead and alive patient's, and inclusive date of death filtering (prior to or included date of death).

    Variables:
        keep_nulls == True
        keep_inclusive == True

    Associated:
        pipeline/create_patient_table::add_hospitalisation_data()
        pipeline/create_patient_table_lib::add_stroke_mi_information()
    """

    # Test Parameters
    ## Define test parameter values
    @dataclass(frozen = True)
    class TestParams(ParamsBase):
        CATEGORY_FIELD = 'category'
        COUNT_FLAG = 'count'
        DATASET_FIELD = 'dataset'
        DATE_OF_DEATH = 'date_of_death'
        DEATH_FLAG = 'death_flag'
        DOB_FIELD = 'birth_date'
        EVENTS_DARS_CATEGORY = 'death'
        EVENTS_DARS_DATASET = 'test_deaths'
        EVENTS_HES_APC_SPELL_CATEGORY= 'spell'
        FLAG_FIELD = 'flag'
        ASSOC_FLAG_FIELD = 'flag_assoc'
        COUNT_FLAG = 'count'
        GLOBAL_JOIN_KEY = ['person_id','birth_date']
        HEARTATTACK_FLAG = 'HEARTATTACK'
        HES_APC_TABLE = 'hes_apc'
        MAX_MI_DATE = 'max_mi_date'
        MAX_STROKE_DATE = 'max_stroke_date'
        MI_COUNT = 'mi_count'
        PID_FIELD = 'person_id'
        RECORD_STARTDATE_FIELD = 'record_date'
        STROKE_COUNT = 'stroke_count'
        STROKE_FLAG = 'STROKE'
    ## Define test parameter class
    test_params = TestParams()

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
        T.StructField('dataset', T.StringType(), False),
        T.StructField('category', T.StringType(), False),
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
        # person_id | birth_date | dataset | category | record_start_date | flag | flag_assoc
        # Patient 1 (Has Died, Valid Records)
        (1,date(2001,1,1),'hes_apc','spell',date(2011,1,1),'STROKE',None),                      # Keep - Before Death
        # Patient 2 (Has Died, Valid Records)
        (2,date(2002,2,2),'hes_apc','spell',date(2012,1,1),'HEARTATTACK',None),                 # Keep - Before Death
        (2,date(2002,2,2),'hes_apc','spell',date(2022,1,1),'HEARTATTACK',None),                 # Keep - On Death
        # Patient 3 (Has Died, Valid and Invalid Records)
        (3,date(2003,3,3),'hes_apc','spell',date(2013,1,1),'HEARTATTACK',None),                 # Keep - Before Death
        (3,date(2003,3,3),'hes_apc','spell',date(2023,1,1),'STROKE',None),                      # Keep - On Death
        (3,date(2003,3,3),'hes_apc','spell',date(2023,1,1),'MULTIPLE',['STROKE','non_cvd']),    # Keep - On Death
        (3,date(2003,3,3),'hes_apc','spell',date(2023,1,2),'STROKE',None),                      # Remove - After Death
        (3,date(2003,3,3),'hes_apc','spell',date(2023,1,2),'MULTIPLE',['HEARTATTACK','STROKE']),# Remove - After Death
        # Patient 4 (Has Died, Invalid Records)
        (4,date(2004,4,4),'hes_apc','spell',date(2024,1,2),'HEARTATTACK',None),                 # Remove - After Death
        # Patient 5 (Alive, Valid and Invalid Records)
        (5,date(2005,5,5),'hes_apc','spell',date(2015,1,1),'MULTIPLE',['HEARTATTACK','STROKE']),# Keep
        (5,date(2005,5,5),'hes_apc','spell',date(2015,1,1),'MULTIPLE',['no_cvd','hf']),         # Remove - No valid flags
        # Patient 6 (Alive, Invalid Records)
        (6,date(2006,6,6),'hes_apc','spell',date(2016,1,1),'hf',None),                          # Remove - No valid flags
        # Patient 7 (Alive, No HES APC Spell Records)
        (7,date(2007,7,7),'hes_apc','episode',date(2017,1,1),'STROKE',None),                    # Remove - Non-Spell
        (7,date(2007,7,7),'cvdp','smoking',date(2017,1,1),'all_smoking',None),                  # Remove - Non-HES
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
    with FunctionPatch('params',test_params):
        stage = CreatePatientTableStage(events_table_input = None,
                                        indicators_table_input = None,
                                        demographic_table_input = None,
                                        patient_table_output = None)
        stage._data_holder['patient'] = df_patient_input
        stage._data_holder['events'] = df_events_input
        stage._add_hospitalisation_data()
        df_actual =  stage._data_holder['patient']

    # Result comparison
    assert compare_results(df_actual, df_expected, join_columns=['person_id','birth_date'])


# COMMAND ----------

@suite.add_test
def test_add_patient_flags():

    @dataclass(frozen = True)
    class TestParams(ParamsBase):
        EVENTS_DARS_DATASET = 'test_deaths'
        EVENTS_DARS_CATEGORY = 'death'
        DATASET_FIELD = 'dataset'
        CATEGORY_FIELD = 'category'
        PID_FIELD = 'person_id'
        DOB_FIELD = 'birth_date'
        RECORD_STARTDATE_FIELD = 'record_date'
        DATE_OF_DEATH = 'death_date'
        FLAG_FIELD = 'flag'
        DEATH_FLAG = 'death_flag'
        HES_APC_TABLE = 'hes_apc'
        EVENTS_HES_APC_SPELL_CATEGORY= 'spell'
        EVENTS_HES_APC_EPISODE_CATEGORY = 'episode'
        STROKE_FLAG = 'STROKE'
        HEARTATTACK_FLAG = 'HEARTATTACK'
        COUNT_FLAG = 'count'
        STROKE_COUNT = 'stroke_count'
        MI_COUNT = 'mi_count'
        MAX_STROKE_DATE = 'max_stroke_date'
        MAX_MI_DATE = 'max_mi_date'
        GLOBAL_JOIN_KEY = ['person_id','birth_date']

    test_params = TestParams()

    df_patient_input = spark.createDataFrame([
    (1, date(2001,1,1) ,date(2024,1,1)),
    (2, date(2002,2,2) ,date(2024,1,1)),
    (3, date(2003,3,3) ,date(2024,1,1)),
    (4, date(2004,4,4) ,date(2020,1,1)),
  ]
  ,['person_id','birth_date','death_date']
  )

    df_flags_input = spark.createDataFrame([
      (1, date(2001,1,1), date(2020,1,1)),
      (2, date(2002,2,2), date(2020,1,1)),
      (3, date(2003,3,3), date(2020,1,1)),
      (4, date(2004,4,4), date(2020,1,1)),
    ], ['person_id', 'birth_date', 'AF_diagnosis_date'])

    df_expected = spark.createDataFrame([
      (1, date(2001,1,1) ,date(2024,1,1), date(2020,1,1)),
      (2, date(2002,2,2) ,date(2024,1,1), date(2020,1,1)),
      (3, date(2003,3,3) ,date(2024,1,1), date(2020,1,1)),
      (4, date(2004,4,4) ,date(2020,1,1), date(2020,1,1)),
    ], ['person_id', 'birth_date', 'death_date', 'AF_diagnosis_date'])

    df_events_input = spark.createDataFrame([
      (1,'episode', date(2001,1,1), date(2020,1,1),'HEARTATTACK','hes_apc'),
      (2,'episode', date(2002,2,2), date(2020,1,1),'HEARTATTACK','hes_apc'),
      (3,'episode', date(2003,3,3), date(2020,1,1),'HEARTATTACK','hes_apc'),
      (4,'spell',   date(2004,4,4), date(2020,1,1),'HEARTATTACK','hes_apc'),
    ],
    ['person_id','category', 'birth_date', 'record_start_date', 'flag','dataset'])


    def temp_create_patient_table_flags(df, events):
      return df

    with FunctionPatch('params',test_params):
      with FunctionPatch('create_patient_table_flags',temp_create_patient_table_flags):
        stage = CreatePatientTableStage(events_table_input = df_events_input,
                                        indicators_table_input = None,
                                        demographic_table_input = None,
                                        patient_table_output = None)
        stage._data_holder['patient'] = df_patient_input
        stage._data_holder['diag_flags'] = df_flags_input
        stage._data_holder['events'] = df_events_input
        stage._add_patient_flags()
        df_actual =  stage._data_holder['patient']

    assert compare_results(df_actual, df_expected, join_columns=['person_id','birth_date'])

# COMMAND ----------

@suite.add_test
def test_add_hypertension_data():
    '''
    create_patient_table::_add_hypertension_data
    Test takes a patient table (containing diagnostic flags) and an events table (containing many
    event types, including blood pressure measurement events) and produces a patient table with an
    appended column of the assigned/calculated hypertension risk group.
    '''

    @dataclass(frozen = True)
    class TestParams(ParamsBase):
        CVDP_HYP_RISK_FIELD = 'htn_risk_group'
        DICT_FLAG_SUFFIX = 'diag_flag'
        DOB_FIELD = 'dob'
        EVENTS_CVDP_HTN_CATEGORY = 'bp'
        EVENTS_CVDP_HTN_DATASET = 'cvd'
        FLAG_FIELD = 'flag'
        HYPERTENSION_FLAG_PREFIX = 'htn'
        LATEST_EXTRACT_DATE = 'extract_date'
        PID_FIELD = 'pid'
        RECORD_STARTDATE_FIELD = 'record_start_date'

    test_params = TestParams()

    df_input_patient = spark.createDataFrame([
        ('001',date(2000,1,1),date(2020,1,1),date(2010,1,1)),
        ('002',date(2000,1,2),date(2020,1,1),date(2010,1,1)),
        ('003',date(2000,1,3),date(2010,1,1),date(2010,1,1)),
        ('004',date(2000,1,4),date(2020,1,1),date(2010,1,1)),
        ('005',date(2000,1,5),date(2020,1,1),None),
        ('006',date(2000,1,6),date(2020,1,1),None),
    ],['pid','dob','extract_date','htn_diag_flag'])

    df_input_events = spark.createDataFrame([
        ('001',date(2000,1,1),date(2019,6,1),'cvd','bp','120/80','A'),
        ('001',date(2000,1,1),date(2020,1,1),'test_ds1','bp','frog','ribbit'),
        ('001',date(2000,1,1),date(2020,1,1),'test_ds2','foo','cat','meow'),
        ('002',date(2000,1,2),date(2017,6,1),'cvd','bp','90/60','A'),
        ('002',date(2000,1,2),date(2019,6,1),'cvd','bp','100/70','A'),
        ('002',date(2000,1,2),date(2019,8,1),'cvd','bp','130/90','C'),
        ('002',date(2000,1,2),date(2011,8,1),'test_ds1','foo','horse','neigh'),
        ('002',date(2000,1,2),date(2017,8,1),'test_ds1','fee','bird','chirp'),
        ('003',date(2000,1,3),date(2009,1,1),'cvd','bp','100/100','F'),
        ('003',date(2000,1,3),date(2011,1,1),'cvd','bp','90/90','A'),
        ('003',date(2000,1,3),date(2006,1,1),'cvd','bp','80/80','C'),
        ('004',date(2000,1,4),date(2000,1,1),'cvd','bp','100/100','B'),
        ('005',date(2020,1,5),date(2002,1,1),'cvd','af','high','low'),
        ('006',date(2000,1,6),date(2019,6,1),'cvd','bp','60/60','A'),
        ('007',date(2000,1,7),date(2020,1,1),'cvd','bp','100/100','A'),
        ('007',date(2000,1,7),date(2020,1,2),'test_ds1','fee','fi','fo'),
    ],['pid','dob','dataset','category','record_start_date','code','flag'])

    df_expected = spark.createDataFrame([
        ('001',date(2000,1,1),date(2020,1,1),date(2010,1,1),'A'),
        ('002',date(2000,1,2),date(2020,1,1),date(2010,1,1),'C'),
        ('003',date(2000,1,3),date(2010,1,1),date(2010,1,1),'F'),
        ('004',date(2000,1,4),date(2020,1,1),date(2010,1,1),'1e'),
        ('005',date(2000,1,5),date(2020,1,1),None,'-1'),
        ('006',date(2000,1,6),date(2020,1,1),None,'-1'),
    ],['pid','dob','extract_date','htn_diag_flag','htn_risk_group'])

    def temp_assign_htn_risk_groups(patient_table, events_table):
        return df_expected

    with FunctionPatch('params',test_params):
      with FunctionPatch('assign_htn_risk_groups',temp_assign_htn_risk_groups):
          stage = CreatePatientTableStage(events_table_input = None,
                                          indicators_table_input = None,
                                          demographic_table_input = None,
                                          patient_table_output = None)
          stage._data_holder['patient'] = df_input_patient
          stage._data_holder['diag_flags'] = None
          stage._data_holder['events'] = df_input_events
          stage._add_hypertension_data()
          df_actual = stage._data_holder['patient']

          assert compare_results(df_actual, df_expected, join_columns=['pid','dob'])

# COMMAND ----------

@suite.add_test
def test_format_patient_table():

    df_input = spark.createDataFrame([
        (0, 'frog', 'ribbit', 'green'),
        (1, 'cat', 'meow', 'ginger')
    ], ['idx', 'animal', 'noise', 'colour'])

    df_expected = spark.createDataFrame([
        (0, 'frog', 'green'),
        (1, 'cat', 'ginger')
    ], ['idx', 'animal', 'colour'])

    stage = CreatePatientTableStage(
        events_table_input = None,
        indicators_table_input = None,
        demographic_table_input = None,
        patient_table_output = None
        )
    stage._output_fields = ['idx','animal','colour']
    stage._data_holder['patient'] = df_input
    stage._format_patient_table()
    df_actual = stage._data_holder['patient']

    assert compare_results(df_actual, df_expected, join_columns=['idx'])

# COMMAND ----------

suite.run()

# COMMAND ----------

dbutils.notebook.exit('pass')