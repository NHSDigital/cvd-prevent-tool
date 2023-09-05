# Databricks notebook source
# MAGIC %run ../../src/test_helpers

# COMMAND ----------

PARAMS_PATH = 'default'

# COMMAND ----------

# MAGIC %run ../../pipeline/pseudonymised_asset_preparation

# COMMAND ----------

import pyspark.sql.functions as F
import pyspark.sql.types as T

from uuid import uuid4
from datetime import datetime, date

from dsp.validation.validator import compare_results

# COMMAND ----------

suite = FunctionTestSuite()

## The following are fake people and data created for test purposes

# COMMAND ----------

# Create test specific parameters
@dataclass(frozen = True)
class TestParams(ParamsBase):
    ASSOC_FLAG_FIELD = 'assoc_flag'
    ASSOC_REC_ID_FIELD = 'assoc_rec'
    CATEGORY_FIELD = 'category'
    CODE_ARRAY_FIELD = 'code_array'
    DATABASE_NAME = 'prevent_tool_collab'
    DATASET_FIELD = 'dataset'
    DEATH_30_HOSPITALISATION = 'dead_30'
    DOB_FIELD = 'dob'
    EVENTS_HES_EPISODE_CATEGORY = 'test_episode'
    EVENTS_HES_SPELL_CATEGORY = 'test_spell'
    FLAG_FIELD = 'flag'
    HES_APC_TABLE = 'test_hes'
    LATEST_PRACTICE_ID = 'latest_practice'
    PSEUDO_DB_PATH = 'prevent_tool_collab'
    PSEUDO_DOB_FIELD = 'dob_desen'
    PSEUDO_DOB_FORMAT = 'y'
    PSEUDO_EVENTS_COLUMNS_DROPPED = ('dob')
    PSEUDO_EVENTS_INCLUSION_CATEGORIES = ['test_1','test_episode','test_spell','test_bp']
    PSEUDO_EVENTS_INCLUSION_DATASETS = ['test_a','test_hes','test_cvd']
    PSEUDO_HES_FLAG_REMOVAL_VALUE = 'no_cvd'
    PSEUDO_PATIENT_COLUMNS_DROPPED = ('test_column')
    PSEUDO_SAVE_OVERWRITE = True
    PSEUDO_TABLE_PREFIX = 'cvdp_linkage'

# COMMAND ----------

@suite.add_test
def test_curate_events_table():
    '''test_curate_events_table

    Test of sub-method _curate_events_table in the PseudoPreparationStage class.
    Test returns a curated events table, that has (A) removed the DOB column and (B)
    removes any records that have a flag of 'no_cvd' (or None). Note: The latter case
    shouldn't be possible as all HES records are assigned a flag (the NO CVD flag is
    the default when no CVD-related conditions are specified).
    '''
    # Setup Test-Specific Parameters
    test_params = TestParams()

    # Schema: Input
    input_schema = T.StructType([
        T.StructField('person_id', T.StringType(), False),
        T.StructField('dob', T.DateType(), False),
        T.StructField('dataset', T.StringType(), False),
        T.StructField('category', T.StringType(), False),
        T.StructField('flag', T.StringType(), True),
    ])

    # Schema: Expected
    expected_schema = T.StructType([
        T.StructField('person_id', T.StringType(), False),
        T.StructField('dataset', T.StringType(), False),
        T.StructField('category', T.StringType(), False),
        T.StructField('flag', T.StringType(), True),
    ])

    # Input: Uncurated events table
    df_input = spark.createDataFrame([
        ('001',date(2000,1,1),'test_hes','test_episode','cvd'),     # Keep   (HES, Episode, CVD-related)
        ('002',date(2000,1,2),'test_hes','test_spell','cvd'),       # Keep   (HES, Spell, CVD-related)
        ('003',date(2000,1,3),'test_cvd','test_bp','foo'),          # Keep   (Non-HES, N/A, N/A)
        ('004',date(2000,1,4),'test_hes','test_episode','no_cvd'),  # Remove (HES, Episode, Non-CVD related)
        ('005',date(2000,1,5),'test_hes','test_episode',None),      # Remove (HES, Episode, NONE)
        ('006',date(2000,1,6),'test_hes','test_spell','no_cvd'),    # Remove (HES, Spell, Non-CVD related)
        ('007',date(2000,1,7),'test_hes','test_spell',None),        # Remove (HES, Spell, NONE)
        ('008',date(2000,1,8),'test_a','test_1',None),              # Keep (Inclusion dataset and category)
        ('009',date(2000,1,9),'test_b','test_1',None),              # Remove (Exclusion dataset)
        ('010',date(2000,1,10),'test_a','test_2',None),             # Remove (Exclusion category)
        ('011',date(2000,1,11),'test_c','test_1',None)              # Remove (Exclusion dataset)
    ],input_schema)

    # Blank patient input
    blank_schema = T.StructType([
        T.StructField('idx', T.IntegerType(), False),
    ])
    df_blank = spark.createDataFrame([], blank_schema)

    # Expected: Curated events table
    df_expected = spark.createDataFrame([
        ('001','test_hes','test_episode','cvd'),
        ('002','test_hes','test_spell','cvd'),
        ('003','test_cvd','test_bp','foo'),
        ('008','test_a','test_1',None),
    ],expected_schema)

    # Main Test Function
    with FunctionPatch('params',test_params):
        # Stage Initialisation
        stage = PseudoPreparationStage(
            events_table_input = 'test_events',
            patient_table_input = 'test_patient',
            curated_events_table_output = 'test_cvdp_linkage_events',
            curated_patient_table_output = None,
            is_integration = False,
        )
        context = PipelineContext('12345', params, [PseudoPreparationStage])
        context['test_events'] = PipelineAsset('test_events', context, df_input)
        context['test_patient'] = PipelineAsset('test_patient', context, df_blank)
        stage._load_uncurated_data_assets(context)
        stage._curate_events_table()
        df_actual = stage._data_holder['events']

    # Test Assertion
    assert compare_results(df_actual, df_expected, join_columns=['person_id'])

# COMMAND ----------

@suite.add_test
def test_curate_patient_table():
    '''test_curate_patient_table

    Test of sub-method _curate_patient_table in the PseudoPreparationStage class.
    Test returns a curated patient table, the date of birth column renamed and date 
    values reformatted to year only. 
    '''
    # Setup Test-Specific Parameters
    test_params = TestParams()
    
    # Schema: Input
    input_schema = T.StructType([
        T.StructField('person_id', T.StringType(), False),
        T.StructField('dob', T.DateType(), False),
        T.StructField('latest_practice', T.StringType(), True)
    ])
    
    # Schema: Expected
    expected_schema = T.StructType([
        T.StructField('person_id', T.StringType(), False),
        T.StructField('dob_desen', T.StringType(), False),
        T.StructField('latest_practice', T.StringType(), True)
    ])
    
    # Input: Uncurated patient table
    df_input = spark.createDataFrame([
        ('001', date(2000,1,1), '00001')
    ],input_schema)
    
    # Blank events input
    blank_schema = T.StructType([
        T.StructField('idx', T.IntegerType(), False),
    ])
    df_blank = spark.createDataFrame([], blank_schema)
    
    # Expected: Curated patient table
    df_expected = spark.createDataFrame([
        ('001','2000','00001')
    ],expected_schema)
    
    # Main Test Function
    with FunctionPatch('params',test_params):
        # Stage Initialisation
        stage = PseudoPreparationStage(
            events_table_input = 'test_events',
            patient_table_input = 'test_patient',
            curated_events_table_output = None,
            curated_patient_table_output = 'test_cvdp_linkage_patient',
            is_integration = False,
        )
        context = PipelineContext('12345', params, [PseudoPreparationStage])
        context['test_events'] = PipelineAsset('test_events', context, df_blank)
        context['test_patient'] = PipelineAsset('test_patient', context, df_input)
        stage._load_uncurated_data_assets(context)
        stage._curate_patient_table()
        df_actual = stage._data_holder['patient']
        
    # Test Assertion
    assert compare_results(df_actual, df_expected, join_columns=['person_id'])

# COMMAND ----------

@suite.add_test
def test_check_curated_assets_pass():
    '''test_check_curated_assets_pass

    Test of sub-method _check_curated_assets in the PseudoPreparationStage class.
    Test checks the assertion statements pass (return True) given the test input events
    and patient tables. If the assertions pass, this test will pass. 
    '''
    # Setup Test-Specific Parameters
    test_params = TestParams()
    
    # Schema: Events
    events_schema = T.StructType([
        T.StructField('person_id', T.StringType(), False),
        T.StructField('dataset', T.StringType(), False),
        T.StructField('category', T.StringType(), False),
        T.StructField('flag', T.StringType(), True),
    ])
    
    # Schema: Patient
    patient_schema = T.StructType([
        T.StructField('person_id', T.StringType(), False),
        T.StructField('dob_desen', T.StringType(), False),
    ])
    
    # Input: Curated events table
    df_events = spark.createDataFrame([
        ('001','test_hes','test_episode','cvd'),
        ('002','test_hes','test_spell','cvd'),
        ('003','test_cvd','test_bp','foo'),
        ('005','test_hes','test_episode',None),
    ],events_schema)
    
    # Input: Curated patient table
    df_patient = spark.createDataFrame([
        ('001','2000')
    ] , patient_schema)
    
    # Stage Setup
    with FunctionPatch('params',test_params):
        # Stage Initialisation
        stage = PseudoPreparationStage(
            events_table_input = 'test_events',
            patient_table_input = 'test_patient',
            curated_events_table_output = 'cvdp_linkage_events_test',
            curated_patient_table_output = 'cvdp_linkage_patient_test',
            is_integration = False,
        )
        context = PipelineContext('12345', params, [PseudoPreparationStage])
        context['test_events'] = PipelineAsset('test_events', context, df_events)
        context['test_patient'] = PipelineAsset('test_patient', context, df_patient)
        stage._load_uncurated_data_assets(context)
        
        # Assertion Unit Test: Try (Pass) Except (Fail)
        try:
          stage._check_curated_assets()
          assert True
        except:
          raise AssertionError('test_check_curated_assets_pass failed (data did not pass assertion checks)')
    

# COMMAND ----------

@suite.add_test
def test_check_curated_assets_fail_events_droppedColumnPresent():
  """test_check_curated_assets_fail_events_droppedColumnPresent
  
  Test criteria:
  - Events table: Column 'dob' present in input dataframe
  - Patient table: all pass
  """
  # Setup Test-Specific Parameters
  test_params = TestParams()
  
  # Schema: Patient
  patient_schema = T.StructType([
      T.StructField('person_id', T.StringType(), False),
      T.StructField('dob_desen', T.StringType(), False),
  ])
  
  # Input: Curated patient table
  df_patient = spark.createDataFrame([
      ('001', '2000')
  ], patient_schema)
    
  # Schema: Events
  events_schema = T.StructType([
        T.StructField('person_id', T.StringType(), False),
        T.StructField('dob', T.DateType(), False)
  ])
 
  # Input: Curated events table
  df_events = spark.createDataFrame([
        ('001', date(2000,1,1))
  ], events_schema)

  # Stage Setup
  with FunctionPatch('params',test_params):
      # Stage Initialisation
      stage = PseudoPreparationStage(
          events_table_input = 'test_events',
          patient_table_input = 'test_patient',
          curated_events_table_output = 'test_events_foo',
          curated_patient_table_output = 'test_patient_foo',
          is_integration = False,
      )
      context = PipelineContext('12345', params, [PseudoPreparationStage])
      context['test_events'] = PipelineAsset('test_events', context, df_events)
      context['test_patient'] = PipelineAsset('test_patient', context, df_patient)
      stage._load_uncurated_data_assets(context)

      # Assertion Unit Test: Try (Fail) Except (Pass)
      try:
          stage._check_curated_assets()
          raise AssertionError('test_check_curated_assets_fail failed (data incorrectly passed assertion checks)')
      except:
          assert True

# COMMAND ----------

@suite.add_test
def test_check_curated_assets_fail_events_excludedDatasetsPresent():
  """test_check_curated_assets_fail_events_excludedDatasetsPresent
  
  Test criteria:
  - Events table: Dataset not in TestParams.PSEUDO_EVENTS_INCLUSION_DATASETS present in input
  - Patient table: all pass
  """
  # Setup Test-Specific Parameters
  test_params = TestParams()
  
  # Schema: Patient
  patient_schema = T.StructType([
      T.StructField('person_id', T.StringType(), False),
      T.StructField('dob_desen', T.StringType(), False),
  ])
  
  # Input: Curated patient table
  df_patient = spark.createDataFrame([
      ('001', '2000')
  ] , patient_schema)
  
  # Schema: Events
  events_schema = T.StructType([
        T.StructField('person_id', T.StringType(), False),
        T.StructField('dataset', T.StringType(), False)
  ])
  
  # Input: Curated event table
  df_events = spark.createDataFrame([
        ('001', 'mystery_dataset')
  ], events_schema)
  
  # Stage Setup
  with FunctionPatch('params',test_params):
      # Stage Initialisation
      stage = PseudoPreparationStage(
          events_table_input = 'test_events',
          patient_table_input = 'test_patient',
          curated_events_table_output = 'test_events_foo',
          curated_patient_table_output = 'test_patient_foo',
          is_integration = False,
      )
      context = PipelineContext('12345', params, [PseudoPreparationStage])
      context['test_events'] = PipelineAsset('test_events', context, df_events)
      context['test_patient'] = PipelineAsset('test_patient', context, df_patient)
      stage._load_uncurated_data_assets(context)

      # Assertion Unit Test: Try (Fail) Except (Pass)
      try:
          stage._check_curated_assets()
          raise AssertionError('test_check_curated_assets_fail failed (data incorrectly passed assertion checks)')
      except:
          assert True

# COMMAND ----------

@suite.add_test
def test_check_curated_assets_fail_events_excludedCategoriesPresent():
  """test_check_curated_assets_fail_events_excludedCategoriesPresent
  
  Test criteria:
  - Events table: Category not in TestParams.PSEUDO_EVENTS_INCLUSION_CATEGORIES present in input
  - Patient table: all pass
  """
  # Setup Test-Specific Parameters
  test_params = TestParams()
  
  # Schema: Patient
  patient_schema = T.StructType([
      T.StructField('person_id', T.StringType(), False),
      T.StructField('dob_desen', T.StringType(), False),
  ])
  
  # Input: Curated patient table
  df_patient = spark.createDataFrame([
      ('001', '2000')
  ] , patient_schema)
  
  # Schema: Events
  events_schema = T.StructType([
        T.StructField('person_id', T.StringType(), False),
        T.StructField('category', T.StringType(), False)
  ])
  
  # Input: Curated event table
  df_events = spark.createDataFrame([
        ('001', 'mystery_category')
  ], events_schema)
  
  # Stage Setup
  with FunctionPatch('params',test_params):
      # Stage Initialisation
      stage = PseudoPreparationStage(
          events_table_input = 'test_events',
          patient_table_input = 'test_patient',
          curated_events_table_output = 'test_events_foo',
          curated_patient_table_output = 'test_patient_foo',
          is_integration = False,
      )
      context = PipelineContext('12345', params, [PseudoPreparationStage])
      context['test_events'] = PipelineAsset('test_events', context, df_events)
      context['test_patient'] = PipelineAsset('test_patient', context, df_patient)
      stage._load_uncurated_data_assets(context)

      # Assertion Unit Test: Try (Fail) Except (Pass)
      try:
          stage._check_curated_assets()
          raise AssertionError('test_check_curated_assets_fail failed (data incorrectly passed assertion checks)')
      except:
          assert True

# COMMAND ----------

@suite.add_test
def test_check_curated_assets_fail_events_nonCvdEpisodesPresent():
  """test_check_curated_assets_fail_events_nonCvdEpisodesPresent
  
  Test criteria:
  - Events table: Flag value for test_episode (Category) records contains TestParams.PSEUDO_HES_FLAG_REMOVAL_VALUE
  - Patient table: all pass
  """
  # Setup Test-Specific Parameters
  test_params = TestParams()
  
  # Schema: Patient
  patient_schema = T.StructType([
      T.StructField('person_id', T.StringType(), False),
      T.StructField('dob_desen', T.StringType(), False),
  ])

  # Input: Curated patient table
  df_patient = spark.createDataFrame([
      ('001', '2000')
  ] , patient_schema)
    
  # Schema: Events
  events_schema = T.StructType([
        T.StructField('person_id', T.StringType(), False),
        T.StructField('category', T.StringType(), False),
        T.StructField('flag', T.StringType(), True)
  ])
  
  # Input: Curated event table
  df_events = spark.createDataFrame([
        ('001', 'test_episode', 'no_cvd')
  ], events_schema)
  
  # Stage Setup
  with FunctionPatch('params',test_params):
      # Stage Initialisation
      stage = PseudoPreparationStage(
          events_table_input = 'test_events',
          patient_table_input = 'test_patient',
          curated_events_table_output = 'test_events_foo',
          curated_patient_table_output = 'test_patient_foo',
          is_integration = False,
      )
      context = PipelineContext('12345', params, [PseudoPreparationStage])
      context['test_events'] = PipelineAsset('test_events', context, df_events)
      context['test_patient'] = PipelineAsset('test_patient', context, df_patient)
      stage._load_uncurated_data_assets(context)

      # Assertion Unit Test: Try (Fail) Except (Pass)
      try:
          stage._check_curated_assets()
          raise AssertionError('test_check_curated_assets_fail failed (data incorrectly passed assertion checks)')
      except:
          assert True

# COMMAND ----------

@suite.add_test
def test_check_curated_assets_fail_events_nonCvdSpellsPresent():
  """test_check_curated_assets_fail_events_nonCvdSpellsPresent
  
  Test criteria:
  - Events table: Flag value for test_spell (Category) records contains TestParams.PSEUDO_HES_FLAG_REMOVAL_VALUE
  - Patient table: all pass
  """
  # Setup Test-Specific Parameters
  test_params = TestParams()
  
  # Schema: Patient
  patient_schema = T.StructType([
      T.StructField('person_id', T.StringType(), False),
      T.StructField('dob_desen', T.StringType(), False),
  ])
  
  # Input: Curated patient table
  df_patient = spark.createDataFrame([
      ('001', '2000')
  ] , patient_schema)
    
  # Schema: Events
  events_schema = T.StructType([
        T.StructField('person_id', T.StringType(), False),
        T.StructField('category', T.StringType(), False),
        T.StructField('flag', T.StringType(), True)
  ])
  
  # Input: Curated event table
  df_events = spark.createDataFrame([
        ('001', 'test_spell', 'no_cvd')
  ], events_schema)
  
  # Stage Setup
  with FunctionPatch('params',test_params):
      # Stage Initialisation
      stage = PseudoPreparationStage(
          events_table_input = 'test_events',
          patient_table_input = 'test_patient',
          curated_events_table_output = 'test_events_foo',
          curated_patient_table_output = 'test_patient_foo',
          is_integration = False,
      )
      context = PipelineContext('12345', params, [PseudoPreparationStage])
      context['test_events'] = PipelineAsset('test_events', context, df_events)
      context['test_patient'] = PipelineAsset('test_patient', context, df_patient)
      stage._load_uncurated_data_assets(context)

      # Assertion Unit Test: Try (Fail) Except (Pass)
      try:
          stage._check_curated_assets()
          raise AssertionError('test_check_curated_assets_fail failed (data incorrectly passed assertion checks)')
      except:
          assert True

# COMMAND ----------

@suite.add_test
def test_check_curated_assets_fail_patient_droppedColumnPresent():
  """test_check_curated_assets_fail_patient_droppedColumnPresent
  
  Test criteria:
  - Events table: all pass
  - Patient table: Patient input contains columns in TestParams.PSEUDO_PATIENT_COLUMNS_DROPPED
  """
  # Setup Test-Specific Parameters
  test_params = TestParams()
  
  # Schema: Events
  events_schema = T.StructType([
      T.StructField('person_id', T.StringType(), False),
      T.StructField('dataset', T.StringType(), False),
      T.StructField('category', T.StringType(), False),
      T.StructField('flag', T.StringType(), True),
  ])

  # Input: Curated events table
  df_events = spark.createDataFrame([
      ('001', 'test_hes', 'test_episode', 'cvd'),
      ('002', 'test_hes', 'test_spell', 'cvd'),
      ('003', 'test_cvd', 'test_bp', 'foo'),
      ('005', 'test_hes', 'test_episode', None),
  ], events_schema)
  
  # Schema: Patient
  patient_schema = T.StructType([
        T.StructField('person_id', T.StringType(), False),
        T.StructField('test_column', T.StringType(), False)
  ])

  # Input: Curated patient table
  df_patient = spark.createDataFrame([
        ('001', '000001')
  ], patient_schema)

  # Stage Setup
  with FunctionPatch('params',test_params):
      # Stage Initialisation
      stage = PseudoPreparationStage(
          events_table_input = 'test_events',
          patient_table_input = 'test_patient',
          curated_events_table_output = 'test_events_foo',
          curated_patient_table_output = 'test_patient_foo',
          is_integration = False,
      )
      context = PipelineContext('12345', params, [PseudoPreparationStage])
      context['test_events'] = PipelineAsset('test_events', context, df_events)
      context['test_patient'] = PipelineAsset('test_patient', context, df_patient)
      stage._load_uncurated_data_assets(context)

      # Assertion Unit Test: Try (Fail) Except (Pass)
      try:
          stage._check_curated_assets()
          raise AssertionError('test_check_curated_assets_fail failed (data incorrectly passed assertion checks)')
      except:
          assert True

# COMMAND ----------

@suite.add_test
def test_check_curated_assets_fail_patient_dobColumnPresent():
  """test_check_curated_assets_fail_patient_dobColumnPresent
  
  Test criteria:
  - Events table: all pass
  - Patient table: Patient input contains column specified in TestParams.DOB_FIELD
  """
  # Setup Test-Specific Parameters
  test_params = TestParams()
  
  # Schema: Events
  events_schema = T.StructType([
      T.StructField('person_id', T.StringType(), False),
      T.StructField('dataset', T.StringType(), False),
      T.StructField('category', T.StringType(), False),
      T.StructField('flag', T.StringType(), True),
  ])

  # Input: Curated events table
  df_events = spark.createDataFrame([
      ('001', 'test_hes', 'test_episode', 'cvd'),
      ('002', 'test_hes', 'test_spell', 'cvd'),
      ('003', 'test_cvd', 'test_bp', 'foo'),
      ('005', 'test_hes', 'test_episode', None),
  ], events_schema)
  
  # Schema: Patient
  patient_schema = T.StructType([
        T.StructField('person_id', T.StringType(), False),
        T.StructField('dob', T.DateType(), False)
  ])
  
  # Input: Curated patient table
  df_patient = spark.createDataFrame([
        ('001', date(2000,1,2))
  ], patient_schema)

  # Stage Setup
  with FunctionPatch('params',test_params):
      # Stage Initialisation
      stage = PseudoPreparationStage(
          events_table_input = 'test_events',
          patient_table_input = 'test_patient',
          curated_events_table_output = 'test_events_foo',
          curated_patient_table_output = 'test_patient_foo',
          is_integration = False,
      )
      context = PipelineContext('12345', params, [PseudoPreparationStage])
      context['test_events'] = PipelineAsset('test_events', context, df_events)
      context['test_patient'] = PipelineAsset('test_patient', context, df_patient)
      stage._load_uncurated_data_assets(context)

      # Assertion Unit Test: Try (Fail) Except (Pass)
      try:
          stage._check_curated_assets()
          raise AssertionError('test_check_curated_assets_fail failed (data incorrectly passed assertion checks)')
      except:
          assert True

# COMMAND ----------

@suite.add_test
def test_check_curated_assets_fail_patient_pseudoDobColumnAbsent():
  """test_check_curated_assets_fail_patient_pseudoDobColumnAbsent
  
  Test criteria:
  - Events table: all pass
  - Patient table: Patient input does not contain column specified in TestParams.PSEUDO_DOB_FIELD
  """
  # Setup Test-Specific Parameters
  test_params = TestParams()
  
  # Schema: Events
  events_schema = T.StructType([
      T.StructField('person_id', T.StringType(), False),
      T.StructField('dataset', T.StringType(), False),
      T.StructField('category', T.StringType(), False),
      T.StructField('flag', T.StringType(), True),
  ])

  # Input: Curated events table
  df_events = spark.createDataFrame([
      ('001', 'test_hes', 'test_episode', 'cvd'),
      ('002', 'test_hes', 'test_spell', 'cvd'),
      ('003', 'test_cvd', 'test_bp', 'foo'),
      ('005', 'test_hes', 'test_episode', None),
  ], events_schema)
  
  # Schema: Patient
  patient_schema = T.StructType([
        T.StructField('person_id', T.StringType(), False),
        T.StructField('asdf', T.StringType(), False)
  ])
  
  # Input: Curated patient table
  df_patient = spark.createDataFrame([
        ('001', 'ghjk')
  ], patient_schema)
  
  # Stage Setup
  with FunctionPatch('params',test_params):
      # Stage Initialisation
      stage = PseudoPreparationStage(
          events_table_input = 'test_events',
          patient_table_input = 'test_patient',
          curated_events_table_output = 'test_events_foo',
          curated_patient_table_output = 'test_patient_foo',
          is_integration = False,
      )
      context = PipelineContext('12345', params, [PseudoPreparationStage])
      context['test_events'] = PipelineAsset('test_events', context, df_events)
      context['test_patient'] = PipelineAsset('test_patient', context, df_patient)
      stage._load_uncurated_data_assets(context)

      # Assertion Unit Test: Try (Fail) Except (Pass)
      try:
          stage._check_curated_assets()
          raise AssertionError('test_check_curated_assets_fail failed (data incorrectly passed assertion checks)')
      except:
          assert True

# COMMAND ----------

@suite.add_test
def test_check_curated_assets_fail_patient_fullBirthDatesPresent():
  """test_check_curated_assets_fail_patient_fullBirthDatesPresent
  
  Test criteria:
  - Events table: all pass
  - Patient table: Patient input column of TestParams.PSEUDO_DOB_FIELD contains date values of DD-MM-YYYY
  """
  # Setup Test-Specific Parameters
  test_params = TestParams()
  
  # Schema: Events
  events_schema = T.StructType([
      T.StructField('person_id', T.StringType(), False),
      T.StructField('dataset', T.StringType(), False),
      T.StructField('category', T.StringType(), False),
      T.StructField('flag', T.StringType(), True),
  ])

  # Input: Curated events table
  df_events = spark.createDataFrame([
      ('001', 'test_hes', 'test_episode', 'cvd'),
      ('002', 'test_hes', 'test_spell', 'cvd'),
      ('003', 'test_cvd', 'test_bp', 'foo'),
      ('005', 'test_hes', 'test_episode', None),
  ], events_schema)
  
  # Schema: Patient
  patient_schema = T.StructType([
        T.StructField('person_id', T.StringType(), False),
        T.StructField('dob_desen', T.StringType(), False)
  ])
  
  # Input: Curated patient table
  df_patient = spark.createDataFrame([
        ('001', str(date(2000,1,1)))
  ], patient_schema)
  
  # Stage Setup
  with FunctionPatch('params',test_params):
      # Stage Initialisation
      stage = PseudoPreparationStage(
          events_table_input = 'test_events',
          patient_table_input = 'test_patient',
          curated_events_table_output = 'test_events_foo',
          curated_patient_table_output = 'test_patient_foo',
          is_integration = False,
      )
      context = PipelineContext('12345', params, [PseudoPreparationStage])
      context['test_events'] = PipelineAsset('test_events', context, df_events)
      context['test_patient'] = PipelineAsset('test_patient', context, df_patient)
      stage._load_uncurated_data_assets(context)

      # Assertion Unit Test: Try (Fail) Except (Pass)
      try:
          stage._check_curated_assets()
          raise AssertionError('test_check_curated_assets_fail failed (data incorrectly passed assertion checks)')
      except:
          assert True

# COMMAND ----------

@suite.add_test
def test_convert_schema_csv_compatible():
    """test_convert_schema_csv_compatible
    
    Test of sub-method _convert_schema_csv_compatible.
    Assertion is based on absence of csv incompatible data types:
    - ArrayType(),
    - MapType(),
    - StructType(),
    - BinaryType()
    """
    
    # Setup Test-Specific Parameters
    test_params = TestParams()

    # Schema: Events (input)
    events_input_schema = T.StructType([
        T.StructField('person_id', T.StringType(), False),
        T.StructField('dob', T.DateType(), False),
        T.StructField('dataset', T.StringType(), False),
        T.StructField('category', T.StringType(), False),
        T.StructField('flag', T.StringType(), True),
        T.StructField('assoc_flag', T.ArrayType(T.StringType()), True),
        T.StructField('assoc_rec', T.ArrayType(T.StringType()), True),
        T.StructField('code_array', T.ArrayType(T.StringType()), True),
    ])

    # Schema: Patient (input)
    patient_input_schema = T.StructType([
        T.StructField('person_id', T.StringType(), False),
        T.StructField('dob', T.DateType(), False),
        T.StructField('latest_practice_identifier', T.StringType(), False),
        T.StructField('dead_30', T.ArrayType(T.StringType()), True),
    ])

    # Schema: Events (input)
    events_expected_schema = T.StructType([
        T.StructField('person_id', T.StringType(), False),
        T.StructField('dob', T.DateType(), False),
        T.StructField('dataset', T.StringType(), False),
        T.StructField('category', T.StringType(), False),
        T.StructField('flag', T.StringType(), True),
        T.StructField('assoc_flag', T.StringType(), True),
        T.StructField('assoc_rec', T.StringType(), True),
        T.StructField('code_array', T.StringType(), True),
    ])

    # Schema: Patient (output)
    patient_expected_schema = T.StructType([
        T.StructField('person_id', T.StringType(), False),
        T.StructField('dob', T.DateType(), False),
        T.StructField('latest_practice_identifier', T.StringType(), False),
        T.StructField('dead_30', T.StringType(), True),
    ])

    # Input: Curated events table
    df_events_input = spark.createDataFrame([
        ('001',date(2000,1,1),'test_hes','test_episode','cvd',['A','B'],['C','D'],['E','F']),
        ('002',date(2000,1,2),'test_hes','test_spell','cvd',['A',None],['C','D'],['E','F']),
        ('003',date(2000,1,3),'test_cvd','test_bp','foo',['A','B'],['C',None],['E','F']),
        ('005',date(2000,1,4),'test_hes','test_episode','no_cvd',['A','B'],['C','D'],['E',None]),
        ('006',date(2000,1,2),'test_hes','test_spell','no_cvd',[None,None],['C','D'],['E','F']),
    ],events_input_schema)

    # Input: Curated patient table
    df_patient_input = spark.createDataFrame([
        ('001',date(2000,1,1),'000001',['A','B']),
        ('002',date(2000,1,2),'000002',['A',None]),
        ('003',date(2000,1,3),'000003',[None,'B']),
        ('004',date(2000,1,4),'000004',[None,None]),
    ],patient_input_schema)

    # Expected: CSV converted events table
    df_events_expected = spark.createDataFrame([
        ('001',date(2000,1,1),'test_hes','test_episode','cvd','A,B','C,D','E,F'),
        ('002',date(2000,1,2),'test_hes','test_spell','cvd','A','C,D','E,F'),
        ('003',date(2000,1,3),'test_cvd','test_bp','foo','A,B','C','E,F'),
        ('005',date(2000,1,4),'test_hes','test_episode','no_cvd','A,B','C,D','E'),
        ('006',date(2000,1,2),'test_hes','test_spell','no_cvd','','C,D','E,F'),
    ],events_expected_schema)

    # Expected: CSV converted patient table
    df_patient_expected = spark.createDataFrame([
        ('001',date(2000,1,1),'000001','A,B'),
        ('002',date(2000,1,2),'000002','A'),
        ('003',date(2000,1,3),'000003','B'),
        ('004',date(2000,1,4),'000004',''),
    ],patient_expected_schema)

    # Stage Setup
    with FunctionPatch('params',test_params):
        # Stage Initialisation
        stage = PseudoPreparationStage(
            events_table_input = 'test_events',
            patient_table_input = 'test_patient',
            curated_events_table_output = 'test_events_foo',
            curated_patient_table_output = 'test_patient_foo',
            is_integration = False,
        )
        context = PipelineContext('12345', params, [PseudoPreparationStage])
        context['test_events'] = PipelineAsset('test_events', context, df_events_input)
        context['test_patient'] = PipelineAsset('test_patient', context, df_patient_input)
        stage._load_uncurated_data_assets(context)
        # Create the actual dataframes
        stage._convert_schema_csv_compatible()
        # Assign actual
        df_actual_events = stage._data_holder['events']
        df_actual_patient = stage._data_holder['patient']

        # Compare
        assert compare_results(df_actual_events, df_events_expected, join_columns=['person_id'])
        assert compare_results(df_actual_patient, df_patient_expected, join_columns=['person_id'])

# COMMAND ----------

suite.run()

# COMMAND ----------

dbutils.notebook.exit('pass')
