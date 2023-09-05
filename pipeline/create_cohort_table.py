# Databricks notebook source
# create_cohort_table

# COMMAND ----------

import collections
import pyspark.sql.functions as F
import warnings

from pyspark.sql import DataFrame

# COMMAND ----------

# MAGIC %run ./pipeline_util

# COMMAND ----------

# MAGIC %run ../params/params

# COMMAND ----------

# MAGIC %run ../src/cvdp/preprocess_cvdp_cohort

# COMMAND ----------

# MAGIC %run ../src/cvdp/preprocess_cvdp_journal

# COMMAND ----------

class CreatePatientCohortTableStage(PipelineStage):
  '''CreatePatientCohortTableStage
  A pipeline stage to create the eligble patient cohort table
  
  Args:
    cohort_opt (str, optional): Previously created eligible cohort table. Defaults to ''.
  
  Returns:
    patient_cohort_table_output (str): Context key for the patient cohort table
    patient_cohort_journal_output (str): Context key for the patient journal table
  
  '''
  
  def __init__(self, patient_cohort_table_output: str, patient_cohort_journal_output: str, cohort_opt: str = ''):
    '''__init__
    Contains the main run() stage definition and the associated class methods.
    
    Args:
      patient_cohort_table_output (str):    Context key for the eligible patient cohort table 
                                            pipeline asset [output].
      patient_cohort_journal_output (str):  Context key for the eligible patient journal table
                                            pipeline asset [output].
      
    Returns:
      self.patient_cohort_table_output (DataFrame):   Eligible patient cohort table dataframe assigned 
                                                      to stage class [PipelineContext(PipelineAsset)].
      self.patient_cohort_journal_output (DataFrame): Eligible patient journal table dataframe assigned 
                                                      to stage class [PipelineContext(PipelineAsset)].
    '''
    self.patient_cohort_table_output: str = patient_cohort_table_output
    self.patient_cohort_journal_output: str = patient_cohort_journal_output
    self._cohort_opt: str = cohort_opt
    self._data_holder: Dict[str, DataFrame] = {}
    super(CreatePatientCohortTableStage, self).__init__(set(), {self.patient_cohort_table_output, self.patient_cohort_journal_output})
  
  
  ### MAIN STAGE RUN
  def _run(self, context, log, limit: bool = False):
    
    log._add_stage(self.name)
    
    log._timer(self.name)
    
    ## COHORT TABLE - CONDITIONAL
    table_bool = table_exists(params.DATABASE_NAME, self._cohort_opt)

    ## COHORT TABLE - NOT DEFINED
    if self._cohort_opt == '':
      ## ELIGIBLE COHORT TABLE PROCESSING
      self._load_cvdp_store_data()
      self._preprocess_cvdp_store_data_cohort()
      
      ## LIMIT ROWS TO N RANDOM ROWS
      if limit == True:
        print('INFO: Pipeline Stage being Ran in Limit Mode')
        self._limit_cohort()
      ## ELIGIBLE JOURNAL TABLE PROCESSING
      self._preprocess_cvdp_store_data_journal()
      ## DATA SCHEMA PROCESSING
      self._transform_cohort_schema()
      self._transform_journal_schema()
      
      log._timer(self.name, end=True)
      
      return {
        self.patient_cohort_table_output:   PipelineAsset(self.patient_cohort_table_output, context, df = self._data_holder['cohort_table'], cache = True),
        self.patient_cohort_journal_output: PipelineAsset(self.patient_cohort_journal_output, context, df = self._data_holder['journal_table'], cache = True),
      }
      
    ## COHORT TABLE - DEFINED
    elif table_bool == True:
      print(f'> LOADING PRE-EXISTING COHORT TABLE ({self._cohort_opt})')
      self._data_holder["cohort_table"] = spark.table(f'{params.DATABASE_NAME}.{self._cohort_opt}')
      # Find journal table with same hash
      journal_table_name = match_latest_journal_table(self._cohort_opt)
      ## LOAD PRE-EXISTING JOURNAL TABLE
      if table_exists(params.DATABASE_NAME, journal_table_name):
        print(f'> LOADING PRE-EXISTING JOURNAL TABLE ({self._cohort_opt})')
        self._data_holder["journal_table"] = spark.table(f'{params.DATABASE_NAME}.{journal_table_name}')
      ## UNABLE TO FIND JOURNAL TABLE  
      else:
        raise ValueError(f'[ERROR] TABLE {journal_table_name} DOES NOT EXIST IN {params.DATABASE_NAME}...\n PLEASE RERUN THE PIPELINE')
      ## CLASS DECLARATION
      return {
        self.patient_cohort_table_output:   PipelineAsset(self.patient_cohort_table_output, context, df = self._data_holder["cohort_table"], cache = True),
        self.patient_cohort_journal_output: PipelineAsset(self.patient_cohort_journal_output, context, df = self._data_holder["journal_table"], cache = True),
      }

    ## UNABLE TO FIND TABLE
    elif table_bool == False:
      raise ValueError(f'[ERROR] TABLE {self._cohort_opt} DOES NOT EXIST IN {params.DATABASE_NAME}...')

        
  ### METHODS
  
  ## CVDP STORE DATA - LOAD DATA
  def _load_cvdp_store_data(self):
    '''_load_cvdp_store_data
    Loads the CVDP Store assets (annual and quarterly) and assign them into the data holder dictionary.
    '''
    self._data_holder['cvdp_annual']     = spark.table(f'{params.CVDP_STORE_DATABASE}.{params.CVDP_STORE_ANNUAL_TABLE}')
    self._data_holder['cvdp_quarterly']  = spark.table(f'{params.CVDP_STORE_DATABASE}.{params.CVDP_STORE_QUARTERLY_TABLE}')
  
  ## CVDP STORE DATA - PREPROCESS COHORT TABLE
  def _preprocess_cvdp_store_data_cohort(self):
    '''
    _preprocess_cvdp_store_data_cohort
    Applies the preprocessing function (see src.cvdp::preprocess_cvdp) to the annual and quarterly data.
    Returns a single CVDP Store dataframe, assigned to the data holder dictionary [cvdp_combined].
    '''
    self._data_holder['cvdp_combined'] = preprocess_cvdp_cohort(
      cvdp_annual       = self._data_holder['cvdp_annual'],
      cvdp_quarterly    = self._data_holder['cvdp_quarterly']
    )
  
  ## CVDP STORE DATA - PREPROCESS JOURNAL TABLE
  def _preprocess_cvdp_store_data_journal(self):
    '''_preprocess_cvdp_store_data_journal
    Applies the preprocessing function (see src.cvdp::preprocess_cvdp_journal) to the combined data [cvdp_combined].
    Returns a journal table, assigned to the data holder dictionary [cvdp_journal].
    '''
    self._data_holder['cvdp_journal'] = preprocess_cvdp_journal(
      df              = self._data_holder['cvdp_combined'],
      add_ref_data    = params.SWITCH_CVDP_JOURNAL_ADD_REF_DATA,
    )
    
  ## COHORT TABLE - SCHEMA TRANSFORMATION
  def _transform_cohort_schema(self):
    '''_transform_cohort_schema
    Transforms the columns of the preprocessed CVDP Store [combined] data into the standard
    columns as defined in the params file (see params/params_util)
    '''
    # Data Loading
    df = self._data_holder['cvdp_combined']
    # Column Mapping
    col_map = params.COHORT_TRANSFORM_MAPPING
    for original_column, mapped_column in col_map.items():
      df = df.withColumnRenamed(original_column, mapped_column)
    # Select Columns for Final Table
    df = df.select(params.COHORT_STAGE_COHORT_COLUMNS)
    # Return
    self._data_holder['cohort_table'] = df

  ## JOURNAL TABLE - SCHEMA TRANSFORMATION
  def _transform_journal_schema(self):
    '''_transform_journal_schema
    Transforms the columns of the preprocessed CVDP Store [journal] data into the standard
    columns as defined in the params file (see params/params_util)
    '''
    # Data Loading
    df = self._data_holder['cvdp_journal']
    # Column Mapping
    col_map = params.JOURNAL_TRANSFORM_MAPPING
    for original_column, mapped_column in col_map.items():
      df = df.withColumnRenamed(original_column, mapped_column)
    # Select Columns for Final Table
    df = df.select(params.COHORT_STAGE_JOURNAL_COLUMNS)
    # Return
    self._data_holder['journal_table'] = df
    
  ## TABLE LIMITER
  def _limit_cohort(self,
    columns: list = [params.CVDP_PID_FIELD,params.CVDP_DOB_FIELD],
    lim_int: int = params.INTEGRATION_TEST_LIMIT):
    """
    Selects a random selection of 'lim_int' rows from the cohort and journal table,
    to be used in integration testing.
    """
    # Select a random selection of lim_int rows
    nhs_dob        = (self._data_holder['cvdp_combined']).select(columns).distinct()
    cohort_count   = nhs_dob.count()
    fraction = lim_int / cohort_count
    #check fraction is less than 1
    if fraction > 1:
      fraction = 1.0
      warnings.warn('Number of rows given to limit the cohort table by is greater than the total number of rows in' \
                    'the cohort table. Running integration test with full cohort table. To change this please change' \
                    f'INTEGRATION_TEST_LIMIT in params_util to less than {cohort_count}')
    nhs_dob_random = nhs_dob.sample(fraction = fraction).limit(lim_int)
    # Return random rows
    self._data_holder['cvdp_combined'] = nhs_dob_random.join(self._data_holder['cvdp_combined'], on = columns, how = "left")

      