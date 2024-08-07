# Databricks notebook source
# preprocess_data

## Overview
# Stage for preprocessing the raw data - eventually used to form the 
# patient and event tables

# COMMAND ----------

from typing import List, Dict, Optional

# COMMAND ----------

# MAGIC %run ../params/params

# COMMAND ----------

# MAGIC %run ./pipeline_util

# COMMAND ----------

# MAGIC %run ./preprocess_raw_data_lib

# COMMAND ----------

# MAGIC %run ../src/clean_dataset

# COMMAND ----------

# MAGIC %run ../src/dars/preprocess_dars

# COMMAND ----------

# MAGIC %run ../src/hes/preprocess_hes

# COMMAND ----------

# MAGIC %run ../src/cvdp/diagnostic_flags_lib

# COMMAND ----------

# MAGIC %run ../src/cvdp/preprocess_cvdp_htn

# COMMAND ----------

class PreprocessRawDataStage(PipelineStage):
  '''
  A pipeline stage that cleans and processes the raw data for saving to local area. Datasets are 
  passed through this stage using the dataclass PreprocessStageDataEntry, specifying the dataset 
  name, database, table and preprocessing options (see preprocess_raw_data_lib::PreprocessStageDataEntry).
  
  Each dataset that is processed must be specified before initiating the class.
  '''
  
  ### DATASET DEFINITIONS ###
  _param_entries: List[PreprocessStageDataEntry] = [

    ## EXAMPLE (see /pipeline/preprocess_raw_data_lib::PreprocessStageDataEntry)
    #   PreprocessStageDataEntry(
    #     dataset_name             = # dataset name must be in params.DATASETS
    #     db                       = # params definition for the database 
    #     table                    = # params definition for the table
    #     filter_eligible_patients = # params definitions for the dataset column that represents NHS Number/Person ID and DOB
    #     preprocessing_func       = # preprocessing function for the dataset
    #     validate_nhs_numbers     = # (True|False) use the clean_dataset function to validate NHS numbers
    #     clean_nhs_number_fields  = # params. definition for the column(s) that contain NHS numbers
    #     clean_null_fields        = # params. definition for column(s) that should have NULL values removed,
    #   ), 

    ## DARS
    PreprocessStageDataEntry(
      dataset_name              = 'dars_bird_deaths',
      db                        = params.DARS_DATABASE,
      table                     = params.DARS_DEATHS_TABLE,
      filter_eligible_patients  = filter_fields(params.PID_FIELD, params.DOB_FIELD),
      preprocessing_func        = preprocess_dars,
      validate_nhs_numbers      = False,
      clean_nhs_number_fields   = [],
      clean_null_fields         = [],
      rename_field_map         = {
        params.DARS_PID_FIELD: params.PID_FIELD,
        params.DARS_DOB_FIELD: params.DOB_FIELD,
        params.DARS_SEX_FIELD: params.SEX_FIELD,
        params.DARS_DOD_FIELD: params.DOD_FIELD,
        params.DARS_UNDERLYING_CODE_FIELD: params.CODE_FIELD,
        params.DARS_COMORBS_CODES_FIELD: params.CODE_ARRAY_FIELD,
        params.DARS_RESIDENCE_FIELD: params.DARS_LSOA_RESIDENCE,
        params.DARS_LOCATION_FIELD: params.DARS_LSOA_LOCATION,
      },
    ),
    
    ## HES APC
    PreprocessStageDataEntry(
      dataset_name             = 'hes_apc',
      db                       = '', #left empty as we do not need to load a raw data asset,
      table                    = '', #instead we load a pre-existing pipeline asset.
      filter_eligible_patients = filter_fields(params.PID_FIELD, params.DOB_FIELD),
      preprocessing_func       = hes_preprocess_apc,
      validate_nhs_numbers     = False,
      clean_nhs_number_fields  = [],
      clean_null_fields        = [],
      rename_field_map         = {
        params.HES_APC_ID_FIELD: params.RECORD_ID_FIELD,
        params.HES_S_APC_PID_FIELD: params.PID_FIELD,
        params.HES_S_APC_DOB_FIELD: params.DOB_FIELD,
        params.HES_APC_SEX_FIELD: params.SEX_FIELD,
        params.HES_APC_LSOA_FIELD: params.LSOA_FIELD,
        params.HES_APC_ETHNICITY_FIELD: params.ETHNICITY_FIELD,
        params.HES_APC_STARTDATE_FIELD: params.RECORD_STARTDATE_FIELD,
        params.HES_APC_ENDDATE_FIELD: params.RECORD_ENDDATE_FIELD,
        params.HES_APC_CODE_FIELD: params.CODE_FIELD,
        params.HES_APC_CODE_LIST_FIELD: params.CODE_ARRAY_FIELD,
        params.HES_S_APC_HESID_FIELD: params.HES_ID_FIELD,
        params.HES_OTR_SPELL_ID: params.HES_SPELL_ID_FIELD,
        params.HES_APC_SPELL_STARTDATE_FIELD: params.HES_STARTDATE_FIELD,
        params.HES_APC_SPELL_ENDDATE_FIELD: params.HES_ENDDATE_FIELD,
        params.HES_APC_SPELL_BEGIN_FIELD: params.HES_SPELL_BEGIN_FIELD,
        params.HES_APC_SPELL_END_INDICATOR_FIELD: params.HES_SPELL_ENDFLAG_FIELD,
        params.HES_APC_SPELL_DUR_FIELD: params.HES_SPELL_DUR_FIELD,
        params.HES_APC_ADMIMETH_FIELD: params.HES_APC_ADMIMETH_FIELD,
      },
    ),

    ## HES OP
    PreprocessStageDataEntry(
      dataset_name             = 'hes_op',
      db                       = '', #left empty as we do not need to load a raw data asset,
      table                    = '', #instead we load a pre-existing pipeline asset.
      filter_eligible_patients = filter_fields(params.PID_FIELD, params.DOB_FIELD),
      preprocessing_func       = hes_preprocess_op,
      validate_nhs_numbers     = False,
      clean_nhs_number_fields  = [],
      clean_null_fields        = [],
      rename_field_map         = {
        params.HES_OP_ID_FIELD: params.RECORD_ID_FIELD,
        params.HES_S_OP_PID_FIELD: params.PID_FIELD,
        params.HES_S_OP_DOB_FIELD: params.DOB_FIELD,
        params.HES_OP_ETHNICITY_FIELD: params.ETHNICITY_FIELD,
        params.HES_OP_STARTDATE_FIELD: params.RECORD_STARTDATE_FIELD,
        params.HES_OP_TRETSPEF_FIELD: params.CODE_FIELD,
        params.HES_OP_CODE_LIST_FIELD: params.CODE_ARRAY_FIELD,
        params.HES_S_OP_HESID_FIELD: params.HES_ID_FIELD,
        params.HES_OP_LSOA_FIELD: params.LSOA_FIELD,
        params.HES_S_OP_SEX_FIELD: params.SEX_FIELD
      },
    ),
    
    ## HES AE
    PreprocessStageDataEntry(
      dataset_name             = 'hes_ae',
      db                       = '', #left empty as we do not need to load a raw data asset,
      table                    = '', #instead we load a pre-existing pipeline asset.
      filter_eligible_patients = filter_fields(params.PID_FIELD, params.DOB_FIELD),
      preprocessing_func       = hes_preprocess_ae,
      validate_nhs_numbers     = False,
      clean_nhs_number_fields  = [],
      clean_null_fields        = [],
      rename_field_map         = {
        params.HES_AE_ID_FIELD: params.RECORD_ID_FIELD,
        params.HES_S_AE_PID_FIELD: params.PID_FIELD,
        params.HES_S_AE_DOB_FIELD: params.DOB_FIELD,
        params.HES_AE_SEX_FIELD: params.SEX_FIELD,
        params.HES_AE_LSOA_FIELD: params.LSOA_FIELD,
        params.HES_AE_ETHNICITY_FIELD: params.ETHNICITY_FIELD,
        params.HES_AE_STARTDATE_FIELD: params.RECORD_STARTDATE_FIELD,
        params.HES_AE_CODE_FIELD: params.CODE_FIELD,
        params.HES_AE_CODE_LIST_FIELD: params.CODE_ARRAY_FIELD,
        params.HES_S_AE_HESID_FIELD: params.HES_ID_FIELD
      },
    ),
    
    ## DIAGNOSTIC FLAGS
    PreprocessStageDataEntry(
      dataset_name             = 'cvdp_diag_flags',
      db                       = '', #left empty as we do not need to load a raw data asset,
      table                    = '', #instead we load a pre-existing pipeline asset.
      filter_eligible_patients = None,
      preprocessing_func       = preprocess_diagnostic,
      validate_nhs_numbers     = False,
      clean_nhs_number_fields  = [],
      clean_null_fields        = [],
      rename_field_map         = None
    ),

    ## HYPERTENSION RISK GROUPS
    PreprocessStageDataEntry(
      dataset_name             = 'cvdp_htn',
      db                       = '', #left empty as we do not need to load a raw data asset,
      table                    = '', #instead we load a pre-existing pipeline asset.
      filter_eligible_patients = None,
      preprocessing_func       = preprocess_cvdp_htn,
      validate_nhs_numbers     = False,
      clean_nhs_number_fields  = [],
      clean_null_fields        = [],
      rename_field_map         = None
    ),
  ]
    
  ### PROPERTIES ###
  @property
  def param_entries(self):
    '''param_entries
    Defines the datasets to process in pipeline stage.

    Info
      Error if dataset (PreprocessStageDataEntry.dataset_name) is not in params.DATASETS

    Notebooks
      params::params

    '''
    entries = []
    for entry in self._param_entries:
      if entry.dataset_name in params.DATASETS:
        entries.append(entry)
      else:
        print(f'[WARNING] {entry.dataset_name} NOT DEFINED IN PARAMS.DATASETS...')
    return entries
    
  ### MAIN STAGE-CLASS DEFINITIONS ###
  def __init__(
    self,
    patient_cohort_input: str,
    patient_cohort_journal_input: str,
    hes_apc_input: str,
    hes_ae_input: str,
    hes_op_input: str,
    dars_output: str,
    hes_apc_output: str,
    hes_op_output: str,
    hes_ae_output: str, 
    cvdp_diag_flags_output: str,
    cvdp_htn_output: str,
    ):
    '''__init__
    Each dataset defined must have a reciprocal output key. This key is passed as an argument
    when calling the pipeline stage in default_pipeline.

    Args:
      patient_cohort_input (str): Pipeline key for the eligible patient cohort table (see pipeline::create_cohort_table)
      patient_cohort_journal_input (str): Pipeline key for the eligible patient cohort journal table (see pipeline::create_cohort_table)
    
    Returns:
      dars_output (str): Pipeline key for the processed DARS data asset.
      hes_apc_output (str): Pipeline key for the processed HES APC data asset.
      cvdp_diag_flags_output (str): Pipeline key for the processed CVDP-derived diagnostic flags data set.
      cvdp_htn_output (str): Pipeline key for the the processed CVDP-derived hypertension data set.

    Notebooks:
      params::params
      pipeline::pipeline_util
      pipeline::preprocess_raw_data_lib
      src::clean_dataset
      src/dars::preprocess_dars
      src/hes::preprocess_hes
      src/cvdp::diagnostic_flags_lib
      src/cvdp::preprocess_cvdp_htn
    
    Info
      The output key (argument in __init__) must be assigned below as self._output_key = output_key
      This is then used to write the finalised dataframe (df) to the output key (examples in comments)
    
    '''
    self._patient_cohort_input = patient_cohort_input
    self._patient_cohort_journal_input = patient_cohort_journal_input
    self._hes_apc_input = hes_apc_input
    self._hes_op_input = hes_op_input
    self._hes_ae_input = hes_ae_input
    self._hes_apc_output = hes_apc_output
    self._hes_op_output = hes_op_output
    self._hes_ae_output = hes_ae_output
    self._dars_output = dars_output
    self._cvdp_diag_flags_output = cvdp_diag_flags_output
    self._cvdp_htn_output = cvdp_htn_output
    self._source_data_holder: Dict[str, DataFrame] = {}
    super(PreprocessRawDataStage, self).__init__(
      {self._patient_cohort_input, self._patient_cohort_journal_input,
       self._hes_apc_input, self._hes_ae_input, self._hes_op_input},
      {
        self._dars_output, self._hes_op_output, self._hes_ae_output, self._hes_apc_output,
        self._cvdp_diag_flags_output, self._cvdp_htn_output,
        }
      )
    
  ### RUN STAGE ###
  def _run(self, context, log):
    '''_run
    Main pipeline stage definition. Cleans and preprocesses the datasets defined in _param_entries.
    Filters the processed datasets on the eligible patient cohort table using NHS Number/Person ID (OPTIONAL)

    Info
      Each processed dataset must be assigned to an output key through the PipelineAsset class in the return dict.
      Example: return {self._output_key: PipelineAsset(self._output_key, context, df = dataframe, cache = True)}

    '''
    log._add_stage(self.name)
    
    log._timer(self.name)
    ## LOAD JOURNAL TABLE
    self._load_journal_table(context)
    ## Load HES Tables
    self._load_hes_tables(context)
    ## CLEAN AND PREPROCESS EACH DATASET
    self._clean_and_preprocess_raw_data()
    ## FILTER DATASETS
    df_eligible_cohort = context[self._patient_cohort_input].df.select(params.PID_FIELD, params.DOB_FIELD).distinct()
    self._filter_data_on_eligible_patients(df_eligible_cohort)
    
    # CHECK DATA OUTPUT
    self._check_output_tables()
    
    log._timer(self.name, end=True)
    
    ## Return pipeline outputs as delta tables
    return {
      ### HES APC
      self._hes_apc_output: PipelineAsset(
        key = self._hes_apc_output,
        context = context,
        db = params.DATABASE_NAME,
        df = self._source_data_holder['hes_apc'],
        cache = False,
        delta_table = True,
        delta_columns = [params.RECORD_ID_FIELD]
        ),
      ### HES OP
      self._hes_op_output: PipelineAsset(
        key = self._hes_op_output,
        context = context,
        db = params.DATABASE_NAME,
        df = self._source_data_holder['hes_op'],
        cache = False,
        delta_table = True,
        delta_columns = [params.RECORD_ID_FIELD]
        ),
      ### HES AE
      self._hes_ae_output: PipelineAsset(
        key = self._hes_ae_output,
        context = context,
        db = params.DATABASE_NAME,
        df = self._source_data_holder['hes_ae'],
        cache = False,
        delta_table = True,
        delta_columns = [params.RECORD_ID_FIELD]
        ),
      ### DARS Deaths
      self._dars_output: PipelineAsset(
        key = self._dars_output,
        context = context,
        db = params.DATABASE_NAME,
        df = self._source_data_holder['dars_bird_deaths'],
        cache = False,
        delta_table = True,
        delta_columns = [params.RECORD_ID_FIELD]
        ),
      ### CVDP Diagnostic Flags
      self._cvdp_diag_flags_output: PipelineAsset(
        key = self._cvdp_diag_flags_output,
        context = context,
        db = params.DATABASE_NAME,
        df = self._source_data_holder['cvdp_diag_flags'],
        cache = False,
        delta_table = True,
        delta_columns = [params.PID_FIELD,params.DOB_FIELD]
        ),
      ### CVDP Blood Pressure and Hypertension
      self._cvdp_htn_output: PipelineAsset(
        key = self._cvdp_htn_output,
        context = context,
        db = params.DATABASE_NAME,
        df = self._source_data_holder['cvdp_htn'],
        cache = False,
        delta_table = True,
        delta_columns = [params.RECORD_ID_FIELD]
        ),
      }
    
    
  ### SUB-METHODS ###
  
  ## LOAD JOURNAL TABLE
  def _load_journal_table(self,context):
    '''_load_journal_table
    Class method: loads the journal table and assigns to the journal_table slot in _source_data_holder
    
    Input:
      context (PipelineContext): Pipeline context object
      
    Output:
      df (DataFrame): Saves the journal table to the self._source_data_holder (key = 'journal_table')
    '''
    df = context[self._patient_cohort_journal_input].df
    if 'META' in df.columns:
        df = df.drop('META')
    self._source_data_holder['journal_table'] = df
    
    ## Load HES Tables
  def _load_hes_tables(self,context):
    """_load_hes_tables
    
    Class method: loads the HES tables and assigns these to the specific HES dataset slot in 
    _source_data_holder

    Args:
        context (PipelineContext): Pipeline context object
    """    
    self._source_data_holder['hes_apc'] = context[self._hes_apc_input].df
    self._source_data_holder['hes_ae']  = context[self._hes_ae_input].df
    self._source_data_holder['hes_op']  = context[self._hes_op_input].df
  
  ## CLEAN AND PREPROCESS DATASETS


  def _clean_and_preprocess_raw_data(self):
    '''_clean_and_preprocess_raw_data
    Class method: applies the cleaning and preprocessing functions to the dataset

    Outputs
      df_dataset_cleaned (DataFrame): Cleaned and preprocessed dataset (saved to _source_data_holder)

    Info
      Method is conditional on dataset (HES-specific pathway vs all other datasets)

    Notebooks
      src::clean_dataset

    '''
    for data_entry in self.param_entries:
      print(f'INFO: Cleaning {data_entry.dataset_name}')
      ## Conditional: HES - Load associated HES dataset
      if data_entry.dataset_name.startswith('hes'):
        if data_entry.dataset_name.startswith('hes_apc'):
          df_dataset = self._source_data_holder['hes_apc']
        elif data_entry.dataset_name.startswith('hes_ae'):
          df_dataset = self._source_data_holder['hes_ae']
        elif data_entry.dataset_name.startswith('hes_op'):
          df_dataset = self._source_data_holder['hes_op']
      ## DIAGNOSTIC FLAGS SPECIFIC CONDITION
      elif data_entry.dataset_name.startswith('cvdp_diag'):
        df_dataset = self._source_data_holder['journal_table']
      ## HYPERTENSION SPECIFIC CONDITION
      elif data_entry.dataset_name.startswith('cvdp_htn'):
        df_dataset = self._source_data_holder['journal_table']
      ## OTHER DATASETS
      else:
        df_dataset = spark.table(f'{data_entry.db}.{data_entry.table}')
      ## CLEANING FUNCTIONS
      df_dataset_cleaned = clean_and_preprocess_dataset(
        df = df_dataset,
        nhs_number_fields = data_entry.clean_nhs_number_fields,
        clean_null_fields = data_entry.clean_null_fields,
        replace_empty_str_fields = data_entry.replace_empty_str_fields,
        preprocessing_func = data_entry.preprocessing_func,
        validate_nhs_numbers = data_entry.validate_nhs_numbers
        )
      
      ## ADD DATASET FIELD 
      df_dataset_cleaned = add_dataset_field(
        df = df_dataset_cleaned,
        dataset_name = data_entry.dataset_name
        )

      ## TRANSFORM
      df_dataset_transformed = transform_to_asset_format(
        df = df_dataset_cleaned,
        rename_field_map = data_entry.rename_field_map
        )                                       
      
      ## STORE DATASETS
      if data_entry.dataset_name in params.DATASETS:
        self._source_data_holder[data_entry.dataset_name] = df_dataset_transformed
        
  def _filter_data_on_eligible_patients(self, patient_filtering_list):
    '''_filter_data_on_eligible_patients
    Filters all datasets (if dataclass.filter_eligible_patients is provided) to only include patients from the eligible
    cohort patient table

    Inputs
      patient_filtering_list: Dataframe from the eligible cohort patient, used to filter data_entry

    Outputs
      df (DataFrame): Filtered dataset (saved to _source_data_holder)

    Notebooks
      pipeline::preprocess_raw_data_lib

    '''
    ## DATAFRAME FILTER
    for data_entry in self.param_entries:
      if data_entry.filter_eligible_patients != None:
        print(f'INFO: Filtering {data_entry.dataset_name}')
        df = filter_eligible_patients(
          patient_list  = patient_filtering_list,
          df_filter     = self._source_data_holder[data_entry.dataset_name],
          filter_fields = data_entry.filter_eligible_patients
          )
        ## STORE DATASETS
        self._source_data_holder[data_entry.dataset_name] = df

        
  def _check_output_tables(self):
    ''' _check_output_tables
    Checks output tables created throughout this stage, currently only checks HES and prints the number of Null Spell IDs
    '''
    hes = self._source_data_holder['hes_apc']
    
    null_spell_id_count = hes.select("*").where(F.col('hes_spell_id').isNull()).count()
    
    if (null_spell_id_count > 0):
      print(f'[INFO] {null_spell_id_count} RECORDS IDENTIFIED WITH NULL SPELL IDs')
      
      