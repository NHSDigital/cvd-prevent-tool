# Databricks notebook source
# create_cohort_table

# COMMAND ----------

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
  A pipeline stage to create the eligble patient cohort table and associated journal table.
  Utilises the extracted CVDP table from the ExtractCVDPDataStage.

  Associated notebooks:
    * pipeline/pipeline_util: Provides abstract classes for pipeline stages
    * src/cvdp/preprocess_cvdp_cohort: Preprocessing function for CVDP cohort processing
    * src/cvdp_preprocess_cvdp_journal: Preprocessing function for CVDP journal processing
  '''

  def __init__(
    self,
    extracted_cvdp_table_input: str,
    patient_cohort_table_output: str,
    patient_cohort_journal_output: str
    ):
    '''__init__
    Contains the main run() stage definition and the associated class methods.

    Args:
      extracted_cvdp_table_input (str)      Context key for the extracted CVDP store table
                                            pipeline asset [input]
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
    self.extracted_cvdp_table_input: str = extracted_cvdp_table_input
    self.patient_cohort_table_output: str = patient_cohort_table_output
    self.patient_cohort_journal_output: str = patient_cohort_journal_output
    self._data_holder: Dict[str, DataFrame] = {}
    super(CreatePatientCohortTableStage, self).__init__(
      {self.extracted_cvdp_table_input},
      {self.patient_cohort_table_output, self.patient_cohort_journal_output})

  # Main Run Statement
  def _run(self, context, log):
    # Initiate Logger
    log._add_stage(self.name)
    log._timer(self.name)
    
    self._load_data_assets(context)
    # Cohort Table Processing
    self._preprocess_cvdp_store_data_cohort(context)
    # Journal Table Processing
    self._preprocess_cvdp_store_data_journal()
    # Data Schema Processing
    self._transform_cohort_schema()
    self._transform_journal_schema()
    # Hash collision checks
    self._check_unique_identifiers_cohort()
    self._check_unique_identifiers_journal()
    # Stop Logger
    log._timer(self.name, end=True)
    # Return Keys
    return {
        self.patient_cohort_table_output: PipelineAsset(
            key=self.patient_cohort_table_output,
            context=context,
            db=params.DATABASE_NAME,
            df=self._data_holder['cohort_table'],
            cache=False,
            delta_table = True,
            delta_columns = [params.CVDP_COHORT_PRIMARY_KEY]
            ),
        self.patient_cohort_journal_output: PipelineAsset(
            key=self.patient_cohort_journal_output,
            context=context,
            db=params.DATABASE_NAME,
            df=self._data_holder['journal_table'],
            cache=False,
            delta_table = True,
            delta_columns = [params.CVDP_JOURNAL_PRIMARY_KEY]
            ),
    }
  
  
  # Load data assets into stage
  def _load_data_assets(self, context):
      """_load_data_assets
      Loads the events table and any additional enrichment data. Stores into _data_holder.
      """
      self._data_holder["extracted_cvdp_store"] = context[self.extracted_cvdp_table_input].df
  
  # Supporting Methods
  ## CVDP STORE DATA - PREPROCESS COHORT TABLE
  def _preprocess_cvdp_store_data_cohort(self, context):
    '''_preprocess_cvdp_store_data_cohort
    Applies the preprocessing function (see src.cvdp::preprocess_cvdp) to the extracted CVDP data.
    Returns a single CVDP Store dataframe, assigned to the data holder dictionary [cvdp_combined].
    '''
    self._data_holder['cvdp_combined'] = preprocess_cvdp_cohort(
      cvdp_extract = self._data_holder["extracted_cvdp_store"]
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

  ## Hash Collision Check
  ### Cohort Table Check
  def _check_unique_identifiers_cohort(self):
      hash_check = check_unique_values(
          df = self._data_holder['cohort_table'],
          field_name = params.CVDP_COHORT_PRIMARY_KEY)
      if hash_check == True:
          pass
      else:
          raise Exception(f'ERROR: Non-unique hash values in Eligible Cohort Table. Pipeline stopped, check hashable fields.')
  ### Journal Table Check
  def _check_unique_identifiers_journal(self):
      hash_check = check_unique_values(
          df = self._data_holder['journal_table'],
          field_name = params.CVDP_JOURNAL_PRIMARY_KEY)
      if hash_check == True:
          pass
      else:
          raise Exception(f'ERROR: Non-unique hash values in Eligible Cohort Journal Table. Pipeline stopped, check hashable fields.')