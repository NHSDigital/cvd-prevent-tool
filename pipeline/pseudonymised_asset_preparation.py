# Databricks notebook source
# pseudonymised_asset_preparation

## Overview
# Stage for curating (desensitising) the pipeline assets (events and patient table) and saving curated assets

# COMMAND ----------

# MAGIC %run ../params/params

# COMMAND ----------

# MAGIC %run ./pipeline_util

# COMMAND ----------

## Environment Setup
import pyspark.sql.functions as F
import pyspark.sql.types as T

# COMMAND ----------

class PseudoPreparationStage(PipelineStage):
    '''PseudoPreparationStage
    
    Pipeline stage for creating curated events and patient tables (removal of sensitive information) and saving to a databricks table,
    prior to pseudonymisation. 
    '''
    
    ### MAIN STAGE-CLASS DEFINITIONS ###
    def __init__(
        self,
        events_table_input: str,
        patient_table_input: str,
        curated_events_table_output: str,
        curated_patient_table_output: str,
        is_integration: bool,
    ):
        self._events_table_input = events_table_input
        self._patient_table_input = patient_table_input
        self._curated_events_table_output = curated_events_table_output
        self._curated_patient_table_output = curated_patient_table_output
        self._is_integration = is_integration
        self._overwrite = params.PSEUDO_SAVE_OVERWRITE
        self._database_name = params.PSEUDO_DB_PATH
        self._data_holder: Dict[str, DataFrame] = {}
        super(PseudoPreparationStage, self).__init__(
            {
                self._events_table_input,
                self._patient_table_input
            },
            {
                self._curated_events_table_output,
                self._curated_patient_table_output
            }
        )
        
    # Protected Run Method
    def _run(self, context, log):
        
        # Pipeline Log - Initialisation
        log._add_stage(self.name)
        log._timer(self.name)
        
        # Load Required Assets
        self._load_uncurated_data_assets(context)
        # Events Table Curation
        self._curate_events_table()
        # Patient Table Curation
        self._curate_patient_table()
        # Convert curated assets to csv compatible columns
        self._convert_schema_csv_compatible()
        # Check curated assets
        self._check_curated_assets()
        
        # Pipeline Log - Termination
        log._timer(self.name, end=True)
        
        # Stage Return - Conditional (Overwrite & is_integration)
        ## Overwrite existing table (no version or full pipeline attributes) only when set to overwrite and not during integration tests
        if self._overwrite == True and self._is_integration == False:
            print('[INFO] Asset save mode set to overwrite cvdp_linkage tables...')
            return {
                self._curated_events_table_output: PipelineAsset(
                    key = self._curated_events_table_output, 
                    context = context, 
                    df = self._data_holder['events'],
                    db = self._database_name, 
                    table = self._curated_events_table_output
                    ),
                self._curated_patient_table_output: PipelineAsset(
                    key = self._curated_patient_table_output,
                    context = context,
                    df = self._data_holder['patient'],
                    db = self._database_name,
                    table = self._curated_patient_table_output
                    ),
                }
        ## Create new table (with version and full pipeline path attributes)
        else:
            return {
                self._curated_events_table_output: PipelineAsset(
                    key = self._curated_events_table_output, 
                    context = context, 
                    df = self._data_holder['events'],
                    cache = True,
                    ),
                self._curated_patient_table_output: PipelineAsset(
                    key = self._curated_patient_table_output,
                    context = context,
                    df = self._data_holder['patient'],
                    cache = True,
                    ),
                }
        
    ## Sub-method: Load Data Assets
    def _load_uncurated_data_assets(self, context):
        '''_load_uncurated_data_assets
        Loads the events and patient table into the stage
        '''
        self._data_holder['events'] = context[self._events_table_input].df.drop('META')
        self._data_holder['patient'] = context[self._patient_table_input].df.drop('META')
        
        
    ## Sub-method: Events Table Curation
    def _curate_events_table(self):
        '''_curate_events_table
        Processes the events table into a shareable state. The following curation steps are made:
        * Remove the birth_date column
        * Remove any non-CVD related events
        '''
        # Data Load
        df_events = self._data_holder['events']
        ## Column dropping
        ## > Conditional: If columns to drop are specified (length > 0)
        if len(params.PSEUDO_EVENTS_COLUMNS_DROPPED) > 0:
          df_events = (
              df_events
              .drop(params.PSEUDO_EVENTS_COLUMNS_DROPPED)
          )
        # Events Processing - Removal of non-specified records
        df_events = (
          df_events
          .filter(
            # Inclusion filter: Keep only specified datasets and categories
            (F.col(params.DATASET_FIELD).isin(params.PSEUDO_EVENTS_INCLUSION_DATASETS)) &
            (F.col(params.CATEGORY_FIELD).isin(params.PSEUDO_EVENTS_INCLUSION_CATEGORIES))
          )
        )
        # HES Specific Processing
        df_events = (
          df_events
          .filter(
            # Keep: Any non-HES events
              (
                  F.col(params.DATASET_FIELD) != params.HES_APC_TABLE
              ) |
              (
                  # Keep: HES Spells where flag is CVD related only
                  (
                      (F.col(params.DATASET_FIELD) == params.HES_APC_TABLE) &
                      (F.col(params.CATEGORY_FIELD) == params.EVENTS_HES_SPELL_CATEGORY) &
                      (F.col(params.FLAG_FIELD) != params.PSEUDO_HES_FLAG_REMOVAL_VALUE)
                  ) |
                  # Keep: HES Episodes where flag is CVD related only
                  (
                      (F.col(params.DATASET_FIELD) == params.HES_APC_TABLE) &
                      (F.col(params.CATEGORY_FIELD) == params.EVENTS_HES_EPISODE_CATEGORY) &
                      (F.col(params.FLAG_FIELD) != params.PSEUDO_HES_FLAG_REMOVAL_VALUE)
                  )
              )
          )
        )
        # Return to data holder
        self._data_holder['events'] = df_events


    ## Sub-method: Patient Table Curation
    def _curate_patient_table(self):
        '''_curate_patient_table
        Processes the patient table into a shareable stage. The following curation steps are made:
        * Replace birth_date values YYYY-MM-DD with YYYY
        * Change column name birth_date to birth_year
        * Drop columns defined in params.PSEUDO_PATIENT_COLUMNS_DROPPED
        '''
        # Data Load
        df_patient = self._data_holder['patient']
        # Date of Birth - Value Formatting
        df_patient = (
            df_patient
            .withColumn(
                params.DOB_FIELD,
                F.date_format(params.DOB_FIELD, params.PSEUDO_DOB_FORMAT)
                )
            )
        # Specific column processing
        ## Birth date
        df_patient = (
            df_patient
            .withColumnRenamed(params.DOB_FIELD,params.PSEUDO_DOB_FIELD)
        )
        ## Column dropping
        ## > Conditional: If columns to drop are specified (length > 0)
        if len(params.PSEUDO_PATIENT_COLUMNS_DROPPED) > 0:
          df_patient = (
              df_patient
              .drop(params.PSEUDO_PATIENT_COLUMNS_DROPPED)
          )
        # Return to data holder
        self._data_holder['patient'] = df_patient

    ## Sub-method: Convert to CSV Compatible columns
    def _convert_schema_csv_compatible(self):
        '''_convert_schema_csv_compatible
        Convert the current schema of the events and patient tables to CSV compatible ones.
        '''
        # Data Load
        df_events = self._data_holder['events']
        df_patient = self._data_holder['patient']
        
        # Events table conversion
        df_events = (
            df_events
            .withColumn(params.CODE_ARRAY_FIELD, F.concat_ws(',', F.col(params.CODE_ARRAY_FIELD)))
            .withColumn(params.ASSOC_FLAG_FIELD, F.concat_ws(',', F.col(params.ASSOC_FLAG_FIELD)))
            .withColumn(params.ASSOC_REC_ID_FIELD, F.concat_ws(',', F.col(params.ASSOC_REC_ID_FIELD)))
        )
        
        # Patient table conversion
        df_patient = (
            df_patient
            .withColumn(params.DEATH_30_HOSPITALISATION, F.concat_ws(',', F.col(params.DEATH_30_HOSPITALISATION)))
        )
        
        # Return to class data holder
        self._data_holder['events'] = df_events
        self._data_holder['patient'] = df_patient
        
    ## Sub-method: Check Curated Assets
    def _check_curated_assets(self):
        '''_check_curated_assets
        Validate the curation steps for the events and patient table (assertion based). Stage will fail (and pipeline)
        if assertion criteria are not met.
        '''
        # Data Load
        df_events = self._data_holder['events']
        df_patient = self._data_holder['patient']
        
        # Events Table
        ## Checks: Column Dropping
        events_col_is_present = any(col_name in df_events.columns for col_name in params.PSEUDO_EVENTS_COLUMNS_DROPPED)
        assert events_col_is_present == False
        ## Checks: Inclusion Events
        assert df_events.filter(~F.col(params.DATASET_FIELD).isin(params.PSEUDO_EVENTS_INCLUSION_DATASETS)).count() == 0
        assert df_events.filter(~F.col(params.CATEGORY_FIELD).isin(params.PSEUDO_EVENTS_INCLUSION_CATEGORIES)).count() == 0
        ## Checks: HES Events - NO_CVD main flagged records should be removed
        assert df_events.filter(
            (F.col(params.DATASET_FIELD) == params.HES_APC_TABLE) &
            (F.col(params.CATEGORY_FIELD) == params.EVENTS_HES_EPISODE_CATEGORY) &
            (F.col(params.FLAG_FIELD) == params.PSEUDO_HES_FLAG_REMOVAL_VALUE)
            ).count() == 0
        assert df_events.filter(
            (F.col(params.DATASET_FIELD) == params.HES_APC_TABLE) &
            (F.col(params.CATEGORY_FIELD) == params.EVENTS_HES_SPELL_CATEGORY) &
            (F.col(params.FLAG_FIELD) == params.PSEUDO_HES_FLAG_REMOVAL_VALUE)
            ).count() == 0
        
        # Patient Table
        ## Checks: Column Dropping
        patient_col_is_present = any(col_name in df_patient.columns for col_name in params.PSEUDO_PATIENT_COLUMNS_DROPPED)
        assert patient_col_is_present == False
        ## Checks: Column Renaming
        assert (params.DOB_FIELD in df_patient.columns) == False 
        assert (params.PSEUDO_DOB_FIELD in df_patient.columns) == True
        ## Checks: Date format 
        date_regex = "([0-9]{4})"
        assert df_patient.filter(~F.col(params.PSEUDO_DOB_FIELD).rlike(date_regex)).count() == 0
        ## Checks: output key table names
        assert (params.PSEUDO_TABLE_PREFIX in self._curated_events_table_output) == True 
        assert (params.PSEUDO_TABLE_PREFIX in self._curated_patient_table_output) == True
        
        # Data Type Checks
        ## Check events and patient table for non-permitted data types (aren't compatible with CSV)
        csv_incompatible = [
            'ArrayType',
            'MapType',
            'StructType',
            'BinaryType'
        ]
        ### Events table
        for column in df_events.schema:
            if any(data_type in str(column.dataType) for data_type in csv_incompatible):
                raise ValueError(f'ERROR: Events table - column {column.name} has a CSV incompatible data type')
        ### Patient table
        for column in df_patient.schema:
            if any(data_type in str(column.dataType) for data_type in csv_incompatible):
                raise ValueError(f'ERROR: Patient table - column {column.name} has a CSV incompatible data type')
            
