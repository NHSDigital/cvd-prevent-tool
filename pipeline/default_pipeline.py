# Databricks notebook source
# MAGIC %run ./write_asset_stage

# COMMAND ----------

# MAGIC %run ./add_auditing_field_stage

# COMMAND ----------

# MAGIC %run ./create_cohort_table

# COMMAND ----------

# MAGIC %run ./preprocess_raw_data

# COMMAND ----------

# MAGIC %run ./create_events_table

# COMMAND ----------

# MAGIC %run ./create_demographic_table

# COMMAND ----------

# MAGIC %run ./create_patient_table

# COMMAND ----------

# MAGIC %run ./create_logger_stage

# COMMAND ----------

# MAGIC %run ./pseudonymised_asset_preparation

# COMMAND ----------

# MAGIC %run ./pipeline_results_checker_stage

# COMMAND ----------

def get_default_pipeline_stages(cohort_opt: str = '', prepare_pseudo_assets: bool = False, is_integration: bool = False):
  '''
  Returns a list of pipeline stages that defines the default pipeline. 
  The default pipeline loads all source datasets, preprocesses and cleans them, and forms the 
  patient table. The patient table is used to define all eligible patients. The events table 
  is created by processing the events data, using the patient table to filter out non-elible 
  patients. These datasets are unioned to form the events table. The final stages run a check 
  stage that creates a report table, and a stage that checks for any failures in the report 
  table.
  
  Pay close attention to where assets are cached (within pipeline stages) and where they are written 
  to disk. When we know an asset is finished an won't change anymore, it is written to disk, we 
  then have the option to continue to use the dataframe, or to use the data from disk. It is always 
  better to read the data from disk as this is like a hard cache. If we used the dataframe then spark 
  would recreate the dataframe from scratch each time we needed it, because of it's lazy evaluation.
  
  See pipeline/pipeline_util::PipelineStage and pipeline/pipeline_util::PipelineContext for more 
  info on how the pipeline works. It is a list of PipelineStage instances. The string values passed 
  into the class constructors are keys that identify where within the PipelineContext the data 
  will be stored. The PipelineContext object is passed around between the pipeline stages so the 
  various dataframes can be accessed from it using the key. There are other options where the key 
  can also be a database and table location such that the data from that location is read into a 
  dataframe.

  Inputs
  - - - - - - -
  cohort_opt: str = Previously created cohort table (default = ''; if not supplied then full pipeline will run)
  prepare_pseudo_assets: bool = Switch for running the full pipeline followed by the pseudonymised asset preperation 
                                data stage (True).
                                Defaults to False.
  is_integration: bool = Switch to tell the pipeline run if being ran as an integration test. Must be set to True in
                         integration test to not overwrite tables in pseudo stage.
                         Defaults to False.
  
  '''
  ### ASSET NAMES ###
  PATIENT_COHORT_TABLE_ASSET               = 'eligible_cohort'
  PATIENT_COHORT_TABLE_WROTE_ASSET         = 'eligible_cohort_wrote'
  PATIENT_COHORT_JOURNAL_TABLE_ASSET       = 'eligible_cohort_journal'
  PATIENT_COHORT_JOURNAL_TABLE_WROTE_ASSET = 'eligible_cohort_journal_wrote'
  DARS_ASSET                               = 'dars_bird_deaths'
  DARS_ASSET_WROTE                         = 'dars_bird_deaths_wrote'
  HES_APC_TABLE_ASSET                      = 'hes_apc'
  HES_APC_TABLE_WROTE_ASSET                = 'hes_apc_wrote'
  HES_OP_TABLE_ASSET                       = 'hes_op'
  HES_OP_TABLE_WROTE_ASSET                 = 'hes_op_wrote'
  HES_AE_TABLE_ASSET                       = 'hes_ae'
  HES_AE_TABLE_WROTE_ASSET                 = 'hes_ae_wrote'
  CVDP_DIAG_FLAGS_ASSET                    = 'cvdp_diag_flags'
  CVDP_DIAG_FLAGS_WROTE_ASSET              = 'cvdp_diag_flags_wrote'
  CVDP_HTN_ASSET                           = 'cvdp_htn'
  CVDP_HTN_ASSET_WROTE                     = 'cvdp_htn_wrote'
  CVDP_SMOKING_STATUS_ASSET                = 'cvdp_smoking_status'
  CVDP_SMOKING_STATUS_WROTE_ASSET          = 'cvdp_smoking_status_wrote'
  CVDP_SMOKING_INTERVENTION_ASSET          = 'cvdp_smoking_intervention'
  CVDP_SMOKING_INTERVENTION_WROTE_ASSET    = 'cvdp_smoking_intervention_wrote'
  PCAREMEDS_ASSET                          = 'pcaremeds'
  PCAREMEDS_ASSET_WROTE                    = 'pcaremeds_wrote'

  EVENTS_TABLE_ASSET                = 'events_table'
  EVENTS_TABLE_WROTE_ASSET          = 'events_table_wrote'
  
  PATIENT_TABLE_ASSET               = 'patient_table'
  PATIENT_TABLE_WROTE_ASSET         = 'patient_table_wrote'
   
  DEMOGRAPHIC_TABLE_ASSET           = 'demographic_table'
  DEMOGRAPHIC_TABLE_WROTE_ASSET     = 'demographic_table_wrote'

  RESULTS_CHECKER_ASSET             = 'results_checker_table'
  RESULTS_CHECKER_WROTE_ASSET       = 'results_checker_table_wrote'
  
  LOGGER_OUTPUT_ASSET               = 'logger_table'
  LOGGER_OUTPUT_WROTE_ASSET         = 'logger_table_wrote'
  
  PSEUDO_EVENTS_TABLE_ASSET = 'cvdp_linkage_events_table'
  PSEUDO_PATIENT_TABLE_ASSET = 'cvdp_linkage_patient_table'
  PSEUDO_EVENTS_TABLE_WROTE_ASSET = 'cvdp_linkage_events_table_wrote'
  PSEUDO_PATIENT_TABLE_WROTE_ASSET = 'cvdp_linkage_patient_table_wrote'
  
  ## COHORT CONDITION: USING PRE-EXISTING COHORT TABLE
  pipeline_cohort = [
    CreatePatientCohortTableStage(
      patient_cohort_table_output = PATIENT_COHORT_TABLE_ASSET,
      patient_cohort_journal_output = PATIENT_COHORT_JOURNAL_TABLE_ASSET,
      cohort_opt = cohort_opt)
      ]
  
  if cohort_opt == '':
    pipeline_cohort.extend([
      AddAuditingFieldStage(passthrough_asset_add_meta_column = PATIENT_COHORT_TABLE_ASSET),
      WriteAssetStage(input_asset_to_save = PATIENT_COHORT_TABLE_ASSET, 
                      output = PATIENT_COHORT_TABLE_WROTE_ASSET)
    ])
    pipeline_cohort.extend([
      AddAuditingFieldStage(passthrough_asset_add_meta_column = PATIENT_COHORT_JOURNAL_TABLE_ASSET),
      WriteAssetStage(input_asset_to_save = PATIENT_COHORT_JOURNAL_TABLE_ASSET, 
                      output = PATIENT_COHORT_JOURNAL_TABLE_WROTE_ASSET)
    ])
  else:
    # COHORT TABLE ALREADY EXISTS
    PATIENT_COHORT_TABLE_WROTE_ASSET = PATIENT_COHORT_TABLE_ASSET
    PATIENT_COHORT_JOURNAL_TABLE_WROTE_ASSET = PATIENT_COHORT_JOURNAL_TABLE_ASSET

  
  ## DEFAULT REMAINING PIPELINE
  default_pipeline_stage = [
    ## PREPROCESS RAW DATA STAGE
    PreprocessRawDataStage(
      patient_cohort_input = PATIENT_COHORT_TABLE_WROTE_ASSET,
      patient_cohort_journal_input = PATIENT_COHORT_JOURNAL_TABLE_WROTE_ASSET,
      dars_output = DARS_ASSET,
      hes_apc_output = HES_APC_TABLE_ASSET,
      hes_op_output = HES_OP_TABLE_ASSET,
      hes_ae_output = HES_AE_TABLE_ASSET,
      cvdp_diag_flags_output = CVDP_DIAG_FLAGS_ASSET,
      cvdp_htn_output= CVDP_HTN_ASSET,
      cvdp_smoking_status_output = CVDP_SMOKING_STATUS_ASSET,
      cvdp_smoking_intervention_output = CVDP_SMOKING_INTERVENTION_ASSET,
      pcaremeds_output = PCAREMEDS_ASSET),

    
    ## WRITE STAGE: DARS OUTPUT
    AddAuditingFieldStage(
      passthrough_asset_add_meta_column = DARS_ASSET),
    WriteAssetStage(input_asset_to_save = DARS_ASSET,
                    output = DARS_ASSET_WROTE),
    
    ## WRITE STAGE: HES APC OUTPUT
    AddAuditingFieldStage(
      passthrough_asset_add_meta_column = HES_APC_TABLE_ASSET),
    WriteAssetStage(
      input_asset_to_save = HES_APC_TABLE_ASSET, output = HES_APC_TABLE_WROTE_ASSET),
    
    ## WRITE STAGE: HES OP OUTPUT
    AddAuditingFieldStage(
      passthrough_asset_add_meta_column = HES_OP_TABLE_ASSET),
    WriteAssetStage(
      input_asset_to_save = HES_OP_TABLE_ASSET, output = HES_OP_TABLE_WROTE_ASSET),
    
    ## WRITE STAGE: HES AE OUTPUT
    AddAuditingFieldStage(
      passthrough_asset_add_meta_column = HES_AE_TABLE_ASSET),
    WriteAssetStage(
      input_asset_to_save = HES_AE_TABLE_ASSET, output = HES_AE_TABLE_WROTE_ASSET),
    
    ## WRITE STAGE: PCAREMEDS OUTPUT
    AddAuditingFieldStage(
      passthrough_asset_add_meta_column = PCAREMEDS_ASSET),
    WriteAssetStage(input_asset_to_save = PCAREMEDS_ASSET,
                    output = PCAREMEDS_ASSET_WROTE),
    
    ### DIAGNOSTIC FLAGS
    AddAuditingFieldStage(
      passthrough_asset_add_meta_column = CVDP_DIAG_FLAGS_ASSET),
    WriteAssetStage(
      input_asset_to_save = CVDP_DIAG_FLAGS_ASSET, output = CVDP_DIAG_FLAGS_WROTE_ASSET),
    
    ### CVDP HYPERTENSION DATA
    AddAuditingFieldStage(
      passthrough_asset_add_meta_column = CVDP_HTN_ASSET),
    WriteAssetStage(
      input_asset_to_save = CVDP_HTN_ASSET, output = CVDP_HTN_ASSET_WROTE),
    
    ### CVDP SMOKING STATUS
    AddAuditingFieldStage(
      passthrough_asset_add_meta_column = CVDP_SMOKING_STATUS_ASSET),
    WriteAssetStage(
      input_asset_to_save = CVDP_SMOKING_STATUS_ASSET, output = CVDP_SMOKING_STATUS_WROTE_ASSET),
    
    ### CVDP SMOKING INTERVENTION
    AddAuditingFieldStage(
      passthrough_asset_add_meta_column = CVDP_SMOKING_INTERVENTION_ASSET),
    WriteAssetStage(
      input_asset_to_save = CVDP_SMOKING_INTERVENTION_ASSET, output = CVDP_SMOKING_INTERVENTION_WROTE_ASSET),

    ## EVENTS TABLE STAGE
    CreateEventsTableStage(
      cvdp_cohort_input   = PATIENT_COHORT_TABLE_WROTE_ASSET,
      cvdp_htn_input      = CVDP_HTN_ASSET_WROTE,
      dars_input          = DARS_ASSET_WROTE,
      hes_apc_input       = HES_APC_TABLE_WROTE_ASSET,
      cvdp_smoking_status_input = CVDP_SMOKING_STATUS_WROTE_ASSET,
      events_table_output = EVENTS_TABLE_ASSET
    ),
    
    ## WRITE STAGE: EVENTS TABLE
    AddAuditingFieldStage(
      passthrough_asset_add_meta_column = EVENTS_TABLE_ASSET),
    WriteAssetStage(
      input_asset_to_save = EVENTS_TABLE_ASSET, output = EVENTS_TABLE_WROTE_ASSET),
    
     ## DEMOGRAPHIC TABLE STAGE
    CreateDemographicTableStage(
      events_table_input = EVENTS_TABLE_WROTE_ASSET,
      cvdp_cohort_journal_input = PATIENT_COHORT_JOURNAL_TABLE_WROTE_ASSET,
      hes_op_input = HES_OP_TABLE_WROTE_ASSET,
      hes_ae_input = HES_AE_TABLE_WROTE_ASSET,
      demographic_table_output = DEMOGRAPHIC_TABLE_ASSET
    ),
    
    ## WRITE STAGE: DEMOGRAPHIC TABLE
    AddAuditingFieldStage(
      passthrough_asset_add_meta_column = DEMOGRAPHIC_TABLE_ASSET),
    WriteAssetStage(
      input_asset_to_save = DEMOGRAPHIC_TABLE_ASSET, output = DEMOGRAPHIC_TABLE_WROTE_ASSET),
    
  
    ## PATIENT TABLE STAGE
    CreatePatientTableStage(
      indicators_table_input  = CVDP_DIAG_FLAGS_WROTE_ASSET,
      events_table_input      = EVENTS_TABLE_WROTE_ASSET,
      demographic_table_input = DEMOGRAPHIC_TABLE_WROTE_ASSET,
      patient_table_output    = PATIENT_TABLE_ASSET,
    ),
    
    ## WRITE STAGE: PATIENT TABLE
    AddAuditingFieldStage(
      passthrough_asset_add_meta_column = PATIENT_TABLE_ASSET),
    WriteAssetStage(
      input_asset_to_save = PATIENT_TABLE_ASSET, output = PATIENT_TABLE_WROTE_ASSET),
    
  ]
  
  # PREPARED PSEUDONUMISATION ASSETS STAGE (ONLY RUNS WHEN prepare_pseudo_assets == True)
  # If ran in integration tests overwrite set to FALSE
  overwrite_bool = False if is_integration else params.PSEUDO_SAVE_OVERWRITE
  
  pseudo_preparation_stages = [
    PseudoPreparationStage(
      events_table_input = EVENTS_TABLE_WROTE_ASSET,
      patient_table_input = PATIENT_TABLE_WROTE_ASSET,
      curated_events_table_output = PSEUDO_EVENTS_TABLE_ASSET,
      curated_patient_table_output = PSEUDO_PATIENT_TABLE_ASSET,
      is_integration = is_integration
    ),
    WriteAssetStage(
      input_asset_to_save = PSEUDO_EVENTS_TABLE_ASSET,
      output = PSEUDO_EVENTS_TABLE_WROTE_ASSET,
      db = params.PSEUDO_DB_PATH,
      table_base_name = PSEUDO_EVENTS_TABLE_ASSET,
      overwrite = overwrite_bool
    ),
    WriteAssetStage(
      input_asset_to_save = PSEUDO_PATIENT_TABLE_ASSET,
      output = PSEUDO_PATIENT_TABLE_WROTE_ASSET,
      db = params.PSEUDO_DB_PATH,
      table_base_name = PSEUDO_PATIENT_TABLE_ASSET,
      overwrite = overwrite_bool
    ),
  ]
  
  #ALWAYS SHOULD GO LAST, LOGGER SHOULD BE LAST STAGE THAT RUNS
  logger_stages = [
    DefaultCreateReportTableStage(
       events_table_input = EVENTS_TABLE_WROTE_ASSET,
       patient_table_input = PATIENT_TABLE_WROTE_ASSET,
       report_table_output = RESULTS_CHECKER_ASSET
    ),
    AddAuditingFieldStage(
       passthrough_asset_add_meta_column = RESULTS_CHECKER_ASSET
    ),
    WriteAssetStage(
      input_asset_to_save = RESULTS_CHECKER_ASSET, 
      output = RESULTS_CHECKER_WROTE_ASSET
    ),
    CreateLoggerOutputStage(
      logger_output  =  LOGGER_OUTPUT_ASSET
    ),
    AddAuditingFieldStage(
      passthrough_asset_add_meta_column = LOGGER_OUTPUT_ASSET
    ),
    WriteAssetStage(
      input_asset_to_save = LOGGER_OUTPUT_ASSET, 
      output = LOGGER_OUTPUT_WROTE_ASSET
    ),
    DefaultCheckResultsStage(
      report_input = RESULTS_CHECKER_WROTE_ASSET
    )
  ]
  
  ## CONCAT STAGES INTO DEFAULT
  if prepare_pseudo_assets == True:
    default_pipeline_stage = pipeline_cohort + default_pipeline_stage + pseudo_preparation_stages + logger_stages
  else:
    default_pipeline_stage = pipeline_cohort + default_pipeline_stage + logger_stages
  
  return default_pipeline_stage