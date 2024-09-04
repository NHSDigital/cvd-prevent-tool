# Databricks notebook source
# MAGIC %run ../params/params

# COMMAND ----------

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

# MAGIC %run ./extract_cvdp_stage

# COMMAND ----------

# MAGIC %run ./archive_asset_stage

# COMMAND ----------

# MAGIC %run ./extract_hes_stage

# COMMAND ----------

def get_default_pipeline_stages(run_logger_stages: bool = False, run_archive_stages: bool = False, pipeline_dev_mode: bool = True):
  """
  Returns a list of pipeline stages that defines the default pipeline.
  The default pipeline loads all source datasets, preprocesses and cleans them, and forms the
  patient table. The patient table is used to define all eligible patients. The events table
  is created by processing the events data, using the patient table to filter out non-elible
  patients. These datasets are unioned to form the events table. The final stages run a check
  stage that creates a report table, and a stage that checks for any failures in the report
  table.

  See pipeline/pipeline_util::PipelineStage and pipeline/pipeline_util::PipelineContext for more
  info on how the pipeline works. It is a list of PipelineStage instances. The string values passed
  into the class constructors are keys that identify where within the PipelineContext the data
  will be stored. The PipelineContext object is passed around between the pipeline stages so the
  various dataframes can be accessed from it using the key. There are other options where the key
  can also be a database and table location such that the data from that location is read into a
  dataframe.

  Pipeline assets are defined using the PIPELINE_ASSET dictionary in params. Stage inputs and output
  keys are defined using PIPELINE_ASSET[ASSET_NAME] where the ASSET_NAME is the desired asset key,
  yields the value of the table name.

  Args:
      run_logger_stages (bool, optional): Switch for running the full pipeline followed by the logger stage. 
        Defaults to False.
      run_archive_stages (bool, optional): Switch for running the Archive stages after the pipeline assets 
        have been written. 
        Defaults to False.
      pipeline_dev_mode (bool, optional): Switch for running the pipeline in development mode (dev_mode). In this
        mode, all tables are prepended with "dev_{x}" where x is a randomly generated string. This mode
        is used when running a full pipeline mode, but wanting to create (rather than update) delta tables
        and assets.
        Defaults to True
  """
  ### DEV MODE CONDITIONS ###
  # If dev_mode is True, create prefix and apply to tables
  PIPELINE_ASSET = params.PIPELINE_ASSETS
  if pipeline_dev_mode:
    PIPELINE_ASSET = update_pipeline_assets(
      pipeline_assets = PIPELINE_ASSET,
      asset_prefix = '_BAU_FINAL')

  # Data Extraction Stages
  data_extraction_stages = [
    ## CVDP Data Extraction
    ExtractCVDPDataStage(
      extracted_cvdp_table_output = PIPELINE_ASSET["EXTRACTED_CVDP_TABLE_ASSET"]
    ),
    WriteAssetStage(
      input_asset_to_save = PIPELINE_ASSET["EXTRACTED_CVDP_TABLE_ASSET"],
      output = PIPELINE_ASSET["EXTRACTED_CVDP_TABLE_WROTE_ASSET"],
      overwrite = True,
    ),
    ArchiveAssetStage(
      input_asset_to_archive = PIPELINE_ASSET["EXTRACTED_CVDP_TABLE_WROTE_ASSET"],
      run_archive_stage = run_archive_stages
    ),
    ## HES Data Extraction
    ExtractHESDataStage(
      extracted_cvdp_table_input = PIPELINE_ASSET["EXTRACTED_CVDP_TABLE_WROTE_ASSET"],
      extracted_hes_apc_table_output = PIPELINE_ASSET["EXTRACTED_HES_APC_TABLE_ASSET"],
      extracted_hes_ae_table_output = PIPELINE_ASSET["EXTRACTED_HES_AE_TABLE_ASSET"],
      extracted_hes_op_table_output = PIPELINE_ASSET["EXTRACTED_HES_OP_TABLE_ASSET"]
    ),
    ## HES Extraction Saving
    ### HES APC
    WriteAssetStage(
      input_asset_to_save = PIPELINE_ASSET["EXTRACTED_HES_APC_TABLE_ASSET"],
      output = PIPELINE_ASSET["EXTRACTED_HES_APC_TABLE_WROTE_ASSET"],
      overwrite = True,
    ),
    ArchiveAssetStage(
      input_asset_to_archive = PIPELINE_ASSET["EXTRACTED_HES_APC_TABLE_WROTE_ASSET"],
      run_archive_stage = run_archive_stages
    ),
    ### HES AE
    WriteAssetStage(
      input_asset_to_save = PIPELINE_ASSET["EXTRACTED_HES_AE_TABLE_ASSET"],
      output = PIPELINE_ASSET["EXTRACTED_HES_AE_TABLE_WROTE_ASSET"],
      overwrite = True,
    ),
    ArchiveAssetStage(
      input_asset_to_archive = PIPELINE_ASSET["EXTRACTED_HES_AE_TABLE_WROTE_ASSET"],
      run_archive_stage = run_archive_stages
    ),
    ### HES OP
    WriteAssetStage(
      input_asset_to_save = PIPELINE_ASSET["EXTRACTED_HES_OP_TABLE_ASSET"],
      output = PIPELINE_ASSET["EXTRACTED_HES_OP_TABLE_WROTE_ASSET"],
      overwrite = True,
    ),
    ArchiveAssetStage(
      input_asset_to_archive = PIPELINE_ASSET["EXTRACTED_HES_OP_TABLE_WROTE_ASSET"],
      run_archive_stage = run_archive_stages
    ),
  ]
  
  # Cohort Processing Stages
  pipeline_cohort = [
    ## Create Cohort and Journal Tables
    CreatePatientCohortTableStage(
      extracted_cvdp_table_input = PIPELINE_ASSET["EXTRACTED_CVDP_TABLE_WROTE_ASSET"],
      patient_cohort_table_output = PIPELINE_ASSET["PATIENT_COHORT_TABLE_ASSET"],
      patient_cohort_journal_output = PIPELINE_ASSET["PATIENT_COHORT_JOURNAL_TABLE_ASSET"]),
    ## Write: Cohort Table
    WriteAssetStage(
      input_asset_to_save = PIPELINE_ASSET["PATIENT_COHORT_TABLE_ASSET"],
      output = PIPELINE_ASSET["PATIENT_COHORT_TABLE_WROTE_ASSET"],
      overwrite = True,
    ),
    ArchiveAssetStage(
      input_asset_to_archive = PIPELINE_ASSET["PATIENT_COHORT_TABLE_WROTE_ASSET"],
      run_archive_stage = run_archive_stages
    ),
    ## Write: Journal Table
    WriteAssetStage(
      input_asset_to_save = PIPELINE_ASSET["PATIENT_COHORT_JOURNAL_TABLE_ASSET"],
      output = PIPELINE_ASSET["PATIENT_COHORT_JOURNAL_TABLE_WROTE_ASSET"],
      overwrite = True,
    ),
    ArchiveAssetStage(
      input_asset_to_archive = PIPELINE_ASSET["PATIENT_COHORT_JOURNAL_TABLE_WROTE_ASSET"],
      run_archive_stage = run_archive_stages
    ),
  ]

  ## DEFAULT REMAINING PIPELINE
  default_pipeline_stage = [
    ## PREPROCESS RAW DATA STAGE
    PreprocessRawDataStage(
      patient_cohort_input = PIPELINE_ASSET["PATIENT_COHORT_TABLE_WROTE_ASSET"],
      patient_cohort_journal_input = PIPELINE_ASSET["PATIENT_COHORT_JOURNAL_TABLE_WROTE_ASSET"],
      hes_apc_input = PIPELINE_ASSET["EXTRACTED_HES_APC_TABLE_WROTE_ASSET"],
      hes_ae_input = PIPELINE_ASSET["EXTRACTED_HES_AE_TABLE_WROTE_ASSET"],
      hes_op_input = PIPELINE_ASSET["EXTRACTED_HES_OP_TABLE_WROTE_ASSET"],
      dars_output = PIPELINE_ASSET["DARS_ASSET"],
      hes_apc_output = PIPELINE_ASSET["HES_APC_TABLE_ASSET"],
      hes_op_output = PIPELINE_ASSET["HES_OP_TABLE_ASSET"],
      hes_ae_output = PIPELINE_ASSET["HES_AE_TABLE_ASSET"],
      cvdp_diag_flags_output = PIPELINE_ASSET["CVDP_DIAG_FLAGS_ASSET"],
      cvdp_htn_output= PIPELINE_ASSET["CVDP_HTN_ASSET"]
      ),

    ## WRITE STAGE: DARS OUTPUT
    WriteAssetStage(
      input_asset_to_save = PIPELINE_ASSET["DARS_ASSET"],
      output = PIPELINE_ASSET["DARS_ASSET_WROTE"],
      overwrite = True,
    ),
    ArchiveAssetStage(
      input_asset_to_archive = PIPELINE_ASSET["DARS_ASSET_WROTE"],
      run_archive_stage = run_archive_stages
    ),

    ## WRITE STAGE: HES APC OUTPUT
    WriteAssetStage(
      input_asset_to_save = PIPELINE_ASSET["HES_APC_TABLE_ASSET"],
      output = PIPELINE_ASSET["HES_APC_TABLE_WROTE_ASSET"],
      overwrite = True,
    ),
    ArchiveAssetStage(
      input_asset_to_archive = PIPELINE_ASSET["HES_APC_TABLE_WROTE_ASSET"],
      run_archive_stage = run_archive_stages
    ),

    ## WRITE STAGE: HES OP OUTPUT
    WriteAssetStage(
      input_asset_to_save = PIPELINE_ASSET["HES_OP_TABLE_ASSET"],
      output = PIPELINE_ASSET["HES_OP_TABLE_WROTE_ASSET"],
      overwrite = True,
    ),
    ArchiveAssetStage(
      input_asset_to_archive = PIPELINE_ASSET["HES_OP_TABLE_WROTE_ASSET"],
      run_archive_stage = run_archive_stages
    ),
    
    ## WRITE STAGE: HES AE OUTPUT
    WriteAssetStage(
      input_asset_to_save = PIPELINE_ASSET["HES_AE_TABLE_ASSET"],
      output = PIPELINE_ASSET["HES_AE_TABLE_WROTE_ASSET"],
      overwrite = True,
    ),
    ArchiveAssetStage(
      input_asset_to_archive = PIPELINE_ASSET["HES_AE_TABLE_WROTE_ASSET"],
      run_archive_stage = run_archive_stages
    ),

    ### DIAGNOSTIC FLAGS
    WriteAssetStage(
      input_asset_to_save = PIPELINE_ASSET["CVDP_DIAG_FLAGS_ASSET"],
      output = PIPELINE_ASSET["CVDP_DIAG_FLAGS_WROTE_ASSET"],
      overwrite = True,
    ),
    ArchiveAssetStage(
      input_asset_to_archive = PIPELINE_ASSET["CVDP_DIAG_FLAGS_WROTE_ASSET"],
      run_archive_stage = run_archive_stages
    ),

    ### CVDP HYPERTENSION DATA
    WriteAssetStage(
      input_asset_to_save = PIPELINE_ASSET["CVDP_HTN_ASSET"],
      output = PIPELINE_ASSET["CVDP_HTN_ASSET_WROTE"],
      overwrite = True,
    ),
    ArchiveAssetStage(
      input_asset_to_archive = PIPELINE_ASSET["CVDP_HTN_ASSET_WROTE"],
      run_archive_stage = run_archive_stages
    ),

    ## EVENTS TABLE STAGE
    CreateEventsTableStage(
      cvdp_cohort_input         = PIPELINE_ASSET["PATIENT_COHORT_TABLE_WROTE_ASSET"],
      cvdp_htn_input            = PIPELINE_ASSET["CVDP_HTN_ASSET_WROTE"],
      dars_input                = PIPELINE_ASSET["DARS_ASSET_WROTE"],
      hes_apc_input             = PIPELINE_ASSET["HES_APC_TABLE_WROTE_ASSET"],
      events_table_output       = PIPELINE_ASSET["EVENTS_TABLE_ASSET"]
    ),

    ## WRITE STAGE: EVENTS TABLE
    WriteAssetStage(
      input_asset_to_save = PIPELINE_ASSET["EVENTS_TABLE_ASSET"],
      output = PIPELINE_ASSET["EVENTS_TABLE_WROTE_ASSET"],
      overwrite = True,
    ),
    ArchiveAssetStage(
      input_asset_to_archive = PIPELINE_ASSET["EVENTS_TABLE_WROTE_ASSET"],
      run_archive_stage = run_archive_stages
    ),

    ## DEMOGRAPHIC TABLE STAGE
    CreateDemographicTableStage(
      events_table_input = PIPELINE_ASSET["EVENTS_TABLE_WROTE_ASSET"],
      cvdp_cohort_journal_input = PIPELINE_ASSET["PATIENT_COHORT_JOURNAL_TABLE_WROTE_ASSET"],
      hes_op_input = PIPELINE_ASSET["HES_OP_TABLE_WROTE_ASSET"],
      hes_ae_input = PIPELINE_ASSET["HES_AE_TABLE_WROTE_ASSET"],
      demographic_table_output = PIPELINE_ASSET["DEMOGRAPHIC_TABLE_ASSET"]
    ),

    ## WRITE STAGE: DEMOGRAPHIC TABLE
    WriteAssetStage(
      input_asset_to_save = PIPELINE_ASSET["DEMOGRAPHIC_TABLE_ASSET"],
      output = PIPELINE_ASSET["DEMOGRAPHIC_TABLE_WROTE_ASSET"],
      overwrite = True,
    ),
    ArchiveAssetStage(
      input_asset_to_archive = PIPELINE_ASSET["DEMOGRAPHIC_TABLE_WROTE_ASSET"],
      run_archive_stage = run_archive_stages
    ),


    ## PATIENT TABLE STAGE
    CreatePatientTableStage(
      indicators_table_input  = PIPELINE_ASSET["CVDP_DIAG_FLAGS_WROTE_ASSET"],
      events_table_input      = PIPELINE_ASSET["EVENTS_TABLE_WROTE_ASSET"],
      demographic_table_input = PIPELINE_ASSET["DEMOGRAPHIC_TABLE_WROTE_ASSET"],
      patient_table_output    = PIPELINE_ASSET["PATIENT_TABLE_ASSET"],
    ),

    ## WRITE STAGE: PATIENT TABLE
    WriteAssetStage(
      input_asset_to_save = PIPELINE_ASSET["PATIENT_TABLE_ASSET"],
      output = PIPELINE_ASSET["PATIENT_TABLE_WROTE_ASSET"],
      overwrite = True,
    ),
    ArchiveAssetStage(
      input_asset_to_archive = PIPELINE_ASSET["PATIENT_TABLE_WROTE_ASSET"],
      run_archive_stage = run_archive_stages
    ),

  ]

  # PREPARED PSEUDONUMISATION ASSETS STAGE (ONLY RUNS WHEN prepare_pseudo_assets == True)
  pseudo_preparation_stages = [
    PseudoPreparationStage(
      events_table_input = PIPELINE_ASSET["EVENTS_TABLE_WROTE_ASSET"],
      patient_table_input = PIPELINE_ASSET["PATIENT_TABLE_WROTE_ASSET"],
      curated_events_table_output = PIPELINE_ASSET["PSEUDO_EVENTS_TABLE_ASSET"],
      curated_patient_table_output = PIPELINE_ASSET["PSEUDO_PATIENT_TABLE_ASSET"],
    ),
    ## CVDP Linkage Events Table
    WriteAssetStage(
      input_asset_to_save = PIPELINE_ASSET["PSEUDO_EVENTS_TABLE_ASSET"],
      output = PIPELINE_ASSET["PSEUDO_EVENTS_TABLE_WROTE_ASSET"],
      overwrite = True,
    ),
    ArchiveAssetStage(
      input_asset_to_archive = PIPELINE_ASSET["PSEUDO_EVENTS_TABLE_WROTE_ASSET"],
      run_archive_stage = run_archive_stages
    ),
    ## CVDP Linkage Patient Table
    WriteAssetStage(
      input_asset_to_save = PIPELINE_ASSET["PSEUDO_PATIENT_TABLE_ASSET"],
      output = PIPELINE_ASSET["PSEUDO_PATIENT_TABLE_WROTE_ASSET"],
      overwrite = True,
    ),
    ArchiveAssetStage(
      input_asset_to_archive = PIPELINE_ASSET["PSEUDO_PATIENT_TABLE_WROTE_ASSET"],
      run_archive_stage = run_archive_stages
    ),
  ]

  #ALWAYS SHOULD GO LAST, LOGGER SHOULD BE LAST STAGE THAT RUNS
  logger_stages = [
    DefaultCreateReportTableStage(
      events_table_input = PIPELINE_ASSET["EVENTS_TABLE_WROTE_ASSET"],
      patient_table_input = PIPELINE_ASSET["PATIENT_TABLE_WROTE_ASSET"],
      report_table_output = PIPELINE_ASSET["RESULTS_CHECKER_ASSET"]
    ),
    AddAuditingFieldStage(
      passthrough_asset_add_meta_column = PIPELINE_ASSET["RESULTS_CHECKER_ASSET"]
    ),
    WriteAssetStage(
      input_asset_to_save = PIPELINE_ASSET["RESULTS_CHECKER_ASSET"],
      output = PIPELINE_ASSET["RESULTS_CHECKER_WROTE_ASSET"],
      overwrite = True,
    ),
    CreateLoggerOutputStage(
      logger_output  =  PIPELINE_ASSET["LOGGER_OUTPUT_ASSET"]
    ),
    AddAuditingFieldStage(
      passthrough_asset_add_meta_column = PIPELINE_ASSET["LOGGER_OUTPUT_ASSET"]
    ),
    WriteAssetStage(
      input_asset_to_save = PIPELINE_ASSET["LOGGER_OUTPUT_ASSET"],
      output = PIPELINE_ASSET["LOGGER_OUTPUT_WROTE_ASSET"],
      overwrite = True,
    ),
    DefaultCheckResultsStage(
      report_input = PIPELINE_ASSET["RESULTS_CHECKER_WROTE_ASSET"]
    )
  ]

  ## CONCAT STAGES INTO DEFAULT
  if run_logger_stages == True:
    default_pipeline_stage = data_extraction_stages + pipeline_cohort + default_pipeline_stage + pseudo_preparation_stages + logger_stages
  else:
    default_pipeline_stage = data_extraction_stages + pipeline_cohort + default_pipeline_stage + pseudo_preparation_stages

  return default_pipeline_stage