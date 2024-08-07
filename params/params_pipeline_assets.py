# Databricks notebook source
### Pipeline Parameters - Defining pipeline assets

# COMMAND ----------

from typing import Dict
from dataclasses import field

# COMMAND ----------

PARAMS_PIPELINE_ASSET_MAPPING: Dict[str,str] = field(default_factory = lambda: {
  # Extracted Assets
  ## CVDP Assets
  'EXTRACTED_CVDP_TABLE_ASSET': 'extracted_cvdp_store',
  'EXTRACTED_CVDP_TABLE_WROTE_ASSET': 'extracted_cvdp_store_wrote',
  ## HES ASSETS
  'EXTRACTED_HES_APC_TABLE_ASSET': 'extracted_hes_apc',
  'EXTRACTED_HES_APC_TABLE_WROTE_ASSET': 'extracted_hes_apc_wrote',
  'EXTRACTED_HES_AE_TABLE_ASSET': 'extracted_hes_ae',
  'EXTRACTED_HES_AE_TABLE_WROTE_ASSET': 'extracted_hes_ae_wrote',
  'EXTRACTED_HES_OP_TABLE_ASSET': 'extracted_hes_op',
  'EXTRACTED_HES_OP_TABLE_WROTE_ASSET': 'extracted_hes_op_wrote',
  # Cohort Assets
  ## Eligible Cohort
  'PATIENT_COHORT_TABLE_ASSET': 'eligible_cohort',
  'PATIENT_COHORT_TABLE_WROTE_ASSET': 'eligible_cohort_wrote',
  ## Eligible Journal
  'PATIENT_COHORT_JOURNAL_TABLE_ASSET': 'eligible_cohort_journal',
  'PATIENT_COHORT_JOURNAL_TABLE_WROTE_ASSET': 'eligible_cohort_journal_wrote',
  # Preprocessed Assets
  ## Death Assets
  'DARS_ASSET': 'dars_bird_deaths',
  'DARS_ASSET_WROTE': 'dars_bird_deaths_wrote',
  ## HES Assets
  'HES_APC_TABLE_ASSET': 'hes_apc',
  'HES_APC_TABLE_WROTE_ASSET': 'hes_apc_wrote',
  'HES_AE_TABLE_ASSET': 'hes_ae',
  'HES_AE_TABLE_WROTE_ASSET': 'hes_ae_wrote',
  'HES_OP_TABLE_ASSET': 'hes_op',
  'HES_OP_TABLE_WROTE_ASSET': 'hes_op_wrote',
  ## CVDP Derived Assets
  'CVDP_DIAG_FLAGS_ASSET': 'cvdp_diag_flags',
  'CVDP_DIAG_FLAGS_WROTE_ASSET': 'cvdp_diag_flags_wrote',
  'CVDP_HTN_ASSET': 'cvdp_htn',
  'CVDP_HTN_ASSET_WROTE': 'cvdp_htn_wrote',
  # Pipeline Outputs
  ## Events Assets
  'EVENTS_TABLE_ASSET': 'events_table',
  'EVENTS_TABLE_WROTE_ASSET': 'events_table_wrote',
  ## Patient Assets
  'PATIENT_TABLE_ASSET': 'patient_table',
  'PATIENT_TABLE_WROTE_ASSET': 'patient_table_wrote',
  ## Demographic Assets
  'DEMOGRAPHIC_TABLE_ASSET': 'demographic_table',
  'DEMOGRAPHIC_TABLE_WROTE_ASSET': 'demographic_table_wrote',
  ## Results Assets
  'RESULTS_CHECKER_ASSET': 'results_checker_table',
  'RESULTS_CHECKER_WROTE_ASSET': 'results_checker_table_wrote',
  ## Logger Assets
  'LOGGER_OUTPUT_ASSET': 'logger_table',
  'LOGGER_OUTPUT_WROTE_ASSET': 'logger_table_wrote',
  ## Pseudonymised Assets
  'PSEUDO_EVENTS_TABLE_ASSET': 'cvdp_linkage_events_table',
  'PSEUDO_PATIENT_TABLE_ASSET': 'cvdp_linkage_patient_table',
  'PSEUDO_EVENTS_TABLE_WROTE_ASSET': 'cvdp_linkage_events_table_wrote',
  'PSEUDO_PATIENT_TABLE_WROTE_ASSET': 'cvdp_linkage_patient_table_wrote',
  })

# COMMAND ----------

