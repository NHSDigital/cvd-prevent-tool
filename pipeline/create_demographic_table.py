# Databricks notebook source
# create_demographic_table

## Overview
# Stage for creating the demographic table from HES & CVDP data

# COMMAND ----------

# MAGIC %run ../params/params

# COMMAND ----------

# MAGIC %run ./pipeline_util

# COMMAND ----------

# MAGIC %run ./create_demographic_table_lib

# COMMAND ----------

# MAGIC %run ./create_patient_table_lib

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.window import Window

from typing import List, Dict
from pyspark.sql import DataFrame

# COMMAND ----------

class CreateDemographicTableStage(PipelineStage):
    '''
    A pipeline stage that combines:
    * CVDP journal data
    * HES APC and CVDP cohort data from the events table
    * HES OP and AE data
    where each row is a patient identifier and a unique demographic field.
    '''

    ### MAIN STAGE-CLASS DEFINITIONS ###
    def __init__(
        self,
        events_table_input: str,
        cvdp_cohort_journal_input: str,
        hes_op_input: str,
        hes_ae_input: str,
        demographic_table_output: str
    ):
        self._events_table_input = events_table_input
        self._cvdp_cohort_journal_input = cvdp_cohort_journal_input
        self._hes_op_input = hes_op_input
        self._hes_ae_input = hes_ae_input
        self._demographic_table_output = demographic_table_output
        self._data_holder: Dict[str, DataFrame] = {}
        self._data_entries: List[DemographicStageDataEntry] = []
        self._data_check_rec: Dict[str,int] = {}
        super(CreateDemographicTableStage, self).__init__(
            {
                self._events_table_input,
                self._cvdp_cohort_journal_input,
                self._hes_op_input,
                self._hes_ae_input
            },
            {
                self._demographic_table_output
            }
        )

    ### RUN STAGE ###
    def _run(self, context, log, limit: bool = False):
        ## SETUP LOGGING
        log._add_stage(self.name)
        log._timer(self.name)

        ## LOAD DATA ASSETS
        self._load_data_assets(context)
        self._restrict_hes_ae_year()

        ## CREATE BASE FROM CVDP COHORT
        self._create_base_table()

        ## ENHANCE ETHNICITY
        self._find_known_ethnicity()
        self._enhance_ethnicity()
        self._consolidate_ethnicities()

        log._timer(self.name, end=True)

        return {
            self._demographic_table_output: PipelineAsset(
                key = self._demographic_table_output,
                context = context,
                db = params.DATABASE_NAME,
                df = self._data_holder["demographic_data"],
                cache = False,
                delta_table = True,
                delta_columns = [params.PID_FIELD,params.DOB_FIELD])
            }
        
    def _load_data_assets(self, context):
        """_load_data_assets
        Loads the events, journal and hes tables. Stores into _data_holder.
        HES APC is extracted from events.

        Functions:
            pipeline/create_patient_table_lib::extract_patient_events
        """
        self._data_holder["events"] = context[self._events_table_input].df.drop("META")
        self._data_holder["journal"] = context[self._cvdp_cohort_journal_input].df.drop("META")
        self._data_holder["hes_op"] = context[self._hes_op_input].df.drop("META")
        self._data_holder["hes_ae"] = context[self._hes_ae_input].df.drop("META")
        self._data_holder["hes_apc"] = extract_patient_events(
          df=self._data_holder["events"],
          value_dataset=params.HES_APC_TABLE,
          value_category=params.EVENTS_HES_APC_EPISODE_CATEGORY
        )
    
    def _restrict_hes_ae_year(self):
        """_restrict_hes_ae_year
        Filters the HES AE data input to the demographic table to after and including a given year

        Functions:
            pipeline/create_demographic_table_lib::filter_from_year
        """
      # Filter AE for last five years
        self._data_holder["hes_ae"] = filter_from_year(self._data_holder["hes_ae"], start_year=params.HES_5YR_START_YEAR, date_field=params.RECORD_STARTDATE_FIELD)

    # Create base table from cvdp cohort (events) table
    def _create_base_table(self):
        """create_base_table
        Creates the demographic table from the events table, cohort based events.
        (dataset = cvdp_cohort; category = cohort_extract).

        Functions:
            pipeline/create_patient_table_lib::extract_patient_events
        """
        # Load the events table
        events_df = self._data_holder["events"]


        # Extract CVDP cohort events from the events table and filter to keep latest only
        cvdp_cohort_df = extract_patient_events(
            df=events_df,
            value_dataset=params.EVENTS_CVDP_COHORT_DATASET,
            value_category=params.EVENTS_CVDP_COHORT_CATEGORY,
            )

        # Select columns that form the base demographic table
        self._data_holder["demographic_data"] = cvdp_cohort_df.select(params.DEMOGRAPHIC_OUTPUT_FIELDS)

    def _find_known_ethnicity(self):
        """_find_known_ethnicity
        Identifies records from the base table with known & unknown ethnicity.
        They are stored in two different _data_holder objects.
        Functions:
          pipeline/create_demographic_table_lib::set_to_null
          pipeline/create_demographic_table_lib::filter_latest_priority_non_null
          pipeline/create_demographic_table_lib::remove_multiple_records
        """
        df = self._data_holder["demographic_data"]

        df = set_to_null(df, field=params.ETHNICITY_FIELD, values_to_set=params.ETHNICITY_UNKNOWN_CODES)
        # Keep latest record, with priority to non-null ethnicities
        df = filter_latest_priority_non_null(df, pid_fields=[params.PID_FIELD, params.DOB_FIELD, params.DATASET_FIELD], date_field=params.RECORD_STARTDATE_FIELD, field=params.ETHNICITY_FIELD)
        # Set ethnicity to null where >1 ethnicity on latest date
        df = set_multiple_records_to_null(df, window_fields=[params.PID_FIELD, params.DOB_FIELD, params.DATASET_FIELD, params.RECORD_STARTDATE_FIELD], field=params.ETHNICITY_FIELD)

        # Identify records that need enhancing
        df_unknown= df.filter(F.col(params.ETHNICITY_FIELD).isNull()).select(params.PID_FIELD, params.DOB_FIELD).distinct()

        # Identify records that are populated at this level already
        df_known = df.filter(F.col(params.ETHNICITY_FIELD).isNotNull()).distinct()

        # Save patient table
        self._data_holder["demographic_known"] = df_known
        self._data_holder["demographic_unknown"] = df_unknown

    def _enhance_ethnicity(self):
        """_enhance_ethnicity
        For those records with no known ethnicity, additional information is taken from other sources.
        Functions:
          pipeline/create_demographic_table_lib::get_journal_ethnicity
          pipeline/create_demographic_table_lib::get_hes_ethnicity
          src/utils::union_multiple_dfs
          pipeline/create_demographic_table_lib::select_priority_ethnicity
        """
        ids = self._data_holder["demographic_unknown"]

        ## FILTER LATEST FROM EACH SOURCE
        journal_df = get_journal_ethnicity(self._data_holder["journal"])
        hes_op = get_hes_ethnicity(self._data_holder["hes_op"])
        hes_ae = get_hes_ethnicity(self._data_holder["hes_ae"])
        hes_apc = get_hes_ethnicity(self._data_holder["hes_apc"])

        combined_df = union_multiple_dfs([journal_df, hes_apc, hes_op, hes_ae])

        ## PRIORITISE ETHNICITY ACCORDING TO LOGIC DEFINED IN select_priority_ethnicity
        single_ethnicity = select_priority_ethnicity(combined_df)

        ## ONLY KEEP THOSE IDENTIFIED AS UNKNOWN
        single_ethnicity = ids.join(single_ethnicity, on=params.GLOBAL_JOIN_KEY, how='left')

        self._data_holder["demographic_enhanced"] = single_ethnicity


    def _consolidate_ethnicities(self):
        """_consolidate_ethnicities
        Join together records with known ethnicity in the base table, with enhanced records.
        Flag enhanced records.
        """

        known_ethnicities = self._data_holder["demographic_known"].withColumn('enhanced_ethnicity_flag', F.lit(0))
        enhanced_ethnicities = self._data_holder["demographic_enhanced"].withColumn('enhanced_ethnicity_flag', F.lit(1))

        all_ethnicities = known_ethnicities.union(enhanced_ethnicities)

        self._data_holder['demographic_data'] = all_ethnicities