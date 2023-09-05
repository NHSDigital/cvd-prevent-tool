# Databricks notebook source
# create_events_table

## Overview
# Stage for creating the events table from eligible patients and preprocessed data

# COMMAND ----------

# MAGIC %run ../params/params

# COMMAND ----------

# MAGIC %run ./pipeline_util

# COMMAND ----------

# MAGIC %run ./create_events_table_lib

# COMMAND ----------

# MAGIC %run ../src/hes/process_hes_events

# COMMAND ----------

# MAGIC %run ../src/cvdp/process_cvdp_smoking_events

# COMMAND ----------

import pyspark.sql.functions as F

from functools import partial
from typing import List, Dict
from pyspark.sql import DataFrame

# COMMAND ----------

class CreateEventsTableStage(PipelineStage):
    '''
    A pipeline stage that combines the processed data to form an events table, where each row
    is an event from the data sources and multiple rows per patient.
    
    Each dataset that is combined must be specified before initiating the class
    '''
    
    ### PRE-INIT DEFINITIONS
    _output_fields = params.EVENTS_OUTPUT_FIELDS
    
    ### MAIN STAGE-CLASS DEFINITIONS ### 
    def __init__(
        self,
        cvdp_cohort_input: str,
        cvdp_htn_input: str,
        dars_input: str,
        hes_apc_input: str,
        cvdp_smoking_status_input: str,
        events_table_output: str
    ):
        self._cvdp_cohort_input = cvdp_cohort_input
        self._cvdp_htn_input = cvdp_htn_input
        self._hes_apc_input = hes_apc_input
        self._dars_input = dars_input
        self._cvdp_smoking_status_input = cvdp_smoking_status_input
        self._events_table_output = events_table_output
        self._data_holder: Dict[str, DataFrame] = {}
        self._events_data: DataFrame = None
        self._data_entries: List[EventsStageDataEntry] = []
        self._data_check_rec: Dict[str,int] = {}
        super(CreateEventsTableStage, self).__init__({
            self._cvdp_cohort_input,
            self._cvdp_htn_input,
            self._dars_input,
            self._hes_apc_input,
            self._cvdp_smoking_status_input
            },{
            self._events_table_output
            }
        )
    
    ### RUN STAGE ###
    def _run(self, context, log):
      
        log._add_stage(self.name)
        
        log._timer(self.name)
        
        ## INITALISE DATA ENTRIES
        self._dataset_definitions()
        
        ## TRANSFORM AND COMBINE EVENTS DATA
        self._transform_data_to_schema_and_combine(context)
        
        ## FORMAT FINAL EVENTS TABLE
        self._format_events_table()
        
        ## DATA CHECK
        self._check_events_table()
        
        log._timer(self.name, end=True)
        
        return {self._events_table_output: PipelineAsset(self._events_table_output, context, df = self._events_data, cache = True)}
    
    ### CLASS METHODS ###
    
    ## DEFINING PREPROCESSED ASSETS
    def _dataset_definitions(self):
        
        ### DATASET DEFINITIONS ###
        _data_entries: List[EventsStageDataEntry] = [
            
            ## CVDP COHORT TABLE
            EventsStageDataEntry(
                dataset_name = 'cvdp_cohort',
                context_key = self._cvdp_cohort_input,
                processing_func = None,
                mapping = EventsStageColumnMapping(
                    pid_field = params.PID_FIELD,
                    dob_field = params.DOB_FIELD,
                    age_field = params.AGE_FIELD,
                    sex_field = params.SEX_FIELD,
                    dataset_field = F.lit(params.EVENTS_CVDP_COHORT_DATASET),
                    category_field = F.lit(params.EVENTS_CVDP_COHORT_CATEGORY),
                    record_id_field = F.concat(F.lit('C'),F.monotonically_increasing_id(),F.unix_timestamp(params.EXTRACT_DATE_FIELD)),
                    record_start_date_field = params.EXTRACT_DATE_FIELD,
                    record_end_date_field = None,
                    lsoa_field = params.LSOA_FIELD,
                    ethnicity_field = params.ETHNICITY_CODE_FIELD,
                    code_field = params.COHORT_FIELD,
                    flag_field = None,
                    code_array_field = (params.PRACTICE_FIELD, partial(split_str_to_array)),
                    flag_array_field = None,
                    assoc_record_id_field = None,
                )),
            
            ## HES APC - EPISODES
            EventsStageDataEntry(
                dataset_name = 'hes_apc',
                context_key = self._hes_apc_input,
                processing_func = process_hes_events_episodes,
                mapping = EventsStageColumnMapping(
                    pid_field = params.PID_FIELD,
                    dob_field = params.DOB_FIELD,
                    age_field = (params.AGE_FIELD, partial(add_age_from_dob_at_date, dob_col = params.DOB_FIELD, at_date_col = params.RECORD_STARTDATE_FIELD)),
                    sex_field = params.SEX_FIELD,
                    dataset_field = params.DATASET_FIELD,
                    category_field = F.lit('episode'),
                    record_id_field = params.RECORD_ID_FIELD,
                    record_start_date_field = params.RECORD_STARTDATE_FIELD,
                    record_end_date_field = params.RECORD_ENDDATE_FIELD,
                    lsoa_field = params.LSOA_FIELD,
                    ethnicity_field = params.ETHNICITY_FIELD,
                    code_field = params.CODE_FIELD,
                    flag_field = params.HES_FLAG_FIELD,
                    code_array_field = params.CODE_ARRAY_FIELD,
                    flag_array_field = params.ASSOC_FLAG_FIELD,
                    assoc_record_id_field = None,
                )),
            
            ## HES APC - HOSPITALISATIONS
            EventsStageDataEntry(
                dataset_name = 'hes_apc',
                context_key = self._hes_apc_input,
                processing_func = process_hes_events_spells,
                mapping = EventsStageColumnMapping(
                    pid_field = params.PID_FIELD,
                    dob_field = params.DOB_FIELD,
                    age_field = (params.AGE_FIELD, partial(add_age_from_dob_at_date, dob_col = params.DOB_FIELD, at_date_col = params.HES_SPELL_START_FIELD)),
                    sex_field = params.SEX_FIELD,
                    dataset_field = params.DATASET_FIELD,
                    category_field = F.lit('spell'),
                    record_id_field = params.HES_SPELL_ID_FIELD,
                    record_start_date_field = params.HES_SPELL_START_FIELD,
                    record_end_date_field = params.HES_SPELL_END_FIELD,
                    lsoa_field = params.LSOA_FIELD,
                    ethnicity_field = params.ETHNICITY_FIELD,
                    code_field = params.CODE_FIELD,
                    flag_field = params.HES_FLAG_FIELD,
                    code_array_field = params.CODE_ARRAY_FIELD,
                    flag_array_field = params.ASSOC_FLAG_FIELD,
                    assoc_record_id_field = params.ASSOC_REC_ID_FIELD,
                )),
            
            ## DARS - MORTALITY
            EventsStageDataEntry(
                dataset_name = 'dars',
                context_key = self._dars_input,
                processing_func = None,
                mapping = EventsStageColumnMapping(
                    pid_field = params.PID_FIELD,
                    dob_field = params.DOB_FIELD,
                    age_field = (params.AGE_FIELD, partial(add_age_from_dob_at_date, dob_col = params.DOB_FIELD, at_date_col = params.DOD_FIELD)),
                    sex_field = params.SEX_FIELD,
                    dataset_field = params.DATASET_FIELD,
                    category_field = F.lit(params.EVENTS_DARS_CATEGORY),
                    record_id_field = params.RECORD_ID_FIELD,
                    record_start_date_field = params.DOD_FIELD,
                    record_end_date_field = None,
                    lsoa_field = params.DARS_LSOA_LOCATION,
                    ethnicity_field = None,
                    code_field = params.CODE_FIELD,
                    flag_field = params.FLAG_FIELD,
                    code_array_field = (params.CODE_ARRAY_FIELD, partial(split_str_to_array, delimiter = ',')),
                    flag_array_field = None,
                    assoc_record_id_field = None,
                )),
            
            ## CVDP - HYPERTENSION
            EventsStageDataEntry(
                dataset_name = 'cvdp_htn',
                context_key = self._cvdp_htn_input,
                processing_func = None,
                mapping = EventsStageColumnMapping(
                    pid_field = params.PID_FIELD,
                    dob_field = params.DOB_FIELD,
                    age_field = params.AGE_FIELD,
                    sex_field = params.SEX_FIELD,
                    dataset_field = params.DATASET_FIELD,
                    category_field = F.lit(params.EVENTS_CVDP_HTN_CATEGORY),
                    record_id_field = F.concat(F.lit('BP'),F.monotonically_increasing_id(),F.unix_timestamp(params.JOURNAL_DATE_FIELD)),
                    record_start_date_field = params.JOURNAL_DATE_FIELD,
                    record_end_date_field = params.EXTRACT_DATE_FIELD,
                    lsoa_field = params.LSOA_FIELD,
                    ethnicity_field = params.ETHNICITY_CODE_FIELD,
                    code_field = F.concat_ws('/',F.col(params.CVDP_SYSTOLIC_BP_READING),F.col(params.CVDP_DIASTOLIC_BP_READING)),
                    flag_field = params.FLAG_FIELD,
                    code_array_field = F.array(F.col(params.CVDP_SYSTOLIC_BP_READING),F.col(params.CVDP_DIASTOLIC_BP_READING)),
                    flag_array_field = None,
                    assoc_record_id_field = None,
                )),
          
            ## CVDP - SMOKING STATUS
            EventsStageDataEntry(
                dataset_name = 'cvdp_smoking_status',
                context_key = self._cvdp_smoking_status_input,
                processing_func = process_cvdp_smoking_status,
                mapping = EventsStageColumnMapping(
                    pid_field = params.PID_FIELD,
                    dob_field = params.DOB_FIELD,
                    age_field = params.AGE_FIELD,
                    sex_field = params.SEX_FIELD,
                    dataset_field = params.DATASET_FIELD,
                    category_field = F.lit(params.EVENTS_CVDP_SMOKING_STATUS_CATEGORY),
                    record_id_field = F.concat(F.lit('SMOK'),F.monotonically_increasing_id(),F.unix_timestamp(params.JOURNAL_DATE_FIELD)),
                    record_start_date_field = params.RECORD_STARTDATE_FIELD,
                    record_end_date_field = params.JOURNAL_DATE_FIELD,
                    lsoa_field = params.LSOA_FIELD,
                    ethnicity_field = params.ETHNICITY_CODE_FIELD,
                    code_field = params.CODE_FIELD,
                    flag_field = params.FLAG_FIELD,
                    code_array_field = None,
                    flag_array_field = None,
                    assoc_record_id_field = None,
                )),
            
        ]
        
        self._data_entries = _data_entries
        
    ## TRANSFORMING DATA -> EVENTS TABLE
    def _transform_data_to_schema_and_combine(self, context):
        
        ## INITIALISE EVENTS TABLE
        df_combined = None
        
        ## LOOP THROUGH EACH DATA ENTRY AND ADD INTO EVENTS TABLE
        for data_entry in self._data_entries:
            print(f'[EVENTS] TRANSFORMING AND COMBINING: {data_entry.dataset_name}')
            df = context[data_entry.context_key].df
            df = df.drop('META')
            
            ## CONDITIONAL: HES SPECIFIC EVENTS PROCESSING
            if data_entry.dataset_name.startswith('hes'):
                print(f'[INFO] PROCESSING HES EVENTS DATAFRAME...')
                df = data_entry.processing_func(df)
                
            ## CONDITIONAL: SMOKING SPECIFIC EVENTS PROCESSING
            if data_entry.dataset_name.startswith('cvdp_smoking'):
                print(f'[INFO] PROCESSING SMOKING EVENTS DATAFRAME...')
                df = data_entry.processing_func(df)
                
            ## TRANSFORM TO BASE SCHEMA
            df = format_source_data_to_combined_schema(data_entry, df, output_field_list = self._output_fields)
            if df_combined is None:
                df_combined = df
            else:
                df_combined = df_combined.union(df)
                
        ## ENSURE DATA TYPES
        # Dates
        df_combined = ensure_date_type(df_combined, params.RECORD_STARTDATE_FIELD)
        # Integers
        df_combined = ensure_int_type(df_combined, params.AGE_FIELD)
        # Strings
        df_combined = ensure_str_type(df_combined, [params.PID_FIELD, params.DATASET_FIELD,
                                                    params.CATEGORY_FIELD, params.RECORD_ID_FIELD, params.SEX_FIELD,
                                                    params.LSOA_FIELD, params.CODE_FIELD])
        
        ## RETURN
        self._events_data = df_combined
        
        
    ## EVENTS TABLE POST-PROCESSING
    
    def _format_events_table(self):
        
        ## LOAD EVENTS DATA
        df_events = self._events_data
        
        ## COLUMN ORDERING
        df_events = df_events.select(self._output_fields)
        
        ## RETURN
        self._events_data = df_events
        
    ## EVENTS TABLE DATA CHECK
    
    def _check_events_table(self):
        
        ## LOAD EVENTS DATA
        df_events = self._events_data
        
        ## NULLS IN PID + DOB FIELD CHECK 
        df_null_pid = df_events.select("*").where(F.col(params.PID_FIELD).isNull()).limit(1)
        df_null_dob = df_events.select("*").where(F.col(params.DOB_FIELD).isNull()).limit(1)
        
        ## OUTPUT MESSAGE
        if df_null_pid.count() > 0:
            print(f'[WARNING] NULLS IDENTIFIED IN COL {params.PID_FIELD}')
            self._data_check_rec['pid'] = 1
        else:
            self._data_check_rec['pid'] = 0
            
        if df_null_dob.count() > 0:
            print(f'[WARNING] NULLS IDENTIFIED IN COL {params.DOB_FIELD}')
            self._data_check_rec['dob'] = 1
        else:
            self._data_check_rec['dob'] = 0
            
        ## DONE