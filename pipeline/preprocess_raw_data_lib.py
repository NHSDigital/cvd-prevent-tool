# Databricks notebook source
## preprocess_raw_data_lib

## Overview
# Class definitions for use in preprocess_raw_data_lib

# COMMAND ----------

import dataclasses
from dataclasses import dataclass
from typing      import Any, Callable, Dict, List, Optional
from pyspark.sql import DataFrame
from collections import namedtuple

import pyspark.sql.functions as F

# COMMAND ----------

### CLASS DEFINITIONS ###

@dataclass(frozen=True)
class PreprocessStageDataEntry():
  '''PreprocessStageDataEntry
  Dataclass that contains all necessary information for loading, preprocessing, and cleaning a single source
  (raw) dataset such that it can be saved to the local environment for use in later stages. 
  
  Inputs
    dataset_name: (Arbitary) name given to each dataset in the pipeline. It must be unique. 
    db:           The database where the source data is
    table:        The table name of the source dataset

  [Optional]
    filter_eligible_patients: str (default = None) A list of fields that should be filtered on using
                              the eligible patient (cohort) table created in CreatePatientCohortTableStage.
    preprocessing_func:       Callable (default = None) A callable with signature Func(DataFrame) -> DataFrame
                              This is the preprocessing function used to apply dataset specific processing 
                              to the source dataset.
    clean_nhs_number_fields:  List[str] (default=None) A list of fields that are NHS numbers and should 
                              be cleaned. If 'validate_nhs_numbers' is False then no cleaning is applied 
                              to these fields. See src/clean_dataset. If None then no cleaning is applied 
                              to any fields.
    clean_null_fields:        List[str] (default = None) A list of which should have various types of null 
                              values removed. See src/clean_dataset. If None then no cleaning is applied 
                              to any fields.
    replace_empty_str_fields: List[str] (default=None) list of field names in which to replace empty 
                              strings ('') with null values. See src/clean_dataset. If None then no cleaning 
                              is applied to any fields. 

  '''
  dataset_name: str
  db: str
  table: str
  
  filter_eligible_patients: namedtuple = None
  preprocessing_func: Callable = None
  validate_nhs_numbers: Any = True
  rename_field_map: Dict[str, str] = None
  clean_nhs_number_fields: List[str] = None
  clean_null_fields: List[str] = None
  replace_empty_str_fields: List[str] = None

# COMMAND ----------

### GLOBAL OBJECT DEFINITIONS
filter_fields = namedtuple('filter_fields','pid_field dob_field')

# COMMAND ----------

### FUNCTION DEFINITIONS ###
def filter_eligible_patients(patient_list: DataFrame, df_filter: DataFrame, filter_fields: namedtuple) -> DataFrame:
  '''filter_eligible_patients
  
  Filters the dataframe (df_filter) using a supplied patient list (patient_list) that contains the person
  identifiers (e.g. NHS Numbers) to filter the dataframe (filter_col) on.
  
  Inputs
    patient_list (DataFrame):   Dataframe of unique person identifiers (e.g. NHS Numbers) with DOBs to filter on
    df_filter (DataFrame):      DataFrame to filter
    filter_fields (namedtuple): Tuple object containing the [nhs number, dob fields] for df_filter
    filter_col_pid (str):       Column name in df_filter for nhs number column
    filter_col_dob (str):       Column name in df_filter for date of birth column
  '''

  filter_col_pid = filter_fields.pid_field
  filter_col_dob = filter_fields.dob_field
  
  ### FILTERING STEPS ###
  ## COLUMN PARITY
  df_filter = df_filter.withColumnRenamed(filter_col_pid, params.PID_FIELD)\
                       .withColumnRenamed(filter_col_dob, params.DOB_FIELD)
  ## JOIN
  df_filter = df_filter.join(patient_list, params.GLOBAL_JOIN_KEY, "inner")
  ## COLUMN RETURN
  df_filter = df_filter.withColumnRenamed(params.PID_FIELD, filter_col_pid)\
                       .withColumnRenamed(params.DOB_FIELD, filter_col_dob)

  ### RETURN
  return df_filter


def add_dataset_field(df: DataFrame, dataset_name: str) -> DataFrame:
  """add_dataset_field

  Inputs
      df (DataFrame): Dataframe object
      dataset_name (str): Str from PreprocessStageDataEntry()

  Returns
      DataFrame: Dataframe with dataset field (params) with dataset name (last column)
  """  
  return df.withColumn(params.DATASET_FIELD, F.lit(dataset_name))



def transform_to_asset_format(df: DataFrame, rename_field_map: Optional[Dict[str, str]] = None) -> DataFrame:
  """transform_to_asset_format

  Inputs
      df (DataFrame): Dataframe object

  Returns
      DataFrame: Dataframe with field renamed
  """  
  if rename_field_map != None:
    for old_field_name, new_field_name in rename_field_map.items():
      df = df.withColumnRenamed(old_field_name, new_field_name)
  return df