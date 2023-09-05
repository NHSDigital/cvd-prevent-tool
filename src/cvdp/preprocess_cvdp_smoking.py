# Databricks notebook source
# MAGIC %run ../../params/params

# COMMAND ----------

# MAGIC %run ../util

# COMMAND ----------

## MODULE IMPORTS
import pyspark.sql.functions as F

from pyspark.sql import DataFrame, Window
from datetime import date
from typing import List

# COMMAND ----------

def extract_smoking_status(
  df: DataFrame,
  codes_current: List[str] = params.CURRENT_SMOKER_SNOMED_CODES,
  codes_ex: List[str] = params.EX_SMOKER_SNOMED_CODES,
  codes_never: List[str] = params.NEVER_SMOKED_SNOMED_CODES,
  field_snomed_code: str = params.CODE_FIELD,
  current_flag: str = params.CURRENT_SMOKER_FLAG,
  ex_flag: str = params.EX_SMOKER_FLAG,
  never_flag: str = params.NEVER_SMOKED_FLAG,
  field_smoking_status: str = params.FLAG_FIELD
) -> DataFrame:
  '''extract_smoking_status
  Extracts all journal table entries related to smoking status and assigns smoking status category.
  
  Args:
    df (DataFrame): Eligible cohort journal table (output of CreatePatientCohortTableStage).
    codes_current (List[str]): List of SNOMED codes associated with current smoker status.
    codes_ex (List[str]): List of SNOMED codes associated with ex-smoker status.
    codes_never (list[str]): List of SNOMED codes associated with never smoked status.
    field_snomed_code (str): Name of column containing the SNOMED codes.
    current_flag (str): Flag for current smoker status.
    ex_flag (str): Flag for ex-smoker status.
    never_flag (str): Flag for never smoked status.
    field_smoking_status (str) : Name of column to contain smoking status category flag.
    
  Returns:
    DataFrame: DataFrame of all journal table entries related to smoking status with status category assigned.
  '''
  
  codes_smoking_all = codes_current + codes_ex + codes_never
  
  #Extract all smoking status records
  df = df.filter(F.col(field_snomed_code).isin(codes_smoking_all))
  
  #Assign status flag
  df = (
      df.withColumn(
            field_smoking_status,
            F.when(F.col(field_snomed_code).isin(codes_current), current_flag)
            .when(F.col(field_snomed_code).isin(codes_ex), ex_flag)
            .when(F.col(field_snomed_code).isin(codes_never), never_flag)
      )
  )
  
  return df
  

# COMMAND ----------

def extract_smoking_intervention(
  df: DataFrame,
  field_cluster: str = params.REF_CLUSTER_FIELD,
  intervention_codes: List[str] = params.SMOKING_INTERVENTION_CLUSTER
) -> DataFrame:
  '''extract_smoking_intervention
  
  Extract all journal table entries with smoking intervention cluster_ID
  
  Args:
    df (DataFrame): Eligible cohort journal table (output of CreatePatientCohortTableStage).
    field_cluster (str): Name of column containing SNOMED cluster id.
    intervention_codes (List[str]): List of cluster_id codes pertaining to smoking intervention
    
  Returns:
    DataFrame: DataFrame of journal table entries related to smoking intervention
  '''
  
  df = df.filter(F.col(field_cluster).isin(intervention_codes))
  
  return df

# COMMAND ----------

def preprocess_smoking_status(
  df: DataFrame,
  codes_current: List[str] = params.CURRENT_SMOKER_SNOMED_CODES,
  codes_ex: List[str] = params.EX_SMOKER_SNOMED_CODES,
  codes_never: List[str] = params.NEVER_SMOKED_SNOMED_CODES,
  field_snomed_code: str = params.CODE_FIELD,
  current_flag: str = params.CURRENT_SMOKER_FLAG,
  ex_flag: str = params.EX_SMOKER_FLAG,
  never_flag: str = params.NEVER_SMOKED_FLAG,
  field_smoking_status: str = params.FLAG_FIELD,
  field_age_at_event: str = params.AGE_FIELD,
  field_journal_date: str = params.JOURNAL_DATE_FIELD,
  fields_final_output: List[str] = params.CVDP_SMOKING_STATUS_OUTPUT_COLUMNS,
  field_person_dob: str = params.DOB_FIELD
) -> DataFrame :
  '''preprocess_smoking_status
  Preprocesses smoking status from journal table. 
  Extracting all journal table entries related to smoking status and assigning them a status category flag.
  
  Args:
    df (DataFrame): Eligible cohort journal table (output of CreatePatientCohortTableStage).
    codes_current (List[str]): List of SNOMED codes associated with current smoker status.
    codes_ex (List[str]): List of SNOMED codes associated with ex-smoker status.
    codes_never (list[str]): List of SNOMED codes associated with never smoked status.
    field_snomed_code (str): Name of column containing the SNOMED codes.
    current_flag (str): Flag for current smoker status.
    ex_flag (str): Flag for ex-smoker status.
    never_flag (str): Flag for never smoked status.
    field_smoking_status (str): Name of column to contain smoking status category flag.
    field_age_at_event (str): New column name for patient's calculated age (using DOB, at date of event).
    field_journal_date (str): Name of column containing date used to calculate patient's age at time of smoking status update.
    field_person_dob (str): Name of column containing date of birth.
    fields_final_output (List[str]): List of columns to keep in final smoking table output.
    
    
  Returns:
    DataFrame: DataFrame of journal table entries related to smoking status limited to one per category per person, flagged with smoking status and calculated age added.
  '''
  
  df_smoking = extract_smoking_status(
      df = df,
      codes_current = codes_current,
      codes_ex = codes_ex,
      codes_never = codes_never,
      field_snomed_code = field_snomed_code,
      current_flag = current_flag,
      ex_flag = ex_flag,
      never_flag = never_flag,
      field_smoking_status = field_smoking_status
  )
  
  df_smoking_age = df_smoking.withColumn(
       field_age_at_event, age_at_date(field_person_dob, field_journal_date)
  )
  
  return df_smoking_age.select(fields_final_output)