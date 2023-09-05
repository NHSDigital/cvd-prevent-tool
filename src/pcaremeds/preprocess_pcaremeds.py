# Databricks notebook source
# MAGIC %run ../util

# COMMAND ----------

# MAGIC %run ../../params/params

# COMMAND ----------

import pyspark.sql.functions as F
from datetime import date, datetime

from pyspark.sql import DataFrame

# COMMAND ----------

def remove_duplicate_loads(df: DataFrame, effective_to_field: str = params.PCAREMEDS_EFFECTIVE_TO) -> DataFrame:
  """remove_duplicate_loads
  removes out of date records
  
  Args:
      df (DataFrame): Dataframe of raw data
      effective_to_field (str): Column name of pcaremeds detailing when record is effective to 
  
  Returns: 
      df (Dataframe): Processed dataframe containing only current data load records
  """
  df = df.filter(
       (F.col(effective_to_field).isNull())
  ).drop(effective_to_field)
  
  return df


def remove_non_england_pharmacies(df: DataFrame, country_code: str = params.PCAREMEDS_COUNRTY_CODE) -> DataFrame:
  """remove_non_england_pharmacies
  removes non England prescription records
  
  Args:
      df (DataFrame): Dataframe of raw data
      country_code (str): name of pcaremeds detailing Country of dispensing
  
  Returns: 
      df (Dataframe): Processed dataframe containing only current prescriptions from England
  """
  df = df.filter(
        (F.col(country_code) == 1)
  ).drop(country_code)
  
  return df


def remove_private_prescriptions(df: DataFrame, private_indicator: str = params.PCAREMEDS_PRIVATE_INDICATOR) -> DataFrame:
  """remove_private_prescriptions
  removes private prescription records
  
  Args:
      df (DataFrame): Dataframe of raw data
      private_indicator (str): Column name of pcaremeds detailing if dispensed privately
  
  Returns: 
      df (Dataframe): Processed dataframe containing only non private prescriptions
  """
  df = df.filter(
        (F.col(private_indicator) == 0)
  ).drop(private_indicator)
  
  return df


def clean_pcaremeds_nhs_number(df: DataFrame, nhs_number: str = params.PCAREMEDS_PID_FIELD) -> DataFrame:
  """clean_pcaremeds_nhs_number
  removes records with null nhs number
  
  Args:
      df (DataFrame): Dataframe of raw data
      nhs_number (str): Column name of pcaremeds detailing the patient nhs number
  
  Returns: 
      df (Dataframe): Processed dataframe containing only non null nhs numbers
  """
  df = df.filter(
        (F.col(nhs_number).isNotNull())
  )
  
  return df

def create_record_id_field(df: DataFrame, prescription_id: str = params.PCAREMEDS_PRESCRIPTION_ID, item_id: str = params.PCAREMEDS_ITEM_ID, id_field: str = params.PCAREMEDS_ID_FIELD) -> DataFrame:
  """create_record_id_field
  concatenates prescription_id and item_id to create unique id for each record
  
  Args:
      df (DataFrame): DataFrame of raw data
      prescripton_id (str): Column name of pcaremeds detailing the id for the prescription
      item_id (str): Column name of pcaremeds detailing the id for an item in a prescription
      id_field (str): Column name for constructed pcaremeds record id
      
  Returns:
        df (DataFrame): Processed dataframe containing new field for a unique record id
  """
  
  df = df.withColumn(id_field, F.concat(prescription_id, item_id))
  
  return df

# COMMAND ----------

def clean_pcaremeds(
    df: DataFrame,
    effective_to_field: datetime = params.PCAREMEDS_EFFECTIVE_TO,
    country_code: str = params.PCAREMEDS_COUNRTY_CODE,
    private_indicator: str = params.PCAREMEDS_PRIVATE_INDICATOR,
    nhs_number: str = params.PCAREMEDS_PID_FIELD,
    prescription_id: str = params.PCAREMEDS_PRESCRIPTION_ID,
    item_id: str = params.PCAREMEDS_ITEM_ID,
    id_field: str = params.PCAREMEDS_ID_FIELD
) -> DataFrame:
  """clean_pcaremeds
  
  Args:
  df (DataFrame): Dataframe of raw data
  effective_to_field (str): Column name of pcaremeds detailing when record is effective to
  country_code (str): name of pcaremeds detailing Country of dispensing
  private_indicator (str): Column name of pcaremeds detailing if dispensed privately
  nhs_number (str): Column name of pcaremeds detailing the patient nhs number
  prescripton_id (str): Column name of pcaremeds detailing the id for the prescription
  item_id (str): Column name of pcaremeds detailing the id for an item in a prescription
  id_field (str): Column name for constructed pcaremeds record id
  
  Returns:
  df_pcaremeds_cleaned_with_id (DataFrame): Cleaned pcaremeds dataframe with new id field
  """
  
  df_pcaremeds = remove_duplicate_loads(df, effective_to_field)
  
  df_pcaremeds_eng = remove_non_england_pharmacies(df_pcaremeds, country_code)
  
  df_pcaremeds_eng_public = remove_private_prescriptions(df_pcaremeds_eng, private_indicator)
  
  df_pcaremeds_cleaned = clean_pcaremeds_nhs_number(df_pcaremeds_eng_public, nhs_number)
  
  df_pcaremeds_cleaned_with_id = create_record_id_field(df_pcaremeds_cleaned, prescription_id, item_id, id_field)
  
  return df_pcaremeds_cleaned_with_id

# COMMAND ----------

def preprocess_pcaremeds(
    df: DataFrame,
    preprocess_columns: List[str] = params.PCAREMEDS_PREPROCESS_COLUMNS
) -> DataFrame:
  """preprocess_pcaremeds
  Main function for preprocessing the pcaremeds dataframe to feed into the PreprocessRawDataStage.
  
  Args:
  df (DataFrame): Dataframe of raw data
  preprocess_columns (List[str]): List of columns to keep in final pcaremeds preprocessed asset
  
  Returns:
  DataFrame: Cleaned pcaremeds dataframe, for use in PreprocessRawDataStage
  """
  
  df_cleaned = clean_pcaremeds(df)
  
  return df_cleaned.select(preprocess_columns)