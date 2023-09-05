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

def get_first_or_last_smoking_category_entry(
  df: DataFrame,
  field_person_id: str = params.PID_FIELD,
  field_person_dob: str = params.DOB_FIELD,
  field_date_journal: str = params.JOURNAL_DATE_FIELD,
  field_smoking_status: str = params.FLAG_FIELD,
  first_switch: bool = True
) -> DataFrame:
  '''get_first_or_last_smoking_category_entry
  Removes all but the latest entry for each smoking status category (current,ex,never) for each person.
  
  Args:
    df (DataFrame): Smoking status preprocessed asset containing journal table entries related to smoking status and with status category flag assigned.
    field_person_id (str): Name of column containing NHS Number.
    field_person_dob (str): Name of column containing date of birth.
    field_date_journal (str): Name of column containing the recorded journal table entry date from CVDP.
    field_smoking_status (str) : Name of column containing smoking status category flag.
    first_switch (bool): Switch to control getting first or last category entry for each person
  
  Returns:
    DataFrame: DataFrame of journal table entries related to smoking status limited to one per category per person.
  '''
  
  # Create window on patient information and smoking status, ordered by journal date (latest first)
  if first_switch:
    windowval = Window.partitionBy(
        field_person_id, field_person_dob, field_smoking_status
    ).orderBy(F.col(field_date_journal).asc())
  else:
    windowval = Window.partitionBy(
        field_person_id, field_person_dob, field_smoking_status
    ).orderBy(F.col(field_date_journal).desc())

  # Window and rank entires: select latest version of smoking status category (if multiples present)
  df_reduced = (
      df.withColumn("rank", F.row_number().over(windowval))
      .filter(F.col("rank") == 1)
      .drop("rank")
  )
    
  return df_reduced

def get_start_date(
  df: DataFrame,
  field_person_id: str = params.PID_FIELD,
  field_person_dob: str = params.DOB_FIELD,
  field_date_journal: str = params.JOURNAL_DATE_FIELD,
  field_smoking_status: str = params.FLAG_FIELD,
  field_start_date: str = params.RECORD_STARTDATE_FIELD
) -> DataFrame:
  '''get_start_date
  Returns date of first entry for each smoking status category for each person.
  
  Args:
    df (DataFrame): Smoking status preprocessed asset containing journal table entries related to smoking status and with status category flag assigned.
    field_person_id (str): Name of column containing NHS Number.
    field_person_dob (str): Name of column containing date of birth.
    field_date_journal (str): Name of column containing the recorded journal table entry date from CVDP.
    field_smoking_status (str) : Name of column containing smoking status category flag.
    
  Returns:
    DataFrame: DataFrame containing nhs number, date of birth, category flag and start date of smoking status category for each person
  '''
  
  df_start_dates = get_first_or_last_smoking_category_entry(
      df = df,
      field_person_id = field_person_id,
      field_person_dob = field_person_dob,
      field_date_journal = field_date_journal,
      field_smoking_status = field_smoking_status,
      first_switch = True
  )
  
  df_start_dates_renamed = df_start_dates.withColumnRenamed(field_date_journal, field_start_date)
  
  return df_start_dates_renamed.select([field_person_id,field_person_dob, field_start_date, field_smoking_status])

def add_start_dates(
  df_latest_record: DataFrame,
  df_start_dates: DataFrame,
  field_person_id: str = params.PID_FIELD,
  field_person_dob: str = params.DOB_FIELD,
  field_smoking_status: str = params.FLAG_FIELD
) -> DataFrame:
  '''add_start_dates
  Adds start date for smoking status onto smoking status event for each category for each person
  
  Args:
    df_latest_record (DataFrame): DataFrame containing latest record of each smoking status category for each person.
    df_start_dates (DataFrame): DataFrame containing start date, nhs number, date of birth and category flag for each status category for each person
    field_person_id (str): Name of column containing NHS Number.
    field_person_dob (str): Name of column containing date of birth.
    field_smoking_status (str) : Name of column containing smoking status category flag.
    
  Returns:
    DataFrame: DataFrame containing latest record of each smoking status of each person combined with the date that status started.
  '''
  
  join_key_full = [field_person_id, field_person_dob, field_smoking_status]
  
  df_smoking_status = df_latest_record.join(df_start_dates, on=join_key_full, how='left')
  
  return df_smoking_status

# COMMAND ----------

def check_number_of_events(
  df: DataFrame,
  field_person_id: str = params.PID_FIELD,
  field_person_dob: str = params.DOB_FIELD,
  field_date_journal: str = params.JOURNAL_DATE_FIELD,
) -> DataFrame:
  '''
  A check to prevent no-more than three smoking status events per person are added to the events table. Assertion error if more than three per person.
  
  Args:
    df (DataFrame): Processed smoking events to be added to the events table.
    field_person_id (str): Name of column containing NHS number.
    field_person_dob (str): Name of column containing date of birth.
    field_date_journal (str): Name of column containing journal date.
  '''
  windowval = Window.partitionBy(field_person_id, field_person_dob).orderBy(F.col(field_date_journal).desc())
  
  assert (df
          .withColumn("rank", F.row_number().over(windowval))
          .filter(F.col("rank") > 3).count()) == 0 

# COMMAND ----------

def process_cvdp_smoking_status(
  df: DataFrame,
  field_person_id: str = params.PID_FIELD,
  field_person_dob: str = params.DOB_FIELD,
  field_date_journal: str = params.JOURNAL_DATE_FIELD,
  field_smoking_status: str = params.FLAG_FIELD,
  field_start_date: str = params.RECORD_STARTDATE_FIELD
) -> DataFrame:
  '''process_cvdp_smoking_status
  Takes the preprocessed smoking table asset and removes all but latest entry for each status category per person adding the date of first record in category as an extra field.
  
  Args:
    df (DataFrame): Smoking status preprocessed asset containing journal table entries related to smoking status and with status category flag assigned.
    field_person_id (str): Name of column containing NHS Number.
    field_person_dob (str): Name of column containing date of birth.
    field_date_journal (str): Name of column containing the recorded journal table entry date from CVDP.
    field_smoking_status (str) : Name of column containing smoking status category flag.
    first_switch (bool): Switch to control getting first or last category entry for each person
  
  Returns:
    Dataframe: Processed smoking status events containing journal entry for most recent category entry for each person and the date of the oldest of each category entry for each person.
  '''
  
  df_latest_entry = get_first_or_last_smoking_category_entry(
      df = df,
      field_person_id = field_person_id,
      field_person_dob = field_person_dob,
      field_date_journal = field_date_journal,
      field_smoking_status = field_smoking_status,
      first_switch = False
  )
  
  df_start_date = get_start_date(
    df = df,
    field_person_id = field_person_id,
    field_person_dob = field_person_dob,
    field_date_journal = field_date_journal,
    field_smoking_status = field_smoking_status,
    field_start_date = field_start_date
  )
  
  df_start_and_end = add_start_dates(
    df_latest_record = df_latest_entry,
    df_start_dates = df_start_date,
    field_person_id = field_person_id,
    field_person_dob = field_person_dob,
    field_smoking_status = field_smoking_status
  )
  
  check_number_of_events(
    df = df_start_and_end,
    field_person_id = field_person_id,
    field_person_dob = field_person_dob,
    field_date_journal = field_date_journal
  )
       
  return df_start_and_end

# COMMAND ----------

