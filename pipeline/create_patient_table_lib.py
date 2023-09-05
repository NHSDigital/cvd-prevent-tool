# Databricks notebook source
## create_patient_table_lib

## Overview
# Class definitions for use in create_patient_table_lib

# COMMAND ----------

# MAGIC %run ../params/params

# COMMAND ----------

import pyspark.sql.functions as F

from typing import Dict, List
from pyspark.sql import DataFrame, Window

# COMMAND ----------

def format_base_patient_table_schema(
    df: DataFrame,
    mapping_cols: Dict[str, str] = params.PATIENT_TABLE_BASE_MAPPING,
    field_practice_identifier: str = params.CODE_ARRAY_FIELD,
    ) -> DataFrame:
    """format_base_patient_table_schema
    Converts columns to correct data type and maps columns in the base table to column names 
    specified in the mapping_cols dictionary {old_column: new_column}.

    Args:
        df (DataFrame): DataFrame containing columns to be mapped
        mapping_cols (Dict[str,str], optional): Dictionary of column mappings {old_column : new column}. 
                                                Defaults to params.PATIENT_TABLE_BASE_MAPPING.
        field_practice_identifier (str, optional): Column name for field containing array with practice identifier. 
                                                Defaults to params.CODE_ARRAY_FIELD.

    Returns:
        DataFrame: DataFrame with mapped columns
    """
    # Practice Identifier - Convert from array to string
    df = (
        df.withColumn(
            field_practice_identifier,
            F.concat_ws("", F.col(field_practice_identifier))
            )
    )
    # Column Mapping
    for original_column, mapped_column in mapping_cols.items():
        df = df.withColumnRenamed(original_column, mapped_column)
    # Return
    return df

# COMMAND ----------

def extract_patient_events(
    df: DataFrame,
    value_dataset: str,
    value_category: str,
    field_dataset: str = params.DATASET_FIELD, 
    field_category: str = params.CATEGORY_FIELD,
) -> DataFrame:
    """extract_patient_events
    Extract events from the event table, for a given dataset and category.

    Args:
        df (DataFrame): Events table
        value_dataset (str): Value(s) to filter the events table on, for datasets.
        value_category (str): Value(s) to filter the events table on, for dataset categories. 
        field_dataset (str, optional): Column name for field containing dataset values. Defaults to params.DATASET_FIELD.
        field_category (str, optional): Column name for field containing dataset category values. Defaults to params.CATEGORY_FIELD.

    Returns:
        DataFrame: Sub-set of events table, filtered by dataset and dataset category. 
    """    
    df = df.filter(
        (F.col(field_dataset) == value_dataset)
        & 
        (F.col(field_category) == value_category)
    )
    return df

# COMMAND ----------

def select_flags_data(flag_table: DataFrame) -> DataFrame:
    return flag_table.select(F.col(params.CVDP_PID_FIELD).alias(params.PID_FIELD), F.col(params.CVDP_DOB_FIELD).alias(params.DOB_FIELD))

# COMMAND ----------


def add_stroke_mi_information(
    patient_table: DataFrame,
    events_table: DataFrame,
    field_death_date: str = params.DATE_OF_DEATH,
    field_event_start: str = params.RECORD_STARTDATE_FIELD,
    field_flag: str = params.FLAG_FIELD,
    field_flag_assoc: str = params.ASSOC_FLAG_FIELD,
    field_flag_count: str = params.COUNT_FLAG,
    field_flag_mi_count: str = params.MI_COUNT,
    field_flag_stroke_count: str = params.STROKE_COUNT,
    field_max_mi_date: str = params.MAX_MI_DATE,
    field_max_stroke_date: str = params.MAX_STROKE_DATE,
    keep_inclusive: bool = True,
    keep_nulls: bool = True,
    key_join: List[str] = params.GLOBAL_JOIN_KEY,
    value_flag_mi: str = params.HEARTATTACK_FLAG,
    value_flag_stroke: str = params.STROKE_FLAG,
) -> DataFrame:
    """add_stroke_mi_information
    
    Adds counts and maximum dates (from events) for Stroke and MI events, per patients. 
    For patients that have died, any events after their date of death are removed.
    Note: These counts should only be generated from spell dataframes when in the CreatePatientTable stage.

    Args:
        patient_table (DataFrame): Intermediate dataframe from the patient table creation stage.
        events_table (DataFrame): Events table dataframe, filtered to only contain HES APC Spells.
        field_death_date (str, optional): Name of column containing date of death values.
            Defaults to params.DATE_OF_DEATH.
        field_event_start (str, optional): Name of column containing start date values for events table.
            Defaults to params.RECORD_START_DATE.
        field_flag (str, optional): Name of column containing flag values for events table.
            Defaults to params.FLAG_FIELD.
        field_flag_assoc (str, optional): Name of column containing associated flag values for events table.
            Defaults to params.ASSOC_FLAG_FIELD.
        field_flag_count (str, optional): Name of column containing count values, derived from events flag.
            Defaults to params.COUNT_FLAG.
        field_flag_mi_count (str, optional): Name of new column to contain number of MI events counts.
            Defaults to params.MI_COUNT.
        field_flag_stroke_count (str, optional): Name of new column to contain number of Stroke events counts.
            Defaults to params.STROKE_COUNT.
        field_max_mi_date (str, optional): Name of new column to contain max (latest) MI event date.
            Defaults to params.MAX_MI_DATE.
        field_max_stroke_date (str, optional): Name of new column to contain max (latest) Stroke event date.
            Defaults to params.MAX_STROKE_DATE.
        keep_inclusive (bool, optional): Switch, filter dates less than and including (True) or just less than (False) date of death.
            Defaults to True.
        keep_nulls (bool, optional): Switch, remove alive individuals (date of death != Null) when False.
            Defaults to True.
        key_join (str, optional): List of column(s) used to join dataframes and group patients by.
            Defaults to params.GLOBAL_JOIN_KEY.
        value_flag_mi (str, optional): Value used in the flag column (events table) to define a heartattack.
            Defaults to params.HEARTATTACK_FLAG.
        value_flag_stroke (str, optional): Value used in the flag column (events table) to define a stroke.
            Defaults to params.STROKE_FLAG.

    Returns:
        patient_table (DataFrame): Intermediate dataframe containing original patient table information, plus the counts
            and maximum dates (latest) for stroke and mi spell events. 
                        
    Associated:
        pipeline/create_patient_table::CreatePatientTableStage._add_hospitalisation_data()
    """

    # Remove any events post-death (keeping nulls and keeping inclusive dates)
    filtered_deaths = remove_elements_after_date(
        df_to_filter=events_table,
        df_dates=patient_table,
        date_to_compare=field_event_start,
        max_date=field_death_date,
        link=key_join,
        keep_nulls=keep_nulls,
        keep_inclusive=keep_inclusive
    )

    # Filter dataframes, for STROKE or MI, where the flag/associated flag fields equal/contain the relevant STROKE/MI flag
    stroke_events = (
      filtered_deaths
      .filter(
        (F.array_contains(F.col(field_flag_assoc), value_flag_stroke)) | 
        (F.col(field_flag) == value_flag_stroke)
      )
    )

    mi_events = (
      filtered_deaths
      .filter(
        (F.array_contains(F.col(field_flag_assoc), value_flag_mi)) |
        (F.col(field_flag) == value_flag_mi)
      )
    )

    # Count of stroke/mi per patient
    stroke_grouped = (
      stroke_events
      .groupBy(key_join)
      .count()
      .withColumnRenamed(field_flag_count, field_flag_stroke_count)
    )
    mi_grouped = (
      mi_events
      .groupBy(key_join)
      .count()
      .withColumnRenamed(field_flag_count, field_flag_mi_count)
    )

    # Most recent (max) stroke or MI event per patient
    stroke_dates = (
      stroke_events
      .groupBy(key_join)
      .agg(
        F.max(field_event_start).cast("date").alias(field_max_stroke_date)
      )
    )
    mi_dates = (
      mi_events
      .groupBy(key_join)
      .agg(
        F.max(field_event_start).cast("date").alias(field_max_mi_date)
      )
    )

    # Combine stroke and mi flags and dates
    stroke_flags = (
      stroke_grouped
      .join(
        stroke_dates, key_join, "inner"
      )
    )
    mi_flags = (
      mi_grouped
      .join(
        mi_dates, key_join, "inner"
      )
    )

    # Add Stroke and MI counts, dates - fill remaining counts with 0
    patient_table = patient_table.join(stroke_flags, key_join, "left")
    patient_table = patient_table.join(mi_flags, key_join, "left")
    patient_table = patient_table.fillna({
      field_flag_stroke_count: 0,
      field_flag_mi_count: 0,
    })

    # Return DataFrame
    return patient_table


# COMMAND ----------


def add_30_days_flag(df: DataFrame, events: DataFrame) -> DataFrame:

    """
    Adds a flag for if an invididual died within 30 days of a hospitalisation
    Take an array of all max hospitalisation entry dates to occur within a 30 day period
    Find flags for these events
    check if individual has died within 30 days of each hospitalisation entry
    """

    ### SELECT RELEVANT COLUMNS, RENAME RECORD START DATE FOR LATER JOIN
    events_selected = events.select(
        F.col(params.PID_FIELD),
        F.col(params.DOB_FIELD),
        F.col(params.RECORD_STARTDATE_FIELD).alias(params.MAX_HOSPITALISATION_ENTRY),
        F.col(params.FLAG_FIELD),
        F.col(params.CATEGORY_FIELD),
    )

    # REMOVES EVENTS AFTER DEATH DATE
    hes_after_death_events_removed = remove_elements_after_date(
        events_selected,
        df,
        params.MAX_HOSPITALISATION_ENTRY,
        params.DATE_OF_DEATH,
        [params.PID_FIELD, params.DOB_FIELD],
        True,
    )

    # GETS A NEW COLUMN CALLED MIN DATE THAT IS 30 DAYS BEFORE DEATH DATE
    hes_after_death_events_removed = hes_after_death_events_removed.withColumn(
        'temp_min_date',
        F.date_add(hes_after_death_events_removed[params.DATE_OF_DEATH], -30),
    )  # get minimum date to filter hospitalisations from

    # GETS ALL EPISODES THAT ARE AFTER MIN DATE
    hes_filtered = hes_after_death_events_removed.filter(
        (F.col(params.MAX_HOSPITALISATION_ENTRY) >= F.col('temp_min_date'))
    )  # gets any hospitalisation that was in the last 30 days of their life

    # AGGREGATES ALL OF THE HOSPITALISATION FLAGS INTO AN ARRAY
    flags = hes_filtered.groupBy(params.PID_FIELD, params.DOB_FIELD).agg(
        F.sort_array(
          F.collect_set(params.FLAG_FIELD)
        ).alias(params.DEATH_30_HOSPITALISATION)
    )

    # JOINS FLAGS ONTO MAIN TABLE
    df = df.join(flags, params.GLOBAL_JOIN_KEY, "left")

    return df


# COMMAND ----------


def create_patient_table_flags(df: DataFrame, events: DataFrame) -> DataFrame:
  
    '''create_patient_table_flags
        SUMMARY:
          Adds two flags to the dataframe, being whether a person has died under 75, and whether they have died within 30 days of a hospitalisation

        Args:
          df (DataFrame): created base table for the patient table
          events (DataFrame): events table containing the hes information

        Returns:
          all_flags (DataFrame): patient table including new flags columns
     '''

    # -------- ADDING FLAGS -------- #

    # ADD DIED UNDER 75 FLAG
    df_flag = add_under_age_flag(
        df = df,
        max_age = 75,
        start_date = params.DOB_FIELD,
        end_date = params.DATE_OF_DEATH,
        flag_col_name = params.DEATH_AGE_FLAG,
        flag_name = params.UNDER_75_FLAG,
        extra_column_check = params.DEATH_FLAG,
        keep_non_nulls = True,
    )
    
    # ADD FLAG FOR DEATH BEFORE 30 DAYS AFTER HOSPITALISATION
    all_flags = add_30_days_flag(df_flag, events)

    # -------- DROPS UNESSECARY COLUMNS -------- #

    all_flags = all_flags.drop(*params.PATIENT_TABLE_COLUMNS_TO_DROP)

    return all_flags

# COMMAND ----------

def assign_htn_risk_groups(
    patient_table: DataFrame,
    events_table: DataFrame,
    col_htn_risk_group: str = params.CVDP_HYP_RISK_FIELD,
    field_htn_diag_date: str = f'{params.HYPERTENSION_FLAG_PREFIX}_{params.DICT_FLAG_SUFFIX}',
    field_htn_events_flag: str = params.FLAG_FIELD,
    field_htn_event_date: str = params.RECORD_STARTDATE_FIELD,
    fields_join_cols: List[str] = [params.PID_FIELD,params.DOB_FIELD],
    field_patient_dob: str = params.DOB_FIELD,
    field_patient_extract_date: str = params.LATEST_EXTRACT_DATE,
    field_patient_pid: str = params.PID_FIELD,
) -> DataFrame:
    """assign_htn_risk_groups
    Extracts the latest hypertension risk group from the events table, for each patient. Joins to the patient
    table and yields the new column with values of hypertension risk group values. Blood pressure events are
    filtered to keep the latest 12 months of readings since a patient's latest extract date. Patients that 
    have no hypertension diagnosis date are assigned the flag '-1' (not hypertensive).

    Args:
        patient_table (DataFrame): Dataframe of patient table data.
        events_table (DataFrame): Dataframe of events table data filtered to blood pressure records only.
        col_htn_risk_group (str, optional): New column name for hypertension risk group values. 
            Defaults to params.CVDP_HYP_RISK_FIELD.
        field_htn_diag_date (str, optional): Patient table column containg the hypertension diagnosis date values.
            Defaults to f'{params.HYPERTENSION_FLAG_PREFIX}_{params.DICT_FLAG_SUFFIX}'.
        field_htn_events_flag (str, optional): Events table column containing calculated hypertension risk groups.
            Defaults to params.FLAG_FIELD.
        field_htn_event_date (str, optional): Events table column containing date to filter (keeping latest record). 
            Defaults to params.RECORD_STARTDATE_FIELD.
        fields_join_cols (List[str], optional): Events table columns use to join to the patient table, yielding the 
            hypertension risk group column. 
            Defaults to [params.PID_FIELD,params.DOB_FIELD].
        field_patient_dob (str, optional): Name of column containing date of birth (date type).
            Defaults to params.DOB_FIELD.
        field_patient_extract_date (str, optional): Patient table column containing the latest extract date from CVDP 
            for each individual.
            Defaults to params.LATEST_EXTRACT_DATE.
        field_patient_pid (str, optional): Name of column containing (NHS Number | Person ID).
            Defaults to params.PID_FIELD.

    Returns:
        DataFrame: Patient table updated with hypertension risk groups for each individual
    """
    
    # Patient table: select the person identifiable columns and the latest extract date columns
    tmp_patient_table = patient_table.select(field_patient_pid,field_patient_dob,field_patient_extract_date)
    
    # Join the Events and Patient tables (tmp) to obtain the latest extract date for each perso
    events_table = (
        events_table
        .join(
            tmp_patient_table,
            [field_patient_pid,field_patient_dob],
            'left'
        )
    )
    
    # Remove any hypertension records that have not occured within the last 12 months of a patients extract date
    events_table = (
        events_table
        .withColumn(
            'tmp_htn_months_between',
            F.months_between(
                F.col(field_patient_extract_date),
                F.col(field_htn_event_date)))
        .filter((F.col('tmp_htn_months_between') >= 0) & (F.col('tmp_htn_months_between') <= 12))
        .drop('tmp_htn_months_between',field_patient_extract_date)
    )
    
    # Keep the latest record per patient from this 12 month window
    events_table = filter_latest(
        df = events_table,
        window_fields = [field_patient_pid,field_patient_dob],
        date_field = field_htn_event_date
    )
    
    # Add the hypertension data to the patient table
    fields_events_select = fields_join_cols + [field_htn_events_flag]
    patient_table = (
        patient_table
        .join(
            events_table.select(fields_events_select),
            fields_join_cols,
            'left'
        )
        .withColumnRenamed(
            field_htn_events_flag,
            col_htn_risk_group
        )
    )
    
    # Add invalid risk group when hypertension risk diagnosis contains a date value but no blood pressure reading is found, -1 if no HTN flag
    patient_table = (
        patient_table
        .withColumn(
            col_htn_risk_group,
            F.when((F.col(field_htn_diag_date).isNotNull()) & (F.col(col_htn_risk_group).isNull()),'1e')
            .when((F.col(field_htn_diag_date).isNull()),'-1')
            .otherwise(F.col(col_htn_risk_group)))
    )
    
    # Return dataframe
    return patient_table