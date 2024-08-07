# Databricks notebook source
## process_hes_events

## Overview
## Collections of functions that are specific for processing HES events in the events table formation

# COMMAND ----------

## LIBRARY IMPORTS
import itertools
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import DataFrame, Window
from typing import List, Optional, Dict, Union
from functools import reduce

# COMMAND ----------

# MAGIC %run ../../params/params

# COMMAND ----------

# MAGIC %run ../util

# COMMAND ----------

def deduplicate_hes_spells(df: DataFrame,
                           dedup_cols: Union[List[str], str] = [params.HES_SPELL_ID_FIELD,
                                                                params.PID_FIELD,
                                                                params.DOB_FIELD]) -> DataFrame:

    ## DEDUPLICATE ON SELECTED COLUMN(S)
    df = df.dropDuplicates(dedup_cols)

    return df

# COMMAND ----------

def process_multiple_spell_indicators(df: DataFrame,
                                       col_codes: str = params.CODE_FIELD,
                                       col_flags: str = params.FLAG_FIELD,
                                       col_assoc_codes: str = params.CODE_ARRAY_FIELD,
                                       col_assoc_flags: str = params.ASSOC_FLAG_FIELD,
                                       value_multi: str = params.EVENTS_HES_MULTIPLE_FLAGS) -> DataFrame:

    ## IDENTIFY INSTANCES OF MULTIPLE CODES AND FLAGS
    df = df.withColumn('_tmp_code_num', F.size(F.col(col_codes)))\
           .withColumn('_tmp_flag_num', F.size(F.col(col_flags)))

    ## CONDITIONAL: IF LEN CODE || FLAG > 1, REPLACE WITH MULTIPLE AND ADD TO ASSOC FIELDS
    df = df.withColumn(col_assoc_codes, F.when(F.col('_tmp_code_num') > 1, F.col(col_codes)).otherwise(None))\
           .withColumn(col_assoc_flags, F.when(F.col('_tmp_flag_num') > 1, F.col(col_flags)).otherwise(None))

    df = df.withColumn(col_codes, F.array_remove(col_codes, 'NO_CVD'))
    df = df.withColumn(col_flags, F.array_remove(col_flags, 'NO_CVD'))

    df = df.withColumn('_tmp_code_num', F.size(F.col(col_codes)))\
           .withColumn('_tmp_flag_num', F.size(F.col(col_flags)))

    ## > MULTIPLE CODES
    df = df.withColumn(col_codes, F.when(F.col('_tmp_code_num') > 1, value_multi)\
                                   .when(F.col('_tmp_code_num') == 1, F.concat_ws('', F.col(col_codes)))
                                   .otherwise('NO_CVD'))

    ## > MULTIPLE FLAGS
    df = df.withColumn(col_flags, F.when(F.col('_tmp_flag_num') > 1, value_multi)\
                                   .when(F.col('_tmp_flag_num') == 1, F.concat_ws('', F.col(col_flags)))
                                   .otherwise('NO_CVD'))

    ## CLEANUP
    df = df.drop('_tmp_code_num','_tmp_flag_num')
    df = replace_value_none(df, col_names = [col_codes,col_flags], filter_values = [''])

    return df

# COMMAND ----------

def keep_latest_lsoa(df: DataFrame,
                     partition_fields: Union[List[str],str] = params.HES_SPELL_ID_FIELD,
                     col_lsoa: str = params.LSOA_FIELD,
                     date_col: str = params.RECORD_STARTDATE_FIELD) -> DataFrame:
    '''
    keep_latest_lsoa 
    keeps LSOA from latest episode in a spell as the spell LSOA. 
    For instances where there are two episodes on the same day the first LSOA code alphabetically is selected

    Args:
        df (DataFrame): HES apc table
        partition_fields (Union[List[str], str], optional): column(s) to partition over
          Defaults to params.HES_SPELL_ID_FIELD.
        col_lsoa (str, optional): column containing LSOA.
          Defaults to params.LSOA_FIELD.
        date_col (str, optional): column containing episode start date.
          Defaults to params.RECORD_STARTDATE_FIELD.

    Returns:
        DataFrame: input DataFrame with one LSOA consistent over all episodes
    '''
    
    if type(partition_fields) == str:
      partition_fields = [partition_fields]
    ## WINDOW OVER SPELL ID BY RECORD DATE
    lsoa_window = Window().partitionBy(*partition_fields).orderBy(F.col(date_col).desc(), F.col(col_lsoa).asc())
    df = df.repartition(*partition_fields)
    df = df.withColumn(col_lsoa, F.first(col_lsoa, True).over(lsoa_window))
    
    return df

# COMMAND ----------

def keep_modal_value(df: DataFrame,
                   partition_fields: Union[List[str],str] = params.HES_SPELL_ID_FIELD,
                   col_to_check: str = params.DOB_FIELD,
                   date_col: str = params.RECORD_STARTDATE_FIELD,
                   filter_list: List[str] = params.HES_APC_OTR_DATE_FILTERS) -> DataFrame:
    '''
    keep_modal_value
    Keeps the most common valid value in a specified column as that value over the whole spell. 
    In instances where there are multiple values with the same amount of usage the first used value chronologically of those is used.
    In instances where there is still multiple options the first value when sorting in descending order is selected arbitrarily.

    Args:
        df (DataFrame): HES apc table
        partition_columns (str, optional): column(s) to partition on
          Defaults to params.HES_SPELL_ID_FIELD.
        col_to_check (str, optional): column to check modal value of.
          Defaults to params.DOB_FIELD.
        date_col (str, optional): column containing episode start date.
          Defaults to params.RECORD_STARTDATE_FIELD.
        filter_list (List[str], optional): list of values to be considered invalid
          Defaults to params.HES_APC_OTR_DATE_FILTERS

    Returns:
        DataFrame: input DataFrame with the most common value in the specified field over all episodes
    '''

    if type(partition_fields) == str:
      partition_fields = [partition_fields]
    # Replace invalid values with nulls
    df = replace_value_none(df, col_names = col_to_check, filter_values = filter_list)

    # Add counts of each spell id/specified value combo to every row
    count_window  = Window().partitionBy(*partition_fields, col_to_check)
    df = df.repartition(*partition_fields)
    df = df.withColumn('_tmp_count', F.count(F.col(col_to_check)).over(count_window))

    # Select most common specified value for each spell
    selection_window = Window().partitionBy(*partition_fields).orderBy(F.col('_tmp_count').desc(), F.col(date_col).asc(), F.col(col_to_check).desc())
    df = df.withColumn(col_to_check, F.first(col_to_check, ignorenulls=True).over(selection_window))

    df = df.drop('_tmp_count')

    return df

# COMMAND ----------

def collect_spell_sets(df: DataFrame,
                       hes_spell_id_col: str = params.HES_SPELL_ID_FIELD,
                       col_episode_id: str = params.RECORD_ID_FIELD,
                       col_codes: str = params.CODE_FIELD,
                       col_flags: str = params.FLAG_FIELD,
                       col_code_array: str = params.CODE_ARRAY_FIELD,
                       col_dob: str = params.DOB_FIELD,
                       col_date: str = params.RECORD_STARTDATE_FIELD,
                       col_pid: str = params.PID_FIELD,
                       col_ethnicity: str = params.ETHNICITY_FIELD,
                       nhs_no_filter_list: List[str] = params.INVALID_NHS_NUMBER_FULL,
                       ethnicity_filter_list: List[str] = params.ETHNICITY_UNKNOWN_CODES,
                       hes_date_filter_list: List[str] = params.HES_APC_OTR_DATE_FILTERS) -> DataFrame:
    
    ## ENSURE CORRECT TYPES
    df = df.withColumn(hes_spell_id_col, F.col(hes_spell_id_col).cast(T.StringType()))\
           .withColumn(col_episode_id, F.col(col_episode_id).cast(T.StringType()))

    ## COLLECT SETS OF VALUES OVER COLUMNS
    set_w = Window().partitionBy(hes_spell_id_col)
    df = df.repartition(hes_spell_id_col)
    df = df.withColumn(col_episode_id, F.collect_set(col_episode_id).over(set_w))\
           .withColumn(col_codes, F.collect_set(col_codes).over(set_w))\
           .withColumn(col_flags, F.collect_set(col_flags).over(set_w))\
           .withColumn(col_code_array, F.collect_set(col_code_array).over(set_w))
           
    ## FLATTEN ARRAYS AND RETAIN UNIQUE ENTRIES
    df = df.withColumn(col_episode_id, F.array_sort(F.array_distinct((col_episode_id))))\
           .withColumn(col_codes, F.array_sort(F.array_distinct((col_codes))))\
           .withColumn(col_flags, F.array_sort(F.array_distinct((col_flags))))\
           .withColumn(col_code_array, F.array_sort(F.array_distinct(F.flatten(col_code_array))))
    
    ## KEEP LATEST LSOA
    df = keep_latest_lsoa(df)
    
    ## Keep most common valid date of birth
    df = keep_modal_value(df, hes_spell_id_col, col_dob, col_date, hes_date_filter_list)
    
    ## Keep most common valid nhs number
    df = keep_modal_value(df, hes_spell_id_col, col_pid, col_date, nhs_no_filter_list)
    
    ## Keep most common valid ethnicity
    df = keep_modal_value(df, hes_spell_id_col, col_ethnicity, col_date, ethnicity_filter_list)
    
    ## REMOVE DUPLICATES
    df = deduplicate_hes_spells(df, [hes_spell_id_col])
           
    return df

# COMMAND ----------

## SPLIT HES EVENTS: EPISODES VS HOSPITALISATIONS
def split_hes_hospitalisations(df: DataFrame,
                               hes_spell_cols: List[str] = params.HES_SPLIT_SPELL_COLS,
                               hes_spell_start_col: str = params.HES_SPELL_START_FIELD,
                               hes_spell_end_col: str = params.HES_SPELL_END_FIELD,
                               hes_spell_id_col: str = params.HES_SPELL_ID_FIELD,
                               record_id_col: str = params.RECORD_ID_FIELD,
                               assoc_id_col: str = params.ASSOC_REC_ID_FIELD,
                               record_start_date_col: str = params.RECORD_STARTDATE_FIELD) -> DataFrame:
    
    ## SELECT RELEVANT FIELDS - HOSPITALISATIONS FRAMES
    df_spells   = df.select(hes_spell_cols)
    
    ## FILTER SPELLS: KEEP ONLY NON-NULL SPELL IDS
    df_spells_nonnull = df_spells.filter(F.col(hes_spell_id_col).isNotNull())
    
    ## FILTER SPELLS: REMOVE SPELLS WITH NULL SPELL START OR END DATE FIELDS
    df_spells_nonnull = df_spells_nonnull.filter(
      (F.col(hes_spell_start_col).isNotNull()) 
      & (F.col(hes_spell_end_col).isNotNull())
    )
    
    ## COLLECT SETS FOR SPELL COLUMNS
    df_spells_nonnull = collect_spell_sets(df_spells_nonnull)
    df_spells_nonnull = process_multiple_spell_indicators(df_spells_nonnull)
    
    ## RENAME RECORD ID COLUMN
    df_spells_nonnull = df_spells_nonnull.withColumnRenamed(record_id_col,assoc_id_col)
    
    ## REMOVE NO LONGER NECESSARY FIELDS
    df_spells_nonnull = df_spells_nonnull.drop(record_start_date_col)
    
    ## RETURN
    return df_spells_nonnull

def split_hes_episodes(df: DataFrame,
                       hes_episode_cols: List[str] = params.HES_SPLIT_EPISODES_COLS) -> DataFrame:
    
    df_episodes = df.select(hes_episode_cols)
    
    df_episodes = df_episodes.withColumn(params.ASSOC_FLAG_FIELD, F.split(F.col(params.HES_APC_ADMIMETH_FIELD), ',')).drop(params.HES_APC_ADMIMETH_FIELD)
    
    return df_episodes


# COMMAND ----------

## PROCESS HES: INDIVIDUAL DATAFRAME RETURNS

def process_hes_events_episodes(df: DataFrame) -> DataFrame:
    return split_hes_episodes(df)

def process_hes_events_spells(df: DataFrame) -> DataFrame:
    return split_hes_hospitalisations(df)
