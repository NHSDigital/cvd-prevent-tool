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
                     hes_spell_id_col: str = params.HES_SPELL_ID_FIELD,
                     col_lsoa: str = params.LSOA_FIELD,
                     date_col: str = params.HES_SPELL_START_FIELD) -> DataFrame:
    
    ## WINDOW OVER SPELL ID BY RECORD DATE
    lsoa_window = Window().partitionBy(hes_spell_id_col).orderBy(F.col(date_col).desc())
    df = df.repartition(hes_spell_id_col)
    df = df.withColumn(col_lsoa, F.first(col_lsoa, True).over(lsoa_window))
    
    return df

# COMMAND ----------

def collect_spell_sets(df: DataFrame,
                       hes_spell_id_col: str = params.HES_SPELL_ID_FIELD,
                       col_episode_id: str = params.RECORD_ID_FIELD,
                       col_codes: str = params.CODE_FIELD,
                       col_flags: str = params.FLAG_FIELD,
                       col_code_array: str = params.CODE_ARRAY_FIELD) -> DataFrame:
    
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
                               assoc_id_col: str = params.ASSOC_REC_ID_FIELD) -> DataFrame:
    
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
