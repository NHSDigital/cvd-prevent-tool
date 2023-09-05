# Databricks notebook source
## preprocessing_hes

## Overview
## Collections of functions that are specific for preprocessing of HES datasets

# COMMAND ----------

## LIBRARY IMPORTS
import itertools 
import pyspark.sql.functions as F
from pyspark.sql import DataFrame, Window
from typing import List, Optional, Dict, Union
from functools import reduce

# COMMAND ----------

# MAGIC %run ../../params/params

# COMMAND ----------

# MAGIC %run ../util

# COMMAND ----------

## UTILITY FUNCTIONS
## Perform operations on the HES dataframes

def clean_code_field(df: DataFrame, code_field: str, prefix: str = '_', invalid_code: str = params.HES_CODE_FILTER) -> DataFrame:
    """_summary_

    Args:
        df (DataFrame): HES Dataframe
        code_field (str): Column name of primary diagnosis code field
        prefix (str): Some diagnosis codes are in the format DIAG_n and others DIAGn, this prefix field accounts for both

    Raises:
        ValueError: Error if diagnostic code is not level 2, 3 or 4

    Returns:
        DataFrame: Dataframe with cleaned (string correctly shortened)code field
    """    
    ## REMOVE WHITESPACE
    df = df.withColumn(code_field, F.regexp_replace(F.col(code_field), " ", ""))
    ## SELECT STRING CHECK/PRUNE
    if f'{prefix}2_' in code_field:
        prune_pos = 2
    elif f'{prefix}3_' in code_field:
        prune_pos = 3
    elif f'{prefix}4_' in code_field:
        prune_pos = 3
        df = df.withColumn(f"{code_field}_temp", F.col(code_field).substr(1, prune_pos + 1))
        #REMOVE INVALID CODES
        df = df.filter(F.col(code_field) != invalid_code)
    else:
        raise ValueError(f'[ERROR] UNABLE TO DETERMINE STRING LENGTH FROM CODE LEVEL FOR {code_field} FIELD')
    ## STRING CLEAN
    df = df.withColumn(code_field, F.col(code_field).substr(1, prune_pos))
    ## REMOVE NULL CODES
    df = df.filter(F.col(code_field).isNotNull())
    return df


def filter_array_of_diag_codes_lambda(col_name: str, code_list: List[str]):
    '''
    See filter_array_of_diag_codes
    '''
    return reduce(or_, [F.array_contains(F.expr(f"transform({col_name}, x -> rlike(x, '^({code})[0-9]?$'))"), True) for code in code_list])


def filter_array_of_diag_codes(df: DataFrame, col_name: str, code_list: List[str]) -> DataFrame:
    '''
    Given a dataframe and an array column col_name, remove records where none of the given code_list codes are elements in the array.
    Codes in the array that match a code in code_lists except with a single extra digit at the end, are also acceptable matches.
    '''
    return df.where(filter_array_of_diag_codes_lambda(col_name, code_list))


def create_list_from_codes(
    df: DataFrame, 
    code_col_prefix: str = params.HES_AE_CODE_PREFIX, 
    list_col_name: str = params.HES_AE_CODE_LIST_FIELD
) -> DataFrame:
    """Create a field that contains a list of concatenated columns
    
    Args:
        df (DataFrame): HES AE dataframe
    
    Returns:
        DataFrame: HES AE dataframe with a concatenated list of diag codes
    """
    # Create list of columns from the prefix (e.g. "DIAG_"), and a number in a range
    # zfill ensures a zero pads out the numbers with zeroes for the number of digits given
    # e.g. ["DIAG_01", "DIAG_02" ...]
    code_cols = [code_col_prefix + str(i).zfill(2) for i in range(1, 13)]

    #make an array of all code columns and remove None values and duplicates.
    df = df.withColumn(list_col_name, F.concat_ws(',',*code_cols))
    
    return df
  
def create_array_from_code_list(df: DataFrame, code_list_field: str) -> DataFrame:
    """_summary_

    Args:
        df (DataFrame): HES dataframe
        code_list_field (str): Column name of the diagnosis _concat field

    Returns:
        DataFrame: HES dataframe with _concat field converted to array of codes
    """    
    ## REMOVE WHITESPACE
    df = df.withColumn(code_list_field, F.regexp_replace(F.col(code_list_field), " ", ""))
    ## STR > ARRAY
    df = df.withColumn(code_list_field, F.split(F.col(code_list_field), ','))
    return df

# COMMAND ----------

def select_hes_primary_coded_events(df: DataFrame, code_field: str) -> DataFrame:
    """_summary_

    Args:
        df (DataFrame): HES Dataframe
        code_field (str): Column name of primary diagnosis code field

    Returns:
        DataFrame: HES dataframe filtered for rows that contain an allowed ICD10 in 
                   the primary code column
    """    
    ## EXTRACT ICD10 CODES FROM MAPPING
    code_list = list(itertools.chain.from_iterable(params.HES_ICD10_CODES_MAP.values()))
    ## CODE MATCHING
    df = df.where(F.col(code_field).isin(code_list))
    
    return df

# COMMAND ----------

def create_outcomes_flag(df: DataFrame, code_field: str) -> DataFrame:
    """_summary_

    Args:
        df (DataFrame): HES Dataframe
        code_field (str): Column name of primary diagnosis code field

    Returns:
        DataFrame: HES Dataframe with added FLAG field, indicating outcomes
    """    
    ## CREATE MAPPING OBJECT
    mapping_expr = create_code_map(params.HES_ICD10_CODES_MAP)
    
    code_list = list(itertools.chain.from_iterable(params.HES_ICD10_CODES_MAP.values()))
        
    df = df.withColumn('tmp_diags', F.array([F.lit(x) for x in params.ALL_CVD_ICD10_CODES]))
    
    ## MAP ONTO NEW COLUMN
    df = df.withColumn(params.HES_FLAG_FIELD, 
                       F.when(F.col(code_field).isin(code_list), mapping_expr[F.col(code_field)]).otherwise(
                       F.when(F.arrays_overlap(F.col(params.HES_APC_CODE_LIST_FIELD), F.col('tmp_diags')), 'CVD_NON_PRIMARY').otherwise(
                       'NO_CVD')))
                         
    return df.drop('tmp_diags')

# COMMAND ----------

def get_hes_dataframes_from_years(hes_dataset_name: str, start_year: int = params.HES_START_YEAR, 
                                  end_year: int = params.HES_END_YEAR, db_hes: str = params.HES_DATABASE, 
                                  db_ahas: str = params.HES_AHAS_DATABASE,  join_filter_columns: Optional[List[str]] = None) -> DataFrame:
    '''
    Read the various hes tables in the given database and search for all tables matching hes_dataset_name. 
    Filter the years based on the two digit integers start_year (inclusive) and end_year (exclusive).

    If the first_year is greater than or equal to 21, use the ahas data instead of the hes data, as 
    specified by the optional arguments.

    This function can be used to read the non-sensistive, sensitive and mps tables by altering the db_hes and db_ahas variables
    '''
    ## NAME FORMAT
    hes_dataset_name = (f'{hes_dataset_name}_' if hes_dataset_name[-1] != '_' else hes_dataset_name)

    ## HES_YEAR TABLE MATCHING
    extract_pattern = f'({hes_dataset_name})([1-2][0-9]{{3}})' 
    old_year_suffixes = [row._year for row in spark.sql(f'SHOW TABLES IN {db_hes}') \
                                                    .withColumn('_year', F.regexp_extract(F.col('tableName'), extract_pattern, 2)) \
                                                    .select('_year').where(F.col('_year') != '').distinct() \
                                                    .sort(F.col('_year').desc()).collect()]
    new_year_suffixes = [row._year for row in spark.sql(f'SHOW TABLES IN {db_ahas}') \
                                                    .withColumn('_year', F.regexp_extract(F.col('tableName'), extract_pattern, 2)) \
                                                    .select('_year').where(F.col('_year') != '').distinct() \
                                                    .sort(F.col('_year').desc()).collect()]
    all_year_suffixes = list(set(old_year_suffixes+new_year_suffixes))
    all_year_suffixes.sort()
    year_suffixes = [year_pattern for year_pattern in all_year_suffixes \
                    if int(year_pattern[:2]) >= start_year and int(year_pattern[:2]) < end_year \
                    and int(year_pattern[2:]) <= end_year]

    ## HES YEARS DATAFRAMES
    df_flat = None
    for year_suffix in year_suffixes:
        _year   = int(year_suffix[:2])
        _db     = (db_hes if _year < 21 else db_ahas)
        if join_filter_columns is None:
          df_hes  = spark.table(f'{_db}.{hes_dataset_name}{year_suffix}')
        elif type(join_filter_columns) == list:
          df_hes  = spark.table(f'{_db}.{hes_dataset_name}{year_suffix}').select(join_filter_columns)
        else:
          raise ValueError('[ERROR] VARIABLE JOIN_FILTER_COLUMNS IS OF NON SUPPORTED TYPE (REQUIRES NONETYPE OR LIST)')
        df_flat = df_hes if df_flat is None else df_flat.union(df_hes)

    return df_flat

# COMMAND ----------

def join_to_hes_sensitive(df: DataFrame, dataset_name: str, limit_col: Optional[bool] = False) -> DataFrame:
    """_summary_

    Args:
        df (DataFrame): HES non-sensitive dataframe
        dataset_name (str): Name of hes table (e.g. hes_apc)
        limit_col (Optional[bool], optional): Limit columns to those defined in params. Defaults to False.

    Returns:
        DataFrame: HES joined dataframe (non-sensitive:sensitive) - [optional] limited columns
    """    
    ## NON-SENSITIVE <> SENSITIVE LINKAGE KEY
    link_key = params.HES_SENSITIVE_LINK_KEY_MAP[dataset_name]

    ## SENSITIVE HES 
    if dataset_name in ['hes_ae','hes_op']: 
      hes_start_year = params.HES_5YR_START_YEAR # only look at previous 5 years for use in demographic table
    else:
      hes_start_year = params.HES_START_YEAR 
    df_hes_s = get_hes_dataframes_from_years(hes_dataset_name = dataset_name, start_year = hes_start_year,
                                                end_year = params.HES_END_YEAR, db_hes = params.HES_S_DATABASE,
                                                db_ahas = params.HES_AHAS_S_DATABASE)

    ## CONDITIONAL - LIMIT COLUMNS IN HES
    if limit_col == True:
        hes_ns_cols = params.HES_PREPROCESSING_COLS_MAP[f'{dataset_name}_ns']
        hes_s_cols  = params.HES_PREPROCESSING_COLS_MAP[f'{dataset_name}_s']
        # HES NON-SENSITIVE
        df = df.select(hes_ns_cols)
        # HES SENSITIVE
        df_hes_s = df_hes_s.select(hes_s_cols)

    ## JOIN NON-SENSITIVE AND SENSITIVE HES                                                
    df = df.join(df_hes_s, [link_key], 'inner')

    return df

# COMMAND ----------

def join_to_hes_apc_otr(df: DataFrame) -> DataFrame:
    """_summary_

    Joins the HES APC OTR table to the HES APC (joined: non-sensitive + sensitive) dataframe 
    to obtain the SUSSPELLID column for each episode

    Args:
        df (DataFrame): HES APC dataframe

    Returns:
        DataFrame: HES APC dataframe with SUSSPELLID column
    """    
    
    ## HES OTR COLUMNS FOR EXTRACTION
    HES_OTR_COLS = [params.HES_OTR_LINK_KEY,params.HES_OTR_SPELL_ID]
    
    ## HES OTR
    df_hes_otr = get_hes_dataframes_from_years(hes_dataset_name = params.HES_APC_OTR_TABLE, start_year = params.HES_START_YEAR,
                                            end_year = params.HES_END_YEAR, db_hes = params.HES_DATABASE,
                                            db_ahas = params.HES_AHAS_DATABASE, join_filter_columns = HES_OTR_COLS)
    
    ## SELECT: LINK KEY AND SPELL ID ONLY
    df_hes_otr = df_hes_otr.select(params.HES_OTR_LINK_KEY,params.HES_OTR_SPELL_ID)
    
    ## JOIN DF AND OTR HES
    df_joined = df.join(df_hes_otr, [params.HES_OTR_LINK_KEY], 'left_outer')
    
    ## CHECK: RECORD NUMBERS
    try:
        assert df.count() == df_joined.count()
    except:
        print(f'[WARNING] HES APC-OTR JOIN - NON-UNIQUE KEYS FOUND: ROW COUNT RECEIVED {df_joined.count()} ROW COUNT EXPECTED {df.count()}')
        
    return df_joined

# COMMAND ----------

def calculate_spell_dates(df: DataFrame, 
                          start_date_col: str = params.HES_APC_SPELL_STARTDATE_FIELD,
                          end_date_col: str = params.HES_APC_SPELL_ENDDATE_FIELD,
                          spell_id_col: str = params.HES_OTR_SPELL_ID,
                          date_filter_list: list = params.HES_APC_OTR_DATE_FILTERS,
                          date_replace_str: str = params.HES_APC_OTR_ADMIDATE_REPLACE_VALUE,
                          spell_startdate_col: str = params.HES_SPELL_START_FIELD,
                          spell_enddate_col: str = params.HES_SPELL_END_FIELD) -> DataFrame:
    """_summary_

    Calculate spell durations, for each spell ID, from the minimum ADMIDATE and maximum DISDATE per spell id. 
    Note: for ADMIDATE, placeholder dates are present of 1800-01-01 and 1801-01-01 (None or Invalid dates).
          these are replaced with the pipeline date placeholder (9999-01-01) so minimum ADMIDATE dates can be
          calculated correctly.

    Args:
        df (DataFrame): HES APC dataframe
        start_date_col (str, optional): Episode start date. Defaults to params.HES_APC_SPELL_STARTDATE_FIELD.
        end_date_col (str, optional): Episode end date. Defaults to params.HES_APC_SPELL_ENDDATE_FIELD.
        spell_id_col (str, optional): Episode spell ID column. Defaults to params.HES_OTR_SPELL_ID.
        date_filter_list (list, optional): List of dates to filter and replace with None. 
                                           Defaults to params.HES_APC_OTR_DATE_FILTERS.
        date_replace_str (str, optional): YYYY-MM-DD str to replace filtered dates with. 
                                          Defaults to params.HES_APC_OTR_ADMIDATE_REPLACE_VALUE.
        spell_startdate_col (str, optional): str for new column name, min start_date_col date for each spell id. 
                                             Defaults to params.HES_SPELL_START_FIELD.
        spell_enddate_col (str, optional): str for new column name, max end_date_col date for each spell id.
                                           Defaults to params.HES_SPELL_END_FIELD.

    Returns:
        DataFrame: HES dataframe with spell ID, spell start and end dates
    """    
    
    ## ADMIDATE - REPLACE HES PLACEHOLDERS WITH PIPELINE PLACEHOLDERS IF IN THE FILTER LIST OR NULL
    df = df.withColumn(start_date_col, 
                       F.when((F.col(start_date_col).isin(date_filter_list)) | (F.col(start_date_col).isNull()), date_replace_str)\
                       .otherwise(F.col(start_date_col)))
    
    
    ## CREATE WINDOW FOR DATAFRAME ON SPELL ID
    spell_window = Window.partitionBy(spell_id_col)
    
    ## CALCULATE MIN AND MAX ADMIDATE AND DISDATE
    df = df.repartition(spell_id_col)
    df = df.withColumn(spell_startdate_col, F.min(start_date_col).over(spell_window))
    df = df.withColumn(spell_enddate_col, F.max(end_date_col).over(spell_window))
    
    ## ENSURE DATETYPES IN ALTERED COLUMNS (ADMIDATE and SPELLSTART)
    df = ensure_date_type_from_str(df, start_date_col)
    df = ensure_date_type_from_str(df, spell_startdate_col)
    
    
    return df

def clean_hes_spell_dates(df: DataFrame, date_filter_list: List[str] = params.HES_APC_OTR_DATE_FILTERS,
                          clean_fields_list: List[str] = [params.HES_APC_SPELL_STARTDATE_FIELD, params.HES_APC_SPELL_ENDDATE_FIELD,
                                                          params.HES_SPELL_START_FIELD, params.HES_SPELL_END_FIELD]) -> DataFrame:
    """_summary_
    Replaces HES and Pipeline dateholders with None. 

    Args:
        df (DataFrame): HES DataFrame
        date_filter_list (List[str], optional): List of date values to replace with None.
                                                Defaults to params.HES_APC_OTR_DATE_FILTERS.
        clean_fields_list (List[str], optional): List of columns/fields to filter dates from.
                                                 Defaults to [params.HES_APC_SPELL_STARTDATE_FIELD,
                                                              params.HES_APC_SPELL_ENDDATE_FIELD,
                                                              params.HES_SPELL_START_FIELD,
                                                              params.HES_SPELL_END_FIELD].

    Returns:
        DataFrame: HES Dataframe with replaced dates (None) for supplied columns
    """    
    ## REPLACE FILTER VALUES WITH NONE
    df = replace_value_none(df, col_names = clean_fields_list, filter_values = date_filter_list)
    
    return df


def clean_hes_spell_id(df: DataFrame, spell_id_col: str = params.HES_OTR_SPELL_ID,
                       spell_id_filter_values: Union[List[str], str] = params.HES_APC_OTR_SPELL_ID_FILTER) -> DataFrame: 
    """_summary_
    Replaces HES SPELL IDs with None when matching supplied filter values

    Args:
        df (DataFrame): HES DataFrame
        spell_id_col (str, optional): Name of column containing the HES SPELL IDs
        spell_id_filter_values (List[str] or str, optional): List or List[str] of values to replace with None.
                                                             Defaults to params.HES_APC_OTR_SPELL_ID_FILTER.

    Returns:
        DataFrame: HES Dataframe with replaced spell ids (None) for supplied columns
    """
    ## REPLACE FILTER VALUES WITH NONE
    df = replace_value_none(df, col_names = spell_id_col, filter_values = spell_id_filter_values)
    
    return df
  
def clean_temp_icd_length_4_cols(df: DataFrame, code_field:str) -> DataFrame:
  """
  Removes columns with ICD10 codes of length 3 and renames the temporary column containing ICD10 codes of length 4.
  
  Args:
    df (Dataframe): HES Dataframe containing flags
    code_field: Which type of ICD10 code is used, e.g. DIAG_3_01
    
  Returns:
    HES Dataframe with no temp columns.
  """
  if "_4_" in code_field:
      df = df.drop(code_field)
      df = df.withColumnRenamed(f"{code_field}_temp",code_field)
      
  return df

# COMMAND ----------

def hes_preprocessing(df: DataFrame, dataset_name: str, limit_col: Optional[bool] = False,
                        select_coded_events: Optional[bool] = params.SWITCH_HES_SELECT_CODED_EVENTS) -> DataFrame:
    """_summary_

    Args:
        df (DataFrame): HES non-sensitive dataframe (from preprocess_hes::get_hes_dataframes_from_years)
        dataset_name (str): Name of hes table (e.g. hes_apc)
        limit_col (Optional[bool], optional): Limit columns to those defined in params. Defaults to False.
        select_coded_events (Optional[bool], optional): Filter dataframe to rows containing allowed ICD10 codes 
                                                        in primary diagnosis code field. Defaults to False.

    Returns:
        DataFrame: Processed HES dataframe
    """    
    ## HES DATASET - SPECIFIC VARIABLES
    code_field = params.HES_CODE_FIELD_MAP[dataset_name]
    code_list_field = params.HES_CODE_LIST_FIELD_MAP[dataset_name]
    ## CREATE CONCAT CODE FIELD WHERE IT DOESN'T EXIST
    if dataset_name == 'hes_ae':
        df = create_list_from_codes(df)
    ## JOIN SENSITIVE HES
    df_hes_flat = join_to_hes_sensitive(df, dataset_name, limit_col)
    ## CLEAN CODE FIELD
    if dataset_name == 'hes_ae':
      df_hes_flat_cleaned = clean_code_field(df_hes_flat, code_field, prefix='')
    else:
      df_hes_flat_cleaned = clean_code_field(df_hes_flat, code_field, prefix='_')
    ## SELECT CODED EVENTS ONLY - OPTIONAL
    if select_coded_events == True:
        df_hes_flat_cleaned = select_hes_primary_coded_events(df_hes_flat_cleaned, code_field)
    ## CONVERT CONCAT CODES TO ARRAY
    df_codeArray = create_array_from_code_list(df_hes_flat_cleaned, code_list_field)
    # #CREATE OUTCOMES FOR APC
    if dataset_name == 'hes_apc':
      ## ADD OUTCOME FLAGS
      df_flagged = create_outcomes_flag(df_codeArray, code_field)
      ## REMOVE ANY TEMP COLUMNS
      df_clean = clean_temp_icd_length_4_cols(df_flagged, code_field)
    else:
      ## REMOVE ANY TEMP COLUMNS
      df_clean = clean_temp_icd_length_4_cols(df_codeArray, code_field)
    
    return df_clean

# COMMAND ----------

def hes_preprocess_apc(df: DataFrame) -> DataFrame:
    df =  hes_preprocessing(df = df, dataset_name = 'hes_apc',
                            limit_col = params.SWITCH_HES_LIMIT_COLUMNS,
                            select_coded_events = params.SWITCH_HES_SELECT_CODED_EVENTS)
    df = join_to_hes_apc_otr(df)
    df = calculate_spell_dates(df)
    df = clean_hes_spell_dates(df)
    df = clean_hes_spell_id(df)
    return df

def hes_preprocess_ae(df: DataFrame) -> DataFrame:
    return hes_preprocessing(df = df, dataset_name = 'hes_ae',
                             limit_col = params.SWITCH_HES_LIMIT_COLUMNS,
                            select_coded_events = params.SWITCH_HES_SELECT_CODED_EVENTS)


def hes_preprocess_op(df: DataFrame) -> DataFrame:
    return hes_preprocessing(df = df, dataset_name = 'hes_op',
                             limit_col = params.SWITCH_HES_LIMIT_COLUMNS,
                            select_coded_events = params.SWITCH_HES_SELECT_CODED_EVENTS)