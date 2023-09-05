# Databricks notebook source
## preprocessing_cvdp_cohort

# COMMAND ----------

# MAGIC %run ../../params/params

# COMMAND ----------

# MAGIC %run ../util

# COMMAND ----------

## MODULE IMPORTS
import pyspark.sql.functions as F

from dsp.udfs import NHSNumber
from pyspark.sql import DataFrame
from typing import List, Union

# COMMAND ----------

## Filter Cohorts
def cvdp_filter_cohorts(df: DataFrame, field_cohort: str, filter_cohort_codes: Union[List[str], str]) -> DataFrame:
    """cvdp_filter_cohorts
    Filters records, keeping patients with cohort codes found in supplied cohort code list. 

    Args:
        df (DataFrame): CVDP Store containing dataframe (annual or quarterly)
        field_cohort (str): Column name for column containing cohort codes
        filter_cohort_codes (Union[List[str], str]): String | List of cohort codes to filter on.

    Returns:
        DataFrame: Cohort filtered dataframe (keep records with cohort code in supplied field)
    """    
    
    # Filter on cohort column
    df = df.filter(
        F.col(field_cohort).isin(filter_cohort_codes)
    )
    # Return
    return df
    
## Remove invalid NHS number records
def cvdp_remove_invalid_nhs(df: DataFrame, field_nhs_number: str, invalid_nhs_numbers: Union[List[str], str], invalid_nhs_prefix: Union[List[str], str]) -> DataFrame:
    """cvdp_remove_invalid_nhs
    Validates NHS Numbers and removes any defined | supplied NHS numbers (e.g. test numbers, test prefixes)

    Args:
        df (DataFrame): CVDP Store containing dataframe (annual or quarterly)
        field_nhs_number (str): Column name for column containing NHS numbers
        invalid_nhs_numbers (Union[List[str], str]): String | List of NHS Numbers (10 digits) to filter on
        invalid_nhs_prefix (Union[List[str], str]): String | List of prefixes of NHS Numbers to filter on

    Returns:
        DataFrame: Dataframe with records removed where NHS numbers matched either invalid conditions (full or prefix)
    """    
    
    # NHS Validator
    df = df.where(NHSNumber(F.col(field_nhs_number)))
    # Type convert: supplied NHS numbers and prefix
    if type(invalid_nhs_numbers) == str:
        invalid_nhs_numbers = [invalid_nhs_numbers]
    if type(invalid_nhs_prefix) == str:
        invalid_nhs_prefix = [invalid_nhs_prefix]
    # Supplied number(s) - remove
    df = df.filter(
        ~F.col(field_nhs_number).isin(invalid_nhs_numbers)
    )
    # Supplied prefix(es) - remove
    reg_prefix = r"^("+ "|".join(invalid_nhs_prefix) + ")"
    df = df.filter(
        ~F.col(field_nhs_number).rlike(reg_prefix)
    )
    # Return
    return df
    
## Remove records with null entries
def cvdp_remove_null_records(df: DataFrame, null_col: Union[List[str], str]) -> DataFrame:
    """cvdp_remove_null_records
    Removes records that have nulls in any of the columns supplied.

    Args:
        df (DataFrame): CVDP Store containing dataframe (annual or quarterly)
        null_col (Union[List[str], str]): String | List of column(s) to filter null values from

    Returns:
        DataFrame: Dataframe with records removed where a null value is present in any of the specified column(s)
    """    
    
    # Type convert: supplied columns
    if type(null_col) == str:
        null_col = [null_col]
    # Drop if any column contains null
    df = df.na.drop(subset = null_col)
    # Return
    return df

## Combine annual and quarterly data
def cvdp_combine_store_data(df_annual: DataFrame, df_quarterly: DataFrame) -> DataFrame:
    """cvdp_combine_store_data
    Unions the annual and quarterly CVDP Store Data

    Args:
        df_annual (DataFrame): CVDP Store containing dataframe (annual)
        df_quarterly (DataFrame): CVDP Store containing dataframe (quarterly)

    Returns:
        DataFrame: Unioned dataframe of annual and quarterly data
    """
    
    # Return
    return df_annual.union(df_quarterly)
    
## Deduplicate cohort entries
def cvdp_deduplicate_cohort_entries(
    df: DataFrame,
    fields_window: Union[List[str],str],
    field_extract_date: str,
    field_journal_table: str,
    field_journal_date: str, 
    fields_null_drop: Union[List[str],str]
) -> DataFrame:
    """cvdp_deduplicate_cohort_entries
    Deduplicates cohort entries from the combined cohort table. The deduplication is applied over the window columns [field_window]. 
    The deduplication process:
    1) Keep latest extract date for the window (skipped if fields_window contains field_extract_date)
    2) Duplicate entries - Deduplicate on max(journal_date)
    3) Duplicate entries - Remove entries with nulls in fields_null_drop
    4) Duplicate entries - Keep entries with max(journal_table).size()
    5) Duplicate entries - [FINAL] Keep random record
    
    Args:
        df (DataFrame): CVDP Store containing dataframe
        fields_window (Union[List[str],str]): Column(s) to create the window for ranking
        field_extract_date (str): Column (containing date values) to order the window by
        field_journal_table (str): Column for the journal table entries (array column)
        field_journal_date (str): Column for the journal table date field (array sub-column)
        fields_null_drop (Union[List[str],str]): Column(s) to remove records if NULL
    Returns:
        DataFrame: Dataframe with the latest record only kept (based on supplied field)
    """    
    
    # Type converters
    if type(fields_window) == str:
        fields_window = [fields_window]
    if type(fields_null_drop) == str:
        fields_null_drop = [fields_null_drop]
    
    ## Deduplication Process
    dedupe_window = Window.partitionBy(fields_window)
    # [1] Duplicate entries - Keep latest extract date
    # > Conditional - skip if the extract date field is in the window fields list
    if field_extract_date not in fields_window:
        df = filter_latest(
                df              = df,
                window_fields   = fields_window,
                date_field      = field_extract_date
            )
    # [2] Deduplicate on max(journal_date)
    df = df.withColumn('_max_journal_date',F.array_max(F.col(f'{field_journal_table}.{field_journal_date}')))
    df = df.withColumn('_max_journal_date_w',F.max(F.col('_max_journal_date')).over(dedupe_window))
    df = (df.filter(F.col('_max_journal_date') == F.col('_max_journal_date_w'))
            .drop('_max_journal_date','_max_journal_date_w'))
    # [3] Duplicate entries - Remove entries with nulls in fields_null_drop
    if fields_null_drop != None:
        df = df.na.drop(subset = fields_null_drop)
    # [4] Duplicate entries - Keep entries with max(journal_date).size()
    df = df.withColumn('_count', F.size(F.col(field_journal_table)))
    df = df.withColumn('_max', F.max(F.col('_count')).over(dedupe_window))
    df = (df.filter(F.col('_count') == F.col('_max'))
            .drop('_count','_max'))
    # [5] Duplicate entries - [FINAL] Keep random record
    df = df.dropDuplicates(fields_window)
    
    # Return
    return df
    
## Main Preprocessing Function
def preprocess_cvdp_cohort(
    cvdp_annual: DataFrame,
    cvdp_quarterly: DataFrame,
    field_cohort        = params.CVDP_COHORT_FIELD,
    filter_cohort_codes = params.CVDP_COHORT_CODES,
    field_nhs_number    = params.CVDP_PID_FIELD,
    invalid_nhs_numbers = params.INVALID_NHS_NUMBER_FULL,
    invalid_nhs_prefix  = params.INVALID_NHS_NUMBERS_PREFIX,
    null_col            = [params.CVDP_PID_FIELD, params.CVDP_DOB_FIELD],
    fields_window       = [params.CVDP_PID_FIELD, params.CVDP_DOB_FIELD, params.CVDP_EXTRACT_DATE],
    field_extract_date  = params.CVDP_EXTRACT_DATE,
    field_journal_table = params.CVDP_JOURNAL_FIELD,
    field_journal_date  = params.CVDP_JOURNAL_DATE_FIELD,
    fields_null_drop    = None
) -> DataFrame:
    """preprocess_cvdp
    Processes and combines the CVDP Annual and Quarterly data and ensures one-record-per-patient

    Args:
        cvdp_annual (DataFrame): CVDP Store containing dataframe (annual)
        cvdp_quarterly (DataFrame): CVDP Store containing dataframe (quarterly)
        (**args): Default assignment as defined in functions

    Returns:
        DataFrame: Dataframe containing the preprocessed CVDP annual and quarterly data for the eligible patient cohort
    """    
    
    # Cohort Filter
    cvdp_annual     = cvdp_filter_cohorts(cvdp_annual, field_cohort, filter_cohort_codes)
    cvdp_quarterly  = cvdp_filter_cohorts(cvdp_quarterly, field_cohort, filter_cohort_codes)
    # NHS Number Validation and Removal
    cvdp_annual     = cvdp_remove_invalid_nhs(cvdp_annual, field_nhs_number, invalid_nhs_numbers, invalid_nhs_prefix)
    cvdp_quarterly  = cvdp_remove_invalid_nhs(cvdp_quarterly, field_nhs_number, invalid_nhs_numbers, invalid_nhs_prefix)
    # Null Value Record Removal
    cvdp_annual     = cvdp_remove_null_records(cvdp_annual, null_col)
    cvdp_quarterly  = cvdp_remove_null_records(cvdp_quarterly, null_col)
    # CVDP Store Data Union
    cvdp_combined = cvdp_combine_store_data(
        df_annual       = cvdp_annual,
        df_quarterly    = cvdp_quarterly
    )
    # Deduplicate records
    cvdp_combined = cvdp_deduplicate_cohort_entries(cvdp_combined, fields_window, field_extract_date,
                                                    field_journal_table, field_journal_date,fields_null_drop)
    # Return
    return cvdp_combined