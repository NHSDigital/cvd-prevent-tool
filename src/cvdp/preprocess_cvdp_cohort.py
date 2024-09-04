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

## Deduplicate cohort entries
def cvdp_deduplicate_cohort_entries(
    df: DataFrame,
    fields_window: Union[List[str],str],
    field_extract_date: str,
    field_journal_table: str,
    field_journal_date: str,
    fields_deduplicate: Union[List[str],str],
    fields_null_drop: Union[List[str],str]
) -> DataFrame:
    """cvdp_deduplicate_cohort_entries
    Deduplicates cohort entries from the combined cohort table. The deduplication is applied over the window columns [field_window].
    The deduplication process:
    1) Keep latest extract date for the window (skipped if fields_window contains field_extract_date)
    2) Duplicate entries - Deduplicate on max(journal_date)
    3) Duplicate entries - Remove entries with nulls in fields_null_drop
    4) Duplicate entries - Keep entries with max(journal_table).size()
    5) Duplicate entries - [FINAL] Keep entries ordered (ascending) on fields_deduplicate

    Args:
        df (DataFrame): CVDP Store containing dataframe
        fields_window (Union[List[str],str]): Column(s) to create the window for ranking
        field_extract_date (str): Column (containing date values) to order the window by
        field_journal_table (str): Column for the journal table entries (array column)
        field_journal_date (str): Column for the journal table date field (array sub-column)
        fields_deduplicate (Union[List[str],str]): Column(s) to order final deduplication on
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
    # [5] Duplicate entries - [FINAL] Keep entries ordered (ascending) on fields_deduplicate
    dedupe_window_final = Window.partitionBy(fields_window).orderBy(*[F.col(c).asc_nulls_last() for c in fields_deduplicate])
    df = (
        df
        .withColumn('final_rank', F.row_number().over(dedupe_window_final))
        .filter(F.col('final_rank') == 1)
        .drop('final_rank')
    )

    # Return
    return df

## Main Preprocessing Function
def preprocess_cvdp_cohort(
    cvdp_extract: DataFrame,
    field_nhs_number: str = params.CVDP_PID_FIELD,
    invalid_nhs_numbers: Union[List[str], str] = params.INVALID_NHS_NUMBER_FULL,
    invalid_nhs_prefix: Union[List[str], str] = params.INVALID_NHS_NUMBERS_PREFIX,
    null_col: Union[List[str], str] = [params.CVDP_PID_FIELD, params.CVDP_DOB_FIELD],
    fields_window: Union[List[str],str] = [params.CVDP_PID_FIELD, params.CVDP_DOB_FIELD, params.CVDP_EXTRACT_DATE],
    field_extract_date: str = params.CVDP_EXTRACT_DATE,
    field_journal_table: str = params.CVDP_JOURNAL_FIELD,
    field_journal_date: str = params.CVDP_JOURNAL_DATE_FIELD,
    fields_deduplicate: Union[List[str],str] = params.CVDP_FIELDS_DEDUPE_COHORT,
    fields_null_drop: Union[List[str],str] = None
) -> DataFrame:
    """preprocess_cvdp_cohort

    Processes the CVDP extract (generated from the ExtractCVDPDataStage) and ensures one-record-per-patient.

    Args:
        cvdp_extract (DataFrame): Combined CVDP data extract (output: ExtractCVDPDataStage)
        field_nhs_number (str, optional): Column name for column containing NHS numbers.
            Defaults to params.CVDP_PID_FIELD.
        invalid_nhs_numbers (Union[List[str], str], optional): String | List of NHS Numbers (10 digits) to filter on.
            Defaults to params.INVALID_NHS_NUMBER_FULL.
        invalid_nhs_prefix (Union[List[str], str], optional): String | List of prefixes of NHS Numbers to filter on.
            Defaults to params.INVALID_NHS_NUMBERS_PREFIX.
        null_col (Union[List[str], str], optional): String | List of column(s) to filter null values from.
            Defaults to [params.CVDP_PID_FIELD, params.CVDP_DOB_FIELD].
        fields_window (Union[List[str], str], optional): Column(s) to create the window for ranking.
            Defaults to [params.CVDP_PID_FIELD, params.CVDP_DOB_FIELD, params.CVDP_EXTRACT_DATE].
        field_extract_date (str, optional): Column (containing date values) to order the window by.
            Defaults to params.CVDP_EXTRACT_DATE.
        field_journal_table (str, optional): Column for the journal table entries (array column).
            Defaults to params.CVDP_JOURNAL_FIELD.
        field_journal_date (str, optional): Column for the journal table date field (array sub-column).
            Defaults to params.CVDP_JOURNAL_DATE_FIELD.
        fields_deduplicate (Union[List[str],str], optional): Column(s) to order final deduplication on.
            Defaults to params.CVDP_FIELDS_DEDUPE_COHORT.
        fields_null_drop (Union[List[str], str], optional): Column(s) to remove records if NULL.
            Defaults to None.

    Returns:
        DataFrame: Combined CVDP dataframe, deduplicated and processed.
    """
    # NHS Number Validation and Removal
    cvdp_combined = cvdp_remove_invalid_nhs(cvdp_extract, field_nhs_number, invalid_nhs_numbers, invalid_nhs_prefix)
    # Null Value Record Removal
    cvdp_combined = cvdp_remove_null_records(cvdp_combined, null_col)
    # Deduplicate records
    cvdp_combined = cvdp_deduplicate_cohort_entries(cvdp_combined, fields_window, field_extract_date,
                                                    field_journal_table, field_journal_date, fields_deduplicate,
                                                    fields_null_drop)
    # Return
    return cvdp_combined