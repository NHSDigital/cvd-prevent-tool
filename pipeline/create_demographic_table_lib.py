# Databricks notebook source
## create_demographic_table_lib

## Overview
# Function definitions for use in create_demographic_table

# COMMAND ----------

from typing      import List
from pyspark.sql import DataFrame

import pyspark.sql.functions as F

# COMMAND ----------

# MAGIC %run ../src/util

# COMMAND ----------

## NON-CLASS FUNCTIONS
def load_ethnicity_ref_codes(
    db: str = params.DSS_CORPORATE_DATABASE,
    table: str = params.REF_ETHNICITY_MAPPING,
    output_fields: List[str] = [params.REF_CODE_FIELD, params.REF_ETHNICITY_CODE_FIELD]
) -> DataFrame:
    '''load_ref_codes
    Creates a dataframe containing the SNOMED codes and all classifications.

    Args:
        db (str): Database where the codes can be found. Defaults to params.DSS_CORPORATE_DATABASE.
        table (str): Table where the codes can be found. Defaults to params.REF_ETHNICITY_MAPPING.
        output_fields (List[str]): List of output fields to select from the final output DataFrame.
            Defaults to [params.REF_CODE_FIELD, params.REF_ETHNICITY_CODE_FIELD]

    Returns:
        df (DataFrame): Processed dataframe
    '''
    ## Load Reference Codes
    ethnicity_ref_codes = spark.table(f'{db}.{table}')
    ## Return
    return ethnicity_ref_codes.select(output_fields).distinct()


def set_to_null(df: DataFrame, field: str = params.ETHNICITY_FIELD, values_to_set: List[Any] = params.ETHNICITY_UNKNOWN_CODES) -> DataFrame:
    '''set_to_null
    Sets the value of a column to null when it is included in `values_to_set`.

    Args:
        df (DataFrame)           : Data
        field (str)              : Name of field to set to null, defaults to params.ETHNICITY_FIELD
        values_to_set (List[str]): List of values to set to null, defaults to params.ETHNICITY_UNKNOWN_CODES

    Returns:
        df (DataFrame)           : Data with values in the field set to null

    '''
    df = df.withColumn(field, F.when(F.col(field).isin(values_to_set), None).otherwise(F.col(field)))

    return df


def get_ethnicity_snomed_codes(snomed_code_field: str = params.REF_CODE_FIELD) -> List[str]:
    '''get_ethnicity_snomed_codes
    Retrieves a list of snomed codes that relate to ethnicity records.

    Args:
        snomed_code_field (str): Name of the snomed_code_field

    Functions:
        pipeline/create_demographic_table_lib::load_ethnicity_ref_codes

    Returns:
        ethnicity_snomed_codes (List[str]): List of snomed codes relating to ethnicity records
    '''
    df = load_ethnicity_ref_codes()
    ethnicity_snomed_codes = [row[snomed_code_field] for row in df.select(snomed_code_field).distinct().collect()]
    return ethnicity_snomed_codes


def filter_journal_by_code(
  journal_df: DataFrame,
  code_field: str = params.CODE_FIELD,
  inclusion_codes: List[str] = get_ethnicity_snomed_codes(),
  output_fields: List[str] = [params.PID_FIELD,params.DOB_FIELD,params.JOURNAL_DATE_FIELD,params.CODE_FIELD]
) -> DataFrame:
    '''filter_journal_by_code
    Filters a journal table to only include some codes.

    Args:
        journal_df (DataFrame): DataFrame containing codes to filter
        code_field (str): Name of column containing codes to filter
        inclusion_codes (List[str]): Codes to include in output, defaults to output of pipeline/create_demographic_table_lib::get_ethnicity_snomed_codes
        output_fields (List[str]): Fields to include in output

    Returns:
        df_filtered (DataFrame): journal table only including codes in inclusion_codes.
    '''
    df_filtered = (
      journal_df
      .select(*output_fields)
      .distinct()
      .filter(F.col(code_field).isin(inclusion_codes))
    )

    return df_filtered


def get_journal_ethnicity(journal_df: DataFrame, dataset_name: str = 'journal') -> DataFrame:
    '''get_journal_ethnicity
    Get the ethnicity relating to ethnicity snomed codes in the journal table.
    Also renames ethnicity and date fields.

    Args:
        journal_df (DataFrame): journal table
        dataset_name: name to store as dataset source, defaults to 'journal'

    Functions:
        pipeline/create_demographic_table_lib::filter_journal_by_code
        pipeline/create_demographic_table_lib::set_to_null
        pipeline/create_demographic_table_lib::filter_latest_non_null
        pipeline/create_demographic_table_lib::filter_latest_non_null

    Returns:
        journal table with only known ethnicity values
    '''
    ## RESTRICT TO ETHNICITY SNOMED CODES
    ethnicity_journal_df = filter_journal_by_code(journal_df)

    ## GET ETHNICITY SNOMED MAPPING
    ethnicity_map = load_ethnicity_ref_codes()

    ## JOIN TO ETHNICITY SNOMED MAPPING AND RENAME FIELD
    ethnicity_journal_df = (
      ethnicity_journal_df
      .join(ethnicity_map, ethnicity_journal_df[params.CODE_FIELD] == ethnicity_map[params.REF_CODE_FIELD], how='inner')
      .withColumnRenamed(params.REF_ETHNICITY_CODE_FIELD, params.ETHNICITY_FIELD)
      .withColumnRenamed(params.JOURNAL_DATE_FIELD, params.RECORD_STARTDATE_FIELD)
      .withColumn(params.DATASET_FIELD, F.lit(dataset_name))
    )

    # SET UNKNOWN ETHNICITY CODES TO NULL
    ethnicity_journal_df = set_to_null(ethnicity_journal_df, field=params.ETHNICITY_FIELD, values_to_set=params.ETHNICITY_UNKNOWN_CODES)

    ## FILTER LATEST KNOWN ETHNICITY VALUES
    ethnicity_journal_df = filter_latest_non_null(
      ethnicity_journal_df,
      pid_fields=[params.PID_FIELD, params.DOB_FIELD, params.DATASET_FIELD],
      date_field=params.RECORD_STARTDATE_FIELD,
      field=params.ETHNICITY_FIELD
    )

    return ethnicity_journal_df.select(*params.DEMOGRAPHIC_OUTPUT_FIELDS)


def filter_from_year(hes_df: DataFrame, start_year: int, date_field: str) -> DataFrame:
    '''filter_from_year
    Filters HES dataframe to only include records after and including the given year.

    Args:
        hes_df (DataFrame): HES data
        start_year (int): First year of data to include
        date_field (str): Date to filter by start year
    '''
    hes_df = (
        hes_df
        .withColumn('_year', F.year(date_field))
        .filter(F.col('_year') >= start_year)
        .drop('_year')
    )

    return hes_df


def get_hes_ethnicity(hes_df: DataFrame) -> DataFrame:
    '''get_hes_ethnicity
    Selects the latest non-null records from HES tables.

    Args:
        hes_df (DataFrame): HES data

    Functions:
        pipeline/create_demographic_table_lib::set_to_null
        pipeline/create_demographic_table_lib::filter_latest_non_null

    Returns:
        hes table with only known ethnicity values
    '''
    ## FILTER LATEST KNOWN ETHNICITY VALUES
    hes_df = set_to_null(hes_df, field=params.ETHNICITY_FIELD, values_to_set=params.ETHNICITY_UNKNOWN_CODES)
    hes_df = filter_latest_non_null(
      hes_df,
      pid_fields=[params.PID_FIELD, params.DOB_FIELD, params.DATASET_FIELD],
      date_field=params.RECORD_STARTDATE_FIELD,
      field=params.ETHNICITY_FIELD
    )
    return hes_df.select(*params.DEMOGRAPHIC_OUTPUT_FIELDS)


def select_priority_ethnicity(
    df: DataFrame,
    date_field: str = params.RECORD_STARTDATE_FIELD,
    pid_fields: List[str] = [params.PID_FIELD, params.DOB_FIELD]
) -> DataFrame:
    '''select_priority_ethnicity

    Selects the highest priority ethnicity value based on recency and dataset.
    The most recent value is used.
    If there are >1 records from >1 datasets on this most recent date, a priority order is applied:
    1. Journal
    2. HES APC
    3. HES AE
    4. HES OP

    Args:
        df (DataFrame): Data
        date_field (str): Name of date field to select most recent record, defaults to params.RECORD_STARTDATE_FIELD,
        pid_fields (List[str]): List of identifier fields, defaults to [params.PID_FIELD, params.DOB_FIELD]

    '''
    latest_df = filter_latest(
              df=df,
              window_fields=pid_fields,
              date_field=date_field
    )

    win = Window.partitionBy(pid_fields)

    # Sets out priority logic- prioritising journal, then HES (APC, AE & OP)
    win_ordered = win.orderBy(F.when(F.col(params.DATASET_FIELD) == 'journal', 1).when(F.col(params.DATASET_FIELD) == 'hes_apc',2).when(F.col(params.DATASET_FIELD) == 'hes_ae',3).when(F.col(params.DATASET_FIELD) == 'hes_op',4))
    latest_df = (
      latest_df
      .withColumn('ethnicity_rank', F.rank().over(win_ordered))
      .filter(F.col('ethnicity_rank') == 1)
      .withColumn('n_ethnicities', F.count(F.col(params.ETHNICITY_FIELD)).over(win))
      .filter(F.col('n_ethnicities') == 1)
    )

    return latest_df.select(*params.DEMOGRAPHIC_OUTPUT_FIELDS).distinct()


def filter_latest_priority_non_null(df: DataFrame, pid_fields: List[str], date_field: str, field: str) -> DataFrame:
    '''filter_latest_priority_non_null
    Filters the latest record that does not return a null value in `field`.
    If no non-null values exist, the latest null value is returned.

    Args:
        df (DataFrame)
        pid_fields (List[str]): List of identifier fields
        date_field (str): Name of date field to select most recent record
        field (str): Name of field to check for nulls
    '''
    w = Window.partitionBy(pid_fields).orderBy(F.when(F.col(field).isNull(), 1).otherwise(0), F.col(date_field).desc())
    df_latest = (
            df.select(
            date_field,
            *pid_fields,
            F.rank().over(w).alias("rwnb"),
            field
        )
        .where(F.col("rwnb") == 1)
        .drop("rwnb")
    )
    return df_latest

def filter_latest_non_null(df: DataFrame, pid_fields: List[str], date_field: str, field: str) -> DataFrame:
    '''filter_latest_non_null
    Filters the latest record that does not return a null value in `field`.

    Args:
        df (DataFrame)
        pid_fields (List[str]): List of identifier fields
        date_field (str): Name of date field to select most recent record
        field (str): Name of field to check for nulls
    '''
    w = Window.partitionBy(pid_fields).orderBy(F.col(date_field).desc())
    df_latest = (
            df.filter(F.col(field).isNotNull()).select(
            date_field,
            *pid_fields,
            F.rank().over(w).alias("rwnb"),
            field
        )
        .where(F.col("rwnb") == 1)
        .drop("rwnb")
    )
    return df_latest


def remove_multiple_records(df: DataFrame, window_fields: List[str], field: str) -> DataFrame:
    '''remove_multiple_records
    Removes records with multiple values of `field` on the same date

    Args:
        df (DataFrame)
        window_fields (List[str]): List of fields to use as keys in the window function
        (e.g. to remove multiples on the same date f, this list would include identifier fields plus a date field)
        field (str): Name of field to check for multiple value
    '''
    w = Window.partitionBy(window_fields)
    df_removed = (
            df.distinct().select(
            *window_fields,
            F.count(F.col(field)).over(w).alias("n_ethnicities"),
            field
        )
        .where(F.col("n_ethnicities") == 1)
        .drop("n_ethnicities")
    )
    return df_removed


def set_multiple_records_to_null(df: DataFrame, window_fields: List[str], field: str) -> DataFrame:
    '''set_multiple_records_to_null
    Sets the value of `field` to null where there are records with multiple values of `field` on the same date

    Args:
        df (DataFrame)
        window_fields (List[str]): List of fields to use as keys in the window function
        (e.g. to remove multiples on the same date f, this list would include identifier fields plus a date field)
        field (str): Name of field to check for multiple value
    '''
    w = Window.partitionBy(window_fields)
    df_field_null = (
            df.distinct().select(
            *window_fields,
            F.count(F.col(field)).over(w).alias("n_ethnicities"),
            field
        )
        .withColumn(field, F.when(F.col("n_ethnicities") != 1, None).otherwise(F.col(field)))
        .distinct()
        .drop("n_ethnicities")
    )
    return df_field_null