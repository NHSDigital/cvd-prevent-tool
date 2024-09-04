# Databricks notebook source
## preprocessing_dars

## Overview
## Collections of functions that are specific for preprocessing of the dars (mortality) dataset

# COMMAND ----------

## LIBRARY IMPORTS
import pyspark.sql.functions as F

from datetime import date
from pyspark.sql import Window, DataFrame
from typing import List, Optional

# COMMAND ----------

# MAGIC %run ../util

# COMMAND ----------

# MAGIC %run ../../params/params

# COMMAND ----------

# CLEANING & FILTERING FUNCTIONS
def remove_cancelled_dars_records(
    df: DataFrame,
    current_field: str = params.DARS_CURRENT_FIELD,
    cancelled_field: str = params.DARS_CANCELLED_FIELD,
) -> DataFrame:
    """remove_cancelled_dars_records
    Remove records that are not current, or cancelled

    Args:
        df (DataFrame): Dataframe of raw data
        current_field (str): Column name for the DARS current record flag
        cancelled_field (str): Column name for the DARS cancelled record flag

    Returns:
        df (DataFrame): Processed dataframe containing current, non-cancelled records
    """
    df = df.filter(
        (F.col(current_field) == "True") & (F.col(cancelled_field) == "N")
    ).drop(current_field, cancelled_field)

    return df


def filter_dars_region(
    df: DataFrame,
    residence_field: str = params.DARS_RESIDENCE_FIELD,
    location_field: str = params.DARS_LOCATION_FIELD,
    region_prefix: Optional[str] = "E",
) -> DataFrame:
    """filter_dars_region
    Filter records to those resident in a region indicated by region_prefix
    Where an address is not provided, the record is kept if the location of death meets the criteria

    Args:
        df (DataFrame): Dataframe of raw data
        residence_field (str, optional): Column name for field containing prefixes of LSOA values. Defaults to params.DARS_RESIDENCE_FIELD.
        location_field (str, optional): Column name for field containing full LSOA values. Defaults to params.DARS_LOCATION_FIELD.
        region_prefix (Optional[str], optional): String prefix indicating region, i.e. 'E' filters records in England. Defaults to 'E'.

    Returns:
        df (DataFrame): Processed dataframe with location/residence filtered to contain region(s) defined by region_prefix.
    """
    df = df.filter(
        (F.col(residence_field).startswith(region_prefix))
        | (
            (F.col(residence_field).isNull())
            & (F.col(location_field).startswith(region_prefix))
        )
    )

    return df


def clean_dars_nhs_number(
    df: DataFrame,
    nhs_number_out_field: str = params.DARS_PID_FIELD,
    nhs_number_origin_field_1: str = params.DARS_PID_ORIGIN_FIELD_1,
    nhs_number_origin_field_2: str = params.DARS_PID_ORIGIN_FIELD_2,
) -> DataFrame:
    """clean_dars_nhs_number
    Uses the confirmed nhs number if provided, else the unconfirmed number.
    Removes records with no nhs number at all

    Args:
        df (DataFrame): Dataframe of raw data
        nhs_number_out_field (str, optional): Column name of field containing NHS numbers. Defaults to params.DARS_PID_FIELD.
        nhs_number_origin_field_1 (str, optional): Column name of field containing NHS numbers (decommissioned). Defaults to params.DARS_PID_ORIGIN_FIELD_1.
        nhs_number_origin_field_2 (str, optional): Column name of field containing NHS numbers (decommissioned). Defaults to params.DARS_PID_ORIGIN_FIELD_2.

    Returns:
        DataFrame: Processed dataframe containing records with a NHS number, null NHS numbers removed.
    """
    df_clean = df.withColumn(
        nhs_number_out_field,
        F.when(
            F.col(nhs_number_origin_field_1).isNotNull(),
            F.col(nhs_number_origin_field_1),
        ).otherwise(F.col(nhs_number_origin_field_2)),
    ).filter((F.col(nhs_number_out_field).isNotNull()))

    return df_clean.drop(nhs_number_origin_field_1, nhs_number_origin_field_2)


# DATE FUNCTIONS
def convert_dars_dates(
    df: DataFrame,
    date_fields: List[str]
    ) -> DataFrame:
    """convert_dars_dates
    Checks to ensure date types

    Args:
        df (DataFrame): Dataframe of raw data
        date_fields (Optional[List[str]], optional): List of fields to check dates. Defaults to params.DARS_DOD_FIELD and params.DARS_DOB_FIELD from params.

    Returns:
        DataFrame: Processed dataframe with corrected date fields
    """
    for date_field in date_fields:
        df = ensure_date_type_from_str(df, date_field, "yyyyMMdd")

    return df


def filter_dars_dates(
    df: DataFrame,
    start_date: date = params.start_date,
    end_date: date = params.end_date,
    dob_field: str = params.DARS_DOB_FIELD,
    dod_field: str = params.DARS_DOD_FIELD,
) -> DataFrame:
    """filter_dars_dates
    Restrict records where date of death > birth, and between a given start and end date.

    Args:
        df (DataFrame): Dataframe of raw data
        start_date (date, optional): Earliest date to allow. Defaults to params.start_date.
        end_date (date, optional): Latest date to allow. Defaults to params.end_date.
        dob_field (str, optional): Column name of field containing date of birth. Defaults to params.DARS_DOB_FIELD.
        dod_field (str, optional): Column name of field containing date of death. Defaults to params.DARS_DOD_FIELD.

    Returns:
        DataFrame: Dataframe filtered by date
    """
    df_converted_dates = convert_dars_dates(df, [dod_field, dob_field])

    df_filtered = df_converted_dates.filter(
        F.col(dod_field) >= F.col(dob_field)
    ).filter(F.col(dod_field).between(start_date, end_date))

    return df_filtered


# COMMAND ----------


def clean_dars(
    df: DataFrame,
    start_date: Optional[date] = params.start_date,
    end_date: Optional[date] = params.end_date,
    residence_field: str = params.DARS_RESIDENCE_FIELD,
    location_field: str = params.DARS_LOCATION_FIELD,
    current_field: str = params.DARS_CURRENT_FIELD,
    cancelled_field: str = params.DARS_CANCELLED_FIELD,
    dob_field: str = params.DARS_DOB_FIELD,
    dod_field: str = params.DARS_DOD_FIELD,
    nhs_number_out_field: str = params.DARS_PID_FIELD,
    nhs_number_origin_field_1: str = params.DARS_PID_ORIGIN_FIELD_1,
    nhs_number_origin_field_2: str = params.DARS_PID_ORIGIN_FIELD_2,
) -> DataFrame:
    """clean_dars

    Args:
        df (DataFrame): Dataframe of raw data
        start_date (Optional[date], optional): Earliest date to allow, used in filter_dars_dates(). Defaults to params.start_date.
        end_date (Optional[date], optional): Earliest date to allow, used in filter_dars_dates(). Defaults to params.end_date.
        residence_field (str, optional):  Column name for field containing prefixes of LSOA values. Defaults to params.DARS_RESIDENCE_FIELD.
        location_field (str, optional): Column name for field containing full LSOA values. Defaults to params.DARS_LOCATION_FIELD.
        current_field (str, optional): Column name for field containing flag for is record current. Defaults to params.DARS_CURRENT_FIELD.
        cancelled_field (str, optional): Column name for field containing flag for is record cancelled (invalid). Defaults to params.DARS_CANCELLED_FIELD.
        dob_field (str, optional): Column name of field containing date of birth. Defaults to params.DARS_DOB_FIELD.
        dod_field (str, optional): Column name of field containing date of death. Defaults to params.DARS_DOD_FIELD.
        nhs_number_out_field (str, optional): Column name of field containing NHS numbers. Defaults to params.DARS_PID_FIELD.
        nhs_number_origin_field_1 (str, optional): Column name of field containing NHS numbers (decommissioned). Defaults to params.DARS_PID_ORIGIN_FIELD_1.
        nhs_number_origin_field_2 (str, optional): Column name of field containing NHS numbers (decommissioned). Defaults to params.DARS_PID_ORIGIN_FIELD_2.

    Returns:
        DataFrame: Cleaned DARS dataframe.
    """
    df_dars = filter_dars_region(df, location_field, residence_field)

    df_dars_current = remove_cancelled_dars_records(
        df_dars, current_field, cancelled_field
    )

    df_dars_current_dates = filter_dars_dates(
        df_dars_current, start_date, end_date, dob_field, dod_field
    )

    df_dars_current_dates_cleaned = clean_dars_nhs_number(
        df_dars_current_dates,
        nhs_number_out_field,
        nhs_number_origin_field_1,
        nhs_number_origin_field_2,
    )

    return df_dars_current_dates_cleaned


# COMMAND ----------


def create_assoc_cod_flags(
    df: DataFrame,
    code_map: Dict[str, str] = params.DARS_ICD10_CODES_MAP,
    flag_field: str = params.FLAG_FIELD,
    comorbs_field: str = params.DARS_COMORBS_CODES_FIELD,
    assoc_join_str: str = "_assoc_",
) -> DataFrame:
    """create_assoc_cod_flags
    Creates a column containing the assoiated cause of death flags (alongside the primary cause of death)

    Args:
        df (DataFrame): DARS containing dataframe
        code_map (Dict[str, str], optional): Mapping of death flag category to ICD 10 code(s). Defaults to params.DARS_ICD10_CODES_MAP.
        flag_field (str, optional): Column name for field containing primary cause of death flag. Defaults to params.FLAG_FIELD.
        comorbs_field (str, optional): Column name for field containing associated cause of death codes (ICD10). Defaults to params.DARS_COMORBS_CODES_FIELD.
        assoc_join_str (str, optional): Deliminator string for column name. Defaults to "_assoc_".

    Returns:
        DataFrame: DARS containing dataframe with associated cause of death fields
    """
    for flag_type, code_list in code_map.items():
        flag_type_lwr = flag_type.lower()
        df = df.withColumn(
            f"{flag_field}{assoc_join_str}{flag_type_lwr}",
            F.when(
                ((F.col(flag_field) != flag_type) | (F.col(flag_field).isNull()))
                & (
                    (
                        F.col(comorbs_field).rlike("|".join(code_list))
                        | (F.col(comorbs_field) == ",".join(code_list))
                    )
                ),
                1,
            ).otherwise(0),
        )
    return df


# COMMAND ----------


def create_cod_flags(
    df: DataFrame,
    flag_field: str = params.FLAG_FIELD,
    code_map: Dict[str, str] = params.DARS_ICD10_CODES_MAP,
    underlying_code_field: str = params.DARS_UNDERLYING_CODE_FIELD,
    comorbs_field: str = params.DARS_COMORBS_CODES_FIELD,
    assoc_cod_flags: bool = True,
) -> DataFrame:
    """create_outcomes_flag

    Args:
        df (DataFrame): Dataframe of raw data
        flag_field (str, optional): String containing the name of the new column to be created. Defaults to params.FLAG_FIELD.
        code_map (Dict[str, str], optional): List of ICD10 codes to include in new column. Defaults to params.DARS_ICD10_CODES_MAP.
        underlying_code_field (str, optional):  Field containing underlying cause of death code. Defaults to params.DARS_UNDERLYING_CODE_FIELD.
        comorbs_field (str, optional): Field containing concatenation of all codes included in mortality data. Defaults to params.DARS_COMORBS_CODES_FIELD.
        assoc_cod_flags (bool, optional): Use the concatenated fields in attributing cause of death. Defaults to True.

    Returns:
        DataFrame: Processed dataframe with new column flagging the cause of death for each patient.
    """
    df_outcomes = df.withColumn(flag_field, F.lit(params.NON_CVD_DEATH))

    for code_name, code_list in code_map.items():
        df_outcomes = df_outcomes.withColumn(
            flag_field,
            F.when(
                F.col(underlying_code_field).rlike(f"^({'|'.join(code_list)})"),
                code_name,
            ).otherwise(F.col(flag_field)),
        )

    if assoc_cod_flags == True:
        df_outcomes = create_assoc_cod_flags(
            df_outcomes, code_map, flag_field, comorbs_field
        )

    return df_outcomes


# COMMAND ----------

def dedup_dars(
    df: DataFrame,
    partition_fields: List[str] = [params.DARS_PID_FIELD, params.DARS_DOB_FIELD],
    death_field: str = params.DARS_DOD_FIELD,
    reg_date_field: str = params.DARS_REG_FIELD,
    id_field: str = params.DARS_ID_FIELD
) -> DataFrame:
    """dedup_dars
    Deduplicates the DARS dataframe where multiple records are present for a single partition field value (NHS number).
    The deduplication method is as follows:
        1) Partition the records on NHS Number
        2) Order multiple records on date of death (ascending) and registration date of death (descending)
        3) As a final catch-all, an additional round of deduplication ordering on the dataset primary key (ascending)

    Args:
        df (DataFrame): Dataframe of raw data
        partition_fields (List[str], optional): List of fields (nhs number and date of birth) to partition dataframe on (window function).
            Defaults to [params.DARS_PID_FIELD, params.DARS_DOB_FIELD.
        death_field (str, optional): Field (death date) to order window partition on, ascending.
            Defaults to params.DARS_DOD_FIELD.
        reg_date_field (str, optional): Field (death registration date) to order window partition on, descending.
            Defaults to params.DARS_REG_FIELD.
        id_field (Str, optional): Field (pre-exisisting DARS record ID) to order window partition on, ascending. Used in the case
            where deduplication does not remove all deduplicated records.
            Defaults to params.DARS_ID_FIELD.
        

    Returns:
        df_dedup (DataFrame): De-duplicated dataframe using NHS Number and Date of Birth as the dataframe partition.
    """
    
    # [1] Deduplicate records
    ## Partition: NHS Number
    ## Order: Date of death (ascending); Registration date (descending)
    dedup_window = (
        Window()
        .partitionBy(*partition_fields)
        .orderBy(F.col(death_field).asc(),F.col(reg_date_field).desc())
    )
    df_dedup = (
        df
        .withColumn("_rn", F.rank().over(dedup_window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )
    
    # [2] Catch-all for remaining multiple records
    ## Partition: NHS Number
    ## Order: Primary Key (ascending)
    dedupe_window_2 = (
        Window()
        .partitionBy(*partition_fields)
        .orderBy(F.col(id_field).asc())
    )
    df_dedup = (
        df_dedup
        .withColumn("_rn", F.row_number().over(dedupe_window_2))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )
    
    # Return DataFrame
    return df_dedup


# COMMAND ----------

def add_dars_record_id(
        df: DataFrame,
        field_record_id: str = params.RECORD_ID_FIELD,
        field_dars_id: str = params.DARS_ID_FIELD,
        field_assoc_record_id: str = params.ASSOC_REC_ID_FIELD,
        field_pid: str = params.DARS_PID_FIELD,
        field_dob: str = params.DARS_DOB_FIELD
) -> DataFrame:
    '''
    add_dars_record_id 
    Creates a record ID from the hash of person ID and date of birth to ensure record ID consistency when a person's death record is updated.
    Takes pre-existing DARS record ID and saves in associated record ID column.

    Args:
        df (DataFrame): cleaned and deduped DARS DataFrame.
        field_record_id (str, optional): Column to contain hashed primary key.
          Defaults to params.RECORD_ID_FIELD.
        field_dars_id (str, optional): Column containing pre-existing DARS record ID.
          Defaults to params.DARS_ID_FIELD.
        field_assoc_record_id (str, optional): column to rename pre-existing DARS record ID to.
          Defaults to params.ASSOC_REC_ID_FIELD.
        field_pid (str, optional): column containing NHS number.
          Defaults to params.DARS_PID_FIELD.
        field_dob (str, optional): column containing date of birth.
          Defaults to params.DARS_DOB_FIELD.

    Returns:
        DataFrame: dars table with hashed primary key record id and other record id stored in associated record id column 
    '''
    
    df = df.withColumnRenamed(field_dars_id, field_assoc_record_id)
    df = add_hashed_key(df, field_record_id, [field_pid,field_dob])

    return df

# COMMAND ----------

def check_unique_identifiers_dars(
        df,
        record_id = params.RECORD_ID_FIELD,
):
      hash_check = check_unique_values(
          df = df,
          field_name = record_id)
      if hash_check == True:
          pass
      else:
          raise Exception(f'ERROR: Non-unique hash values in preprocessed DARS. Pipeline stopped, check hashable fields.')

# COMMAND ----------

## Main Preprocess Function
def preprocess_dars(
    df: DataFrame,
    dars_flag_field: str = params.FLAG_FIELD,
    dars_code_map: Dict[str, str] = params.DARS_ICD10_CODES_MAP,
    dars_underlying_code_field: str = params.DARS_UNDERLYING_CODE_FIELD,
    dars_comorbs_field: str = params.DARS_COMORBS_CODES_FIELD,
    dars_pid_field: str = params.DARS_PID_FIELD,
    dars_death_field: str = params.DARS_DOD_FIELD,
    dars_reg_date_field: str = params.DARS_REG_FIELD,
    dars_id_field: str = params.DARS_ID_FIELD,
    dars_assoc_cod_flags: bool = True,
    dars_output_columns: List[str] = params.DARS_PREPROCESS_COLUMNS,
    dars_dob_field: str  = params.DARS_DOB_FIELD,
    field_record_id: str = params.RECORD_ID_FIELD,
    field_assoc_record_id: str = params.ASSOC_REC_ID_FIELD
) -> DataFrame:
    """preprocess_dars
    Main function for preprocessing the DARS dataframe to feed into the PreprocessRawDataStage.

    Args:
        df (DataFrame): Raw DARS dataframe
        dars_flag_field (str, optional): String containing the name of the new column to be created. Defaults to params.FLAG_FIELD.
        dars_code_map (Dict[str, str], optional): String containing the name of the new column to be created. Defaults to params.DARS_ICD10_CODES_MAP.
        dars_underlying_code_field (str, optional): Field containing underlying cause of death code. Defaults to params.DARS_UNDERLYING_CODE_FIELD.
        dars_comorbs_field (str, optional): Field containing concatenation of all codes included in mortality data. Defaults to params.DARS_COMORBS_CODES_FIELD.
        dars_pid_field (str, optional): Field (nhs number) to partition dataframe on (window function). Defaults to params.DARS_PID_FIELD.
        dars_death_field (str, optional): Field (death date) to order window partition on, ascending. Defaults to params.DARS_DOD_FIELD.
        dars_reg_date_field (str, optional): Field (death registration date) to order window partition on, descending. Defaults to params.DARS_REG_FIELD.
        dars_id_field (str, optional): Field (dataset primary key) to order winfow partition on, ascending. Used in the case
            where deduplication does not remove all deduplicated records. Defaults to params.DARS_ID_FIELD.
        dars_assoc_cod_flags (bool, optional): Use the concatenated fields in attributing cause of death. Defaults to True.
        dars_output_columns (List[str], optional): Define columns to select for returned processed DARS dataframe.
            Defaults to params.DARS_PREPROCESS_COLUMNS
        dars_dob_field (str, optional): Field containg patient date of birth. Defaults to params.DARS_DOB_FIELD.
        field_record_id (str, optional): Column name to add created record id to. Defaults to params.RECORD_ID_FIELD.
        field_assoc_record_id (str, optional): Column name to rename pre-exisiting DARS record id field to. Defaults to params.ASSOC_REC_ID_FIELD

    Returns:
        DataFrame: Processed DARS dataframe, for use in PreprocessRawDataStage
    """
    # Clean NHS numbers
    df = clean_dars(df)

    # Create the cause of death flags
    df = create_cod_flags(
        df=df,
        flag_field=dars_flag_field,
        code_map=dars_code_map,
        underlying_code_field=dars_underlying_code_field,
        comorbs_field=dars_comorbs_field,
        assoc_cod_flags=dars_assoc_cod_flags,
    )

    # Deduplicate death entries
    df = dedup_dars(
        df=df,
        partition_fields=[dars_pid_field,dars_dob_field],
        death_field=dars_death_field,
        reg_date_field=dars_reg_date_field,
        id_field=dars_id_field,
    )

    df = add_dars_record_id(
        df = df,
        field_record_id=field_record_id,
        field_dars_id=dars_id_field,
        field_assoc_record_id=field_assoc_record_id,
        field_pid=dars_pid_field,
        field_dob=dars_dob_field
    )

    # Select columns to return final dars dataframe
    df = df.select(dars_output_columns)
    
    #check new record ID is unique
    check_unique_identifiers_dars(
      df = df,
      record_id = field_record_id
    )

    # Return DARS dataframe
    return df