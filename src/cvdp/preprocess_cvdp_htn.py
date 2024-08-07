# Databricks notebook source
## preprocess_cvdp_htn

# COMMAND ----------

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

## Hypertension Sub-Functions
def split_and_process_htn_entries_single(
    df: DataFrame,
    codes_bp_diastolic: List[str] = params.DIASTOLIC_BP_SNOMED_CODES,
    codes_bp_systolic: List[str] = params.SYSTOLIC_BP_SNOMED_CODES,
    field_values_array: str = params.CODE_ARRAY_FIELD,
    field_snomed_code: str = params.CODE_FIELD,
    col_systolic: str = params.CVDP_SYSTOLIC_BP_READING,
    col_diastolic: str = params.CVDP_DIASTOLIC_BP_READING,
) -> DataFrame:
    """split_and_process_htn_entries_single
    Extracts the single (one-row-per systolic or diastolic reading) BP readings from the journal table, using SNOMED
    code identifiers. Assigns reading values to the systolic or diastolic column, respectively. Only records where
    the first value is non-null, second value is null are kept.

    Args:
        df (DataFrame): Eligible cohort journal table (output of CreatePatientCohortTableStage)
        codes_bp_diastolic (List[str], optional): List of SNOMED codes that define a diastolic blood pressure reading.
            Defaults to params.DIASTOLIC_BP_SNOMED_CODES.
        codes_bp_systolic (List[str], optional): List of SNOMED codes that define a systolic blood pressure reading.
            Defaults to params.SYSTOLIC_BP_SNOMED_CODES.
        field_values_array (str, optional): Name of column containing array of readings [value_1,value_2].
            Defaults to params.CODE_ARRAY_FIELD.
        field_snomed_code (str, optional): Name of column containing the SNOMED codes.
            Defaults to params.CODE_FIELD.
        col_systolic (str, optional): New column name for extracted systolic BP reading.
            Defaults to params.CVDP_SYSTOLIC_BP_READING.
        col_diastolic (str, optional): New column name for extracted diastolic BP reading.
            Defaults to params.CVDP_DIASTOLIC_BP_READING.

    Returns:
        DataFrame: Dataframe with row-per-reading for systolic or diastolic reading, derived from SNOMED code matches
    """

    # Concatenate SNOMED lists
    codes_bp_all = codes_bp_diastolic + codes_bp_systolic

    # Filter and split single bp measurements
    df = df.filter(F.col(field_snomed_code).isin(codes_bp_all))

    # Keep first item in code array (remove nulls) when code_array size (non-null) == 1
    df = (
        df
        .withColumn(
            f"tmp_{field_values_array}",
            F.array_except(F.col(field_values_array), F.array(F.lit(None))),
        )
        .filter(
            (F.col(field_values_array)[0].isNotNull()) & (F.size(F.col(f"tmp_{field_values_array}")) == 1)
            )
        .drop(f"tmp_{field_values_array}")
    )

    # Create columns for diastolic and systolic measurements
    df = (
        df
        .withColumn(
            col_systolic,
            F.when(
                F.col(field_snomed_code).isin(codes_bp_systolic), F.col(field_values_array)[0]
                ).otherwise(None),
        ).withColumn(
            col_diastolic,
            F.when(
                F.col(field_snomed_code).isin(codes_bp_diastolic), F.col(field_values_array)[0]
            ).otherwise(None),
        )
    )

    # Return
    return df


def split_and_process_htn_entries_multi(
    df: DataFrame,
    codes_bp_clusters: List[str] = params.BP_SNOMED_CLUSTER,
    field_values_array: str = params.CODE_ARRAY_FIELD,
    field_cluster_id: str = params.REF_CLUSTER_FIELD,
    col_systolic: str = params.CVDP_SYSTOLIC_BP_READING,
    col_diastolic: str = params.CVDP_DIASTOLIC_BP_READING,
) -> DataFrame:
    """split_and_process_htn_entries_multi
    Extracts the multiple (one-row-per systolic and diastolic reading) BP readings from the journal table, using SNOMED
    code identifiers. Assigns reading values to the systolic or diastolic column when both values are present (neither are
    not None).

    Args:
        df (DataFrame): Eligible cohort journal table (output of CreatePatientCohortTableStage)
        codes_bp_clusters (List[str], optional): List of Cluster ID codes that define a multiple blood pressure reading.
            Defaults to params.BP_SNOMED_CLUSTER.
        field_values_array (str, optional): Name of column containing array of readings [value_1,value_2].
            Defaults to params.CODE_ARRAY_FIELD.
        field_cluster_id (str, optional): Name of column containing the Cluster ID REF codes.
            Defaults to params.REF_CLUSTER_FIELD.
        col_systolic (str, optional): New column name for extracted systolic BP reading.
            Defaults to params.CVDP_SYSTOLIC_BP_READING.
        col_diastolic (str, optional): New column name for extracted diastolic BP reading.
            Defaults to params.CVDP_DIASTOLIC_BP_READING.

    Returns:
        DataFrame: Dataframe with row-per-reading for systolic and diastolic readings, derived from Cluster ID code matches
    """

    # Filter and split single bp measurements
    df = df.filter(F.col(field_cluster_id).isin(codes_bp_clusters))

    # Keep code_array values that do not contain nulls
    df = (
        df.withColumn(
            f"tmp_{field_values_array}",
            F.array_except(F.col(field_values_array), F.array(F.lit(None))),
        )
        .filter(F.size(F.col(f"tmp_{field_values_array}")) == 2)
        .drop(f"tmp_{field_values_array}")
    )

    # Create columns for diastolic and systolic measurements
    df = df.withColumn(col_systolic, F.col(field_values_array)[0]).withColumn(
        col_diastolic, F.col(field_values_array)[1]
    )

    # Return
    return df


def union_bp_frames(df_single: DataFrame, df_multi: DataFrame) -> DataFrame:
    """union_bp_frames
    Combined the dataframes containing the single and multiple BP readings.

    Args:
        df_single (DataFrame): Dataframe containing single BP readings (output of split_and_process_htn_entries_single)
        df_multi (DataFrame): Dataframe containing multiple BP readings (output of split_and_process_htn_entries_multi)

    Returns:
        DataFrame: Combined BP readings dataframe
    """

    # Create combined frame
    df_comb = df_single.union(df_multi)

    # Return
    return df_comb

def filter_bp_frames(
    df: DataFrame,
    age_min_value: int = params.HTN_PATIENT_MIN_AGE,
    age_max_value: int = params.PATIENT_MAX_AGE,
    bp_min_diastolic: int = params.MINIMUM_DIASTOLIC_BP,
    bp_min_systolic: int = params.MINIMUM_SYSTOLIC_BP,
    col_age_at_event: str = params.AGE_FIELD,
    field_bp_diastolic: str = params.CVDP_DIASTOLIC_BP_READING,
    field_bp_systolic: str = params.CVDP_SYSTOLIC_BP_READING,
    field_calculation_date: str = params.JOURNAL_DATE_FIELD,
    field_person_dob: str = params.DOB_FIELD,
) -> DataFrame:
    """_summary_

    Args:
        df (DataFrame): Dataframe containing the unioned single and multiple bp readings.
        age_min_value (int, optional): Minimum age a patient must be (at event date) to be a valid reading.
            Defaults to params.HTN_PATIENT_MIN_AGE.
        age_max_value (int, optional): Maximum age a patient must be (at event date) to be a valid reading.
            Defaults to params.PATIENT_MAX_AGE.
        bp_min_diastolic (int, optional): Minimum value for a valid reading, for diastolic blood pressure measurement.
            Defaults to params.MINIMUM_DIASTOLIC_BP.
        bp_min_systolic (int, optional): Minimum value for a valid reading, for systolic blood pressure measurement.
            Defaults to params.MINIMUM_SYSTOLIC_BP.
        col_age_at_event (str, optional): New column name for patient's calculated age (using DOB, at date of event).
            Defaults to params.AGE_FIELD.
        field_bp_diastolic (str, optional): Name of column containing the diastolic blood pressure measurement. .
            Defaults to params.CVDP_DIASTOLIC_BP_READING.
        field_bp_systolic (str, optional): Name of column containing the systolic blood pressure measurement.
            Defaults to params.CVDP_SYSTOLIC_BP_READING
        field_calculation_date (str, optional): Name of the column containing the date used to calculate a patients age (at time of event).
            Defaults to params.JOURNAL_DATE_FIELD.
        field_person_dob (str, optional): Name of column containing date of birth (date type).
            Defaults to params.DOB_FIELD.

    Returns:
        DataFrame: Dataframe contianing the filtered (valid) blood pressure measurements
    """

    # Add age
    df = df.withColumn(
        col_age_at_event, age_at_date(field_person_dob, field_calculation_date)
    )

    # Filter on age and minimum systolic and diastolic values
    df = (
        df
        .filter(
            (F.col(col_age_at_event) >= age_min_value) &
            (F.col(col_age_at_event) <= age_max_value) &
            ((F.col(field_bp_diastolic).isNotNull() | (F.col(field_bp_systolic).isNotNull()))) &
            ((F.col(field_bp_diastolic) >= bp_min_diastolic) | (F.col(field_bp_diastolic).isNull())) &
            ((F.col(field_bp_systolic) >= bp_min_systolic) | (F.col(field_bp_systolic).isNull()))
        )
    )

    # Return dataframe
    return df


def calculate_min_bp_readings(
    df: DataFrame,
    field_person_id: str = params.PID_FIELD,
    field_person_dob: str = params.DOB_FIELD,
    field_date_extract: str = params.EXTRACT_DATE_FIELD,
    field_date_journal: str = params.JOURNAL_DATE_FIELD,
    field_bp_systolic: str = params.CVDP_SYSTOLIC_BP_READING,
    field_bp_diastolic: str = params.CVDP_DIASTOLIC_BP_READING,
) -> DataFrame:
    """calculate_min_bp_readings
    Calculates, independently, the minimum systolic and diastolic reading from the combined BP dataframe. The minimum of each BP reading is
    calculated for each journal_date, per patient, per extract date.

    Args:
        df (DataFrame): Dataframe containing combined single and multiple BP readings, extracted and processed from
            the journal table (output of union_bp_frames).
        field_person_id (str, optional): Name of column containing (NHS Number | Person ID).
            Defaults to params.PID_FIELD.
        field_person_dob (str, optional): Name of column containing date of birth (date type).
            Defaults to params.DOB_FIELD.
        field_date_extract (str, optional): Name of column containing the extraction date from the CVDP extract (date type).
            Defaults to params.EXTRACT_DATE_FIELD.
        field_date_journal (str, optional): Name of column containing the recorded journal table entry date from CVDP (date type).
            Defaults to params.JOURNAL_DATE_FIELD.
        field_bp_systolic (str, optional): Name of column containing the systolic blood pressure measurement.
            Defaults to params.CVDP_SYSTOLIC_BP_READING.
        field_bp_diastolic (str, optional): Name of column containing the diastolic blood pressure measurement.
            Defaults to params.CVDP_DIASTOLIC_BP_READING.

    Returns:
        DataFrame: Dataframe containing the minimum blood pressure measurements (min systolic, min diastolic) for every journal
            entry (per person, per extract).
    """

    # Create window on patient, extract and journal information
    windowval = Window.partitionBy(
        field_person_id, field_person_dob, field_date_extract, field_date_journal
    )

    # Calculate minimum systolic and diastolic values over window
    df = df.withColumn(
        f"min_{field_bp_systolic}", F.min(field_bp_systolic).over(windowval)
    ).withColumn(f"min_{field_bp_diastolic}", F.min(field_bp_diastolic).over(windowval))

    # Deduplicate on window + minimum systolic and diastolic readings
    df = df.dropDuplicates(
        [
            field_person_id,
            field_person_dob,
            field_date_extract,
            field_date_journal,
            f"min_{field_bp_systolic}",
            f"min_{field_bp_diastolic}",
        ]
    )

    # Overwrite original bp columns with new minimum values
    df = (
        df
        .withColumn(field_bp_diastolic, F.col(f"min_{field_bp_diastolic}"))
        .withColumn(field_bp_systolic, F.col(f"min_{field_bp_systolic}"))
        .drop(f"min_{field_bp_diastolic}",f"min_{field_bp_systolic}")
    )
    # drop if either systolic or diastolic is missing
    df = df.filter((F.col(field_bp_diastolic).isNotNull() & (F.col(field_bp_systolic).isNotNull())))
    # Return
    return df


def keep_latest_journal_updates(
    df: DataFrame,
    field_person_id: str = params.PID_FIELD,
    field_person_dob: str = params.DOB_FIELD,
    field_date_extract: str = params.EXTRACT_DATE_FIELD,
    field_date_journal: str = params.JOURNAL_DATE_FIELD,
) -> DataFrame:
    """keep_latest_journal_updates
    Deduplicates the minimised blood pressure readings to ensure that, where multiple bp measurements have been assigned to an identical
    journal date, the latest extract version is kept (superseding the older extract associated records).

    Args:
        df (DataFrame): Dataframe containing the minimum blood pressure readings (output of calculate_min_bp_readings)
        field_person_id (str, optional): Name of column containing (NHS Number | Person ID).
            Defaults to params.PID_FIELD.
        field_person_dob (str, optional): Name of column containing date of birth (date type).
            Defaults to params.DOB_FIELD.
        field_date_extract (str, optional): Name of column containing the extraction date from the CVDP extract (date type)..
            Defaults to params.EXTRACT_DATE_FIELD.
        field_date_journal (str, optional): Name of column containing the recorded journal table entry date from CVDP (date type)..
            Defaults to params.JOURNAL_DATE_FIELD.

    Returns:
        DataFrame: Dataframe containing the minimum blood pressure readings, deduplicated to keep superseded readings (identical
            journal dates, later extraction date).
    """
    # Create window on patient and journal information, ordered by extraction date (latest first)
    windowval = Window.partitionBy(
        field_person_id, field_person_dob, field_date_journal
    ).orderBy(F.col(field_date_extract).desc())

    # Window and rank entires: select latest extract version of record (if multiples present)
    df = (
        df.withColumn("rank", F.rank().over(windowval))
        .filter(F.col("rank") == 1)
        .drop("rank")
    )

    # Return
    return df


def classify_htn_risk_group(
    df: DataFrame,
    col_risk_group: str = params.FLAG_FIELD,
    field_age_at_event: str = params.AGE_FIELD,
    field_bp_diastolic: str = params.CVDP_DIASTOLIC_BP_READING,
    field_bp_systolic: str = params.CVDP_SYSTOLIC_BP_READING,
) -> DataFrame:
    """classify_htn_risk_group
    For each blood pressure measurement, assigns a hypertension risk group based on (1) age, (2) systolic and (3) diastolic values.

    Args:
        df (DataFrame): Dataframe containing the minimum, deduplicated blood pressure readings (output of keep_latest_journal_updates)
        col_risk_group (str, optional): New column name for a patient's calculated hypertension risk group (at date of event).
            Defaults to params.FLAG_FIELD.
        field_age_at_event (str, optional): Name of column for patient's calculated age.
            Defaults to params.AGE_FIELD.
        field_bp_systolic (str, optional): Name of column containing the systolic blood pressure measurement.
            Defaults to params.CVDP_SYSTOLIC_BP_READING
        field_bp_diastolic (str, optional): Name of column containing the diastolic blood pressure measurement. .
            Defaults to params.CVDP_DIASTOLIC_BP_READING.

    Returns:
        DataFrame: Dataframe with appended hypertension risk groups, calculated for each blood pressure reading.
    """

    # Classify
    df = df.withColumn(
        col_risk_group,
        # Either measurement is None - set to None
        F.when(
            (F.col(field_bp_systolic).isNull()) | (F.col(field_bp_diastolic).isNull()),
            None,
        )
        # Treated to Target (< 80)
        .when(
            (F.col(field_age_at_event) < 80)
            & ((F.col(field_bp_systolic) >= 50) & (F.col(field_bp_systolic) <= 140))
            & ((F.col(field_bp_diastolic) >= 20) & (F.col(field_bp_diastolic) <= 90)),
            "0.1",
        )
        # Treated to Target (> 80)
        .when(
            (F.col(field_age_at_event) >= 80)
            & ((F.col(field_bp_systolic) >= 50) & (F.col(field_bp_systolic) <= 150))
            & ((F.col(field_bp_diastolic) >= 20) & (F.col(field_bp_diastolic) <= 90)),
            "0.2",)
        # Very High Risk
        .when(
            ((F.col(field_bp_systolic) > 180) | (F.col(field_bp_diastolic) > 120)),
            "1d")
        # High Risk
        .when(
            (
                ((F.col(field_bp_systolic) > 160) & (F.col(field_bp_systolic) <= 180)) |
                ((F.col(field_bp_diastolic) > 100) & (F.col(field_bp_diastolic) <= 120))),
                "1c",
        )
        # Moderate Risk (< 80)
        .when(
            (
                (F.col(field_age_at_event) < 80) &
                (((F.col(field_bp_systolic) > 140) & (F.col(field_bp_systolic) <= 160)) |
                ((F.col(field_bp_diastolic) > 90) & (F.col(field_bp_diastolic) <= 100)))),
            "1a",
        )
        # Moderate Risk (> 80)
        .when(
            (
                (F.col(field_age_at_event) >= 80) &
                ((F.col(field_bp_systolic) > 150) & (F.col(field_bp_systolic) <= 160)) |
                ((F.col(field_bp_diastolic) > 90) & (F.col(field_bp_diastolic) <= 100))),
            "1b",
        )
        # Default to None
        .otherwise(None)
    )

    # Return
    return df


def format_processed_htn_output(
    df: DataFrame,
    fields_select: List[str] = params.CVDP_HTN_OUTPUT_COLUMNS,
    field_bp_date: date = params.CVDP_JOURNAL_DATE_FIELD,
    filter_start_date: date = params.CVDP_BP_START_DATE,
    col_primary_key: str = params.RECORD_ID_FIELD,
    fields_hashable: List[str] = params.CVDP_HTN_HASHABLE_FIELDS,
) -> DataFrame:
    """format_processed_htn_output
    Applies final filtering and formatting steps to the processed hypertension data

    Args:
        df (DataFrame): Processed CVDP (to hypertension) dataframe
        fields_select (List[str], optional): List of column names to select for final output table.
            Defaults to params.CVDP_HTN_OUTPUT_COLUMNS.
        field_bp_date (date, optional): Name of column containing date for measurement filtering - remove measurements prior to this.
            Defaults to params.CVDP_JOURNAL_DATE_FIELD.
        filter_start_date (date, optional): Date value to use in filtering of field_bp_date.
            Defaults to params.CVDP_BP_START_DATE.
        col_primary_key (str, optional): Column name to use when creating primary key.
            Defaults to params.RECORD_ID_FIELD.
        fields_hashable (List[str], optional): Column names to use as hashable fields in creation of primary key.
            Defaults to params.CVDP_HTN_HASHABLE_FIELDS.
    Returns:
        DataFrame: Formatted CVDP HTN table (for use in preprocess raw data stage)
    """
    # Filter BP measurement dates - remove records prior to filter start date
    df = df.filter(F.col(field_bp_date) >= filter_start_date)
    # Select columns
    df = df.select(fields_select)
    # Create unique identifier for blood pressure readings
    df = add_hashed_key(
        df=df,
        col_hashed_key=col_primary_key,
        fields_to_hash=fields_hashable
    )
    # Return
    return df

# COMMAND ----------

def preprocess_cvdp_htn(
    df: DataFrame,
    age_min_value: int = params.HTN_PATIENT_MIN_AGE,
    age_max_value: int = params.PATIENT_MAX_AGE,
    bp_min_diastolic: int = params.MINIMUM_DIASTOLIC_BP,
    bp_min_systolic: int = params.MINIMUM_SYSTOLIC_BP,
    codes_bp_clusters: List[str] = params.BP_SNOMED_CLUSTER,
    codes_bp_diastolic: List[str] = params.DIASTOLIC_BP_SNOMED_CODES,
    codes_bp_systolic: List[str] = params.SYSTOLIC_BP_SNOMED_CODES,
    col_age_at_event: str = params.AGE_FIELD,
    col_diastolic: str = params.CVDP_DIASTOLIC_BP_READING,
    col_risk_group: str = params.FLAG_FIELD,
    col_systolic: str = params.CVDP_SYSTOLIC_BP_READING,
    field_calculation_date: str = params.JOURNAL_DATE_FIELD,
    field_cluster_id: str = params.REF_CLUSTER_FIELD,
    field_values_array: str = params.CODE_ARRAY_FIELD,
    field_date_extract: str = params.EXTRACT_DATE_FIELD,
    field_date_journal: str = params.JOURNAL_DATE_FIELD,
    field_person_dob: str = params.DOB_FIELD,
    field_person_id: str = params.PID_FIELD,
    field_snomed_code: str = params.CODE_FIELD,
    field_bp_date: date = params.CVDP_JOURNAL_DATE_FIELD,
    filter_start_date: date = params.CVDP_BP_START_DATE,
    fields_final_output: List[str] = params.CVDP_HTN_OUTPUT_COLUMNS,
    col_primary_key: str = params.RECORD_ID_FIELD,
    fields_hashable: List[str] = params.CVDP_HTN_HASHABLE_FIELDS,
) -> DataFrame:
    """preprocess_cvdp_htn
    Main prepocessing function for hypertension events, extracted from the journal table (output of CreatePatientCohortTableStage).
    This functions performs the following operations:
    1) Extract and process the single (row-per systolic or diastolic) BP readings
    2) Extract and process the multiple (row-per systolic and diastolic) BP readings
    3) Combine the single and multiple BP reading data extracts
    4) Calculate the minimum systolic and diastolic BP readings for each journal entry (journal date, for each person and extract)
    5) Keep superseded BP entries (if multiple journal dates present across multiple extracts) - defaults to latest update
    6) Classify the BP readings into hypertension risk groups
    7) Return the dataframe with pre-selected fields
    Returns the dataframe required for PreprocessRawDataStage.

    Args:
        df (DataFrame): Eligible cohort journal table (output of CreatePatientCohortTableStage)
        age_min_value (int, optional): Minimum age a patient must be (at event date) to be a valid reading.
            Defaults to params.HTN_PATIENT_MIN_AGE.
        age_max_value (int, optional): Maximum age a patient must be (at event date) to be a valid reading.
            Defaults to params.PATIENT_MAX_AGE.
        bp_min_diastolic (int, optional): Minimum value for a valid reading, for diastolic blood pressure measurement.
            Defaults to params.MINIMUM_DIASTOLIC_BP.
        bp_min_systolic (int, optional): Minimum value for a valid reading, for systolic blood pressure measurement.
            Defaults to params.MINIMUM_SYSTOLIC_BP.
        codes_bp_clusters (List[str], optional): List of Cluster ID codes that define a multiple blood pressure reading.
            Defaults to params.BP_SNOMED_CLUSTER.
        codes_bp_diastolic (List[str], optional): List of SNOMED codes that define a diastolic blood pressure reading.
            Defaults to params.DIASTOLIC_BP_SNOMED_CODES.
        codes_bp_systolic (List[str], optional): List of SNOMED codes that define a systolic blood pressure reading.
            Defaults to params.SYSTOLIC_BP_SNOMED_CODES.
        col_age_at_event (str, optional): New column name for patient's calculated age (using DOB, at date of event).
            Defaults to params.AGE_FIELD.
        col_diastolic (str, optional): New column name for extracted diastolic BP reading.
            Defaults to params.CVDP_DIASTOLIC_BP_READING.
        col_risk_group (str, optional): New column name for a patient's calculated hypertension risk group (at date of event).
            Defaults to params.FLAG_FIELD.
        col_systolic (str, optional): New column name for extracted systolic BP reading.
            Defaults to params.CVDP_SYSTOLIC_BP_READING.
        field_calculation_date (str, optional): Name of the column containing the date used to calculate a patients age (at time of event).
            Defaults to params.JOURNAL_DATE_FIELD.
        field_cluster_id (str, optional): Name of column containing the Cluster ID REF codes.
            Defaults to params.REF_CLUSTER_FIELD.
        field_values_array (str, optional): Name of column containing array of readings [value_1,value_2].
            Defaults to params.CODE_ARRAY_FIELD.
        field_date_extract (str, optional): Name of column containing the extraction date from the CVDP extract (date type).
            Defaults to params.EXTRACT_DATE_FIELD.
        field_date_journal (str, optional): Name of column containing the recorded journal table entry date from CVDP (date type).
            Defaults to params.JOURNAL_DATE_FIELD.
        field_person_dob (str, optional): Name of column containing date of birth (date type).
            Defaults to params.DOB_FIELD.
        field_person_id (str, optional): Name of column containing (NHS Number | Person ID).
            Defaults to params.PID_FIELD.
        field_snomed_code (str, optional): Name of column containing the SNOMED codes.
            Defaults to params.CODE_FIELD.
        field_snomed_code (List[str], optional): List of columns to select for final output (pass to PreprocessRawDataStage).
            Defaults to params.CVDP_HTN_OUTPUT_COLUMNS.
        col_primary_key (str, optional): Column name to use when creating primary key.
            Defaults to params.RECORD_ID_FIELD.
        fields_hashable (List[str], optional): Column names to use as hashable fields in creation of primary key.
            Defaults to params.CVDP_HTN_HASHABLE_FIELDS.
    Returns:
        DataFrame: Processed journal table input, containing minimised, deduplicated blood pressure readings with calculated
            hypertension risk groups (per valid blood pressure measurement)
    """

    # Filter and process journal table on SNOMED/Cluster IDs and split into single|multi bp frames
    df_single = split_and_process_htn_entries_single(
        df=df,
        codes_bp_diastolic=codes_bp_diastolic,
        codes_bp_systolic=codes_bp_systolic,
        field_values_array=field_values_array,
        field_snomed_code=field_snomed_code,
        col_systolic=col_systolic,
        col_diastolic=col_diastolic,
    )
    df_multi = split_and_process_htn_entries_multi(
        df=df,
        codes_bp_clusters=codes_bp_clusters,
        field_values_array=field_values_array,
        field_cluster_id=field_cluster_id,
        col_systolic=col_systolic,
        col_diastolic=col_diastolic,
    )

    # Union Single and Multiple BP Dataframes
    df_comb = union_bp_frames(df_single, df_multi)

    # Filter: Invalid age, systolic and diastolic blood pressure measurements
    df_comb = filter_bp_frames(
        df = df_comb,
        age_min_value = age_min_value,
        age_max_value = age_max_value,
        bp_min_diastolic = bp_min_diastolic,
        bp_min_systolic = bp_min_systolic,
        col_age_at_event = col_age_at_event,
        field_bp_diastolic = col_diastolic,
        field_bp_systolic = col_systolic,
        field_calculation_date = field_calculation_date,
        field_person_dob = field_person_dob,
    )

    # Calculate minimum systolic and diastolic (per extract_date - journal_date)
    df_comb = calculate_min_bp_readings(
        df=df_comb,
        field_person_id=field_person_id,
        field_person_dob=field_person_dob,
        field_date_extract=field_date_extract,
        field_date_journal=field_date_journal,
        field_bp_systolic=col_systolic,
        field_bp_diastolic=col_diastolic,
    )

    # Keep the readings, for each journal date, from the latest extract date
    df_comb = keep_latest_journal_updates(
        df=df_comb,
        field_person_id=field_person_id,
        field_person_dob=field_person_dob,
        field_date_extract=field_date_extract,
        field_date_journal=field_date_journal,
    )

    # Apply hypertension classification
    df_comb = classify_htn_risk_group(
        df=df_comb,
        col_risk_group=col_risk_group,
        field_age_at_event=col_age_at_event,
        field_bp_diastolic=col_diastolic,
        field_bp_systolic=col_systolic,
    )

    # Filter dates and format columns into preprocessed asset form
    df_comb = format_processed_htn_output(
        df=df_comb,
        fields_select=fields_final_output,
        field_bp_date=field_bp_date,
        filter_start_date=filter_start_date,
        col_primary_key=col_primary_key,
        fields_hashable=fields_hashable)

    # Return
    return df_comb