# Databricks notebook source
# MAGIC %run ../../params/params

# COMMAND ----------

# MAGIC %run ../util

# COMMAND ----------

import pyspark.sql.functions as F
from itertools import chain

# COMMAND ----------

selected_flags = {
    flag: values
    for flag, values in params.DISEASE_FLAG_DICTIONARY.items()
    if flag in params.FLAGS_TO_PROCESS
}

mapping_dict = {}
res_dict = {}

for i in selected_flags.keys():
    for a in selected_flags[i]["diag_code"]:
        mapping_dict[a] = i
    if selected_flags[i]["res_code"]:
        res_dict[selected_flags[i]["res_code"][0]] = i

mapping_expr = F.create_map([F.lit(x) for x in chain(*mapping_dict.items())])
res_mapping = F.create_map([F.lit(x) for x in chain(*res_dict.items())])

# COMMAND ----------

def store_initial_patients(
    df: DataFrame,
    field_pid: str = params.PID_FIELD,
    field_dob: str = params.DOB_FIELD
    ):
    """store_initial_patients()
    
    Stores the patients from the journal table prior to any diagnostic flag processing. This
    is saved as a dataframe, used in a final join at the end of the preprocessing function.

    Args:
        df (DataFrame): INput dataframe of journal table (prior to any preprocessing steps).
        field_pid (str, optional): Column name containing person id/NHS Number. Defaults to params.PID_FIELD.
        field_dob (str, optional): Column name containing date of birth. Defaults to params.DOB_FIELD.

    Returns:
        df (DataFrame): Dataframe of person_id and birth_date, each row is a distinct combination.
    """
    # Obtain distinct patients
    df = df.select(field_pid,field_dob).distinct()
    # Return
    return df

# COMMAND ----------

def map_diagnosis_codes(df: DataFrame) -> DataFrame:
    df = df.withColumn(
        "diagnosis",
        F.when(
            (F.col(params.REF_CLUSTER_FIELD).isin(list(mapping_dict.keys()))),
            mapping_expr[F.col(params.REF_CLUSTER_FIELD)],
        )
        .when(
            (F.col(params.CVDP_CODE_FIELD).isin(list(mapping_dict.keys()))),
            mapping_expr[F.col(params.CVDP_CODE_FIELD)],
        )
        .when(
            (
                ~(F.col(params.REF_CLUSTER_FIELD).isin(list(mapping_dict.keys())))
                & (
                    ~(F.col(params.CVDP_CODE_FIELD).isin(list(mapping_dict.keys())))
                    & (F.col(params.REF_CLUSTER_FIELD).isin(list(res_dict.keys())))
                )
            ),
            F.col(params.REF_CLUSTER_FIELD),
        )
        .otherwise(None),
    )

    return df.filter(F.col("diagnosis").isNotNull())


def get_max_date(df: DataFrame) -> DataFrame:
    """
    gets the max date for each 'diagnosis', this will get the max date for each set of cluster IDs together and for each Resolution code
    """

    dedupe_window = Window.partitionBy(
        params.PID_FIELD, params.DOB_FIELD, "diagnosis"
    )  ## list of columns

    return df.withColumn(
        "max_date", F.max(F.col(params.CVDP_JOURNAL_DATE_FIELD)).over(dedupe_window)
    )


def create_parent_max_date(df: DataFrame) -> DataFrame:
    """
    Links together the diagnosis with the resolution codes and gets the max date from that.
    """

    mapped = df.withColumn(
        "parent",
        F.when(
            (F.col("diagnosis").isin(list(res_dict.keys()))),
            res_mapping[F.col("diagnosis")],
        ).otherwise(F.col("diagnosis")),
    )

    dedupe_window = Window.partitionBy(
        params.PID_FIELD, params.DOB_FIELD, "parent"
    )  ## list of columns

    return mapped.withColumn(
        params.CVDP_MAX_DIAGNOSIS_DATE, F.max(F.col("max_date")).over(dedupe_window)
    )


def filter_res_dates(df: DataFrame) -> DataFrame:
    """
    Filters out diagnosis dates that are less than the max (meaning the max must have been after it), or remove max dates that are a resolved code
    """
    return df.filter(
        (F.col("max_date") == F.col(params.CVDP_MAX_DIAGNOSIS_DATE))
        & (
            (F.col(params.REF_CLUSTER_FIELD).isin(list(mapping_dict.keys())))
            | (F.col(params.CVDP_CODE_FIELD).isin(list(mapping_dict.keys())))
        )
    )


def split_diagnosis_dates(df: DataFrame) -> DataFrame:
    """
    Pivots on the diagnosis to get a table with columns for each diagnosis date,
    """

    return (
        df.groupBy(params.PID_FIELD, params.DOB_FIELD)
        .pivot("diagnosis")
        .agg(F.max(F.col(params.CVDP_JOURNAL_DATE_FIELD)))
        .drop("null")
    )


# COMMAND ----------


def filter_by_age_range(df: DataFrame) -> DataFrame:

    """
    Filters patients by age.
    """

    df_filtered = df.withColumn(
        "age_at_event", age_at_date(params.DOB_FIELD, params.CVDP_JOURNAL_DATE_FIELD)
    )

    return df_filtered.filter(
        (F.col("age_at_event") >= 16) & (F.col("age_at_event") <= 120)
    ).drop("age_at_event")


# COMMAND ----------

def ensure_cols(df: DataFrame) -> DataFrame:
  for column in params.FLAGS_TO_PROCESS:
    if column not in df.columns:
      df = df.withColumn(f'{column}', F.lit(None).cast('date'))
    df = df.withColumnRenamed(column, f'{column}_{params.DICT_FLAG_SUFFIX}')
  return df

# COMMAND ----------

def get_diagnostic_codes(df: DataFrame) -> DataFrame:

    df = map_diagnosis_codes(df)

    df = get_max_date(df)

    df = create_parent_max_date(df)

    df = filter_res_dates(df)

    return split_diagnosis_dates(df)

# COMMAND ----------

def join_to_initial_patients(
    df_initial: DataFrame,
    df_flags: DataFrame,
    field_pid: str = params.PID_FIELD,
    field_dob: str = params.DOB_FIELD
    ):
    """join_to_initial_patients()
    
    Joins the final diagnostic lib table to the initial patient table (output of 
    store_initial_patients()) to produce the final table. This ensures that individuals
    with no diagnosis date recorded (e.g. resolved date) are accounted for.

    Args:
        df_initial (DataFrame): DataFrame output of store_initial_patients().
        df_flags (Dataframe): DataFrame output of preprocessing function.
        field_pid (str, optional): Column name containing person id/NHS Number. Defaults to params.PID_FIELD.
        field_dob (str, optional): Column name containing date of birth. Defaults to params.DOB_FIELD.

    Returns:
        df (DataFrame): DataFrame containing all initial individuals, alongside their updated diagnosis
            dates.
    """ 
    # Join to original dataframe
    df = df_initial.join(df_flags, [field_pid,field_dob], 'left')
    # Return dataframe
    return df

# COMMAND ----------

def preprocess_diagnostic(df: DataFrame, field_pid: str = params.PID_FIELD, field_dob: str = params.DOB_FIELD) -> DataFrame:
    df_initial = store_initial_patients(df,field_pid,field_dob)
    df = filter_by_age_range(df)
    flags_df = get_diagnostic_codes(df)
    flags_df = ensure_cols(flags_df)
    flags_df = join_to_initial_patients(df_initial,flags_df,field_pid,field_dob)
    return flags_df