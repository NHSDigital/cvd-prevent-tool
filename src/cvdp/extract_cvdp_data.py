# Databricks notebook source
# extract_cvdp_data

# COMMAND ----------

# MAGIC %run ../../params/params

# COMMAND ----------

# MAGIC %run ../util

# COMMAND ----------

# Extract Data Process
def process_cvdp_extract(
    cvdp_annual: DataFrame,
    cvdp_quarterly: DataFrame,
    field_cohort: str=params.CVDP_COHORT_FIELD,
    filter_cohort_codes: List[str]=params.CVDP_COHORT_CODES,
    col_hashed_key: str=params.CVDP_PRIMARY_KEY,
    fields_to_hash: List[str]=params.CVDP_HASHABLE_FIELDS,
) -> DataFrame:
    """process_cvdp_extract

    Processes the extracted CVDP annual and quaterly tables to produce a cohort filtered, combined
    dataframe. Dataframe also has additional column of primary key, generated from hashing a list
    of columns.

    Args:
        cvdp_annual (DataFrame): Dataframe loaded from CVDP annual data.
        cvdp_quarterly (DataFrame): Dataframe loaded from CVDP quarterly data.
        field_cohort (str, optional): Column name containing cohort codes.
            Defaults to params.CVDP_COHORT_FIELD.
        filter_cohort_codes (List[str], optional): List of cohort codes to filter on(keep).
            Defaults to params.CVDP_COHORT_CODES.
        col_hashed_key (str, optional): Column name for additional column containing primary key.
            Defaults to params.CVDP_PRIMARY_KEY.
        fields_to_hash (List[str], optional): Column names to hash for primary key.
            Defaults to params.CVDP_HASHABLE_FIELDS.

    Returns:
        DataFrame: CVDP combined dataframe, filtered on cohorts with primary (hashed) key.
    """
    # Filter eligible cohorts
    cvdp_annual     = filter_col_by_values(cvdp_annual, field_cohort, filter_cohort_codes)
    cvdp_quarterly  = filter_col_by_values(cvdp_quarterly, field_cohort, filter_cohort_codes)
    # Create combined asset with primary key
    cvdp_combined   = union_multiple_dfs([cvdp_annual,cvdp_quarterly])
    cvdp_combined   = add_hashed_key(cvdp_combined,col_hashed_key,fields_to_hash)
    # Return
    return cvdp_combined