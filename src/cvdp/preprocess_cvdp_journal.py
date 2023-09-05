# Databricks notebook source
## preprocessing_cvdp_journal

# COMMAND ----------

# MAGIC %run ../../params/params

# COMMAND ----------

# MAGIC %run ../util

# COMMAND ----------

## MODULE IMPORTS
import pyspark.sql.functions as F

from pyspark.sql import DataFrame, Window
from typing import List, Union

# COMMAND ----------

## Explode Journal Columns
def cvdp_explode_journal_columns(
    df: DataFrame, 
    fields_select: List[str] = params.CVDP_JOURNAL_INPUT_COLUMNS, 
    field_explode: str = params.CVDP_JOURNAL_FIELD,
    field_explode_select: List[str] = params.CVDP_JOURNAL_EXPLODE_COLUMNS
) -> DataFrame:
    """cvdp_explode_journal_columns
    Processes the journal table column and explodes the nested array to produce the full journal table
    data. 

    Args:
        df (DataFrame): DataFrame containing the journal table array column.
        fields_select (List[str], optional): List of fields to subset from the DataFrame, forms the base journal table.
            Defaults to params.CVDP_JOURNAL_INPUT_COLUMNS.
        field_explode (str, optional): Column containing the journal table entries. 
            Defaults to params.CVDP_JOURNAL_FIELD.
        field_explode_select (List[str], optional): Columns within the journal table array to select and extract. 
            Defaults to params.CVDP_JOURNAL_EXPLODE_COLUMNS.

    Returns:
        DataFrame: DataFrame with addded journal table entries as defined by function arguments. 
    """    
    ## Subset and Explode Struct Column in DataFrame
    fields_full_select = fields_select + [field_explode]
    df = (
        df.withColumn(field_explode, F.explode(field_explode))
        .select(fields_full_select)
    )
    ## Subset flattend dataframe using {field_explode}.{field_explode_select[x]} elements
    cols_exploded = [f'{field_explode}.{x}' for x in field_explode_select]
    cols_select = fields_select + cols_exploded
    df = df.select(cols_select)
    ## Return
    return df

## Create Journal Array
def cvdp_create_journal_array(
    df: DataFrame,
    field_array_code: str = params.CODE_ARRAY_FIELD,
    field_array_values: Union[List[str], str] = [params.CVDP_CODE_VALUE1_FIELD, params.CVDP_CODE_VALUE2_FIELD]) -> DataFrame:
    """cvdp_create_journal_array
    Creates a new field containing an array combining fields from the eligible cohort table. 

    Args:
        df (DataFrame): DataFrame output from preprocess_cvdp_cohort (see src.cvdp::preprocess_cvdp_cohort)
        field_array_code (str, optional): Name of new array column. Defaults to params.CODE_ARRAY_FIELD
        field_array_values (Union[List[str], str], optional): List of fields to combine to make array. 
            Defaults to [params.CVDP_CODE_VALUE1_FIELD, params.CVDP_CODE_VALUE2_FIELD]

    Returns:
        DataFrame: DataFrame including new array field
    """    
    ## Type convert: supplied columns
    if type(field_array_values) == str:
        field_array_values = [field_array_values]
    ## Create array
    df = create_array_field(df = df, array_field_name = field_array_code, array_value_fields = field_array_values)
    ## Return
    return df

## Deduplicate journal entries
def cvdp_deduplicate_journal_entries(
    df: DataFrame,
    fields_window_partition: List[str] = params.CVDP_JOURNAL_DEDUPLICATION_FIELDS,
    field_window_order: str = params.CVDP_EXTRACT_DATE,
    order_descending_date: bool = params.SWITCH_CVDP_JOURNAL_ORDER_DESC
) -> DataFrame:
    """cvdp_deduplicate_journal_entries
    Deduplicates the exploded and array-formatted journal table on partitions, and orders the deduplication by the window order 
    argument. By default, this keeps the latest journal entry (code) and associated values (value 1 and value 2), per extraction 
    date - keeping the latest journal record for that particular entry. This ensures that if journal records have been updated 
    in a later extract, only that record is retained.

    Args:
        df (DataFrame): Dataframe containing the mid-processed (exploded and array-formatted) journal table.
        fields_window_partition (List[str], optional): List of column names to partition the window (deduplication) on.
                                            Defaults to params.CVDP_JOURNAL_DEDUPLICATION_FIELDS.
        field_window_order (str, optional): Column name for a date containing cilumn to order the deduplication on.
                                            Defaults to params.CVDP_EXTRACT_DATE.
        order_descending_date (bool, optional): Boolean switch - order date column for window (True = descending order, latest first).
                                            Defaults to params.SWITCH_CVDP_JOURNAL_ORDER_DESC.

    Returns:
        DataFrame: Journal table with entries deduplicated on partition fields, ordered by date column to yield a single row for values 
                    in the partition fields.
    """    
    
    # Create deduplication window (partition and order)
    # > Conditional: order_descending_date == True
    if order_descending_date == True:
        windowval = Window.partitionBy(fields_window_partition).orderBy(F.col(field_window_order).desc())
    else:
        windowval = Window.partitionBy(fields_window_partition).orderBy(F.col(field_window_order).asc())

    # Deduplicate journal entries
    df = (
        df
        .withColumn('rank', F.rank().over(windowval))
        .filter(F.col('rank') == 1)
        .drop('rank')
    )
    
    # Return dataframe
    return df

## Load REF Data
def load_ref_codes(
    db: str = params.DSS_CORPORATE_DATABASE, 
    table: str = params.REF_CODES_TABLE, 
    active_field: str = params.REF_ACTIVE_FIELD,
    output_fields: List[str] = [params.REF_CODE_FIELD, params.REF_CLUSTER_FIELD]
) -> DataFrame:
    '''load_ref_codes
    Creates a dataframe containing the SNOMED codes and all classifications.

    Args:
        db (str): Database where the codes can be found. Defaults to params.DSS_CORPORATE_DATABASE.
        table (str): Table where the codes can be found. Defaults to params.REF_CODES_TABLE.
        active_field (str): Field to filter out inactive references. Defaults to params.REF_ACTIVE_FIELD.
        output_fields (List[str]): List of output fields to select from the final output DataFrame. 
            Defaults to [params.REF_CODE_FIELD, params.REF_CLUSTER_FIELD]

    Returns:
        df (DataFrame): Processed dataframe 
    '''
    ## Load Reference Codes
    ref_codes = spark.table(f'{db}.{table}')
    ref_codes = ref_codes.filter(F.col(active_field) == 1)
    ## Return
    return ref_codes.select(output_fields)

## Join REF Data
def join_ref_data(
    df: DataFrame, 
    join_field: str = params.CVDP_CODE_FIELD, 
    ref_join_field: str = params.REF_CODE_FIELD
) -> DataFrame:
    '''join_ref_data
    Loads ref data and joins on to dataframe

    Args:
        df (DataFrame): Data to join reference data to
        join_field (str): Field from df, to join onto ref dataframe. Defaults to params.CVDP_CODE_FIELD.
        ref_join_field (str): Field from reference data, to join to df. Defaults to params.REF_CODE_FIELD.

    Returns:
        df (DataFrame): Processed dataframe 
    '''
    ## Load REF Data
    ref_codes = load_ref_codes()
    ## Join Data
    joined_df = df.join(
        ref_codes,
        df[join_field] == ref_codes[ref_join_field],
        "left"
    )
    ## Return
    return joined_df.drop(ref_join_field)

## Main Preprocessing Function*
def preprocess_cvdp_journal(
    df: DataFrame,
    fields_cohort_window: List[str] = [params.CVDP_PID_FIELD, params.CVDP_DOB_FIELD],
    field_extract_date: str = params.CVDP_EXTRACT_DATE,
    add_ref_data: bool = params.SWITCH_CVDP_JOURNAL_ADD_REF_DATA,
    fields_select: str = params.CVDP_JOURNAL_INPUT_COLUMNS, 
    field_explode: str = params.CVDP_JOURNAL_FIELD,
    field_explode_select: str = params.CVDP_JOURNAL_EXPLODE_COLUMNS,
    field_array_code: str = params.CODE_ARRAY_FIELD,
    field_array_values: List[str] = [params.CVDP_CODE_VALUE1_FIELD, params.CVDP_CODE_VALUE2_FIELD],
    join_field: str = params.CVDP_CODE_FIELD, 
    ref_join_field: str = params.REF_CODE_FIELD,
    fields_window_partition: List[str] = params.CVDP_JOURNAL_DEDUPLICATION_FIELDS
) -> DataFrame:
    """preprocess_cvdp_journal
    Processes the eligible cohort table (from src.cvdp::preprocess_cvdp_cohort) and converts into a journal table

    Args:
        df (DataFrame): DataFrame containing the eligible patient cohort data (see src.cvdp::preprocess_cvdp_cohort)
        fields_cohort_window (List[str], optional): Column names for fields containing patient information to window for
                                                    filtering latest extract.
                                                    Defaults to [params.CVDP_PID_FIELD, params.CVDP_DOB_FIELD].
        field_extract_date (str, optional): Column name for field field containing the extract date of the CVDP cohort event.
                                                    Defaults to params.CVDP_EXTRACT_DATE.
        (**args): Default function arguments as defined in params.params_util.

    Returns:
        DataFrame: Dataframe containing the processed journal table from eligible patient cohort data
    """    
    ## Explode Nested Structure
    df = cvdp_explode_journal_columns(df,fields_select,field_explode,field_explode_select)
    ## Create Journal Array
    df = cvdp_create_journal_array(df,field_array_code,field_array_values)
    ## Deduplicate journal entries
    df = cvdp_deduplicate_journal_entries(df,fields_window_partition,field_extract_date)
    ## Conditional: Add REF Data
    if add_ref_data == True:
        df = join_ref_data(df,join_field,ref_join_field)
    ## Return
    return df