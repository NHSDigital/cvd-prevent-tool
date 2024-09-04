# Databricks notebook source
## create_events_table_lib

## Overview
# Class definitions for use in create_events_table_lib

# COMMAND ----------

from dataclasses import dataclass
from typing      import Any, Callable, List, NewType, Union, Tuple
from pyspark.sql import DataFrame, Column
from functools   import partial

import pyspark.sql.functions as F
import pyspark.sql.types as T

# COMMAND ----------

# MAGIC %run ../src/util

# COMMAND ----------

### FUNCTION DEFINITIONS ###
def ensure_england_lsoa(df: DataFrame, col_name: str = params.LSOA_FIELD) -> DataFrame:
    '''
    Removes LSOA values that do not start with 'E', thus selecting only records with an English LSOA.
    Raises an error if the input column is not a string. Null LSOA values are accepted and not dropped.

    df: input dataframe
    col_name: LSOA field name in the given dataframe.
    '''
    check_type(df, col_name, T.StringType)
    return df.where(F.substring(F.col(col_name), 1, 1) == F.lit('E'))\
        .where(F.col(col_name).isNotNull())

def ensure_int_type(df: DataFrame, col_name: str) -> DataFrame:
    '''
    Ensure the field type in the given colum name and datafame is an integer. If the type is any of
    integer, string, short, long or double, the type will be cast to integer. If the input type is none
    of these types then an error is raised.
    '''
    check_type(df, col_name, [T.IntegerType, T.StringType, T.ShortType, T.LongType, T.DoubleType])
    return df.withColumn(col_name, F.col(col_name).cast('int'))

def ensure_str_type(df: DataFrame, col_names: Union[List[str], str]) -> DataFrame:
    '''
    Ensure the field type in the given column name and dataframe is a string.
    '''
    if type(col_names) == str:
        col_names = [col_names]
    for col_name in col_names:
        df = df.withColumn(col_name, F.col(col_name).cast(T.StringType()))
    return df

def _run_field_mapping(df: DataFrame, field_name: str, mapping_value: Any) -> DataFrame:
    if isinstance(mapping_value, str):
        df = df.withColumnRenamed(mapping_value, field_name)
    elif isinstance(mapping_value, tuple):
        col_name, func = mapping_value
        df = func(df, col_name)
        df = df.withColumnRenamed(col_name, field_name)
    else:
        df = df.withColumn(field_name, F.lit(mapping_value))
    return df

def format_source_data_to_combined_schema(data_entry, df: DataFrame, output_field_list: List[str]) -> DataFrame:
    '''
    Run over the source dataset dataframe and apply the necessary transformations to change the data into
    the event table schema so that it can be combined with other source datasets. The given instance of
    EventsStageDataEntry contains all the information necessary to convert a source dataset into
    the event table schema. The mappings are run in the order that they are defined in the
    EventsStageColumnMapping iterator, thus the value from an earlier mapping can be used in
    a later mapping, as the newly mapped value will exist. Although the order is fixed and cannot be
    changed by the user.

    See EventsStageDataEntry and EventsStageColumnMapping.

    data_entry: An instance of EventsStageDataEntry.
    df: dataframe of source data.
    output_field_list: List of column names to select from the final transformed asset. Typically the keys of
    the mappings dict. These must match the keys in the mapping dict as those are the new column names.
    '''
    mapping = data_entry.mapping

    for key, value in mapping:
        df = _run_field_mapping(df, key, value)

    return df.select(output_field_list)

# COMMAND ----------

### CLASS DEFINITIONS ###

EVENT_STAGE_MAPPING_TYPE = NewType('EVENT_STAGE_MAPPING_TYPE', Union[str, Tuple, Callable, Column])

@dataclass(frozen=True)
class EventsStageColumnMapping():
    '''
    This dataclass contains all information needed to map a preprocessed and cleaned (see
    EventsStageDataEntry) source data asset into the event table schema, such that many source
    datasets can then be union together.

    See also 'format_source_data_to_combined_schema'.

    The keys of the dataclass are the target column names, i.e. the event table schema column names. The
    values of the dataclass are one of three options. The first is a single string value, where the value
    is the column name in the source datset. The code in 'format_source_data_to_combined_schema' will
    map that source column name to the event table schema without applying transformations. The second
    option is that value can be a single literal value (lit('sgss')) where the code in
    'format_source_data_to_combined_schema' will set the value of every record in the event table schema
    to be the literal value. The third option is a tuple of (string, Callable), where the Callable has
    signature Func(DataFrame, string). This allows the user to apply functions to format the data. Notice
    that many of the functions in this file, and in src/util have the required signature such that they
    can be used as mapping functions here. The zeroth value in the tuple is passed into the Callable as
    the first argument.

    For example, if you want to run 'convert_mf_sex_to_code' on column 'input_sex' to
    transform it to the event table schema 'sex' column, the mapping entry will be:

        ...
        sex_field = ('input_sex', convert_mf_sex_to_code),
        ...

    which behind the scenes will run:

        df = convert_mf_sex_to_code(df, 'input_sex').

    An iterator is supplied with the class that defines a fixed order in which the values are returned,
    and is used by 'format_source_data_to_combined_schema' to define the order in which the mapping are
    applied.

    '''
    pid_field: EVENT_STAGE_MAPPING_TYPE
    dob_field: EVENT_STAGE_MAPPING_TYPE
    age_field: EVENT_STAGE_MAPPING_TYPE
    sex_field: EVENT_STAGE_MAPPING_TYPE
    dataset_field: EVENT_STAGE_MAPPING_TYPE
    category_field: EVENT_STAGE_MAPPING_TYPE
    record_id_field: EVENT_STAGE_MAPPING_TYPE
    record_start_date_field: EVENT_STAGE_MAPPING_TYPE
    record_end_date_field: EVENT_STAGE_MAPPING_TYPE
    lsoa_field: EVENT_STAGE_MAPPING_TYPE
    ethnicity_field: EVENT_STAGE_MAPPING_TYPE
    code_field: EVENT_STAGE_MAPPING_TYPE
    flag_field: EVENT_STAGE_MAPPING_TYPE
    code_array_field: EVENT_STAGE_MAPPING_TYPE
    flag_array_field: EVENT_STAGE_MAPPING_TYPE
    assoc_record_id_field: EVENT_STAGE_MAPPING_TYPE

    def __iter__(self):
        field_mapping = {
            params.PID_FIELD: self.pid_field,
            params.DOB_FIELD: self.dob_field,
            params.AGE_FIELD: self.age_field,
            params.SEX_FIELD: self.sex_field,
            params.DATASET_FIELD: self.dataset_field,
            params.CATEGORY_FIELD: self.category_field,
            params.RECORD_ID_FIELD: self.record_id_field,
            params.RECORD_STARTDATE_FIELD: self.record_start_date_field,
            params.RECORD_ENDDATE_FIELD: self.record_end_date_field,
            params.LSOA_FIELD: self.lsoa_field,
            params.ETHNICITY_FIELD: self.ethnicity_field,
            params.CODE_FIELD: self.code_field,
            params.FLAG_FIELD: self.flag_field,
            params.CODE_ARRAY_FIELD: self.code_array_field,
            params.ASSOC_FLAG_FIELD: self.flag_array_field,
            params.ASSOC_REC_ID_FIELD: self.assoc_record_id_field
        }
        for tup in field_mapping.items():
            yield tup


# COMMAND ----------

### CLASS DEFINITIONS ###
@dataclass(frozen=True)
class EventsStageDataEntry():

    dataset_name: str
    context_key: str

    processing_func: Callable = None
    mapping: EventsStageColumnMapping = None