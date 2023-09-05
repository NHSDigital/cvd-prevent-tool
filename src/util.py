# Databricks notebook source
## util

## Description
# Notebook for containing generic utility functions (dataset agnostic)

# COMMAND ----------

## LIBRARY IMPORTS
import traceback
import inspect
from typing import List, Dict, Union, Optional
from functools import reduce
from operator import or_
from uuid import uuid4
import dateutil.parser
from dateutil.relativedelta import relativedelta
import pandas as pd
import io
import numpy as np
import pyspark.sql.functions as F
from pyspark.sql import DataFrame, Window
from pyspark.sql.types import TimestampType, StringType, DateType, IntegerType, ShortType, LongType, BooleanType
from pyspark.sql.functions import lit, col, unix_timestamp, avg as psavg
from pyspark.sql.window import Window
import itertools
import re

# COMMAND ----------

## TABLE FUNCTIONS
def create_table(df, db, table, overwrite=False, owner_db='prevent_tool_collab'):
  '''
  Create a table by writing the given data to disk.
  
  If the given db matches with the given owner_db then the owner of the created table is changed 
  to owner_db.
  '''
  asset_name = f'{db}.{table}'   
  if overwrite is True:
    df.write.saveAsTable(asset_name, mode='overwrite')
  else:
    df.write.saveAsTable(asset_name)
      
  if db == owner_db:
    spark.sql(f'ALTER TABLE {asset_name} OWNER TO {owner_db}')
  

def drop_table(db, table):
  spark.sql(f'drop table {db}.{table}')

  
def table_exists(db, table):
  tables = [r.tableName for r in spark.sql(f'show tables in {db}').select('tableName').collect()]
  return table in tables

# COMMAND ----------

## TYPE CHECKERS
def check_type(df, col_name, expected_types):
  '''
  Raises TypeError if column of given name in the dataframe is not the given type, or list of types.
  '''  
  if is_of_type(df, col_name, expected_types):
    return
  else:
    raise TypeError(f"Column '{col_name}' in given dataframe is not of expected type(s) {expected_types} ({get_type(df, col_name)})")


def get_type(df, col_name):
  '''Returns the type of the given column name in the given dataframe.'''
  if col_name[-1] == '.':
    raise ValueError(f"Given column name '{col_name}' is invalid.")
  return type(df.select(col_name).schema[col_name.split('.')[-1]].dataType)

    
def is_of_type(df, col_name, expected_types):
  '''
  Returns True if column of given name in the dataframe is of the given type, or list of types. 
  Returns False otherwise.
  '''
  if not isinstance(expected_types, list):
    expected_types = [expected_types]
    
  for t in expected_types:
    if not inspect.isclass(t):
      raise AttributeError('Given type must be a class (DateType) not an instance (DateType()).')
    if get_type(df, col_name) is t:
      return True
  else:
    return False
  
def ensure_short_type(df: DataFrame, col_name: str) -> DataFrame:
  '''Converts an integer, string, short or long type to a short.'''
  check_type(df, col_name, [IntegerType, StringType, ShortType, LongType])
  return df.withColumn(col_name, col(col_name).cast('short'))


## DATE TYPE

def ensure_date_type(df, col_name):
  '''Converts date and timestamp columns to date type.'''
  check_type(df, col_name, [DateType, TimestampType])
  return df.withColumn(col_name, col(col_name).cast('date'))

def dateValidateParse(dateObj:str):
  try:
       dateParsed = dateutil.parser.parse(dateObj)
  except:
    dateParsed = None
  return dateParsed

udf_dateValidateParse = udf(lambda z: dateValidateParse(z),returnType = TimestampType())

def ensure_date_type_from_str(df, col_name, str_format='', end_trim=0):
  '''
  Converts string type columns to dates.
  
  str_format: The format of the string date
  end_trim (optional): Optionally removes characters from the end of a string before converting to 
  a date using the given format. This is used to remove timezone information, or date/datetime strings 
  that are too long for pyspark to interpret.
  '''
  check_type(df, col_name, StringType)
  if end_trim > 0:
    df = df.withColumn(col_name, F.expr(f"substring({col_name}, 1, length({col_name})-{end_trim})"))
  df =  df.withColumn(col_name, udf_dateValidateParse(df[col_name]))
  return ensure_date_type(df, col_name)


# COMMAND ----------

## DATAFRAME FUNCTIONS
def rolling_average(df, time_col_name, rolling_average_name):
  '''
  Performs the rolling 7 day average on values in time_col_name column and adds the results to a 
  new columns with name given by rolling_average_name.
  '''
  df_grouped = df.groupBy(time_col_name).count()
  window_rolling_av = Window.orderBy(unix_timestamp(col(time_col_name))).rangeBetween(-6 * 86400, 0)
  df_rolling_average = df_grouped.withColumn(rolling_average_name, psavg('count').over(window_rolling_av))
  return df_rolling_average


def is_empty(df: DataFrame):
  return len(df.head(1)) == 0


def filter_latest(df: DataFrame, window_fields: List[str], date_field: str, new_col_name: Optional[str] = None):
  '''filter_latest
  
  Input:
    df (DataFrame): Dataframe of raw data
    window_fields (List[str]): list of fields to use in window function
    date_field (str): Field to calculate latest
    
  Output:
    df (DataFrame): Processed dataframe 
  '''
  window = Window.partitionBy(*window_fields)
  df = (
    df
    .withColumn(date_field, F.to_date(date_field))
    .withColumn(
      f'max_{date_field}',
      F.max(F.col(date_field)).over(window))
    .filter(F.col(date_field) == F.col(f'max_{date_field}'))
    .drop(f'max_{date_field}')
  )
  
  if new_col_name is not None:
    df = df.withColumnRenamed(date_field, new_col_name)
  return df


def filter_months(df: DataFrame, start_date_col: str, date_col: str, month_window: int = 12, before: bool = True):
  '''filter_prev_months
  Filters a column to restrict to a number of months before or after a date in start_date_col
  
  Input:
    df (DataFrame): Dataframe of raw data
    start_date_col str: Field to use as starting point for filter 
    date_col (str): Field to filter according to a point relative to start_date_col
    month_window (int): Number of months to filter 
    before (bool): If true, use months before, if false, use months after
    
  Output:
    df (DataFrame): Processed dataframe 
  '''
  if before:
    month_window = -(month_window)
  df_filtered = df.filter(F.col(date_col) > F.add_months(F.col(start_date_col), month_window))
  return df_filtered


# COMMAND ----------

## DATA IMPORT
def read_csv_string_to_df(csv_data):
  '''
  Given a big string of CSV data, load that data into a dataframe. The delimeter must be a comma. 
  Empty strings and (pandas) NaN values are replaced by None.
  '''
  pdf_csv_data = pd.read_csv(io.StringIO(csv_data), header=0, delimiter=',', dtype=str).replace(np.nan, '', regex=True)
  df_csv_data = spark.createDataFrame(pdf_csv_data)
  for col_name in df_csv_data.columns:
    df_csv_data = df_csv_data.withColumn(
      col_name, 
      F.when(col(col_name)==lit(''), None).otherwise(col(col_name)))
  return df_csv_data

# COMMAND ----------

## DATABASE PARAMATERS
def find_latest_cohort_table(db: str):
  df = spark.sql(f'SHOW TABLES IN {db}')
  pattern = "\\d{4}_\\d{2}_\\d{2}"
  df = df.select('tableName')\
         .where(F.col('tableName').like('%eligible_cohort%'))\
         .where(~F.col('tableName').like('%_tmp_%'))\
         .where(~F.col('tableName').like('%_journal_%'))\
         .withColumn('date',F.regexp_extract('tableName', pattern, 0))
  w  = Window.orderBy(F.col('date').desc())
  df = df.withColumn('rank',F.row_number().over(w))
  return df.filter(df.rank == 1).select('tableName').collect()[0][0]


def match_latest_journal_table(table_name: str, table_prefix: str = 'eligible_cohort', output_table_prefix: str = 'journal'):
  '''
  Returns the corresponding journal table to a given cohort table
  '''
  pattern = "^" + re.escape(table_prefix) + "_(\d{4}_\d{2}_\d{2})_(.*)"
  date = re.findall(pattern, table_name)[0][0]
  version = re.findall(pattern, table_name)[0][1]
  journal_table = f"{table_prefix}_{output_table_prefix}_{date}_{version}"
  return journal_table

# COMMAND ----------

## TYPE CONVERSION FUNCTIONS
def create_code_map(code_dict: Dict) -> Dict:
    """_summary_

    Args:
        code_dict (Dict): Mapping (outcome:list of icd10 codes) dictionary for ICD10 codes

    Returns:
        mapping_expr (map): Mapping object for primary code column to outcome flag
    """    
    ## KEY EXPANSION
    key_list = []
    for key in code_dict.keys():
        key_list.extend([key] * len(code_dict[key]))
    ## VALUES
    value_list = list(itertools.chain.from_iterable(code_dict.values()))
    ## DICTIONARY CREATION (SWAP VALUE:CODES TO KEY)
    expanded_dict = dict(zip(value_list,key_list))
    ## MAPPING OBJECT
    mapping_expr = F.create_map([F.lit(x) for x in itertools.chain(*expanded_dict.items())])

    return mapping_expr

  
def create_array_field(df: DataFrame, array_field_name: str, array_value_fields: List[str], drop_value_fields: bool = True) -> DataFrame:
  '''create_array_field
  Creates a new field containing an array combining fields
  Input:
    df (DataFrame): Dataframe of raw data
    array_field_name (str): Name of new array column
    array_value_fields: List[str]: List of fields to combine to make array 
    drop_value_fields: bool: Drops original fields 
  
  Output:
    (DataFrame): DataFrame including new array field
    
  '''
  array_value_dtypes = [df.schema[field_name].dataType for field_name in array_value_fields]
  if len(list(set(array_value_dtypes))) > 1:
    raise TypeError("Columns are not of same datatype")
  df_with_array = (
    df.withColumn(array_field_name, F.array(*[F.col(field_name) for field_name in array_value_fields]))
  )
  if drop_value_fields:
    return df_with_array.drop(*array_value_fields)
  else: return df_with_array
  
def split_str_to_array(df: DataFrame, col_name: str, delimiter: str = ',') -> DataFrame:
  """split_str_to_array
  Splits string values in column into an array with each item being the string elements split
  on the delimiter.

  Args:
      df (DataFrame): Dataframe of raw data
      col_name (str): Column name of the field containing splittable strings
      delimiter (str, optional): Delimiter used to seperate string items. Defaults to ','.

  Returns:
      DataFrame: Dataframe with column (col_name) containing array of string items.
  """  
  check_type(df, col_name, StringType)
  df = df.withColumn(col_name,F.split(F.col(col_name), delimiter))
  return df

# COMMAND ----------

## DATAFRAME COLUMN REPLACEMENTS
  
def replace_value_none_lambda(column: str, values: List[str]):
    '''
    Keeps column values [column] when they do not match items in a list [values] - otherwise replaces with None.
    '''
    return F.when(column.isin(values), F.lit(None)).otherwise(column)

def replace_value_none(df: DataFrame, col_names: Union[List[str], str], filter_values: Union[List[str], str]) -> DataFrame:
    '''
    Keeps column values [col_names] when they do not match items in list [filter_values] - otherwise replaces with None.
    See replace_value_none_multi_lambda
    '''
    if type(col_names) == str:
      col_names = [col_names]
    if type(filter_values) == str:
      filter_values = [filter_values]
    reduce_list = [df] + col_names
    df = reduce(lambda d, c: d.withColumn(c, replace_value_none_lambda(F.col(c), filter_values)), reduce_list)
    return df

# COMMAND ----------

## DATE OF BIRTH
def ensure_dob(df: DataFrame, col_name: str, dob_col: str, ts_col: str, max_age_year: int) -> DataFrame:
  '''
  Ensure a date or timestamp type is a date of birth. The birth date must be after the given 
  timestamp, and cannot be more than 130 years from the timestamp. Bad birth dates are replaced by 
  null values.
  '''
  check_type(df, ts_col, [DateType, TimestampType])
  df = ensure_date_type(df, dob_col)
  return df.withColumn(col_name, F.when((F.datediff(col(ts_col), col(dob_col)) / (365.25)) > max_age_year, None).when(col(dob_col) > col(ts_col), None).otherwise(col(dob_col)))


def ensure_dob_from_str(df: DataFrame, col_name: str, dob_col: str, ts_col: str, str_format: str, end_trim=0, max_age_year: int = 130):
  '''See ensure_dob and ensure_date_type_from_str'''
  check_type(df, dob_col, StringType)
  temp_dob_col = f'_dob_ensure_dob_from_str_{uuid4().hex}'
  df = ensure_date_type_from_str(df.withColumn(temp_dob_col, col(dob_col)), temp_dob_col, str_format, end_trim=end_trim)
  return ensure_dob(df, col_name, temp_dob_col, ts_col, max_age_year).drop(temp_dob_col)

# COMMAND ----------

## AGE FUNCTIONS
def ensure_age(df: DataFrame, col_name: str, age_col: str) -> DataFrame:
  '''
  Ensure a string, integer, short or long type is an age by converting it to a short and making sure 
  it is greater than or equal to zero. Values less than zero are replaced by null.
  '''
  check_type(df, age_col, [StringType, IntegerType, ShortType, LongType])
  df = ensure_short_type(df, age_col)
  df = df.withColumn(col_name, F.when(df[age_col] >= 0, df[age_col]).otherwise(None))
  return df

age_at_date = lambda dob, at_date: F.when(F.col(dob).isNull(), None).when(F.col(at_date).isNull(), None).when(F.col(dob) > F.col(at_date), None).otherwise((F.datediff(F.col(at_date), F.col(dob)) / 365.25).cast('bigint'))

def add_age_from_dob_at_date(df: DataFrame, col_name: str, dob_col: str, at_date_col, max_age_year: int = 130):
  '''
  Add age to the dataframe based on the given date of birth and at the given date. 
  See also ensure_dob, ensure_age.
  '''
  check_type(df, dob_col, DateType)
  check_type(df, at_date_col, [DateType, TimestampType])
    
  temp_dob_col = '_dob_add_age_from_dob_at_date'
  df = ensure_dob(df, temp_dob_col, dob_col=dob_col, ts_col=at_date_col, max_age_year = max_age_year)
    
  temp_age_col = '_age_add_age_from_dob_at_date'
  df_wage = df.withColumn(temp_age_col, age_at_date(temp_dob_col, at_date_col)).drop(temp_dob_col)
      
  return ensure_age(df_wage, col_name, age_col=temp_age_col).drop(temp_age_col)

# COMMAND ----------

def remove_elements_after_date(df_to_filter: DataFrame, df_dates: DataFrame, date_to_compare: str, max_date: str, link: List[str], keep_nulls: bool = False, keep_inclusive: bool = False):
  
  """
  INPUTS: df_to_filter    -> the dataframe we want to filter dates on, and remove any dates after max_date
          df_dates        -> the dataframe which provides the max dates to filter by
          date_to_compare -> the column we want to compare
          max_date        -> any dates compared after this are filtered out
          link            -> the column upon which the data is linked (usually nhs_number and date_of_birth)
          keep_nulls      -> if nulls are df_dates, keep them if true (especially important when looking at date of death, as a patient may not have died.)
          keep_inclusive  -> if True, filter after max_date. When false, inclusive filtereing (including and from max_date)
  """
  
  dates_to_filter_by = df_dates.select(*link , max_date)
  
  df_to_remove_joined = df_to_filter.join(dates_to_filter_by, link, 'left')
  
  if (keep_nulls):
    if (keep_inclusive):
      filtered_df = df_to_remove_joined.filter((F.col(date_to_compare) <= F.col(max_date)) | F.col(max_date).isNull())
    else:
      filtered_df = df_to_remove_joined.filter((F.col(date_to_compare) < F.col(max_date)) | F.col(max_date).isNull())
  else:
    if (keep_inclusive):
      filtered_df = df_to_remove_joined.filter((F.col(date_to_compare) <= F.col(max_date)))
    else:
      filtered_df = df_to_remove_joined.filter((F.col(date_to_compare) < F.col(max_date)))
  
  return filtered_df

# COMMAND ----------

def add_under_age_flag (df: DataFrame, max_age: int, start_date: str, end_date: str, flag_col_name:str, flag_name: str, extra_column_check: str = None, keep_non_nulls: bool = False) -> DataFrame:

  """
  INPUTS :
    df                 -> dataframe to be used
    max_age            -> age to be checked against
    start_date         -> date to start the calculation (if age, usually will be date_of_birth)
    end_date           -> date to end the calculation (if age, will usually be date_of_death)
    flag_col_name      -> what to call the flag column
    
    extra_column_check -> this can be used as an extra check, for example if you want to get a flag under x years 
                          but also require another column to not be null, such asif a person has died, but you only
                          want to count them if they have had a certain illness, for example the death flag is not null.
                          
    keep_non_nulls     -> bool specifying whether to use the extra column check or not
    
  Adds flag for when patient is under specified years old for start date and end date.
  """ 
    
  df_filtered = df.withColumn('tmp_age', age_at_date(start_date,end_date)) #calls util function 
  
  if (keep_non_nulls):
    df_filtered = df_filtered.withColumn(flag_col_name, F.when( (col(extra_column_check).isNotNull()) & (col('tmp_age') < max_age), flag_name))
  else: 
    df_filtered = df_filtered.withColumn(flag_col_name, F.when((col('tmp_age') < max_age), flag_name))

  return df_filtered.drop('tmp_age')

# COMMAND ----------

def union_multiple_dfs(dfs: List[DataFrame]) -> DataFrame:
  """
  Unions multiple dataframes.
  INPUTS:
    dfs (List[DataFrame])  -> list of dataframes to union

  """
  return reduce(DataFrame.union, dfs)
