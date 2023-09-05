# Databricks notebook source
## clean_dataset
##
## Overview
## Provide functions for the cleaning and processing of datasets

# COMMAND ----------

## LIBRARY IMPORTS
from typing import List, Dict, Callable
from uuid import uuid4

from dsp.udfs import NHSNumber

import pyspark.sql.functions as F
from pyspark.sql import DataFrame

# COMMAND ----------

# MAGIC %run ./util

# COMMAND ----------

def clean_and_preprocess_dataset(df: DataFrame, 
                                 nhs_number_fields: List[str], 
                                 clean_null_fields: List[str], 
                                 replace_empty_str_fields: List[str]=None, 
                                 preprocessing_func: Callable[[DataFrame], DataFrame]=None, 
                                 validate_nhs_numbers: bool=True) -> DataFrame:
  '''
  This function takes the raw source dataset, runs the given preprocessing function to do dataset 
  specific cleaning, then cleans the columns based on the given list of fields.
   
  df:                       Raw source data.
  nhs_number_fields:        A list of fields that represent NHS numbers. See 'validate_nhs_numbers'
  clean_null_fields:        A list of fields that are cleaned for null values. Null values are removed 
                            along with strings, '', 'null', 'NULL', 'Null'. Records failing this check
                            are dropped.
  replace_empty_str_fields: A list of fields in which empty string ('') is to be replace by None.
  preprocessing_func:       A function with signature Func(DataFrame) -> DataFrame. It can be used to run 
                            source dataset specific cleaning and deduplication.
  validate_nhs_numbers:     If True the list of NHS number fields in 'nhs_number_fields' will be run
                            through a function that validates NHS numbers. If False, nothing is done 
                            with those fields. Records failing this check are dropped.
  '''
  if not preprocessing_func is None:
    df = preprocessing_func(df)
  
  for col_name in clean_null_fields:
    df = df.where((F.col(col_name) != F.lit('')) 
                  & (F.col(col_name) != F.lit('null')) 
                  & (F.col(col_name) != F.lit('NULL')) 
                  & (F.col(col_name) != F.lit('Null')))
  
  if validate_nhs_numbers:
    for col_name in nhs_number_fields:
      df = df.where(NHSNumber(F.col(col_name)))
  
  if not replace_empty_str_fields is None:
    for col_name in replace_empty_str_fields:
      df = df.withColumn(col_name, F.when((F.col(col_name) == F.lit(''))
                                                | (F.col(col_name) == F.lit('null'))
                                                | (F.col(col_name) == F.lit('Null'))
                                                | (F.col(col_name) == F.lit('NULL')), None)
                                          .otherwise(F.col(col_name)))
  
  return df

# COMMAND ----------

