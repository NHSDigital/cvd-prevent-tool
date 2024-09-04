# Databricks notebook source
# This file contains the main classes and functions for defining and running a pipeline. The
# PipelineContext object contains lots of params information, and has a dictionary to contain all
# the pipeline assets, which can be accessed by their key. The pipeline context is passed from one
# pipeline stage to the next. The PipelineAsset class is a wrapper for all assets (dataframes, or
# data on disk) used by the pipeline. They are stored in the pipeline context so that can be accessed
# by their key. The PipelineStage class is the base class for all pipeline stages. It makes sure that
# all the input keys are present before the stage runs, and all the output keys are present after the
# stage has run. The pipeline stage base class creates PipelineAsset objects for data returned at the
# end of the stage. The run_pipeline functions takes in a list of pipeline stages. It creates a
# pipeline context, and runs over each pipeline stage in the list in turn.

# COMMAND ----------

# MAGIC %run ../params/params_util

# COMMAND ----------

from abc import ABC, abstractmethod
from typing import FrozenSet, NewType, Union, Set, List
from datetime import datetime
import time
import json
import warnings

from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType
from py4j.protocol import Py4JError
from delta import DeltaTable

# COMMAND ----------

class PipelineContext():
  '''
  Class to hold global pipeline information and to pass information and data between pipeline stage.

  The pipeline context is a dictionary with string keys and PipelineAsset instance values. The input
  and output keys that are passed into PipelineStage instances are the exact same keys as in the
  context dictionary. The inputs to the pipeline stages are the keys of which data assets to use from
  the context, and the output keys given to the pipeline stage are where the resulting data will be
  stored in the contex dictionary.

  The context contains the params object also, so params values, such as the date, can be read from
  the context. See params/params_util.
  '''

  VERSION_AUDIT_FIELD: str = 'version'
  PARAMS_VERSION_AUDIT_FIELD: str = 'params_version'
  DATE_AUDIT_FIELD: str = 'date'
  DATABASE_AUDIT_FIELD: str = 'database'
  STAGE_INDEX_AUDIT_FIELD: str = 'stage_index'
  PIPELINE_DEFINITION_AUDIT_FIELD: str = 'pipeline_definition'
  PARAMS_AUDIT_FIELD: str = 'params'
  STAGE_DEFINITION_NAME_AUDIT_FIELD: str = 'name'
  STAGE_DEFINITION_INPUTS_AUDIT_FIELD: str = 'inputs'
  STAGE_DEFINITION_OUTPUTS_AUDIT_FIELD: str = 'outputs'

  def __init__(self, version: str, params: Params, stages: List):
    self._version = version
    self._date = params.params_date
    self._stages = stages
    self._params_version = params.version
    self._pipeline_assets = {}
    self._db = params.DATABASE_NAME
    self._stage_index = 0
    self._params = params

  def __setitem__(self, key, value):
    self._pipeline_assets[key] = value

  def __getitem__(self, key):
    return self._pipeline_assets[key]

  def __contains__(self, key):
    return key in self._pipeline_assets

  @property
  def params_version(self):
    return self._params_version

  @property
  def date(self):
    return self._date

  @property
  def default_db(self):
    return self._db

  @property
  def version(self):
    return self._version

  def get_auditing_schema(self):
    '''
    Return the schema if the auditing information to be stored in the META field, see
    pipeline/add_auditing_field_stage.
    '''
    return StructType([
      StructField(self.VERSION_AUDIT_FIELD, StringType(), False),
      StructField(self.PARAMS_VERSION_AUDIT_FIELD, StringType(), False),
      StructField(self.DATE_AUDIT_FIELD, DateType(), False),
      StructField(self.DATABASE_AUDIT_FIELD, StringType(), False),
      StructField(self.STAGE_INDEX_AUDIT_FIELD, IntegerType(), False),
      StructField(self.PARAMS_AUDIT_FIELD, StringType(), False),
      StructField(self.PIPELINE_DEFINITION_AUDIT_FIELD, StringType(), False),
    ])

  def get_auditing_struct(self):
    '''
    Returns auditing information data to be stored in the META field, see
    pipeline/add_auditing_field_stage.
    '''
    audit_dict = {
      self.VERSION_AUDIT_FIELD: self._version,
      self.PARAMS_VERSION_AUDIT_FIELD: self._params_version,
      self.DATE_AUDIT_FIELD: self._date,
      self.DATABASE_AUDIT_FIELD: self._db,
      self.STAGE_INDEX_AUDIT_FIELD: self._stage_index,
      self.PARAMS_AUDIT_FIELD: self._params.to_json(),
    }

    pipeline_definition = []
    for stage in self._stages:
      pipeline_definition.append(stage.as_dict(self))

    audit_dict[self.PIPELINE_DEFINITION_AUDIT_FIELD] = json.dumps(pipeline_definition)

    return audit_dict

  def increment_stage(self):
    '''
    Holds an index of the current stage that gets incremented at the end of each stage.
    '''
    self._stage_index += 1


# COMMAND ----------

# Pipeline Fail Exceptions
## Standard Pipeline Assets
class PipelineAssetWriteFailed(Exception):
  pass

class PipelineAssetMissingDatabaseOrTableAttributes(PipelineAssetWriteFailed):
  pass

class PipelineAssetEmptyDataFrame(PipelineAssetWriteFailed):
  pass

## Reading Pipeline Assets
class PipelineAssetReadFailed(Exception):
  pass

## Delta Table Pipeline Assets
class PipelineAssetDeltaTableFailed(Exception):
  pass

class PipelineAssetDeltaTableMergeFailed(PipelineAssetDeltaTableFailed):
  pass

class PipelineAssetDeltaTableMergeColumnsMissing(PipelineAssetDeltaTableFailed):
  pass

class PipelineAssetDeltaTableCloneFailed(PipelineAssetDeltaTableFailed):
  pass

## Delta Table Optimisation Fails
class PipelineAssetDeltaTableOptimiseFailed(Exception):
  pass

class PipelineAssetDeltaTableMissingTable(PipelineAssetDeltaTableOptimiseFailed):
  pass

# COMMAND ----------

class PipelineAsset():
  '''
  Represents an asset in the pipeline, which is a dataframe and/or a database and table location.

  The given key represents the key of this asset in the pipeline context.

  If a db and table base_name are given they will be used as the read and write locations of the data.

  If a base_name is not given, the lower() of the given key is used instead.

  If a dataframe is not given, and a db and base_name table location are given, then the data will be read
  into a dataframe from that location.

  base_name is used as the base table name, the actual table is the base name plus the version number and date.
  To overwrite this behaviour, the table parameter can be given. It uses that exact name as the table and does
  not add the version or date.

  If the flag 'cache' is set to True, the dataframe will be cached, and then unpersisted when this object goes
  out of scope.

  If the flag 'delta_table' is set to True, then the PipelineAsset will be written using the _create_delta_table
  method in place of _create_table.

  If the variable 'delta_columns' is provided, these will be used (if delta_table == True) when merging (update
  and/or insert) into a pre-existing delta table. This variable is required prior to writing an asset.

  '''
  def __init__(self, key: str, context: PipelineContext, df: DataFrame=None, db: str=None,
               base_name: str=None, table: str=None, cache: bool=False, delta_table: bool=False,
               delta_columns: List[str]=None
               ):
    """__init__

    Class initialisation method.

    Args:
        key (str): string key that names this asset.
        context (PipelineContext): Current PipelineContext object for the pipeline.
        df (DataFrame, optional): The dataframe containing the data.. Defaults to None.
        db (str, optional): The database name, if given, along with the base_name, the data will be read from
          that location on disk and the dataframe will be populated.
          Defaults to None.
        base_name (str, optional): The table base name, if given, along with the db, the data will be read from
          that location on disk and the dataframe will be populated. The current date, git version, and params
          version are added to the table base name to create the actual table name. If no base_name is given
          the lowercase of the key is used by default.
          Defaults to None.
        table (str, optional): Works the same as base_name but the current date, git version, and params version
          are not added to the name before reading/writing. An error is raised if both base_name and table are
          set.
          Defaults to None.
        cache (bool, optional): Cache the dataframe and unpersist when this object goes out of scope.
          Defaults to False.
        delta_table (bool, optional): PipelineAsset is delta table, informs methods to use delta table versions.
          Defaults to False.
        delta_columns (List[str], optional): Columns to use in merging delta table (if delta table for
          PipelineAsset already exists).
          Defaults to None.
    """
    self._key = key
    self._context = context
    self._db = db
    self._cache = cache
    self._delta_table = delta_table
    self._delta_columns = delta_columns

    # Check: ensure base_name or table is defined
    if base_name is not None and table is not None:
      raise ValueError('Only one of base_name and table can be set.')

    # Check: ensure that when not a delta table (False) there are no columns (delta_columns == None)
    if delta_table is False and delta_columns is not None:
      raise PipelineAssetDeltaTableMergeColumnsMissing(
        f'Delta merge columns supplied for non delta-table PipelineAsset {self._key}'
        )

    # Check: assign variable in case of overwrite
    if table is not None:
      self._base_name = table
      self._overwrite_table_name = True
    else:
      self._base_name = base_name
      self._overwrite_table_name = False

    # Final: Setup dataframe
    self._df = self._setup_df(df)

  def __del__(self):
    if self._cache:
      try:
        self._df = self._df.unpersist()
      except Py4JError:
        warnings.warn(f"Cannot unpersist cached data for PipelineAsset '{self.key}'. The data was probably " \
                       "unpersisted already. This can happen in unit tests, but should not happen when " \
                       "running the pipeline.")

  @property
  def asset_name(self) -> str:
    '''
    Full database and table location of the data on disk. Note that the data may not exist if it has
    not been read from disk and/or has not been written to disk.
    '''
    if self.db is None or self.table is None:
      return None
    return f'{self.db}.{self.table}'

  @property
  def base_name(self) -> str:
    '''Table base name'''
    return self._base_name or self._key.lower()

  @property
  def db(self) -> str:
    return self._db

  @property
  def df(self) -> DataFrame:
    return self._df

  @property
  def key(self) -> str:
    return self._key

  @property
  def delta_table(self) -> bool:
    return self._delta_table

  @property
  def table(self) -> str:
    '''Full table name
    Generates the full table name for the pipeline asset. Conditional based on the overwrite and delta_table arguments.

    Conditions:
      self._delta_table: If TRUE, use the supplied base table name (self.base_name)
      self._overwrite_table_name: If TRUE, use the supplied base table name (self.base_name)

    Returns:
      Table name for pipeline asset
    '''
    if self._delta_table:
      return self.base_name
    elif self._overwrite_table_name:
      return self.base_name
    else:
      context_date = str(self._context.date).replace('-', '_')
      return f'{self.base_name}_{context_date}_v{self._context.version}_cv{self._context.params_version}'

  @property
  def archive_name(self) -> str:
    """archive_name
    Creates the full table name for archiving (self._clone_delta_table)
    """
    context_date = str(self._context.date).replace('-', '_')
    return f'{self.base_name}_{context_date}_v{self._context.version}_cv{self._context.params_version}'

  #protected
  def _set_key(self, new_key: str):
    self._key = new_key

  # External Checking Functions
  ## Check if asset flagged as delta table
  def check_delta(self):
    """check_delta
    Used by external class methods and functions to check if asset is flagged as a delta table.
    """
    return self._delta_table

  # Write Functions
  ## Standard Pipeline Asset (Non-delta-table)
  def write(self, db: str=None, base_name: str=None, overwrite=False) -> Tuple[str]:
    '''
    Write the data to disk. The data in the dataframe is written to disk at location
    {self.db}.{self.table}, by default. The optional parameters can overwrite this behaviour so
    the data can be saved to some other location on disk.

    db (optional): If given, along wth base_name, the data will be stored to that location on disk.
    If not given, the data is written to the location defined when the class instance was created.
    base_name (optional): If given, along wth db, the data will be stored to that location on disk.
    If not given, the data is written to the location defined when the class instance was created.
    overwrite (optional, default=False): If True the data will be written to the location on disk
    even if there is already data at that location. If False, and there is already data on disk then
    an error will be raised.

    '''
    if self._check_asset_name_args(db, base_name) is False:
      raise PipelineAssetMissingDatabaseOrTableAttributes(f'PipelineAsset {self.key} failed to write data as db '
                                                          f'({self.db}) and/or base table name ({self.base_name}) '
                                                          f'were not given.')

    if self._df is None:
      raise PipelineAssetEmptyDataFrame(f'PipelineAsset {self.key} failed to write data as the dataframe is None.')

    self._create_table(self.df, self.db, self.table, overwrite=overwrite)

    return self.db, self.base_name

  ## Delta Table Pipeline Asset
  ### Merging delta tables
  def write_delta(self,db: str=None,base_name: str=None,merge_columns: List[str]=None, overwrite: bool=False) -> Tuple[str]:
    """write_delta

    Delta table merging and writing function. Performs upsert into delta table (Update and Insert) records from a
    source dataframe (df_source) into a target delta table (db.base_name). If table does not exist (e.g. first
    pipeline run) then the method will flag it is creating, rather than merging, the delta table.
    
    Merging vs Overwriting: Merging a delta table retains all current data in the table, and updates or inserts
    records based on a unique identifier specified for the delta table (merge_columns). Overwriting replaces all
    data in the table with the input dataframe. Overwriting is used in cases where the data (a) contains no unique
    identifiers or (b) there is a lack of unique columns/values to create a unique identifier to merge on. In both
    cases, the previous table (prior to merge or overwrite) is retrievable from the delta table version history.
    
    Note: Both merging and overwriting require the tables have matching schemas (column names and data types). Delta
    tables can merge or update the schema when a table is merged or overwritten, but this is not supported in the current
    version of databricks availabl on the DAE. If a table has a change in the dataframe schema, it will need to be deleted
    and re-created, or merged/overwitten in a manual process. This can and should be updated if the codebase is used on a 
    version of databricks that supports the altering of schemas on merging/overwriting. 

    Args:
        db (str, Optional): String of db name where delta table is stored.
          Defaults to database given when class was initialised.
        base_name (str, Optional): String of table name for delta table.
          Defaults to table name given when class was initialised.
        merge_columns (List[str], Optional): Columns used to assess and perform delta table merge.
          Defaults to columns given when class was initialised.
        overwrite (bool, Optional): Boolean argument to indicate whether delta table should be 
          overwritten (rather than merged). Defaults to false.

    Raises:
        PipelineAssetMissingdbOrTableAttributes: Error raised if db or base_name are not defined (None)
        PipelineAssetEmptyDataFrame: Error raised if dataframe used for merging is empty (no additional records)
        PipelineAssetDeltaTableMergeColumnsMissing: Error raised if delta table merge columns are not provided
        PipelineAssetDeltaTableMergeFailed: Error raised if delta table merge fails

    Returns:
        Tuple[str]: db and table name location for resulting delta table
    """
    # Prior requirement checks
    ## Check for database and table name
    if self._check_asset_name_args(db,base_name) is False:
      raise PipelineAssetMissingDatabaseOrTableAttributes(f'PipelineAsset {self.key} failed to write data as db '
                                                          f'({self.db}) and/or base table name ({self.base_name}) '
                                                          f'were not given.')
    ## Check dataframe is not empty
    if self._df is None:
      raise PipelineAssetEmptyDataFrame(
        f'PipelineAsset {self.key} failed to upsert into the delta table as the dataframe is None.')

    # Conditional: Table presence
    ## Table not present: Create delta table
    if self._table_exists_delta(self.db, self.table) is False:
      print(f'INFO: PipelineAsset key {self.key} asset not present. Creating delta table for {self.db}.{self.table}.')
      ## Create table
      self._create_delta_table(self.df, self.db, self.table)
      ## Return Statement - Asset Location
      return self.db, self.table

    ## Table present: Merge delta table or Overwrite
    else:
      ## Prerequisite: Check overwrite statement
      if overwrite is True:
        print(f'INFO: PipelineAsset key {self.key} asset present and overwrite is TRUE. Overwriting delta table for {self.db}.{self.table}.')
        ## Create table
        self._create_delta_table(self.df, self.db, self.table, overwrite)
        ## Return Statement - Asset Location
        return self.db, self.table
      else:
        ## Prerequisite: Check merge columns
        if self._delta_merge_checks(merge_columns) is False:
          raise PipelineAssetDeltaTableMergeColumnsMissing(
            f'ERROR: No columns supplied for delta table write of Pipeline Asset {self.key}.'
            f'See argument merge_columns.')
        ## Delta table merge
        try:
          self._merge_delta_table(self.db,self.table,self._df,self._delta_columns)
        except Exception as e:
          raise PipelineAssetDeltaTableMergeFailed(
              f'ERROR: Unable to merge into delta table, see error message for more detail. Message: {str(e)}')
        ## Return statement
        return self.db, self.table


  ### Optimising delta tables
  def optimise_delta(self,db: str=None,base_name: str=None):
    """optimise_delta

    Delta table optimisation, usually performed after updating a delta table. Optimisation is automated
    using the SQL OPTIMIZE function.

    Args:
        db (str, optional): Database name. Defaults to database given when class was initialised.
        base_name (str, optional): Table name. Defaults to base_name given when class was initialised.

    Raises:
        PipelineAssetMissingDatabaseOrTableAttributes: Error raised if database/table name were not specified.
        PipelineAssetDeltaTableMissingTable: Error raise if delta table does not exist at asset location.
        PipelineAssetDeltaTableOptimiseFailed: Error raised if delta table is unable to be optimised.
    """
    # Prior requirement checks
    ## Check for database and table name
    if self._check_asset_name_args(db,base_name) is False:
      raise PipelineAssetMissingDatabaseOrTableAttributes(
        f'PipelineAsset {self.key} failed to optimise table as db '
        f'({self.db}) and/or base table name ({self.base_name}) '
        f'were not given.')
    ## Check for table presence
    if self._table_exists(self.db, self.table) is False:
      raise PipelineAssetDeltaTableMissingTable(
        f'PipelineAsset {self.key} cannot be optimised as {self.db}.{self.table} does not exist.')

    # Delta Table Optimisation
    try:
      self._optimise_delta_table(self.db,self.table)
    except Exception as e:
      raise PipelineAssetDeltaTableOptimiseFailed(
        f'ERROR: Unable to optimise delta table, see error message for more detail. Message: {str(e)}')


  ### Clone delta table
  def clone_delta(self,db: str=None,base_name: str=None,clone_name: str=None):
    """clone_delta

    Creates an archived (cloned) version of a PipelineAsset delta table. Cloned asset is saved to location
    of self.archive_name (table_name + context information) - this is identical to the table name format
    used in the standard _create_table() method.

    Args:
        db (str, optional): Database name. Defaults to database given when class was initialised.
        base_name (str, optional): Table name. Defaults to base_name given when class was initialised.
        clone_name (str, optional): Cloned table name. Defaults to self.archive_name.

    Raises:
        PipelineAssetMissingDatabaseOrTableAttributes: Error raised if database/table name were not specified.
        PipelineAssetDeltaTableMissingTable: Error raise if delta table does not exist at asset location.
        PipelineAssetDeltaTableCloneFailed: Error raised if delta table is unable to be cloned.
    """
    # Prior Requirement Checks
    ## Check for database and table name
    if self._check_asset_name_args(db,base_name) is False:
      raise PipelineAssetMissingDatabaseOrTableAttributes(
        f'PipelineAsset {self.key} failed to write data as db '
        f'({self.db}) and/or base table name ({self.base_name}) '
        f'were not given.')
    ## Check for clone table name
    if self._check_cloned_asset_name(db,clone_name) is False:
      raise PipelineAssetMissingDatabaseOrTableAttributes(
        f'PipelineAsset {self.key} failed to clone table from '
        f'{self.db}.{self.table} to {self.db}.{clone_name} '
        f'as table clone name was not given.')
    ## Check for table presence
    if self._table_exists(self.db, self.table) is False:
      raise PipelineAssetDeltaTableMissingTable(
        f'PipelineAsset {self.key} cannot be cloned as {self.db}.{self.table} does not exist.')

    # Delta table clone
    try:
      self._clone_delta_table(self.db,self.table,self._clone_name)
    except Exception as e:
      raise PipelineAssetDeltaTableCloneFailed(
        f'ERROR: Unable to clone delta table for PipelineAsset {self.key}'
        f'See error message for more detail. Message: {str(e)}')


  # Additional Class Methods
  def _check_asset_name_args(self, db: str=None, base_name: str=None):
    self._db = db or self._db
    self._base_name = base_name or self._base_name
    if self._db is None or self.base_name is None:
      return False
    else:
      return True

  def _check_cloned_asset_name(self, db: str=None, clone_name: str=None):
    """_check_cloned_asset_name

    Ensures that cloned asset name is populated prior to table cloning.

    Args:
        db (str, optional): Database name. Defaults to database given when class was initialised.
        clone_name (str, optional): Cloned table name. Defaults to self.archive_name.

    Returns:
        bool: True if database and cloned names are populated.
    """
    self._db = db or self._db
    self._clone_name = clone_name or self.archive_name
    if self._db is None or self._clone_name is None:
      return False
    else:
      return True

  def _create_table(self, df: DataFrame, db_name: str, full_table_name: str, overwrite=False):
    print(f'INFO: PipelineAsset key {self.key} writing data to asset {db_name}.{full_table_name}.')
    create_table(df, db_name, full_table_name, overwrite=overwrite)

  def _create_delta_table(self, df: DataFrame, db_name: str, full_table_name: str, overwrite: bool=False):
    """_create_delta_table

    Function wrapper for the util::create_delta_table function. Creates the delta table and provides a console
    message update.

    Args:
        df (DataFrame): Dataframe to write to delta table.
        db_name (str): Database name.
        full_table_name (str): Table name of table to be created.
        overwrite (bool, Optional): Overwrite (True) existing delta table. Defaults to False.
    """
    print(f'INFO: PipelineAsset key {self.key} writing data to asset delta table {db_name}.{full_table_name}.')
    create_delta_table(df, db_name, full_table_name, overwrite = overwrite)

  def _merge_delta_table(self, db_name: str, target_table_name: str, df_source: DataFrame, delta_merge_columns: List[str]):
    """_merge_delta_table

    Function wrapper for the util::merge_delta_table function. Merges the dataframe (source) into the pre-existing (target)
    delta table and provides a console update.

    Args:
        db_name (str): Database name.
        target_table_name (str): Table name for pre-existing [delta] table.
        df_source (DataFrame): DataFrame used to merge (update+insert) into target delta table.
        delta_merge_columns (List[str]): List of columns used to merge the source dataframe with the target delta table.
    """
    print(f'INFO: PipelineAsset key {self.key} updating data in asset delta table {db_name}.{target_table_name}.')
    merge_delta_table(db_name,target_table_name,df_source,delta_merge_columns)

  def _optimise_delta_table(self, db_name: str, full_table_name: str):
    """_optimise_delta_table

    Function wrapper for the util::optimise_table function. Performs SQL-based [auto] optimisation on the specified table,
    and provides a console update.

    Args:
        db_name (str): Database name.
        full_table_name (str): Table name for table to be optimised.
    """
    print(f'INFO: PipelineAsset key {self.key} optimising asset delta table {db_name}.{full_table_name}.')
    optimise_table(db_name,full_table_name)

  def _clone_delta_table(self, db_name: str, table_name_source: str, table_name_target: str):
    """_clone_delta_table

    Function wrapper for the util::clone_table function. Clones the source delta table to the target delta table, both located
    within the specified database, and provides a console update.

    Args:
        db_name (str): Database name, for database containing both existing and post clone table.
        table_name_source (str): Table name for table to be cloned (source).
        table_name_target (str): Table name for cloned table to be written to (target).
    """
    print(f'INFO: PipelineAsset key {self.key} cloning asset delta table {db_name}.{table_name_source} to {db_name}.{table_name_target}.')
    clone_table(db_name,table_name_source,table_name_target)

  def _setup_df(self, df: DataFrame):
    if df is not None:
      pass
    elif self._read_checks():
      df = self._read()
    else:
      return None

    if self._cache:
      df = df.cache()

    return df

  def _read(self, db: str=None, base_name: str=None):
    if self._read_checks(db, base_name) is False:
      raise PipelineAssetReadFailed(f'PipelineAsset {self.key} read checks failed, see above info message.')
    else:
      return spark.table(self.asset_name)

  def _read_checks(self, db: str=None, base_name: str=None):
    if self._check_asset_name_args(db, base_name) is False:
      print(f'INFO: PipelineAsset {self.key} will not read a dataframe as db ({self._db}) or base table name ({self.base_name}) were not given.')
      return False

    if getattr(self, '_df', None) is not None:
      print(f'INFO: PipelineAsset {self.key} will not read a dataframe as the attribute dataframe is already populated.')
      return False

    if self._table_exists(self.db, self.table) is False:
      print(f'INFO: PipelineAsset {self.key} will not read a dataframe from {self.asset_name} as the location does not exist.')
      return False

    return True

  def _table_exists(self, db, full_table_name):
    return table_exists(db, full_table_name)

  def _table_exists_delta(self, db: str, full_table_name: str):
    """_table_exists_delta

    Function wrapper for table_exists, used to differentiate from standard _table_exists wrapper (requirement for integration tests and any
    subsequent function patching).

    Args:
        db (str): Database name for check.
        full_table_name (str): Table name for check.

    Returns:
        bool: True if table exists at db.full_table_name
    """
    return table_exists(db, full_table_name)

  def _delta_merge_checks(self, merge_columns: List[str]=None):
    """_delta_merge_checks

    Checks that the delta merge columns have been populated (are not None). If columns are supplied, function will overwrite delta_columns in
    PipelineAsset.

    Args:
        merge_columns (List[str], optional): List of column names to perform delta table merge on. Defaults to None.

    Returns:
        bool: True if delta merge columns are not None.
    """
    self._delta_columns = merge_columns or self._delta_columns
    if self._delta_columns is None:
      return False
    else:
      return True


# COMMAND ----------

DF_OR_ASSET_TYPE = NewType('DF_OR_ASSET_TYPE', Union[PipelineAsset, DataFrame])

# COMMAND ----------

class PipelineStageMissingInputKey(Exception):
  pass


class PipelineStageOutputNotExpectedLength(Exception):
  pass


class PipelineStageMissingOutputKeys(Exception):
  pass


class PipelineStageEmptyPipelineAsset(Exception):
  pass


# COMMAND ----------

class PipelineStage(ABC):
  '''
  The abstract base class of all pipeline stages. This class has an abstract method called "_run" that
  must be defined in all child classes.

  The arguments when creating this class are an input and output set of string keys. The input keys will
  match with PipelineAsset instances stored in the PipelineContext object. The output keys instruct the
  class where in the contexnt to store the outputs from this class.

  This class checks that all input keys are present in the pipeline context, if not an error is raised.
  The class then runs the "_run" functions. After the run function, this class checks that all output
  keys are present in the results, and then creates PipelineAsset instances to hold the results, and
  stores these in the context.

  '''

  def __init__(self, inputs: Union[FrozenSet[str], Set[str]], outputs: Union[FrozenSet[str], Set[str]], *args, **kwargs):
    '''
    inputs: set of input keys of data in the pipeline context that will be used by this stage.
    output: set of output keys that should be created by this stage
    '''
    if not isinstance(inputs, frozenset) and isinstance(inputs, set):
      inputs = frozenset(inputs)

    if not isinstance(outputs, frozenset) and isinstance(outputs, set):
      outputs = frozenset(outputs)

    self._inputs = inputs
    self._outputs = outputs
    self._args = args
    self._kwargs = kwargs

  @abstractmethod
  def _run(self, context: PipelineContext) -> Dict[str, DF_OR_ASSET_TYPE]:
    raise NotImplementedError('This is an abstract function and must be defined by inheriting classes.')

  @property
  def name(self):
    '''Name of class'''
    return self.__class__.__name__
  
  @property
  def run_stage(self):
    '''Boolean of if the stage should be run or not'''
    if 'run_stage' in self._kwargs.keys() and self._kwargs['run_stage'] == False:
      return False
    return True

  def as_dict(self, context: PipelineContext):
    '''
    Returns the name input and outputs of this class as a dictionary. It is used by the
    PipelineContext to create the auditing data used by the auditing stage, see
    pipeline/add_auditing_field_stage.
    '''
    return {
      context.STAGE_DEFINITION_NAME_AUDIT_FIELD: self.name,
      context.STAGE_DEFINITION_INPUTS_AUDIT_FIELD: list(self._inputs),
      context.STAGE_DEFINITION_OUTPUTS_AUDIT_FIELD: list(self._outputs)
    }

  def run(self, context, log, limit: bool = False):
    '''
    The main run function. It checks the input keys are present in the context. If there is a dot in
    the key name it assumes that is a data location on disk and attempts to load data from that location.
    This function then runs the "_run" function. It then checks the output and makes sure the expected
    output keys are present. It then wraps the returned data in a PipelineAsset instance and stores it
    in the pipeline context.

    It increments the stage index held by the context.

    Returns: PipelineContext.
    '''

    for input_key in self._inputs:
      if input_key not in context:
        # special case where a path is given instead of a key
        if '.' in input_key:
          db = input_key.split('.')[0]
          table = input_key.split('.')[1]
          new_asset = PipelineAsset(input_key, context, db=db, table=table)
          context[input_key] = new_asset
        else:
          raise PipelineStageMissingInputKey(f'Given input key "{input_key}" is not in the pipeline context')

    if self.name in ["ExtractCVDPDataStage","ExtractHESDataStage"]:
      output_dict = self._run(context,log,limit)
    elif self.name == 'AddAuditingFieldStage':
      output_dict = self._run(context)
    else:
      output_dict = self._run(context, log)

    if self.name != "ArchiveAssetStage":
      returned_output_keys = set(output_dict.keys())
      expected_output_keys = self._outputs

      if len(returned_output_keys) != len(expected_output_keys):
        raise PipelineStageOutputNotExpectedLength('Output of PipelineStage run is not of expected length')

      missing_returned = returned_output_keys.difference(expected_output_keys)
      missing_expected = expected_output_keys.difference(returned_output_keys)
      for missing_list, label in zip([missing_returned, missing_expected], ['returned', 'expected']):
        if len(missing_list) != 0:
          raise PipelineStageMissingOutputKeys(f'The {label} output(s) of the pipeline stage are missing required elements. ({missing_list})')

      for k, df_or_asset_output in output_dict.items():
        if isinstance(df_or_asset_output, PipelineAsset):
          #update the asset key to match the new context key. This is to ensure the context key matches the PipelineAsset key.
          df_or_asset_output._set_key(k)
          if df_or_asset_output.df is None:
            raise PipelineStageEmptyPipelineAsset(f'PipelineAsset key {df_or_asset_output.key} contains no dataframe after stage run.')
          context[k] = df_or_asset_output
        elif isinstance(df_or_asset_output, DataFrame):
          context[k] = PipelineAsset(k, context, df=df_or_asset_output, db=context.default_db)

    context.increment_stage()
    return context


# COMMAND ----------

class PipelineLogger():
  '''
    SUMMARY:
    main class for the pipeline logger, this class sets up and holds a dictionary for each stage in the pipeline and adds necessary information to it:

    logger results example :
    { 'stage_1':
        {'start_time': 123, 'end_time': 456}
    }

  '''

  def __init__(self, version):
    self.results = {'Full Pipeline': {'pipeline_start_date': datetime.now().strftime("%m/%d/%Y %H:%M:%S"), 'start_time': time.time(), 'pipeline_hash': version}}
    return None

  def _run(self, context):
    return None

  def _add_stage(self, stage_name):
    self.results[stage_name] = {}


  def _timer(self, stage_name, end = False):
    if end:
      self.results[stage_name]['end_time'] = time.time()
      self.results[stage_name]['total_time'] = self.results[stage_name]['end_time'] - self.results[stage_name]['start_time']
    else:
      self.results[stage_name]['start_time'] = time.time()


  def _get_column_count(self, stage_name, df):
    self.results[stage_name]['col_count'] = len(df.columns)


  def _get_row_count(self, stage_name, df):
    self.results[stage_name]['row_count'] = df.count()


  def _count_distinct_nhs_dob(self, stage_name, df):
    self.results[stage_name]['distinct_patient_count'] =  df.select(params.PID_FIELD, params.DOB_FIELD).distinct().count()


  def _get_events_counts(self, stage_name, df):
    '''
      gets relevant counts for events table.
      For each category / dataset, pulls the row count and stores in the results

      e.g. dataset_hes: 123, dataset_dars = 456, category_spell = 12, category_episode = 23
    '''
    for item in ['category', 'dataset']:
      grouped_df = df.groupby(item).count()
      for x,y in grouped_df.collect():
        self.results[stage_name][f'{item}_{x}'] = y


  def _get_patient_counts(self, stage_name, df):
    '''
    gets relevant counts for patient table
    gets counts for all diagnosis dates where the column is not null
    gets count of where date of death is not null
    gets count of each death flag

    e.g. death_AF = 123, death_AAA = 234, death_stroke = 345
    '''
    columns = [x for x in df.columns if '_diagnosis_date' in x]

    for x in columns:
      self.results[stage_name][f'{x.split("_")[0]}_diagnoses'] = df.select(x).where(F.col(x).isNotNull()).count()

    self.results[stage_name]['total_deaths'] = df.select('date_of_death').where(F.col('date_of_death').isNotNull()).count()

    grouped_df = (df.where(F.col('death_flag').isNotNull()).groupby('death_flag').count())
    for x,y in grouped_df.collect():
      self.results[stage_name][f'death_{x}'] = y


  def _get_enhanced_demographic_counts(self, stage_name, df):
    '''
    gets counts of how many records are enhanced
    '''
    self.results[stage_name]['enhanced_ethnicity'] = df.where(F.col('enhanced_ethnicity_flag') == 1).count()


  def _get_counts(self, stage_name, df):
    if stage_name == 'WriteAssetStage_events_table':
      self._get_events_counts(stage_name, df)

    elif stage_name == 'WriteAssetStage_demographic_table':
      self._get_enhanced_demographic_counts(stage_name, df)

    elif stage_name == 'WriteAssetStage_patient_table':
      self._get_patient_counts(stage_name, df)

    self._get_column_count(stage_name, df)
    self._get_row_count(stage_name, df)
    self._count_distinct_nhs_dob(stage_name, df)


  def _get_latest_date(self, stage_name, df, date_field: str, criteria_name: str):
    self.results[stage_name][criteria_name] = df.agg({date_field: "max"}).collect()[0][0]


  def _get_dates(self, stage_name, df):
    if stage_name == 'WriteAssetStage_eligible_cohort':
      self._get_latest_date(stage_name, df, date_field=params.CVDP_EXTRACT_DATE, criteria_name='latest_extract')

    elif 'hes' in stage_name:
      self._get_latest_date(stage_name, df, date_field=params.RECORD_STARTDATE_FIELD, criteria_name='latest_record_start_date')

    elif stage_name == 'WriteAssetStage_dars_bird_deaths':
      self._get_latest_date(stage_name, df, date_field=params.DOD_FIELD, criteria_name='latest_recorded_death')

    else:
      pass # don't record a date for other stages


  def _print_out_data(self):
    print(self.results)


  def _get_dict(self):
    return self.results

# COMMAND ----------

def run_pipeline(version: str, _params: Params, stages: List[PipelineStage], limit: bool = False):
  '''
  The main function for running a pipeline, which is defined as a list of pipeline stages. This
  function loops over each stage in turn and calls its "run" function. Timing information about
  each stage and the total pipeline is printed.

  version: The six character code version.
  _params: The current params object, see params/params_util.
  stages: A list of PipelineStage instances that defines the pipeline to run.
  '''

  print(f'Starting pipeline at {datetime.now()}')

  context = PipelineContext(version, _params, stages)
  log = PipelineLogger(version)

  for stage in stages:
    if stage.run_stage:
      print('-' * 80)
      print(f'Running pipeline stage {stage.name} at {datetime.now()}.')
      context = stage.run(context, log, limit)
      print(f'Finished pipeline stage {stage.name} at {datetime.now()}.')

  print('-' * 80)
  print(f'Pipeline finished at {datetime.now()}')
  return context