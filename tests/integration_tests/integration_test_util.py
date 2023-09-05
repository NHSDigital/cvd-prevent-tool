# Databricks notebook source
# MAGIC %run ../../params/params_util

# COMMAND ----------

# MAGIC %run ../../src/test_helpers

# COMMAND ----------

from uuid import uuid4
from dataclasses import dataclass
from typing import Callable
from datetime import datetime

# COMMAND ----------

# MAGIC %run ../../pipeline/pipeline_util

# COMMAND ----------

class PipelineAssetHasAlreadyBeenRedirected(Exception):
  """This class is a child of PipelineAsset and is used when raising an error for the following condition:
  a PipelineAsset (e.g. events table) has already been flagged as a temporary table as part of the integration
  tests but an overwrite is attempted, raising an error"""
  pass

# COMMAND ----------

@dataclass(frozen=True)
class PipelineAssetValues():
  '''
  A dataclass that holds the key discriminating information about PipelineAsset instances (see 
  pipeline/pipeline_util).
  
  This class holds the database and table name of the PipelineAsset. It is used by 
  'IntegrationTestPipelineAsset' to map from real assets (where we don't want to store the data) to 
  temporary assets (where we do want to store the data).
  
  '''
  db: str
  full_table_name: str
    
  def __hash__(self):
    if '.' in self.db or '.' in self.full_table_name:
      raise ValueError('The character "." is reserved and cannot be used in a database or table name.')
    return hash('.'.join([self.db, self.full_table_name]))
  
  def __eq__(self, other):
    return isinstance(other, PipelineAssetValues) and hash(self) == hash(other)
  
  @staticmethod
  def from_pipeline_asset(asset: PipelineAsset):
    return PipelineAssetValues(db=asset.db, full_table_name=asset.table)


class IntegrationTestPipelineAsset(PipelineAsset):
  '''
  In the integration test (see 'run_integration_test' and 'IntegrationTestManager' below) this class is 
  used to mock the 'PipelineAsset' class (see pipeline/pipeline_util). This class inherits from and 
  overwrites the read and write functionality of the PipelineAsset and redirects the data to/from a 
  temporary table. This class has a class variable (_redirected_asset_mapping) that stores the mappings 
  from the real locations, to the redirected fake locations. Note that a class variable means that the 
  mapping is shared between all instances of this class.
  
  This class provides a function for deleting the fake tables after the tests have finished.
  '''

  #class variables that are the same for all instances of this class.
  _redirected_asset_mapping: Dict[PipelineAssetValues, PipelineAssetValues] = {}
  test_db: str = None

  @property
  def asset_name(self):
    '''
    Returns the fake asset name if it exists in the mapping, or the real name otherwise.
    '''
    values = PipelineAssetValues.from_pipeline_asset(self)
    if values in self._redirected_asset_mapping:
      values = self._redirected_asset_mapping[values]
    return f'{values.db}.{values.full_table_name}'
    
  def _create_table(self, df: DataFrame, db_name: str, full_table_name: str, overwrite=False):
    real_values = PipelineAssetValues.from_pipeline_asset(self)
    temp_table_name = f'_tmp_integration_{real_values.full_table_name}_{datetime.now().strftime("runtime_%H_%M_%S")}'
    fake_values = PipelineAssetValues(db=self.test_db, full_table_name=temp_table_name)
    
    if real_values in self._redirected_asset_mapping:
      raise PipelineAssetHasAlreadyBeenRedirected(f'PipelineAsset with key "{real_values.key}" has '
                                                  'already been redirected. A feature to overwrite '
                                                  'redirects has not been implemented.')
    
    print(f'INFO: Redirecting asset {real_values.db}.{real_values.full_table_name} to '
          f'{fake_values.db}.{fake_values.full_table_name} for the integration test.')
    self._redirected_asset_mapping[real_values] = fake_values
    
    super(IntegrationTestPipelineAsset, self)._create_table(df, fake_values.db, fake_values.full_table_name, 
                                                            overwrite=overwrite)
    
  def _table_exists(self, db, full_table_name):
    values = PipelineAssetValues(db=db, full_table_name=full_table_name)
    
    if values in self._redirected_asset_mapping:
      values = self._redirected_asset_mapping[values]

    super(IntegrationTestPipelineAsset, self)._table_exists(values.db, values.full_table_name)
    
  @staticmethod
  def assert_real_assets_not_created():
    '''
    Check that none of the real asset values in the mapping were accidentally create. Raises and 
    error if any were created.
    '''
    for key in IntegrationTestPipelineAsset._redirected_asset_mapping.keys():
      if table_exists(key.db, key.full_table_name):
        raise AssertionError(f'Real asset {key.db}.{key.full_table_name} was created during the test. '
                              'Therefore the test has failed.')
  
  @staticmethod
  def delete_temp_tables():
    '''
    Delete all the temporary/fake tables that have been created.
    '''
    for value in IntegrationTestPipelineAsset._redirected_asset_mapping.values():
      if table_exists(value.db, value.full_table_name) is False:
        raise AssertionError(f'Temporary asset {value.db}.{value.full_table_name} was NOT created during '
                              'the test. Therefore the test has failed.')
      else:
        print(f'INFO: Deleting temp table {value.db}.{value.full_table_name} that was created during the integration test.')
        drop_table(value.db, value.full_table_name)
        assert table_exists(value.db, value.full_table_name) is False
    

class IntegrationTestManager():
  '''
  Class to facilitate integration testing. This class is a context manager. It replaces the 
  'PipelineAsset' class with the 'IntegrationTestPipelineAsset' class which then redirects the data 
  to temporary tables instead of the real tables. When the context ends this class checks to make 
  sure none of the real data locations have been accidentally created, an error is raised if there is 
  a problem. When the context ends all of the temporary tables are deleted.
  
  '''
  def __init__(self, test_db, save_integration_output):
    self._test_db = test_db
    self._save_integration_output = save_integration_output
    
  def __enter__(self):
    IntegrationTestPipelineAsset.test_db = self._test_db
    self._pipeline_asset_mock = FunctionPatch('PipelineAsset', IntegrationTestPipelineAsset)
    self._pipeline_asset_mock.__enter__()
    return self

  def __exit__(self, exc_type, exc_value, tb):
    if exc_type is not None:
      traceback.print_exception(exc_type, exc_value, tb)
    self._pipeline_asset_mock.__exit__(exc_type, exc_value, tb)
    try:
      IntegrationTestPipelineAsset.assert_real_assets_not_created()
    finally:
      if not self._save_integration_output:
        IntegrationTestPipelineAsset.delete_temp_tables()
    

# COMMAND ----------

def run_integration_test(_param: Params, stages: List[PipelineStage], test_db: str, limit: bool,
                         save_integration_output: bool = False, check_func: Callable[[PipelineContext], PipelineContext] = None) -> PipelineContext:
  '''
  Run the given pipeline in integration test mode. This runs the entire pipeline in the usual way, but it 
  redirects the read and write functions of the pipeline such that any data is saved in temporary tables 
  that are then deleted at the end of the test. See the top of this file and the associated classes for 
  more information.
  
  Using temporary table names means that anyone can run integration tests at the same time, without any 
  collisions.
  
  _config: An instance of the Config object.
  stages: The list of PipelineStage class instances that define the pipeline.
  test_db: The database where the temporary tables will be created.
  check_func: An optional function with the signature Func(PipelineContext) -> PipelineContext. It is run 
  after the pipeline and can be used to check the resulting data.
  
  '''
  version = 'intver'
  with IntegrationTestManager(test_db, save_integration_output) as manager:  
    context = run_pipeline(version, _param, stages, limit)
    
    if check_func is not None:
        context = check_func(context)
       
  return context

# COMMAND ----------

