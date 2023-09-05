# Databricks notebook source
# lib containing commom util functions to help with testing

# COMMAND ----------

from typing import Union, Callable
from uuid import uuid4
import traceback
import unittest
from functools import wraps

from pyspark.sql import DataFrame
from pyspark.sql.types import StructField

# COMMAND ----------

# MAGIC %run ./util

# COMMAND ----------

class TemporaryTable():
  '''
  A context manager for creating a temporary table with a random name and then deleting it.
  '''
  
  def __init__(self, df_or_schema: Union[DataFrame, StructField], db: str='prevent_tool_collab', 
               create: bool=True, overwrite: bool=False, table_suffix: str=None):
    '''
    df_or_schema: If a datafrme is given that data is stored in the temporary table. If a schema is 
    given, and empty table with that schema is created.
    db (optional): database where the table will be created.
    create (optional, default=True): If False, the initial setup of the context manager will not 
    create a table. This is useful for testing where the function under test will create the table, 
    this class then generates a random name for the table, doesn't create it, but then deletes it 
    when the context ends.
    overwrite (optional, default=False): Option to overwrite the data if it already exists.
    table_suffix (optional): If given, this string will be added to the end of the randomly chosen 
    table name. This is useful for testing where the function under test creates tables with a 
    specific name suffix.
    '''
    self._df_or_schema = df_or_schema
    self._db = db
    self._create = create
    self._overwrite = overwrite
    self._name = f'_tmp_{uuid4().hex}'
    self._asset = f'{self._db}.{self._name}'
    self._table_suffix = table_suffix or ''
      
  def __enter__(self):
    if not self._create:
      return self
    
    if isinstance(self._df_or_schema, DataFrame):
      df = self._df_or_schema
    elif isinstance(self._df_or_schema, StructType):
      df = spark.createDataFrame([], self._df_or_schema)
    else:
      raise TypeError('Given df_or_schema must be a dataframe or a schema.')
      
    create_table(df, self._db, self.name, overwrite=self._overwrite)      
    assert table_exists(self._db, self.name)
    return self
  
  def __exit__(self, exc_type, exc_value, tb):
    
    if exc_type is not None:
      traceback.print_exception(exc_type, exc_value, tb)
    
    if not table_exists(self._db, self.name):
      raise AssertionError(f'Table {self._asset} was already dropped')
    drop_table(self._db, self.name)
    if table_exists(self._db, self.name):
      raise AssertionError(f'Failed to drop table {self._asset}')
     
  @property
  def name(self):
    '''Temporary table name.'''
    return self._name + self._table_suffix
  
  @property
  def db(self):
    '''Database where the temporary table is.'''
    return self._db
  

# COMMAND ----------

class FunctionPatch():
  '''
  This class is a context manager that allows patching of functions/classes "imported" from another 
  notebook using %run.
  
  The patch function must be at global scope (i.e. top level) thus it won't work on class methods. 
  To patch a class method you can create a mock class that inherits from the class you want to patch 
  then overwrite the class method in the mock class with a mock class method, then use this function 
  patch to replace the real class with the mock class.
  '''
  def __init__(self, real_func_name: str, patch_func: Callable):
    self._real_func_name = real_func_name
    self._patch_func = patch_func
    self._backup_real_func = None
    
  def __enter__(self):
    self._backup_real_func = globals()[self._real_func_name]
    globals()[self._real_func_name] = self._patch_func
    
  def __exit__(self, exc_type, exc_value, tb):
    if exc_type is not None:
      traceback.print_exception(exc_type, exc_value, tb)
      
    globals()[self._real_func_name] = self._backup_real_func
    

# COMMAND ----------

class FunctionTestSuite(object):
  '''
  Defines class for registering/running tests.

  Example usage:

  >> suite = FunctionTestSuite()
  >>
  >> def foo():
  >>   ...
  >>
  >> @suite.add_test
  >> def test_foo():
  >>   ...
  >>  
  >> suite.run()
  
 
  '''
  def __init__(self):
    self._suite = unittest.TestSuite()
    self._runner = unittest.TextTestRunner()
    
  def add_test(self, test_func: Callable[[None], bool]) -> None:
    '''
    Add a test function to the suite. 
    
    Example: 
    >> def foo():
    >>   ...
    >>
    >> @suite.add_test
    >> def test_foo():
    >>   ...
    >>  
    '''
    
    @wraps(test_func)
    def clean_up_func():
      result = test_func()
      return result
    
    test_case = unittest.FunctionTestCase(clean_up_func)
    self._suite.addTest(test_case)
    
  def run(self) -> unittest.TestResult:
    '''
    Run the tests & print the output to the console. 
    
    This method can be called once: further tests will need 
    to be assigned to a new object instance.
    
    Returns:
      unittest.TestResult: 
    '''
    if not self._runner.run(self._suite).wasSuccessful():
      raise AssertionError()
