# Databricks notebook source
# MAGIC %run ../../src/test_helpers

# COMMAND ----------

PARAMS_PATH = 'default'

# COMMAND ----------

# MAGIC %run ../../params/params

# COMMAND ----------

# MAGIC %run ../../src/hes/process_hes_events

# COMMAND ----------

from uuid import uuid4
from datetime import datetime, date

from dsp.validation.validator import compare_results

# COMMAND ----------

suite = FunctionTestSuite()

## The following are fake people and data created for test purposes

# COMMAND ----------

@suite.add_test
def test_deduplicate_hes_spells():
    
    deduplicate_cols = ['id','pid','dob']
    
    df_input = spark.createDataFrame([
        # different dobs
        ('0001', '000', date(2000,1,1)),
        ('0001', '000', date(2001,1,1)),
        # all identical
        ('1001', '001', date(2000,1,1)),
        ('1001', '001', date(2000,1,1)),
        ('1001', '001', date(2000,1,1)),
        # different pids
        ('2001', '002', date(2000,1,1)),
        ('2001', '020', date(2000,1,1)),
        ('2001', '002', date(2000,1,1)),
        # different ids 
        ('3001', '003', date(2000,1,1)),
        ('3002', '003', date(2000,1,1)),
        ('3003', '003', date(2000,1,1)),
        # different ids + different dob
        ('4001', '004', date(2000,1,1)),
        ('4001', '004', date(2001,1,1)),
        ('4002', '004', date(2000,1,1)),
        # NONE in pid
        ('5001', '004', date(2000,1,1)),
        ('5001', None, date(2001,1,1)),
    ],['id','pid','dob'])

    df_expected = spark.createDataFrame([
        ('0001', '000', date(2000,1,1)),
        ('0001', '000', date(2001,1,1)),
        ('1001', '001', date(2000,1,1)),
        ('2001', '002', date(2000,1,1)),
        ('2001', '020', date(2000,1,1)),
        ('3001', '003', date(2000,1,1)),
        ('3002', '003', date(2000,1,1)),
        ('3003', '003', date(2000,1,1)),
        ('4001', '004', date(2000,1,1)),
        ('4001', '004', date(2001,1,1)),
        ('4002', '004', date(2000,1,1)),
        ('5001', '004', date(2000,1,1)),
        ('5001', None, date(2001,1,1)),
    ],['id','pid','dob'])

    df_actual = deduplicate_hes_spells(df_input, deduplicate_cols)
    assert compare_results(df_actual, df_expected, join_columns = ['id','pid','dob'])
    

# COMMAND ----------

@suite.add_test
def test_process_multiple_spell_indicators():
    
    df_input = spark.createDataFrame([
        (0, ['A'], ['F1']),
        (1, ['A','B'], ['F1']),
        (2, ['A'], ['F1','F2']),
        (3, ['A','B','C'], ['F1','F2','F3']),
        (4, [None], ['F1']),
        (5, ['A'], [None]),
        (6, [None], [None]),
        (7, [None], ['A', 'NO_CVD']),
        (8, [None], ['NO_CVD', 'A']),
        (9, [None], ['A', 'B', 'NO_CVD']),      
        (10,[None], ['NO_CVD']),
        (11,[None], ['NO_CVD', 'NO_CVD'])
    ], ['idx','codes','flags'])

    df_expected = spark.createDataFrame([
        (0, 'A', 'F1', None, None),
        (1, 'MULTIPLE', 'F1', ['A','B'], None),
        (2, 'A', 'MULTIPLE', None, ['F1','F2']),
        (3, 'MULTIPLE', 'MULTIPLE', ['A','B','C'], ['F1','F2','F3']),
        (4, None, 'F1', None, None),
        (5, 'A', None, None, None),
        (6, None, None, None, None),
        (7, None, 'A', None, ['A', 'NO_CVD']),
        (8, None, 'A', None, ['NO_CVD', 'A']),
        (9, None, 'MULTIPLE', None, ['A', 'B', 'NO_CVD']),
        (10,None, 'NO_CVD', None, None),
        (11,None, 'NO_CVD', None, ['NO_CVD', 'NO_CVD'])
    ], ['idx','codes','flags','code_array','flag_array'])

    df_actual = process_multiple_spell_indicators(df_input, 'codes', 'flags', 'code_array', 'flag_array', 'MULTIPLE')
    
    assert compare_results(df_actual, df_expected, join_columns = ['idx'])

# COMMAND ----------

@suite.add_test
def test_collect_spell_sets():
    
  df_input = spark.createDataFrame([
    # one episode
    ('0001', '00001', 'A', 'F1', ['A']),
    # multiple episodes - same codes and flags and code arrays
    ('1001', '10001', 'A', 'F1', ['A']),
    ('1001', '10002', 'A', 'F1', ['A']),
    # multiple episodes - same codes and flags, different code arrays
    ('2001', '20001', 'A', 'F1', ['A','B']),
    ('2001', '20002', 'A', 'F1', ['A']),
    ('2001', '20003', 'A', 'F1', ['A','C']),
    # multiple episodes - different codes, same flags, different code arrays
    ('3001', '30001', 'A', 'F1', ['A','B']),
    ('3001', '30002', 'A', 'F1', ['A']),
    ('3001', '30003', 'C', 'F1', ['A','C']),
    # multiple episodes - same codes, different flags, different code arrays
    ('4001', '40001', 'A', 'F1', ['A']),
    ('4001', '40002', 'A', 'F2', ['A']),
    ('4001', '40003', 'A', 'F1', ['A','C']),
    # multiple episodes - different codes, different flags, different code arrays
    ('5001', '50001', 'A', 'F1', ['A','B']),
    ('5001', '50002', 'B', 'F2', ['B']),
    ('5001', '50003', 'C', 'F3', ['A','C']),
  ], ['spell_id','epi_id','code','flag','code_array'])

  df_expected = spark.createDataFrame([
    # one episode
    ('0001', ['00001'], ['A'], ['F1'], ['A']),
    # multiple episodes - same codes and flags and code arrays
    ('1001', ['10001','10002'], ['A'], ['F1'], ['A']),
    # multiple episodes - same codes and flags, different code arrays
    ('2001', ['20001','20002','20003'], ['A'], ['F1'], ['A','B','C']),
    # multiple episodes - different codes, same flags, different code arrays
    ('3001', ['30001','30002','30003'], ['A','C'], ['F1'], ['A','B','C']),
    # multiple episodes - same codes, different flags, different code arrays
    ('4001', ['40001','40002','40003'], ['A'], ['F1','F2'], ['A','C']),
    # multiple episodes - different codes, different flags, different code arrays
    ('5001', ['50001','50002','50003'], ['A','B','C'], ['F1','F2','F3'], ['A','B','C']),
  ], ['spell_id','epi_id','code','flag','code_array'])
  
  def mock_keep_latest_lsoa(df):
    return df
  
  with FunctionPatch('keep_latest_lsoa', mock_keep_latest_lsoa):
    df_actual = collect_spell_sets(df_input, 'spell_id', 'epi_id','code', 'flag', 'code_array')
    assert compare_results(df_actual, df_expected, join_columns = ['spell_id'])
    

# COMMAND ----------

@suite.add_test
def test_keep_latest_lsoa():
    
  df_input = spark.createDataFrame([
      (0, '001', 'E001', date(2000,1,1)),
      (1, '002', 'E001', date(2000,1,1)),
      (2, '002', 'E002', date(2002,1,1)),
      (3, '003', 'E001', date(2002,1,1)),
      (4, '003', 'E002', date(2000,1,1)),
  ], ['idx','spell_id','lsoa','date'])

  df_expected = spark.createDataFrame([
      (0, '001', 'E001', date(2000,1,1)),
      (1, '002', 'E002', date(2000,1,1)),
      (2, '002', 'E002', date(2002,1,1)),
      (3, '003', 'E001', date(2002,1,1)),
      (4, '003', 'E001', date(2000,1,1)),
  ], ['idx','spell_id','lsoa','date'])

  df_actual = keep_latest_lsoa(df_input, 'spell_id', 'lsoa', 'date')
  assert compare_results(df_actual, df_expected, join_columns = ['idx'])
    

# COMMAND ----------

suite.run()

# COMMAND ----------

dbutils.notebook.exit('pass')
