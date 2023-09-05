# Databricks notebook source
# MAGIC %run ../../pipeline/write_asset_stage

# COMMAND ----------

# MAGIC %run ../../src/test_helpers

# COMMAND ----------

from dsp.validation.validator import compare_results

# COMMAND ----------

suite = FunctionTestSuite()

## The following are fake people and data created for test purposes

# COMMAND ----------

@dataclass(frozen=True)
class TestParams(ParamsBase):

  params_date: date = date(2021, 1, 3)
  DATABASE_NAME: str = 'test_db'

# COMMAND ----------

@suite.add_test
def test_write_asset_stage_location_in_asset():
  """
  SUMMARY:
      Testing the write asset stage to make sure the dataframe is written properly, and to the right location.
      
  NOTE:
    If a test is using the logger stage, and will use a function from it - an empty temp class may be required to be created as is in this test.
  """
  df_input = spark.createDataFrame([
    (0, 'a'),
  ], ['index', 'value'])
  
  class tempLog():
    def __init__():
      return None
    def _run():
      return None
    def _add_stage(name):
      return
    def _timer(name, end = False):
      return
    def _get_counts(name, df):
      return
    def _get_dates(name, df):
      return
    def temp_log_counts(context, log):
      return log

  write_asset_stage = WriteAssetStage(input_asset_to_save='ASSET_TO_SAVE', output='OUTPUT_ASSET')
  test_params = TestParams()
  log = PipelineLogger('')
  context = PipelineContext('12345', test_params, [write_asset_stage])  

  table_suffix = '_2021_01_03_v12345_cv5c0c9a'
  
  with TemporaryTable(df_input, create=False, table_suffix=table_suffix) as tmp_table: 
      context['ASSET_TO_SAVE'] = PipelineAsset('ASSET_TO_SAVE', context, df_input, db=tmp_table.db, 
                                               base_name=tmp_table.name[:-1 * len(table_suffix)])

      context = write_asset_stage.run(context, tempLog)

      assert table_exists(tmp_table.db, tmp_table.name)
      df_actual = spark.table(f'{tmp_table.db}.{tmp_table.name}')
      assert compare_results(df_actual, df_input, join_columns=['index'])
      assert compare_results(context['OUTPUT_ASSET'].df, df_input, join_columns=['index'])

# COMMAND ----------

@suite.add_test
def test_write_asset_stage_location_in_stage():
  df_input = spark.createDataFrame([
    (0, 'a'),
  ], ['index', 'value'])
  
  class tempLog():
    def __init__():
      return None
    def _run():
      return None
    def _add_stage(name):
      return
    def _timer(name, end = False):
      return
    def _get_counts(name, df):
      return
    def _get_dates(name, df):
      return
    def temp_log_counts(context, log):
      return log
  
  test_params = TestParams()
  table_suffix = '_2021_01_03_v12345_cv5c0c9a'
  
  
  with TemporaryTable(df_input, create=False, table_suffix=table_suffix) as tmp_table:   
    write_asset_stage = WriteAssetStage(input_asset_to_save='ASSET_TO_SAVE', output='OUTPUT_ASSET', 
                                        db=tmp_table.db, table_base_name=tmp_table.name[:-1 * len(table_suffix)])
    context = PipelineContext('12345', test_params, [write_asset_stage])

    context['ASSET_TO_SAVE'] = PipelineAsset('ASSET_TO_SAVE', context, df_input)
    
    
    context = write_asset_stage.run(context, tempLog)

    assert table_exists(tmp_table.db, tmp_table.name)
    df_actual = spark.table(f'{tmp_table.db}.{tmp_table.name}')
    assert compare_results(df_actual, df_input, join_columns=['index'])
    assert compare_results(context['OUTPUT_ASSET'].df, df_input, join_columns=['index'])

# COMMAND ----------

suite.run()

# COMMAND ----------

dbutils.notebook.exit('pass')
