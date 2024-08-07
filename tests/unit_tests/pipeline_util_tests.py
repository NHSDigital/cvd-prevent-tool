# Databricks notebook source
# MAGIC %run ../../pipeline/pipeline_util

# COMMAND ----------

# MAGIC %run ../../src/test_helpers

# COMMAND ----------

PARAMS_PATH = 'default'

# COMMAND ----------

# MAGIC %run ../../params/params

# COMMAND ----------

from datetime import date
import json

from dsp.validation.validator import compare_results

from pyspark.sql.utils import AnalysisException

# COMMAND ----------

suite = FunctionTestSuite()

# COMMAND ----------

@dataclass(frozen=True)
class TestParams(ParamsBase):

  params_date: date = date(2021, 1, 3)
  DATABASE_NAME: str = 'test_db'

# COMMAND ----------

@suite.add_test
def test_pipeline_context():

  test_params = TestParams()
  pipeline_context = PipelineContext('test_code_version', test_params, [])

  assert pipeline_context.params_version == '5c0c9a'
  assert pipeline_context.date == date(2021, 1, 3)
  assert pipeline_context.default_db == 'test_db'
  assert pipeline_context.version == 'test_code_version'
  assert len(pipeline_context._pipeline_assets) == 0
  pipeline_context['0'] = 'item_0'
  pipeline_context['1'] = 'item_1'
  pipeline_context['2'] = 'item_2'
  assert len(pipeline_context._pipeline_assets) == 3
  assert pipeline_context['0'] == 'item_0'
  assert pipeline_context['1'] == 'item_1'
  assert pipeline_context['2'] == 'item_2'

# COMMAND ----------

class TestPipelineStage(PipelineStage):

  def __init__(self, input, output_a, output_b):
    self._input = input
    self._output_a = output_a
    self._output_b = output_b
    super(TestPipelineStage, self).__init__({self._input}, {self._output_a, self._output_b})

  def _run(self, context, log):
    df_input = context[self._input].df
    df_a = df_input.where(col('value') == 'a')
    df_b = df_input.where(col('value') == 'b')
    return {
      self._output_a: df_a,
      self._output_b: PipelineAsset(self._output_b, context, df_b, db='alternate_db',
                                    base_name='_tmp_24573456'),
    }

# COMMAND ----------

@suite.add_test
def test_pipeline_context_increment_stage():

  df_input = spark.createDataFrame([
    (0, 'a'),
    (1, 'a'),
    (2, 'b'),
  ], ['index', 'value'])

  log = PipelineLogger('')
  pipeline_stage = TestPipelineStage('STAGE_INPUT', 'OUTPUT_A', 'OUTPUT_B')
  test_params = TestParams()
  context = PipelineContext('12345', test_params, [pipeline_stage, 's2'])
  context['STAGE_INPUT'] = PipelineAsset('STAGE_INPUT', context, df_input)

  assert context._stage_index == 0
  pipeline_stage.run(context, log)
  assert context._stage_index == 1

# COMMAND ----------

@suite.add_test
def test_pipeline_context_get_auditing_struct():

  class TestPipelineStage2(PipelineStage):

    def __init__(self):
      super(TestPipelineStage2, self).__init__({}, {})

    def _run(self, context):
      return {}

  pipeline_stage = TestPipelineStage('STAGE_INPUT', 'OUTPUT_A', 'OUTPUT_B')
  test_params = TestParams()
  context = PipelineContext('12345', test_params, [pipeline_stage, TestPipelineStage2()])

  actual_struct = context.get_auditing_struct()
  del actual_struct['params']

  assert actual_struct['version'] == '12345'
  assert actual_struct['params_version'] == '5c0c9a'
  assert actual_struct['date'] == date(2021, 1, 3)
  assert actual_struct['database'] == 'test_db'
  assert actual_struct['stage_index'] == 0
  pipeline_definition = json.loads(actual_struct['pipeline_definition'])
  assert pipeline_definition[0]['name'] == 'TestPipelineStage'
  assert pipeline_definition[0]['inputs'] == ['STAGE_INPUT']
  assert sorted(pipeline_definition[0]['outputs']) == ['OUTPUT_A', 'OUTPUT_B']
  assert pipeline_definition[1]['name'] == 'TestPipelineStage2'
  assert pipeline_definition[1]['inputs'] == []
  assert pipeline_definition[1]['outputs'] == []

# COMMAND ----------

@suite.add_test
def test_pipeline_asset_no_asset():

  test_params = TestParams()
  context = PipelineContext('12345', test_params, [])

  df_input = spark.createDataFrame([
    (0, 'a'),
  ], ['index', 'value'])

  pipeline_asset = PipelineAsset('TEST_KEY', context, df=df_input)

  assert pipeline_asset.asset_name is None
  assert pipeline_asset.base_name == 'test_key'
  assert compare_results(pipeline_asset.df, df_input, join_columns=['index'])
  assert pipeline_asset.key == 'TEST_KEY'
  assert pipeline_asset.table == 'test_key_2021_01_03_v12345_cv5c0c9a'

  try:
    pipeline_asset.write()
    assert False
  except PipelineAssetMissingDatabaseOrTableAttributes:
    assert True

  table_suffix = '_2021_01_03_v12345_cv5c0c9a'
  with TemporaryTable(df_input, create=False, table_suffix=table_suffix) as tmp_table:
    db_name, _ = pipeline_asset.write(tmp_table.db, tmp_table.name[:-1 * len(table_suffix)])
    df_actual = spark.table(f'{db_name}.{pipeline_asset.table}')
    assert compare_results(df_actual, df_input, join_columns=['index'])

# COMMAND ----------

@suite.add_test
def test_pipeline_asset_read_df():

  test_params = TestParams()
  context = PipelineContext('12345', test_params, [])

  df_input = spark.createDataFrame([
    (0, 'a'),
  ], ['index', 'value'])

  table_suffix = '_2021_01_03_v12345_cv5c0c9a'
  with TemporaryTable(df_input, table_suffix=table_suffix) as tmp_table:
    pipeline_asset = PipelineAsset('TEST_KEY', context, df=None, db=tmp_table.db,
                                   base_name=tmp_table.name[:-1 * len(table_suffix)])
    assert pipeline_asset.df is not None
    assert compare_results(pipeline_asset.df, df_input, join_columns=['index'])

    #fail because the data already exists in the table
    try:
      pipeline_asset.write()
      raise ValueError()
    except AnalysisException:
      assert True

    #we have to set a new df otherwise there's an error saying you can't read from and write to the
    #same table
    pipeline_asset._df = df_input
    pipeline_asset.write(overwrite=True)

    df_actual = spark.table(pipeline_asset.asset_name)
    assert compare_results(df_actual, df_input, join_columns=['index'])

# COMMAND ----------

@suite.add_test
def test_pipeline_asset_set_key():

  test_params = TestParams()
  context = PipelineContext('12345', test_params, [])

  pipeline_asset = PipelineAsset('TEST_KEY', context)
  assert pipeline_asset.key == 'TEST_KEY'
  assert pipeline_asset.base_name == 'test_key'
  pipeline_asset._set_key('NEW_KEY')
  assert pipeline_asset.key == 'NEW_KEY'
  assert pipeline_asset.base_name == 'new_key'

# COMMAND ----------

@suite.add_test
def test_pipeline_asset_write_without_df():

  test_params = TestParams()
  context = PipelineContext('12345', test_params, [])

  pipeline_asset = PipelineAsset('TEST_KEY', context, None, params.DATABASE_NAME, 'fake_table')

  try:
    pipeline_asset.write()
    assert False
  except PipelineAssetEmptyDataFrame:
    assert True

# COMMAND ----------

@suite.add_test
def test_pipeline_asset_overwrite_table_name():

  df_input = spark.createDataFrame([
    (0, 'a'),
  ], ['index', 'value'])

  test_params = TestParams()
  context = PipelineContext('12345', test_params, [])

  with TemporaryTable(df_input) as tmp_table:

    pipeline_asset = PipelineAsset('TEST_KEY', context, df=None, db=tmp_table.db, base_name=None,
                                   table=tmp_table.name)

    assert pipeline_asset.df is not None
    assert compare_results(pipeline_asset.df, df_input, join_columns=['index'])


# COMMAND ----------

@suite.add_test
def test_pipeline_asset_base_and_table_both_set():

  try:
    pipeline_asset = PipelineAsset('TEST_KEY', None, df=None, db='tes_db', base_name='test_base_name',
                                   table='test_table')
    assert False
  except ValueError:
    assert True


# COMMAND ----------

@suite.add_test
def test_pipeline_asset_with_df_cache():

  test_params = TestParams()
  context = PipelineContext('12345', test_params, [])

  df_input = spark.createDataFrame([
    (0, 'a'),
  ], ['index', 'value'])

  assert df_input.storageLevel.useMemory is False

  pipeline_asset = PipelineAsset('TEST_KEY', context, df=df_input, cache=True)

  assert pipeline_asset.df.storageLevel.useMemory is True

# COMMAND ----------

@suite.add_test
def test_pipeline_asset_read_df_with_cache():

  test_params = TestParams()
  context = PipelineContext('12345', test_params, [])

  df_input = spark.createDataFrame([
    (0, 'a'),
  ], ['index', 'value'])

  table_suffix = '_2021_01_03_v12345_cv5c0c9a'
  with TemporaryTable(df_input, table_suffix=table_suffix) as tmp_table:
    pipeline_asset = PipelineAsset('TEST_KEY', context, df=None, db=tmp_table.db,
                                   base_name=tmp_table.name[:-1 * len(table_suffix)], cache=True)
    assert pipeline_asset.df.storageLevel.useMemory is True

# COMMAND ----------

@suite.add_test
def test_pipeline_stage():

  df_input = spark.createDataFrame([
    (0, 'a'),
    (1, 'a'),
    (2, 'b'),
  ], ['index', 'value'])

  df_a_expected = spark.createDataFrame([
    (0, 'a'),
    (1, 'a'),
  ], ['index', 'value'])

  df_b_expected = spark.createDataFrame([
    (2, 'b'),
  ], ['index', 'value'])

  pipeline_stage = TestPipelineStage('STAGE_INPUT', 'OUTPUT_A', 'OUTPUT_B')
  test_params = TestParams()
  log = PipelineLogger('')
  context = PipelineContext('12345', test_params, [pipeline_stage])
  context['STAGE_INPUT'] = PipelineAsset('STAGE_INPUT', context, df_input)

  assert pipeline_stage.name == 'TestPipelineStage'

  context = pipeline_stage.run(context, log)
  asset_a = context['OUTPUT_A']
  asset_b = context['OUTPUT_B']

  assert asset_a.db == 'test_db'
  assert asset_b.db == 'alternate_db'
  assert asset_a.table == 'output_a_2021_01_03_v12345_cv5c0c9a'
  assert asset_b.table == '_tmp_24573456_2021_01_03_v12345_cv5c0c9a'
  assert compare_results(asset_a.df, df_a_expected, join_columns=['index'])
  assert compare_results(asset_b.df, df_b_expected, join_columns=['index'])

# COMMAND ----------

@suite.add_test
def test_pipeline_stage_missing_input():

  pipeline_stage = TestPipelineStage('STAGE_INPUT', 'OUTPUT_A', 'OUTPUT_B')
  test_params = TestParams()
  log = PipelineLogger('')
  context = PipelineContext('12345', test_params, [pipeline_stage])

  try:
    context = pipeline_stage.run(context, log)
    assert False
  except PipelineStageMissingInputKey:
    assert True

# COMMAND ----------

@suite.add_test
def test_pipeline_stage_missing_output():

  class TestPipelineStageMissingOutput(TestPipelineStage):

    def _run(self, context, log):
      df_input = context[self._input].df
      df_b = df_input.where(col('value') == 'b')
      return {
        self._output_b: PipelineAsset(self._output_b, context, df_b, db='alternate_db',
                                      base_name='_tmp_24573456'),
      }

  df_input = spark.createDataFrame([
    (0, 'a'),
    (1, 'a'),
    (2, 'b'),
  ], ['index', 'value'])

  pipeline_stage = TestPipelineStageMissingOutput('STAGE_INPUT', 'OUTPUT_A', 'OUTPUT_B')
  test_params = TestParams()
  log = PipelineLogger('')
  context = PipelineContext('12345', test_params, [pipeline_stage])
  context['STAGE_INPUT'] = PipelineAsset('STAGE_INPUT', context, df_input)

  try:
    context = pipeline_stage.run(context, log)
    assert False
  except PipelineStageOutputNotExpectedLength:
    assert True

# COMMAND ----------

@suite.add_test
def test_pipeline_stage_wrong_output_labels():

  class TestPipelineStageWrongOutputLabels(TestPipelineStage):

    def _run(self, context, log):
      df_input = context[self._input].df
      df_a = df_input.where(col('value') == 'a')
      df_b = df_input.where(col('value') == 'b')
      return {
        'wrong_output_label': df_a,
        self._output_b: PipelineAsset(self._output_b, context, df_b, db='alternate_db',
                                      base_name='_tmp_24573456'),
      }

  df_input = spark.createDataFrame([
    (0, 'a'),
    (1, 'a'),
    (2, 'b'),
  ], ['index', 'value'])

  pipeline_stage = TestPipelineStageWrongOutputLabels('STAGE_INPUT', 'OUTPUT_A', 'OUTPUT_B')
  test_params = TestParams()
  log = PipelineLogger('')
  context = PipelineContext('12345', test_params, [pipeline_stage])
  context['STAGE_INPUT'] = PipelineAsset('STAGE_INPUT', context, df_input)

  try:
    context = pipeline_stage.run(context, log)
    assert False
  except PipelineStageMissingOutputKeys:
    assert True

# COMMAND ----------

@suite.add_test
def test_pipeline_stage_input_as_path():

  df_input = spark.createDataFrame([
    (0, 'a'),
    (1, 'a'),
    (2, 'b'),
  ], ['index', 'value'])

  df_a_expected = spark.createDataFrame([
    (0, 'a'),
    (1, 'a'),
  ], ['index', 'value'])

  df_b_expected = spark.createDataFrame([
    (2, 'b'),
  ], ['index', 'value'])

  input_table_name = f'_tmp_{uuid4().hex}'
  df_input.createOrReplaceGlobalTempView(input_table_name)

  pipeline_stage = TestPipelineStage('global_temp.' + input_table_name, 'OUTPUT_A', 'OUTPUT_B')
  test_params = TestParams()
  log = PipelineLogger('')
  context = PipelineContext('12345', test_params, [pipeline_stage])

  assert pipeline_stage.name == 'TestPipelineStage'

  context = pipeline_stage.run(context, log)
  asset_a = context['OUTPUT_A']
  asset_b = context['OUTPUT_B']

  assert asset_a.db == 'test_db'
  assert asset_b.db == 'alternate_db'
  assert asset_a.table == 'output_a_2021_01_03_v12345_cv5c0c9a'
  assert asset_b.table == '_tmp_24573456_2021_01_03_v12345_cv5c0c9a'
  assert compare_results(asset_a.df, df_a_expected, join_columns=['index'])
  assert compare_results(asset_b.df, df_b_expected, join_columns=['index'])

# COMMAND ----------

@suite.add_test
def test_run_pipeline():

  df_input = spark.createDataFrame([
    (0, 'a'),
    (1, 'a'),
    (2, 'b'),
  ], ['index', 'value'])

  class TestStartPipeline(PipelineStage):

    def __init__(self, output):
      self._output = output
      super(TestStartPipeline, self).__init__({}, {self._output})

    def _run(self, context, log):
      return {self._output: df_input}

  test_params = TestParams()

  stages = [
    TestStartPipeline('PIPELINE_INPUT'),
    TestPipelineStage('PIPELINE_INPUT', 'OUTPUT_A', 'OUTPUT_B')
  ]

  df_a_expected = spark.createDataFrame([
    (0, 'a'),
    (1, 'a'),
  ], ['index', 'value'])

  df_b_expected = spark.createDataFrame([
    (2, 'b'),
  ], ['index', 'value'])

  context = run_pipeline('test_version', test_params, stages)

  assert compare_results(context['PIPELINE_INPUT'].df, df_input, join_columns=['index'])
  assert compare_results(context['OUTPUT_A'].df, df_a_expected, join_columns=['index'])
  assert compare_results(context['OUTPUT_B'].df, df_b_expected, join_columns=['index'])

# COMMAND ----------

@suite.add_test
def test_logger_timer():
  log = PipelineLogger('')

  #set up stage
  log._add_stage('test_stage')

  #get start_time
  log._timer('test_stage')

  #get end time
  log._timer('test_stage', True)

  dictionary = log.results

  assert dictionary['test_stage'] != None
  assert ((dictionary['test_stage']['start_time'] != None) & (dictionary['test_stage']['end_time'] != None))
  assert ((dictionary['test_stage']['start_time'] > 0) & (dictionary['test_stage']['end_time'] > 0))


@suite.add_test
def test_get_column_count():
  log = PipelineLogger('')
  log._add_stage('test_stage')

  df = spark.createDataFrame([
    (1,2,3,4,5,6,7)
  ], ['1','2','3','4','5','6','7'])

  log._get_column_count('test_stage', df)

  dictionary = log.results

  assert dictionary['test_stage'] != None
  assert dictionary['test_stage']['col_count'] == 7


@suite.add_test
def test_get_row_count():
  log = PipelineLogger('')
  log._add_stage('test_stage')

  df = spark.createDataFrame([
    (1, 'a'), (2,'a'), (1,'a'), (3, 'b'), (4, 'c'), (5, 'd')
  ], ['index', 'let'])

  log._get_row_count('test_stage', df)

  dictionary = log.results

  assert dictionary['test_stage'] != None
  assert dictionary['test_stage']['row_count'] == 6


@suite.add_test
def test_count_distinct_nhs_dob():

  log = PipelineLogger('')

  log._add_stage('test_stage')

  df = spark.createDataFrame([
    (1, 'a'), (2,'a'), (1,'a'),
    (3, 'b'), (4, 'c'), (5, 'd')
  ], ['person_id', 'birth_date'])

  log._count_distinct_nhs_dob('test_stage', df)

  dictionary = log.results

  assert dictionary['test_stage'] != None
  assert dictionary['test_stage']['distinct_patient_count'] == 5

# COMMAND ----------

@suite.add_test
def test_get_events_counts():
  log = PipelineLogger('')
  log._add_stage('test_stage')

  df = spark.createDataFrame([
    ('cat_1', 'dataset_1'), ('cat_2','dataset_1'), ('cat_1','dataset_1'),
    ('cat_2', 'dataset_1'), ('cat_1', 'dataset_2'), ('cat_3', 'dataset_3')
  ], ['category', 'dataset'])

  log._get_events_counts('test_stage', df)

  dictionary = log.results

  assert dictionary['test_stage'] != None
  assert ((dictionary['test_stage']['category_cat_1'] == 3) & (dictionary['test_stage']['category_cat_2'] == 2) & (dictionary['test_stage']['category_cat_3'] == 1))
  assert ((dictionary['test_stage']['dataset_dataset_1'] == 4) & (dictionary['test_stage']['dataset_dataset_2'] == 1) & (dictionary['test_stage']['dataset_dataset_3'] == 1))


@suite.add_test
def test_get_patient_counts():
  log = PipelineLogger('')
  log._add_stage('test_stage')

  df = spark.createDataFrame([
    (date(2020,1,1), date(2020,1,1), date(2020,1,1), date(2020,1,1), 'AF'), # one of everything
    (None, None, None, None, 'AF'),
    (date(2020,1,1), None, None, None, 'AF'),
    (date(2020,1,1), None, None, None, 'AF'),
    (None, None, date(2020,1,1), None, None),
    (None, None, date(2020,1,1), None, None),
    (None, None, date(2020,1,1), None, 'CKD'),
    (None, None, None, None, None),
  ], ['AAA_diagnosis_date','AF_diagnosis_date','CKD_diagnosis_date', 'date_of_death', 'death_flag'])

  log._get_patient_counts('test_stage', df)

  dictionary = log.results

  assert dictionary['test_stage'] != None
  assert ((dictionary['test_stage']['AAA_diagnoses'] == 3) & (dictionary['test_stage']['AF_diagnoses'] == 1) & (dictionary['test_stage']['CKD_diagnoses'] == 4))
  assert ((dictionary['test_stage']['total_deaths'] == 1) & (dictionary['test_stage']['death_AF'] == 4) & (dictionary['test_stage']['death_CKD'] == 1))

# COMMAND ----------

@suite.add_test
def test_add_stage():
  log = PipelineLogger('')
  log._add_stage('test_stage')

  assert log.results['test_stage'] != None
  assert log.results['test_stage'] == {}


# COMMAND ----------

suite.run()

# COMMAND ----------

dbutils.notebook.exit('pass')