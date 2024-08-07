# Databricks notebook source
# MAGIC %run ../../pipeline/add_auditing_field_stage

# COMMAND ----------

# MAGIC %run ../../src/test_helpers

# COMMAND ----------

from dsp.validation.validator import compare_results

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

# COMMAND ----------

suite = FunctionTestSuite()

# COMMAND ----------

@dataclass(frozen=True)
class TestParams(ParamsBase):
  #version is '5c0c9a'

  params_date: date = date(2021, 1, 3)
  DATABASE_NAME: str = 'test_db'

# COMMAND ----------

@suite.add_test
def test_add_auditing_field_stage():

  class TestPipelineStage(PipelineStage):

    def __init__(self, input_a):
      self._input_a = input_a
      super(TestPipelineStage, self).__init__({self._input_a}, {})

    def _run(self, context, log):
      return

  input_schema = StructType([
    StructField('index', IntegerType(), False),
    StructField('value', StringType(), False),
  ])

  df_input = spark.createDataFrame([
    (0, 'a'),
    (1, 'b'),
  ], input_schema)

  expected_schema = StructType([
    StructField('index', IntegerType(), False),
    StructField('value', StringType(), False),
    StructField('META', StructType([
      StructField('version', StringType(), False),
      StructField('params_version', StringType(), False),
      StructField('date', DateType(), False),
      StructField('database', StringType(), False),
      StructField('stage_index', IntegerType(), False),
      StructField('params', StringType(), False),
      StructField('pipeline_definition', StringType(), False),
    ]), False),
  ])

  test_params = TestParams()

  expected_struct = {
    'version': '12345',
    'params_version': '5c0c9a',
    'date': date(2021, 1, 3),
    'database': 'test_db',
    'stage_index': 0,
    'params': test_params.to_json(),
    'pipeline_definition': '[{"name": "AddAuditingFieldStage", "inputs": ["INPUT_A"], "outputs": ["INPUT_A"]}, ' \
                           '{"name": "TestPipelineStage", "inputs": ["STAGE_2_INPUT"], "outputs": []}]'
  }

  df_expected = spark.createDataFrame([
    (0, 'a', expected_struct),
    (1, 'b', expected_struct),
  ], expected_schema)

  add_auditing_stage = AddAuditingFieldStage(passthrough_asset_add_meta_column='INPUT_A')
  write_asset_stage = TestPipelineStage(input_a='STAGE_2_INPUT')
  log = PipelineLogger('')
  context = PipelineContext('12345', test_params, [add_auditing_stage, write_asset_stage])
  context['INPUT_A'] = PipelineAsset('INPUT_A', context, df_input)

  context = add_auditing_stage.run(context, log)

  df_actual = context['INPUT_A'].df

  assert 'sampling' in df_actual.schema['META'].dataType.names

  actual_outside_range_size = df_actual.where(
    (df_actual.META.sampling < 0) & (df_actual.META.sampling > 1))
  assert actual_outside_range_size.count() == 0

  struct_cols = df_actual.schema['META'].dataType.names
  struct_cols.remove('sampling')
  df_actual = df_actual.withColumn('META',F.struct(*([col('META')[c].alias(c) for c in struct_cols])))

  assert compare_results(df_actual, df_expected, join_columns=['index'])

# COMMAND ----------

@suite.add_test
def test_add_auditing_field_stage_meta_already_exists():

  df_input = spark.createDataFrame([
    (0, 'a', 't'),
    (1, 'b', 'q'),
  ], ['index', 'value', 'META'])

  test_params = TestParams()

  add_auditing_stage = AddAuditingFieldStage(passthrough_asset_add_meta_column='INPUT_A')
  log = PipelineLogger('')
  context = PipelineContext('12345', test_params, [add_auditing_stage])
  context['INPUT_A'] = PipelineAsset('INPUT_A', context, df_input)

  try:
    context = add_auditing_stage.run(context, log)
    assert False
  except ValueError:
    assert True

# COMMAND ----------

suite.run()

# COMMAND ----------

dbutils.notebook.exit('pass')