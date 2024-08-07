# Databricks notebook source
# MAGIC %run ../../pipeline/archive_asset_stage

# COMMAND ----------

# MAGIC %run ../../src/test_helpers

# COMMAND ----------

from dsp.validation.validator import compare_results

# COMMAND ----------

suite = FunctionTestSuite()

# COMMAND ----------

@dataclass(frozen=True)
class TestParams(ParamsBase):
    params_date: date = date(2021, 1, 3)
    DATABASE_NAME: str = 'test_db'

# COMMAND ----------

@suite.add_test
def test_archive_asset_stage_location_in_asset():
    '''test_archive_asset_stage_location_in_asset
    Test of the archive asset stage when PipelineAsset values are used
    '''
    # Setup Dataframe for initial table
    df_input = spark.createDataFrame([
    (0, 'a'),
    ], ['index', 'value'])

    # Create test logger
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

    # Setup Test Archive Stage
    archive_asset_stage = ArchiveAssetStage(input_asset_to_archive='ASSET_TO_ARCHIVE', run_archive_stage=True)
    test_params = TestParams()
    log = PipelineLogger('')
    context = PipelineContext('12345', test_params, [archive_asset_stage])
    table_suffix = '_2021_01_03_v12345_cv5c0c9a'

    # Run test stage
    with TemporaryTable(df_input, create=False, table_suffix=table_suffix) as tmp_table:
        ## Store base table name
        tmp_table_base_name = tmp_table.name[:-1 * len(table_suffix)]
        ## Create delta table pipeline asset
        context['ASSET_TO_ARCHIVE'] = PipelineAsset(
            key='ASSET_TO_ARCHIVE',
            context=context,
            df=df_input,
            db=tmp_table.db,
            base_name=tmp_table.name[:-1 * len(table_suffix)],
            delta_table=True,
            delta_columns=['index']
        )
        ### Create Delta Table
        try:
          create_delta_table(df_input,tmp_table.db,tmp_table_base_name)
          assert table_exists(tmp_table.db,tmp_table_base_name)
          ### Run Stage
          context = archive_asset_stage.run(context, tempLog)
          ### Test Statements
          assert table_exists(tmp_table.db, tmp_table.name)
          df_actual = spark.table(f'{tmp_table.db}.{tmp_table.name}')
          assert compare_results(df_actual,df_input,join_columns=['index'])
        finally:
          ### Drop Original Table
          drop_table(tmp_table.db,tmp_table_base_name)

# COMMAND ----------

@suite.add_test
def test_archive_asset_stage_location_in_stage():
    '''test_archive_asset_stage_location_in_stage
    Test of the archive asset stage when stage arugments are used
    '''
    # Setup Dataframe for initial table
    df_input = spark.createDataFrame([
    (0, 'a'),
    ], ['index', 'value'])

    # Create test logger
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

    # Setup Test Archive Stage
    test_params = TestParams()
    log = PipelineLogger('')
    table_suffix = '_tmp_archive_table_test'

    # Run test stage
    with TemporaryTable(df_input, create=False, table_suffix=table_suffix) as tmp_table:
        ## Store base table name
        tmp_table_base_name = tmp_table.name[:-1 * len(table_suffix)]
        ## Stage Setup
        archive_asset_stage = ArchiveAssetStage(
            input_asset_to_archive='ASSET_TO_ARCHIVE',
            db=tmp_table.db,
            table_base_name=tmp_table.name[:-1 * len(table_suffix)],
            table_clone_name=tmp_table.name,
            run_archive_stage=True
            )
        context = PipelineContext('12345', test_params, [archive_asset_stage])
        ## Create delta table pipeline asset
        context['ASSET_TO_ARCHIVE'] = PipelineAsset(
            key='ASSET_TO_ARCHIVE',
            context=context,
            df=df_input,
            db=tmp_table.db,
            base_name=tmp_table.name[:-1 * len(table_suffix)],
            delta_table=True,
            delta_columns=['index']
        )
        # Create temporary delta table
        try:
            ### Create Delta Table
            create_delta_table(df_input,tmp_table.db,tmp_table_base_name)
            assert table_exists(tmp_table.db,tmp_table_base_name)
            ### Run Stage
            context = archive_asset_stage.run(context, tempLog)
            ### Test Statements
            assert table_exists(tmp_table.db, tmp_table.name)
            df_actual = spark.table(f'{tmp_table.db}.{tmp_table.name}')
            assert compare_results(df_actual,df_input,join_columns=['index'])
        finally:
            ### Drop Original Table
            drop_table(tmp_table.db,tmp_table_base_name)

# COMMAND ----------

suite.run()

# COMMAND ----------

dbutils.notebook.exit('pass')