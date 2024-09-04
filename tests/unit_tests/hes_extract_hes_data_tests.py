# Databricks notebook source
# MAGIC %run ../../src/test_helpers

# COMMAND ----------

# MAGIC %run ../../src/clean_dataset

# COMMAND ----------

PARAMS_PATH = 'default'

# COMMAND ----------

# MAGIC %run ../../params/params

# COMMAND ----------

# MAGIC %run ../../src/hes/extract_hes_data

# COMMAND ----------

from dsp.validation.validator import compare_results
import pyspark.sql.types as T
from datetime import datetime
from uuid import uuid4

# COMMAND ----------

suite = FunctionTestSuite()

## The following are fake people and data created for test purposes

# COMMAND ----------

@suite.add_test
def test_get_hes_dataframes_from_years():
  """test_get_hes_dataframes_from_years
  src/hes/extract_hes_data_tests::get_hes_dataframes_from_years

  Tests the extraction and union of HES dataframes from multiple years and HES
  source databases. Test creates temporary tables and reads them as HES dataframes.
  """

  # Input Data
  ## Create input datasets for multiple HES years (2019 - 2023)
  ### 2019-2021
  df_input_hes_19 = spark.createDataFrame([
      ('109','I167','2019'),
      ('110','I161','2020')
  ], ['EPIKEY','DIAG_3_01','FYEAR'])
  df_input_hes_20 = spark.createDataFrame([
      ('111','I167','2020'),
      ('112','I161','2021')
  ], ['EPIKEY','DIAG_3_01','FYEAR'])
  ### 2021-2022
  df_input_hes_21 = spark.createDataFrame([
      ('113','I167','2021'),
      ('114','I161','2022')
  ], ['EPIKEY','DIAG_3_01','FYEAR'])
  ### 2022-2023
  df_input_hes_22 = spark.createDataFrame([
      ('115','I167','2022'),
      ('116','I161','2023')
  ], ['EPIKEY','DIAG_3_01','FYEAR'])

  # Expected Data
  df_expected = spark.createDataFrame([
      ('109','I167','2019'),
      ('110','I161','2020'),
      ('111','I167','2020'),
      ('112','I161','2021'),
      ('113','I167','2021'),
      ('114','I161','2022'),
      ('115','I167','2022'),
      ('116','I161','2023'),
  ], ['EPIKEY','DIAG_3_01','FYEAR'])

  # Test Parameters Setup
  ## Test table ID
  tmp_prefix = uuid4().hex
  ## Function arguments
  tmp_hes_dataset_name = f"_tmp_{tmp_prefix}_extract_hes_"
  tmp_start_year = 19
  tmp_end_year = 23
  tmp_db_hes = "global_temp"
  tmp_db_ahas = "global_temp"
  ## Temporary table definitions
  owner_db = "global_temp"
  input_name_hes_19 = f"_tmp_{tmp_prefix}_extract_hes_1920"
  input_name_hes_20 = f"_tmp_{tmp_prefix}_extract_hes_2021"
  input_name_hes_21 = f"_tmp_{tmp_prefix}_extract_hes_2122"
  input_name_hes_22 = f"_tmp_{tmp_prefix}_extract_hes_2223"

  # Run Test
  try:
    ## Create temporary tables for HES test assets
    df_input_hes_19.createOrReplaceGlobalTempView(input_name_hes_19)
    df_input_hes_20.createOrReplaceGlobalTempView(input_name_hes_20)
    df_input_hes_21.createOrReplaceGlobalTempView(input_name_hes_21)
    df_input_hes_22.createOrReplaceGlobalTempView(input_name_hes_22)
    ## Run function with test parameters
    df_actual = get_hes_dataframes_from_years(
      hes_dataset_name = tmp_hes_dataset_name,
      start_year = tmp_start_year,
      end_year = tmp_end_year,
      db_hes = tmp_db_hes,
      db_ahas = tmp_db_ahas,
    )
    ## Assert check - actual matches expected
    assert compare_results(df_actual, df_expected, join_columns=['EPIKEY'])
  # Remove any temporary tables from test
  finally:
    drop_table(owner_db, input_name_hes_19)
    drop_table(owner_db, input_name_hes_20)
    drop_table(owner_db, input_name_hes_21)
    drop_table(owner_db, input_name_hes_22)


# COMMAND ----------

@suite.add_test
def test_get_hes_dataframes_from_years_limitYears():
  """test_get_hes_dataframes_from_years_limitYears
  src/hes/extract_hes_data_tests::get_hes_dataframes_from_years

  Tests the extraction and union of HES dataframes from multiple years and HES
  source databases. Test creates temporary tables and reads them as HES dataframes.
  Years are limited to exclude the hes_19 dataframe.
  """

  # Input Data
  ## Create input datasets for multiple HES years (2019 - 2023)
  ### 2019-2021
  df_input_hes_19 = spark.createDataFrame([
      ('109','I167','2019'),
      ('110','I161','2020')
  ], ['EPIKEY','DIAG_3_01','FYEAR'])
  df_input_hes_20 = spark.createDataFrame([
      ('111','I167','2020'),
      ('112','I161','2021')
  ], ['EPIKEY','DIAG_3_01','FYEAR'])
  ### 2021-2022
  df_input_hes_21 = spark.createDataFrame([
      ('113','I167','2021'),
      ('114','I161','2022')
  ], ['EPIKEY','DIAG_3_01','FYEAR'])
  ### 2022-2023
  df_input_hes_22 = spark.createDataFrame([
      ('115','I167','2022'),
      ('116','I161','2023')
  ], ['EPIKEY','DIAG_3_01','FYEAR'])

  # Expected Data
  df_expected = spark.createDataFrame([
      ('111','I167','2020'),
      ('112','I161','2021'),
      ('113','I167','2021'),
      ('114','I161','2022'),
      ('115','I167','2022'),
      ('116','I161','2023'),
  ], ['EPIKEY','DIAG_3_01','FYEAR'])

  # Test Parameters Setup
  ## Test table ID
  tmp_prefix = uuid4().hex
  ## Function arguments
  tmp_hes_dataset_name = f"_tmp_{tmp_prefix}_extract_hes_"
  tmp_start_year = 20
  tmp_end_year = 23
  tmp_db_hes = "global_temp"
  tmp_db_ahas = "global_temp"
  ## Temporary table definitions
  owner_db = "global_temp"
  input_name_hes_19 = f"_tmp_{tmp_prefix}_extract_hes_1920"
  input_name_hes_20 = f"_tmp_{tmp_prefix}_extract_hes_2021"
  input_name_hes_21 = f"_tmp_{tmp_prefix}_extract_hes_2122"
  input_name_hes_22 = f"_tmp_{tmp_prefix}_extract_hes_2223"

  # Run Test
  try:
    ## Create temporary tables for HES test assets
    df_input_hes_19.createOrReplaceGlobalTempView(input_name_hes_19)
    df_input_hes_20.createOrReplaceGlobalTempView(input_name_hes_20)
    df_input_hes_21.createOrReplaceGlobalTempView(input_name_hes_21)
    df_input_hes_22.createOrReplaceGlobalTempView(input_name_hes_22)
    ## Run function with test parameters
    df_actual = get_hes_dataframes_from_years(
      hes_dataset_name = tmp_hes_dataset_name,
      start_year = tmp_start_year,
      end_year = tmp_end_year,
      db_hes = tmp_db_hes,
      db_ahas = tmp_db_ahas,
    )
    ## Assert check - actual matches expected
    assert compare_results(df_actual, df_expected, join_columns=['EPIKEY'])
  # Remove any temporary tables from test
  finally:
    drop_table(owner_db, input_name_hes_19)
    drop_table(owner_db, input_name_hes_20)
    drop_table(owner_db, input_name_hes_21)
    drop_table(owner_db, input_name_hes_22)


# COMMAND ----------

@suite.add_test
def test_get_hes_dataframes_from_years_dropColumns():
  """test_get_hes_dataframes_from_years_dropColumns
  src/hes/extract_hes_data_tests::get_hes_dataframes_from_years

  Tests the extraction and union of HES dataframes from multiple years and HES
  source databases. Test creates temporary tables and reads them as HES dataframes.
  drop_columns argument is specified, should return dataframe without column name.
  """

  # Input Data
  ## Create input datasets for multiple HES years (2020 - 2023)
  ### 2020-2021
  df_input_hes_20 = spark.createDataFrame([
      ('111','I167','2020'),
      ('112','I161','2021')
  ], ['EPIKEY','DIAG_3_01','hes_year'])
  ### 2021-2022
  df_input_hes_21 = spark.createDataFrame([
      ('113','I167','2021'),
      ('114','I161','2022')
  ], ['EPIKEY','DIAG_3_01','hes_year'])
  ### 2022-2023
  df_input_hes_22 = spark.createDataFrame([
      ('115','I167','2022'),
      ('116','I161','2023')
  ], ['EPIKEY','DIAG_3_01','hes_year'])

  # Expected Data
  df_expected = spark.createDataFrame([
      ('111','I167'),
      ('112','I161'),
      ('113','I167'),
      ('114','I161'),
      ('115','I167'),
      ('116','I161'),
  ], ['EPIKEY','DIAG_3_01'])

  # Test Parameters Setup
  ## Test table ID
  tmp_prefix = uuid4().hex
  ## Function arguments
  tmp_hes_dataset_name = f"_tmp_{tmp_prefix}_extract_hes_"
  tmp_start_year = 20
  tmp_end_year = 23
  tmp_db_hes = "global_temp"
  tmp_db_ahas = "global_temp"
  tmp_drop_column = "hes_year"
  ## Temporary table definitions
  owner_db = "global_temp"
  input_name_hes_20 = f"_tmp_{tmp_prefix}_extract_hes_2021"
  input_name_hes_21 = f"_tmp_{tmp_prefix}_extract_hes_2122"
  input_name_hes_22 = f"_tmp_{tmp_prefix}_extract_hes_2223"

  # Run Test
  try:
    ## Create temporary tables for HES test assets
    df_input_hes_20.createOrReplaceGlobalTempView(input_name_hes_20)
    df_input_hes_21.createOrReplaceGlobalTempView(input_name_hes_21)
    df_input_hes_22.createOrReplaceGlobalTempView(input_name_hes_22)
    ## Run function with test parameters
    df_actual = get_hes_dataframes_from_years(
      hes_dataset_name = tmp_hes_dataset_name,
      start_year = tmp_start_year,
      end_year = tmp_end_year,
      db_hes = tmp_db_hes,
      db_ahas = tmp_db_ahas,
      drop_columns = tmp_drop_column
    )
    ## Assert check - actual matches expected
    assert compare_results(df_actual, df_expected, join_columns=['EPIKEY'])
  # Remove any temporary tables from test
  finally:
    drop_table(owner_db, input_name_hes_20)
    drop_table(owner_db, input_name_hes_21)
    drop_table(owner_db, input_name_hes_22)


# COMMAND ----------

@suite.add_test
def test_get_hes_dataframes_from_years_joinFilterColumns():
  """test_get_hes_dataframes_from_years_joinFilterColumns
  src/hes/extract_hes_data_tests::get_hes_dataframes_from_years

  Tests the extraction and union of HES dataframes from multiple years and HES
  source databases. Test creates temporary tables and reads them as HES dataframes.
  join_filter_columns is specified - should limit all HES dataframes to column list.
  """

  # Input Data
  ## Create input datasets for multiple HES years (2020 - 2023)
  ### 2020-2021
  df_input_hes_20 = spark.createDataFrame([
      ('111','I167','2020','FOO_1'),
      ('112','I161','2021','FOO_2')
  ], ['EPIKEY','DIAG_3_01','FYEAR','BAR'])
  ### 2021-2022
  df_input_hes_21 = spark.createDataFrame([
      ('113','I167','2021','FOO_3'),
      ('114','I161','2022','FOO_4')
  ], ['EPIKEY','DIAG_3_01','FYEAR','BAR'])
  ### 2022-2023
  df_input_hes_22 = spark.createDataFrame([
      ('115','I167','2022','FOO_5'),
      ('116','I161','2023','FOO_6')
  ], ['EPIKEY','DIAG_3_01','FYEAR','BAR'])

  # Expected Data
  df_expected = spark.createDataFrame([
      ('111','I167','2020'),
      ('112','I161','2021'),
      ('113','I167','2021'),
      ('114','I161','2022'),
      ('115','I167','2022'),
      ('116','I161','2023'),
  ], ['EPIKEY','DIAG_3_01','FYEAR'])

  # Test Parameters Setup
  ## Test table ID
  tmp_prefix = uuid4().hex
  ## Function arguments
  tmp_hes_dataset_name = f"_tmp_{tmp_prefix}_extract_hes_"
  tmp_start_year = 20
  tmp_end_year = 23
  tmp_db_hes = "global_temp"
  tmp_db_ahas = "global_temp"
  tmp_join_filter = ['EPIKEY','DIAG_3_01','FYEAR']
  ## Temporary table definitions
  owner_db = "global_temp"
  input_name_hes_20 = f"_tmp_{tmp_prefix}_extract_hes_2021"
  input_name_hes_21 = f"_tmp_{tmp_prefix}_extract_hes_2122"
  input_name_hes_22 = f"_tmp_{tmp_prefix}_extract_hes_2223"

  # Run Test
  try:
    ## Create temporary tables for HES test assets
    df_input_hes_20.createOrReplaceGlobalTempView(input_name_hes_20)
    df_input_hes_21.createOrReplaceGlobalTempView(input_name_hes_21)
    df_input_hes_22.createOrReplaceGlobalTempView(input_name_hes_22)
    ## Run function with test parameters
    df_actual = get_hes_dataframes_from_years(
      hes_dataset_name = tmp_hes_dataset_name,
      start_year = tmp_start_year,
      end_year = tmp_end_year,
      db_hes = tmp_db_hes,
      db_ahas = tmp_db_ahas,
      join_filter_columns = tmp_join_filter
    )
    ## Assert check - actual matches expected
    assert compare_results(df_actual, df_expected, join_columns=['EPIKEY'])
  # Remove any temporary tables from test
  finally:
    drop_table(owner_db, input_name_hes_20)
    drop_table(owner_db, input_name_hes_21)
    drop_table(owner_db, input_name_hes_22)


# COMMAND ----------

@suite.add_test
def test_join_to_hes_sensitive():

    df_input_nonsensitive = spark.createDataFrame([
        ('111',  'I167'),
        ('123', 'I161')
    ], ['EPIKEY', 'DIAG_3_01'])

    df_input_sensitive = spark.createDataFrame([
        ('111', '123123', datetime(2020, 5, 12)),
        ('123', '11111', datetime(2020, 5, 17))
    ], ['EPIKEY', 'NEWNHSNO', 'DOB'])

    df_expected = spark.createDataFrame([
        ('111', 'I167', datetime(2020, 5, 12), '123123'),
        ('123', 'I161', datetime(2020, 5, 17), '11111')
    ], ['EPIKEY', 'DIAG_3_01', 'DOB', 'NEWNHSNO'])

    def mock_get_hes_dataframes_from_years(hes_dataset_name, start_year, end_year, db_hes, db_ahas, field_pseudo_id, field_token_id, field_person_id, field_hes_id, drop_columns):
        current_year_value = int(datetime.today().strftime('%y'))
        next_year_value = current_year_value + 1
        assert hes_dataset_name == 'hes_apc'
        assert start_year == 11
        assert current_year_value <= end_year <= next_year_value
        assert db_hes == 'flat_hes_s'
        assert db_ahas == 'hes_ahas_s'
        assert drop_columns == 'FYEAR'
        return df_input_sensitive

    with FunctionPatch('get_hes_dataframes_from_years', mock_get_hes_dataframes_from_years):
        df_actual = join_to_hes_sensitive(
            df=df_input_nonsensitive, dataset_name='hes_apc')
        assert compare_results(df_actual, df_expected, join_columns=['EPIKEY'])

# COMMAND ----------

@suite.add_test
def test_join_to_hes_ae_sensitive():

    df_input_nonsensitive = spark.createDataFrame([
        ('111',  'I167'),
        ('123', 'I161')
    ], ['AEKEY', 'DIAG3_01'])

    df_input_sensitive = spark.createDataFrame([
        ('111', '123123', datetime(2020, 5, 12)),
        ('123', '11111', datetime(2020, 5, 17))
    ], ['AEKEY', 'NEWNHSNO', 'DOB'])

    df_expected = spark.createDataFrame([
        ('111', 'I167', datetime(2020, 5, 12), '123123'),
        ('123', 'I161', datetime(2020, 5, 17), '11111')
    ], ['AEKEY', 'DIAG3_01', 'DOB', 'NEWNHSNO'])

    def mock_get_hes_dataframes_from_years(hes_dataset_name, start_year, end_year, db_hes, db_ahas, field_pseudo_id, field_token_id, field_person_id, field_hes_id, drop_columns):
        current_year_value = int(datetime.today().strftime('%y'))
        next_year_value = current_year_value + 1
        assert hes_dataset_name == 'hes_ae'
        assert start_year == 11
        assert current_year_value <= end_year <= next_year_value
        assert db_hes == 'flat_hes_s'
        assert db_ahas == 'hes_ahas_s'
        assert drop_columns == 'FYEAR'
        return df_input_sensitive

    with FunctionPatch('get_hes_dataframes_from_years', mock_get_hes_dataframes_from_years):
        df_actual = join_to_hes_sensitive(
            df=df_input_nonsensitive, dataset_name='hes_ae')
        assert compare_results(df_actual, df_expected, join_columns=['AEKEY'])

# COMMAND ----------

@suite.add_test
def test_join_to_hes_op_sensitive():

    df_input_nonsensitive = spark.createDataFrame([
        ('111',  'I167'),
        ('123', 'I161')
    ], ['ATTENDKEY', 'DIAG_3_01'])

    df_input_sensitive = spark.createDataFrame([
        ('111', '123123', datetime(2020, 5, 12)),
        ('123', '11111', datetime(2020, 5, 17))
    ], ['ATTENDKEY', 'NEWNHSNO', 'DOB'])

    df_expected = spark.createDataFrame([
        ('111', 'I167', datetime(2020, 5, 12), '123123'),
        ('123', 'I161', datetime(2020, 5, 17), '11111')
    ], ['ATTENDKEY', 'DIAG_3_01', 'DOB', 'NEWNHSNO'])

    def mock_get_hes_dataframes_from_years(hes_dataset_name, start_year, end_year, db_hes, db_ahas, field_pseudo_id, field_token_id, field_person_id, field_hes_id, drop_columns):
        current_year_value = int(datetime.today().strftime('%y'))
        next_year_value = current_year_value + 1
        assert hes_dataset_name == 'hes_op'
        assert start_year == 19
        assert current_year_value <= end_year <= next_year_value
        assert db_hes == 'flat_hes_s'
        assert db_ahas == 'hes_ahas_s'
        assert drop_columns == 'FYEAR'
        return df_input_sensitive

    with FunctionPatch('get_hes_dataframes_from_years', mock_get_hes_dataframes_from_years):
        df_actual = join_to_hes_sensitive(
            df=df_input_nonsensitive, dataset_name='hes_op')
        assert compare_results(df_actual, df_expected,
                               join_columns=['ATTENDKEY'])

# COMMAND ----------

@suite.add_test
def test_join_to_hes_sensitive_limit_columns():

    df_input_nonsensitive = spark.createDataFrame([
        ('11111', datetime(2000, 1, 1), datetime(2000, 2, 1), 'I61', 'I61, I62',
         1, 'E0000', 1, 'BAR_1', date(2021, 1, 1), date(2021, 2, 2), 'Y', 5, 0, 1),
        ('22222', datetime(2002, 1, 1), datetime(2002, 2, 1), 'I71', 'I71, I72',
            2, 'E0001', 2, 'BAR_2', date(2022, 1, 1), date(2022, 2, 2), 'N', 7, 1, 1),
    ], ['EPIKEY', 'EPISTART', 'EPIEND', 'DIAG_4_01', 'DIAG_4_CONCAT', 'SEX', 'LSOA11',
        'ETHNOS', 'FOO', 'ADMIDATE', 'DISDATE', 'SPELEND', 'SPELDUR_CALC', 'SPELBGIN', 'ADMIMETH'])

    df_input_sensitive = spark.createDataFrame([
        ('11111', '001', 'H001', 'ABC 1YZ', datetime(1993, 1, 1), 'FOO_1'),
        ('22222', '002', 'H002', 'DEF 2YZ', datetime(1994, 1, 1), 'FOO_2'),
    ], ['EPIKEY', 'NEWNHSNO', 'HESID', 'HOMEADD', 'DOB', 'BAR'])

    df_expected = spark.createDataFrame([
        ('11111', datetime(2000, 1, 1), datetime(2000, 2, 1), 'I61', 'I61, I62', 1, 'E0000', 1, date(
            2021, 1, 1), date(2021, 2, 2), 'Y', 5, 0, '001', 'H001', datetime(1993, 1, 1), 1),
        ('22222', datetime(2002, 1, 1), datetime(2002, 2, 1), 'I71', 'I71, I72', 2, 'E0001', 2,
            date(2022, 1, 1), date(2022, 2, 2), 'N', 7, 1, '002', 'H002', datetime(1994, 1, 1), 1),
    ], ['EPIKEY', 'EPISTART', 'EPIEND', 'DIAG_4_01', 'DIAG_4_CONCAT', 'SEX', 'LSOA11', 'ETHNOS',
        'ADMIDATE', 'DISDATE', 'SPELEND', 'SPELDUR_CALC', 'SPELBGIN', 'NEWNHSNO', 'HESID', 'DOB', 'ADMIMETH'])

    def mock_get_hes_dataframes_from_years(hes_dataset_name, start_year, end_year, db_hes, db_ahas, field_pseudo_id, field_token_id, field_person_id, field_hes_id, drop_columns):
        current_year_value = int(datetime.today().strftime('%y'))
        next_year_value = current_year_value + 1
        assert hes_dataset_name == 'hes_apc'
        assert start_year == 11
        assert current_year_value <= end_year <= next_year_value
        assert db_hes == 'flat_hes_s'
        assert db_ahas == 'hes_ahas_s'
        assert drop_columns == 'FYEAR'
        return df_input_sensitive

    with FunctionPatch('get_hes_dataframes_from_years', mock_get_hes_dataframes_from_years):
        df_actual = join_to_hes_sensitive(
            df=df_input_nonsensitive, dataset_name='hes_apc', limit_col=True)
        assert compare_results(df_actual, df_expected, join_columns=['EPIKEY'])

# COMMAND ----------

@suite.add_test
def test_join_to_hes_ae_sensitive_limit_columns():

    df_input_nonsensitive = spark.createDataFrame([
        ('11111', datetime(2000, 1, 1), 'I61', 'I61, I62', 1, 'E0000', 'A', 1111111111111111, 'BAR_1'),
        ('22222', datetime(2002, 1, 1), 'I71', 'I71, I72', 2, 'E0001', 'L', None, 'BAR_2'),
    ], ['AEKEY', 'ARRIVALDATE', 'DIAG3_01', 'DIAG3_CONCAT', 'SEX', 'LSOA11', 'ETHNOS', 'EPIKEY','FOO'])

    df_input_sensitive = spark.createDataFrame([
        ('11111', '001', 'H001', 'ABC 1YZ', datetime(1993, 1, 1), 'FOO_1'),
        ('22222', '002', 'H002', 'DEF 2YZ', datetime(1994, 1, 1), 'FOO_2'),
    ], ['AEKEY', 'NEWNHSNO', 'HESID', 'HOMEADD', 'DOB', 'BAR'])

    df_expected = spark.createDataFrame([
        ('11111', datetime(2000, 1, 1), 'I61', 'I61, I62',
         1, 'E0000', 'A', 1111111111111111, '001', 'H001', datetime(1993, 1, 1)),
        ('22222', datetime(2002, 1, 1), 'I71', 'I71, I72',
         2, 'E0001', 'L', None, '002', 'H002', datetime(1994, 1, 1)),
    ], ['AEKEY', 'ARRIVALDATE', 'DIAG3_01', 'DIAG3_CONCAT', 'SEX', 'LSOA11', 'ETHNOS', 'EPIKEY', 'NEWNHSNO', 'HESID', 'DOB'])

    def mock_get_hes_dataframes_from_years(hes_dataset_name, start_year, end_year, db_hes, db_ahas, field_pseudo_id, field_token_id, field_person_id, field_hes_id, drop_columns):
        current_year_value = int(datetime.today().strftime('%y'))
        next_year_value = current_year_value + 1
        assert hes_dataset_name == 'hes_ae'
        assert start_year == 11
        assert current_year_value <= end_year <= next_year_value
        assert db_hes == 'flat_hes_s'
        assert db_ahas == 'hes_ahas_s'
        assert drop_columns == 'FYEAR'
        return df_input_sensitive

    with FunctionPatch('get_hes_dataframes_from_years', mock_get_hes_dataframes_from_years):
        df_actual = join_to_hes_sensitive(
            df=df_input_nonsensitive, dataset_name='hes_ae', limit_col=True)
        assert compare_results(df_actual, df_expected, join_columns=['AEKEY'])

# COMMAND ----------

@suite.add_test
def test_join_to_hes_op_sensitive_limit_columns():

    df_input_nonsensitive = spark.createDataFrame([
        ('11111', datetime(2000, 1, 1), 'I61', 'I61, I62', 'A', 'BAR_1'),
        ('22222', datetime(2002, 1, 1), 'I71', 'I71, I72', 'L', 'BAR_2'),
    ], ['ATTENDKEY', 'APPTDATE', 'DIAG_3_01', 'DIAG_3_CONCAT', 'ETHNOS', 'FOO'])

    df_input_sensitive = spark.createDataFrame([
        ('11111', '001', 'H001', 'ABC 1YZ', datetime(1993, 1, 1), 'FOO_1'),
        ('22222', '002', 'H002', 'DEF 2YZ', datetime(1994, 1, 1), 'FOO_2'),
    ], ['ATTENDKEY', 'NEWNHSNO', 'HESID', 'HOMEADD', 'DOB', 'BAR'])

    df_expected = spark.createDataFrame([
        ('11111', datetime(2000, 1, 1), 'I61', 'I61, I62',
         'A', '001', 'H001', datetime(1993, 1, 1)),
        ('22222', datetime(2002, 1, 1), 'I71', 'I71, I72',
            'L', '002', 'H002', datetime(1994, 1, 1)),
    ], ['ATTENDKEY', 'APPTDATE', 'DIAG_3_01', 'DIAG_3_CONCAT', 'ETHNOS', 'NEWNHSNO', 'HESID', 'DOB'])

    def mock_get_hes_dataframes_from_years(hes_dataset_name, start_year, end_year, db_hes, db_ahas, field_pseudo_id, field_token_id, field_person_id, field_hes_id, drop_columns):
        current_year_value = int(datetime.today().strftime('%y'))
        next_year_value = current_year_value + 1
        assert hes_dataset_name == 'hes_op'
        assert start_year == 19
        assert current_year_value <= end_year <= next_year_value
        assert db_hes == 'flat_hes_s'
        assert db_ahas == 'hes_ahas_s'
        assert drop_columns == 'FYEAR'
        return df_input_sensitive

    with FunctionPatch('get_hes_dataframes_from_years', mock_get_hes_dataframes_from_years):
        df_actual = join_to_hes_sensitive(
            df=df_input_nonsensitive, dataset_name='hes_op', limit_col=True)
        assert compare_results(df_actual, df_expected,
                               join_columns=['ATTENDKEY'])

# COMMAND ----------

@suite.add_test
def test_join_to_hes_apc_otr():

    df_input = spark.createDataFrame([
        ('111', 'I167', datetime(2020, 5, 12), '123123'),
        ('123', 'I161', datetime(2020, 5, 17), '11111'),
        ('145', 'I162', datetime(2020, 5, 19), '22222')
    ], ['EPIKEY', 'DIAG_3_01', 'DOB', 'NEWNHSNO'])

    df_hes_apc_otr = spark.createDataFrame([
        ('111', '5748315798258', 'foo_1'),
        ('456', '2345981829402', 'foo_2'),
        ('145', None, 'foo_2'),
    ], ['EPIKEY', 'SUSSPELLID', 'BAR'])

    df_expected = spark.createDataFrame([
        ('111', 'I167', datetime(2020, 5, 12), '123123', '5748315798258'),
        ('123', 'I161', datetime(2020, 5, 17), '11111', None),
        ('145', 'I162', datetime(2020, 5, 19), '22222', None)
    ], ['EPIKEY', 'DIAG_3_01', 'DOB', 'NEWNHSNO', 'SUSSPELLID'])

    def mock_get_hes_dataframes_from_years(hes_dataset_name, start_year, end_year, db_hes, db_ahas, field_pseudo_id, field_token_id, field_person_id, field_hes_id, join_filter_columns):
        current_year_value = int(datetime.today().strftime('%y'))
        next_year_value = current_year_value + 1
        assert hes_dataset_name == 'hes_apc_otr'
        assert start_year == 11
        assert current_year_value <= end_year <= next_year_value
        assert db_hes == 'hes'
        assert db_ahas == 'hes_ahas'
        assert join_filter_columns == ['EPIKEY', 'SUSSPELLID']
        return df_hes_apc_otr

    with FunctionPatch('get_hes_dataframes_from_years', mock_get_hes_dataframes_from_years):
        df_actual = join_to_hes_apc_otr(df=df_input)
        assert compare_results(df_actual, df_expected, join_columns=['EPIKEY'])

# COMMAND ----------

suite.run()

# COMMAND ----------

dbutils.notebook.exit('pass')
