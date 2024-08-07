# Databricks notebook source
# Notebook overview: Contains the PipelineResultsStage, see Pipeline Stages Confluence page for more info.

# COMMAND ----------

# MAGIC %run ../params/params

# COMMAND ----------

# MAGIC %run ./pipeline_util

# COMMAND ----------

# MAGIC %run ../src/util

# COMMAND ----------

import warnings
import inspect
from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType

# COMMAND ----------

REPORT_LEVEL_INFO = 'info'
REPORT_LEVEL_PASS = 'pass'
REPORT_LEVEL_WARN = 'warning'
REPORT_LEVEL_FAIL = 'fail'

# COMMAND ----------

class DefaultCreateReportTableStage(PipelineStage):
  '''
  This pipeline stage run checks on the outputs from pipeline/default_pipeline and makes a table
  reporting info and fatal errors. The errors are raised in a later stage called
  DefaultCheckResultsStage.

  The created reporting table has schema as defined in _REPORT_SCHEMA. The check columns contains
  the function name that ran the check. Some functions below run multiple checks so the same check
  value can appear multiple times in the table. The level is 'pass', 'warning', 'fail' or 'info':
  - 'pass' means the check passed, and the associated message will be empty.
  - 'warning' means the check did not pass due to a data quality issue in the output tables (i.e.
    an unmappable value) as opposed to a data integrity issue, with an accompanying message.
  - 'fail' means the check has failed (there has been a data integrity issue) and there will be
    an associated message describing the issue.
  - 'info' is general information where the message will contain a count or some other value.

  '''
  _REPORT_SCHEMA = StructType([
    StructField('level', StringType(), False),
    StructField('check', StringType(), False),
    StructField('message', StringType(), True),
  ])

  def __init__(self, events_table_input: str, patient_table_input: str, report_table_output: str):
    self._events_table_input = events_table_input
    self._patient_table_input = patient_table_input
    self._report_table_output = report_table_output
    self._df_report = self._create_report_table()
    super(DefaultCreateReportTableStage, self).__init__({self._events_table_input, self._patient_table_input},
                                                        {self._report_table_output})

  def _run(self, context, log):

    log._add_stage(self.name)

    log._timer(self.name)

    df_events = context[self._events_table_input].df
    df_patients = context[self._patient_table_input].df

    self.check_events_table_all_datasets_present(df_events)
    self.check_events_table_not_empty(df_events)
    self.check_events_table_any_fields_all_nulls(df_events)
    self.check_events_table_non_nullable_columns_for_nulls(df_events)
    self.check_events_table_schema_equality(df_events)
    self.check_patients_table_not_empty(df_patients)
    self.check_patients_table_all_cohorts_present(df_patients)
    self.check_patients_table_any_fields_all_nulls(df_patients)
    self.check_patients_table_non_nullable_columns_for_nulls(df_patients)
    self.check_patients_table_schema_equality(df_patients)

    log._timer(self.name, end=True)

    return {self._report_table_output: PipelineAsset(self._report_table_output, context, df = self._df_report, cache = True)}

  def _create_report_table(self) -> DataFrame:
    """Creates report table output.
    """
    return spark.createDataFrame([], self._REPORT_SCHEMA)

  def _add_pass_or_fail(self, passed: bool, fail_message: str) -> None:
    """Creates a pass or fail record for the report table.
    """
    level = REPORT_LEVEL_PASS if passed else REPORT_LEVEL_FAIL
    message = fail_message if not passed else None
    self._add_record(level, message)

  def _add_pass_or_warning(self, passed: bool, warn_message: str) -> None:
    """Creates a pass or warning record for the report table.
    """
    level = REPORT_LEVEL_PASS if passed else REPORT_LEVEL_WARN
    message = warn_message if not passed else None
    self._add_record(level, message)

  def _add_info(self, message: str) -> None:
    """Creates a pass or info record for the report table.
    """
    self._add_record(REPORT_LEVEL_INFO, message)

  def _add_record(self, level: str, message: str) -> None:
    """Adds a record to the report table output based on the check carried out.
    """
    check_function_name = inspect.stack()[2].function
    df_row = spark.createDataFrame([(level, check_function_name, message)], self._REPORT_SCHEMA)
    self._df_report = self._df_report.union(df_row)

  def _get_all_null_fields(self, df: DataFrame) -> List[str]:
    """Return a list of the fields that only contain nulls
    """
    null_fields = []
    for field in df.columns:
      if df.where(col(field).isNotNull()).count() == 0:
        null_fields.append(field)
    return null_fields

  def check_events_table_all_datasets_present(self, df_events: DataFrame) -> None:
    """Checks Events table for missing or extra datasets present.
    """
    datasets_present = set([r[0] for r in df_events.select(params.DATASET_FIELD).distinct().collect()])
    missing_datasets = set(params.DATASETS_RESULTS_CHECKER).difference(datasets_present)
    extra_datasets = datasets_present.difference(set(params.DATASETS_RESULTS_CHECKER))
    missing_passed = len(missing_datasets) == 0
    extra_passed = len(extra_datasets) == 0
    self._add_pass_or_warning(missing_passed, f'Event table is missing records for datasets: {missing_datasets}')
    self._add_pass_or_warning(extra_passed, f'Event table contains extra datasets that should not be present: '
                                         f'{extra_datasets}')

  def check_patients_table_all_cohorts_present(self, df_patients: DataFrame) -> None:
    """Checks Patients table for missing or extra cohorts present.
    """
    cohorts_present = set([r[0] for r in df_patients.select(params.COHORT_FIELD).distinct().collect()])
    missing_cohorts = set(params.COHORTS_RESULTS_CHECKER).difference(cohorts_present)
    extra_cohorts = cohorts_present.difference(set(params.COHORTS_RESULTS_CHECKER))
    missing_passed = len(missing_cohorts) == 0
    extra_passed = len(extra_cohorts) == 0
    self._add_pass_or_warning(missing_passed, f'Patients table is missing records for cohorts: {missing_cohorts}')
    self._add_pass_or_warning(extra_passed, f'Patients table contains extra cohorts that should not be present: '
                                         f'{extra_cohorts}')

  def check_events_table_not_empty(self, df_events: DataFrame) -> None:
    """Checks whether Events table is completely empty, no records.
    """
    passed = not is_empty(df_events)
    self._add_pass_or_fail(passed, 'Event table is empty.')

  def check_events_table_any_fields_all_nulls(self, df_events: DataFrame) -> None:
    """Checks Events table for columns only containing null values.
    """
    null_fields = self._get_all_null_fields(df_events)
    passed = len(null_fields) == 0
    self._add_pass_or_warning(passed, f'Event table has fields which only contain null values: {null_fields}')

  def check_patients_table_not_empty(self, df_patients: DataFrame) -> None:
    """Checks whether Patients table is completely empty, no records.
    """
    passed = not is_empty(df_patients)
    self._add_pass_or_fail(passed, 'Patient table is empty.')

  def check_patients_table_any_fields_all_nulls(self, df_patients: DataFrame) -> None:
    """Checks Patients table for columns only containing null values.
    """
    null_fields = self._get_all_null_fields(df_patients)
    passed = len(null_fields) == 0
    self._add_pass_or_warning(passed, f'Patient table has fields which only contain null values: {null_fields}')

  def check_patients_table_non_nullable_columns_for_nulls(self, df_patients: DataFrame) -> None:
    """Checks Patients table non-nullable columns if they contain null values.
    """
    cols = [params.PID_FIELD, params.DOB_FIELD, params.LATEST_EXTRACT_DATE]
    null_fields = []
    for c in cols:
      if df_patients.where(F.col(c).isNull()).count() > 0:
        null_fields.append(c)
    passed = len(null_fields) == 0
    self._add_pass_or_warning(passed, f'Patients table has non-nullable fields which contain null values: {null_fields}')

  def check_events_table_non_nullable_columns_for_nulls(self, df_events: DataFrame) -> None:
    """Checks Events table non-nullable columns if they contain null values.
    """
    cols = [params.PID_FIELD, params.DATASET_FIELD, params.CATEGORY_FIELD, params.RECORD_ID_FIELD, params.RECORD_STARTDATE_FIELD]
    null_fields = []
    for c in cols:
      if df_events.where(F.col(c).isNull()).count() > 0:
        null_fields.append(c)
    passed = len(null_fields) == 0
    self._add_pass_or_warning(passed, f'Events table has non-nullable fields which contain null values: {null_fields}')

  def check_patients_table_schema_equality(self, df_patients: DataFrame) -> None:
    """Assert schema equality on column names, data types and nullables status.
    """
    # create new dataframe with ground truth schema
    df1 = spark.createDataFrame([], params.EXPECTED_PATIENTS_SCHEMA)

    # remove META column due to chaotic schema
    df_patients = df_patients.drop(F.col("META"))

    if df1.schema != df_patients.schema:
      s1 = df1.schema
      s2 = df_patients.schema
      a = []
      zipped = list(itertools.zip_longest(s1, s2))
      for sf1, sf2 in zipped:
        if sf1 == sf2:
          pass
        else:
          a.append([sf1, sf2])
      passed = len(a) == 0
      print("\nPatients table (ground truth left, output right):")
      [print(*x) for x in a][0]
      self._add_pass_or_warning(passed, f'Found Patients schema missmatch: {a}')
    else:
      passed = df1.schema == df_patients.schema
      self._add_pass_or_warning(passed, f'Check function for bugs.')

  def check_events_table_schema_equality(self, df_events: DataFrame) -> None:
    """Assert schema equality on column names, data types and nullables status.
    """
    # create new dataframe with ground truth schema
    df1 = spark.createDataFrame([], params.EXPECTED_EVENTS_SCHEMA)

    # remove META column due to chaotic schema
    df_events = df_events.drop(F.col("META"))

    if df1.schema != df_events.schema:
      s1 = df1.schema
      s2 = df_events.schema
      a = []
      zipped = list(itertools.zip_longest(s1, s2))
      for sf1, sf2 in zipped:
        if sf1 == sf2:
          pass
        else:
          a.append([sf1, sf2])
      passed = len(a) == 0
      print("\nEvents table (ground truth left, output right):")
      [print(*x) for x in a][0]
      self._add_pass_or_warning(passed, f'Found Events schema missmatch: {a}')
    else:
      passed = df1.schema == df_events.schema
      self._add_pass_or_warning(passed, f'Check function for bugs.')

# COMMAND ----------

class DefaultCheckResultsStage(PipelineStage):
  '''This pipeline stage reads the report table created in DefaultCreateReportTableStage and raises
  an error if any of the level values in the report table are 'fail'. It prints all messages with
  'info', 'warning' and 'fail' levels.

  Input:
    PipelineStage
  Output:
    Prints messages with varying status levels.
  '''

  def __init__(self, report_input: str):
    self._report_input = report_input
    super(DefaultCheckResultsStage, self).__init__({self._report_input}, set())

  def _run(self, context, log):

    log._add_stage(self.name)

    log._timer(self.name)

    df_report = context[self._report_input].df
    self.report(df_report)

    log._timer(self.name, end=True)

    return {}

  def report(self, df_report: DataFrame) -> None:
    df_info = df_report.where(col('level') == REPORT_LEVEL_INFO)
    for row in df_info.collect():
      print(f'INFO: {row.check}: {row.message}')

    df_warning = df_report.where(col('level') == REPORT_LEVEL_WARN)
    if df_warning.count() > 0:
      for row in df_warning.collect():
        print(f'WARNING: {row.check}: {row.message}')
      warnings.warn('Check results stage has %d warnings, see errors above and check report table.' % df_warning.count())

    df_fail = df_report.where(col('level') == REPORT_LEVEL_FAIL)
    if df_fail.count() > 0:
      for row in df_fail.collect():
        print(f'ERROR: {row.check}: {row.message}')
      warnings.warn('Check results stage has %d failures, see errors above and check report table.' % df_fail.count())
      raise AssertionError('Check results stage has %d failures, see errors above and check report table.' % df_fail.count())