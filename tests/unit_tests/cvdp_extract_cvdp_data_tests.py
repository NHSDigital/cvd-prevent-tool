# Databricks notebook source
PARAMS_PATH = 'default'

# COMMAND ----------

# MAGIC %run ../../params/params

# COMMAND ----------

# MAGIC %run ../../src/test_helpers

# COMMAND ----------

# MAGIC %run ../../src/cvdp/extract_cvdp_data

# COMMAND ----------

import pyspark.sql.functions as F
import pyspark.sql.types as T

from datetime import date
from dsp.validation.validator import compare_results

# COMMAND ----------

suite = FunctionTestSuite()

## The following are fake people and data created for test purposes

# COMMAND ----------

@suite.add_test
def test_process_cvdp_extract():
    '''test_process_cvdp_extract
    Test of the full extract CVDP preprocessing function. Process is responsible for combining,
    filtering (on cohort) and adding the primary (hashed) id key and produce the final extract
    CVDP assset.
    '''
    # Test Dataframe Setup: Inputs, Filter Values and Hashable Fields
    df_input_annual = spark.createDataFrame([
        (1,'C01','A'),
        (3,None,'C'),
    ], ['idx','cohort','val'])

    df_input_quarterly = spark.createDataFrame([
        (2,'C02','B'),
        (4,'C03','D')
    ], ['idx','cohort','val'])

    test_cohort_codes = ['C01','C02']
    test_hashable_fields = ['idx','val']

    # Test Dataframe Setup: Expected output
    df_expected = spark.createDataFrame([
        (1,'C01','A','b6d4913b6c53f6699e89d883423f6e86ff80042dc21b64127205e6b8f4dd1e5b'),
        (2,'C02','B','e264a61e6e7bd8a03a58ef0dfc980a6064f6e816ccaa5549330368e5d9954df3'),
    ], ['idx','cohort','val','hash_id'])

    # Run Test
    df_actual = process_cvdp_extract(
        cvdp_annual = df_input_annual,
        cvdp_quarterly = df_input_quarterly,
        field_cohort = 'cohort',
        filter_cohort_codes = test_cohort_codes,
        col_hashed_key = 'hash_id',
        fields_to_hash = test_hashable_fields
    )
    assert compare_results(df_actual, df_expected, join_columns = ['idx'])

# COMMAND ----------

suite.run()

# COMMAND ----------

dbutils.notebook.exit('pass')
