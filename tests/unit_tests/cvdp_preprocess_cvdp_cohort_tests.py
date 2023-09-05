# Databricks notebook source
PARAMS_PATH = 'default'

# COMMAND ----------

# MAGIC %run ../../params/params

# COMMAND ----------

# MAGIC %run ../../src/test_helpers

# COMMAND ----------

# MAGIC %run ../../src/cvdp/preprocess_cvdp_cohort

# COMMAND ----------

import pyspark.sql.types as T

from datetime import date
from dsp.validation.validator import compare_results

# COMMAND ----------

suite = FunctionTestSuite()

## The following are fake people and data created for test purposes

# COMMAND ----------

@suite.add_test
def test_cvdp_filter_cohorts():
    
    df_input = spark.createDataFrame([
        (0, 'A'),
        (1, 'B'),
        (2, 'C'),
        (3, 'D'),
        (4, 'AA'),
        (5, 'AB'),
        (6, None),
    ], ['idx','cohort_col'])
    
    
    df_expected = spark.createDataFrame([
        (0, 'A'),
        (3, 'D'),
    ], ['idx','cohort_col'])
    
    df_actual = cvdp_filter_cohorts(
        df                  = df_input,
        field_cohort        = 'cohort_col',
        filter_cohort_codes = ['A','D']
    )

    assert compare_results(df_actual, df_expected, join_columns = ['idx'])

# COMMAND ----------


@suite.add_test
def test_cvdp_remove_invalid_nhs():
    ## Note: valid NHS Numbers are replaced with xxxxxxxxxx to avoid the posibility of PII being shared
    df_input = spark.createDataFrame([
        (0, 'xxxxxxxxxx'),
        (1, '222222222'),   # Removed: 9 digits
        (2, '33333333333'), # Removed: 11 digits
        (3, '0000000000'),  # Removed: Matching full nhs number
        (4, '1111111111'),  # Removed: Matching full nhs number
        (5, '9999999999'),  # Removed: Matching prefix
        (6, None)           # Removed: NULL entry
    ], ['idx', 'id'])


    df_expected = spark.createDataFrame([
        (0, 'xxxxxxxxxx'),
    ], ['idx', 'id'])

    df_actual = cvdp_remove_invalid_nhs(
        df = df_input,
        field_nhs_number    = 'id',
        invalid_nhs_numbers = ['0000000000', '1111111111'],
        invalid_nhs_prefix  = '9'
    )

    assert compare_results(df_actual, df_expected, join_columns = ['idx'])

# COMMAND ----------

@suite.add_test
def test_cvdp_remove_null_records():
    
    df_input = spark.createDataFrame([
        (0, 'A', date(2000,1,1), 'BAR'),    # Keep:     PID (T) | DOB (T) | FOO (T)
        (1, None, date(2000,1,2), 'BAR'),   # Removed:  PID (F) | DOB (T) | FOO (T)
        (2, 'C', None, 'BAR'),              # Removed:  PID (T) | DOB (F) | FOO (T)
        (3, None, None, None),              # Removed:  PID (F) | DOB (F) | FOO (F)
        (4, None, None, 'BAR'),             # Removed:  PID (F) | DOB (F) | FOO (F)
        (5, 'F', date(2000,1,6), None),     # Keep:     PID (T) | DOB (T) | FOO (F)
    ], ['idx', 'PID', 'DOB', 'FOO'])
    
    
    df_expected = spark.createDataFrame([
        (0, 'A', date(2000,1,1), 'BAR'),
        (5, 'F', date(2000,1,6), None),
    ], ['idx', 'PID', 'DOB', 'FOO'])
    
    df_actual = cvdp_remove_null_records(
        df          = df_input,
        null_col    = ['PID','DOB']
    )

    assert compare_results(df_actual, df_expected, join_columns = ['idx'])

# COMMAND ----------

@suite.add_test
def test_cvdp_combine_store_data():
    
    df_input_01 = spark.createDataFrame([
        (0, 'A', date(2000,1,1)),
        (1, 'B', date(2000,1,2)),
        (2, 'C', date(2000,1,3)),
    ], ['idx','PID','DOB'])
    
    df_input_02 = spark.createDataFrame([
        (3, 'D', date(2001,1,1)),
        (4, 'E', date(2001,1,2)),
        (5, 'F', date(2001,1,3)),
    ], ['idx','PID','DOB'])
    
    
    df_expected = spark.createDataFrame([
        (0, 'A', date(2000,1,1)),
        (1, 'B', date(2000,1,2)),
        (2, 'C', date(2000,1,3)),
        (3, 'D', date(2001,1,1)),
        (4, 'E', date(2001,1,2)),
        (5, 'F', date(2001,1,3)),
    ], ['idx','PID','DOB'])
    
    df_actual = cvdp_combine_store_data(
        df_annual       = df_input_01,
        df_quarterly    = df_input_02
    )

    assert compare_results(df_actual, df_expected, join_columns = ['idx'])

# COMMAND ----------

@suite.add_test
def test_cvdp_deduplicate_cohort_entries():
    '''test_cvdp_deduplicate_cohort_entries
    
    Tests the deduplication of the CVDP extract table when the extract_date field is not specified as a window field.
    This defaults to standard function behaviour - keep the latest extract date. The deduplication process is then 
    carried out with the default behaviour. 
    '''
    data_schema = T.StructType([
        T.StructField('PID', T.StringType(), False),
        T.StructField('DOB', T.DateType(), False),
        T.StructField('LSOA', T.StringType(), True),
        T.StructField('EXTRACT', T.DateType(), False),
        T.StructField('JOURNAL', T.ArrayType(
            T.StructType([
                T.StructField('DATE', T.DateType(), False),
                T.StructField('COND_01', T.StringType(), True),
                T.StructField('COND_02', T.StringType(), True),
            ])), False),
    ])

    df_input = spark.createDataFrame([
        ('A', date(2020,1,1), 'E001', date(2000,1,1),[{'DATE': date(2000,1,1),'COND_01':'A01','COND_02':'A03'}]),   # [A] Keep - Single
        ('B', date(2020,1,1), 'E002', date(2000,1,1),[{'DATE': date(2000,1,1),'COND_01':'A01','COND_02':'A03'}]),   # [B] Remove
        ('B', date(2020,1,1), 'E002', date(2000,1,1),[{'DATE': date(2000,1,2),'COND_01':'A01','COND_02':'A03'}]),   # [B] Keep - Latest Journal Date
        ('C', date(2020,1,1), 'E002', date(2000,1,1),[{'DATE': date(2000,1,1),'COND_01':'A01','COND_02':'A03'}]),   # [C] Remove
        ('C', date(2020,1,1), 'E002', date(2000,1,2),[{'DATE': date(2000,1,1),'COND_01':'A01','COND_02':'A03'}]),   # [C] Keep - Latest Extract Date
        ('D', date(2020,1,1), None, date(2000,1,1),[{'DATE': date(2000,1,1),'COND_01':'A01','COND_02':'A03'}]),     # [D] Remove
        ('D', date(2020,1,1), 'E002', date(2000,1,1),[{'DATE': date(2000,1,1),'COND_01':'A01','COND_02':'A03'}]),   # [D] Keep - Non-null LSOA
        ('E', date(2020,1,1), 'E002', date(2000,1,1),[{'DATE': date(2000,1,2),'COND_01':'A01','COND_02':'A03'}]),   # [E] Remove
        ('E', date(2020,1,1), 'E002', date(2000,1,1),[{'DATE': date(2000,1,1),'COND_01':'A01','COND_02':'A03'},
                                                      {'DATE': date(2000,1,2),'COND_01':'A02','COND_02':None}]),    # [E] Keep - Max Journal Size   
        ('F', date(2020,1,1), 'E002', date(2000,1,1),[{'DATE': date(2000,1,2),'COND_01':'A01','COND_02':'A03'}]),   # [F] Keep Random
        ('F', date(2020,1,1), 'E002', date(2000,1,1),[{'DATE': date(2000,1,2),'COND_01':'A01','COND_02':'A03'}]),   # [F] Keep Random
    ], data_schema)

    df_expected = spark.createDataFrame([
        ('A', date(2020,1,1), 'E001', date(2000,1,1),[{'DATE': date(2000,1,1),'COND_01':'A01','COND_02':'A03'}]),
        ('B', date(2020,1,1), 'E002', date(2000,1,1),[{'DATE': date(2000,1,2),'COND_01':'A01','COND_02':'A03'}]),
        ('C', date(2020,1,1), 'E002', date(2000,1,2),[{'DATE': date(2000,1,1),'COND_01':'A01','COND_02':'A03'}]),
        ('D', date(2020,1,1), 'E002', date(2000,1,1),[{'DATE': date(2000,1,1),'COND_01':'A01','COND_02':'A03'}]),
        ('E', date(2020,1,1), 'E002', date(2000,1,1),[{'DATE': date(2000,1,1),'COND_01':'A01','COND_02':'A03'},
                                      {'DATE': date(2000,1,2),'COND_01':'A02','COND_02':None}]),
        ('F', date(2020,1,1), 'E002', date(2000,1,1),[{'DATE': date(2000,1,2),'COND_01':'A01','COND_02':'A03'}]),
    ], data_schema)

    df_actual = cvdp_deduplicate_cohort_entries(
        df = df_input,
        fields_window = ['PID','DOB'],
        field_extract_date = 'EXTRACT',
        field_journal_table = 'JOURNAL',
        field_journal_date = 'DATE',
        fields_null_drop = 'LSOA',
    )

    assert compare_results(df_actual, df_expected, join_columns = ['PID','DOB'])

# COMMAND ----------

@suite.add_test
def test_cvdp_deduplicate_cohort_entries_all_extracts():
    '''test_cvdp_deduplicate_cohort_entries_all_extracts
    
    Tests the deduplication of the CVDP extract table when the extract_date field is specified as a window field.
    This alters the behaviour of the code to keep one-record-per-patient-per-extract date. The deduplication process 
    is then carried out with the default behaviour. 
    '''

    data_schema = T.StructType([
        T.StructField('PID', T.StringType(), False),
        T.StructField('DOB', T.DateType(), False),
        T.StructField('LSOA', T.StringType(), True),
        T.StructField('EXTRACT', T.DateType(), False),
        T.StructField('JOURNAL', T.ArrayType(
            T.StructType([
                T.StructField('DATE', T.DateType(), False),
                T.StructField('COND_01', T.StringType(), True),
                T.StructField('COND_02', T.StringType(), True),
            ])), False),
    ])

    df_input = spark.createDataFrame([
        ('A', date(2020,1,1), 'E001', date(2000,1,1),[{'DATE': date(2000,1,1),'COND_01':'A01','COND_02':'A03'}]),   # [A] Keep - Single
        ('A', date(2020,1,1), 'E001', date(2001,1,1),[{'DATE': date(2000,1,1),'COND_01':'A01','COND_02':'A03'}]),   # [A] Keep - Single
        ('A', date(2020,1,1), 'E001', date(2002,1,1),[{'DATE': date(2000,1,1),'COND_01':'A01','COND_02':'A03'}]),   # [A] Keep - Single
        ('B', date(2020,1,1), 'E002', date(2000,1,1),[{'DATE': date(2000,1,1),'COND_01':'A01','COND_02':'A03'}]),   # [B] Remove
        ('B', date(2020,1,1), 'E002', date(2000,1,1),[{'DATE': date(2000,1,2),'COND_01':'A01','COND_02':'A03'}]),   # [B] Keep - Latest Journal Date
        ('B', date(2020,1,1), 'E002', date(2001,1,1),[{'DATE': date(2000,1,2),'COND_01':'A01','COND_02':'A03'}]),   # [B] Keep - Latest Journal Date
        ('C', date(2020,1,1), 'E002', date(2000,1,1),[{'DATE': date(2000,1,1),'COND_01':'A01','COND_02':'A03'}]),   # [C] Remove
        ('C', date(2020,1,1), 'E002', date(2000,1,1),[{'DATE': date(2000,1,2),'COND_01':'A01','COND_02':'A03'}]),   # [C] Keep - Latest Extract Date
        ('D', date(2020,1,1), None, date(2000,1,1),[{'DATE': date(2000,1,1),'COND_01':'A01','COND_02':'A03'}]),     # [D] Remove
        ('D', date(2020,1,1), 'E002', date(2000,1,1),[{'DATE': date(2000,1,1),'COND_01':'A01','COND_02':'A03'}]),   # [D] Keep - Non-null LSOA
        ('E', date(2020,1,1), 'E002', date(2000,1,1),[{'DATE': date(2000,1,2),'COND_01':'A01','COND_02':'A03'}]),   # [E] Remove
        ('E', date(2020,1,1), 'E002', date(2000,1,1),[{'DATE': date(2000,1,1),'COND_01':'A01','COND_02':'A03'},
                                                      {'DATE': date(2000,1,2),'COND_01':'A02','COND_02':None}]),    # [E] Keep - Max Journal Size   
        ('F', date(2020,1,1), 'E002', date(2000,1,1),[{'DATE': date(2000,1,2),'COND_01':'A01','COND_02':'A03'}]),   # [F] Keep Random
        ('F', date(2020,1,1), 'E002', date(2000,1,1),[{'DATE': date(2000,1,2),'COND_01':'A01','COND_02':'A03'}]),   # [F] Keep Random
        ('G', date(2020,1,1), 'E002', date(2000,1,1),[{'DATE': date(2000,1,2),'COND_01':'A01','COND_02':'A03'}]),   # [G] Keep Random - Early Extract
        ('G', date(2020,1,1), 'E002', date(2000,1,1),[{'DATE': date(2000,1,2),'COND_01':'A01','COND_02':'A03'}]),   # [G] Keep Random - Early Extract
        ('G', date(2021,1,1), 'E002', date(2000,1,1),[{'DATE': date(2000,1,2),'COND_01':'A01','COND_02':'A03'}]),   # [G] Keep Random - Later Extract
        ('G', date(2021,1,1), 'E002', date(2000,1,1),[{'DATE': date(2000,1,2),'COND_01':'A01','COND_02':'A03'}]),   # [G] Keep Random - Later Extract
    ], data_schema)

    df_expected = spark.createDataFrame([
        ('A', date(2020,1,1), 'E001', date(2000,1,1),[{'DATE': date(2000,1,1),'COND_01':'A01','COND_02':'A03'}]),
        ('A', date(2020,1,1), 'E001', date(2001,1,1),[{'DATE': date(2000,1,1),'COND_01':'A01','COND_02':'A03'}]),
        ('A', date(2020,1,1), 'E001', date(2002,1,1),[{'DATE': date(2000,1,1),'COND_01':'A01','COND_02':'A03'}]),
        ('B', date(2020,1,1), 'E002', date(2000,1,1),[{'DATE': date(2000,1,2),'COND_01':'A01','COND_02':'A03'}]),
        ('B', date(2020,1,1), 'E002', date(2001,1,1),[{'DATE': date(2000,1,2),'COND_01':'A01','COND_02':'A03'}]),
        ('C', date(2020,1,1), 'E002', date(2000,1,1),[{'DATE': date(2000,1,2),'COND_01':'A01','COND_02':'A03'}]),
        ('D', date(2020,1,1), 'E002', date(2000,1,1),[{'DATE': date(2000,1,1),'COND_01':'A01','COND_02':'A03'}]),
        ('E', date(2020,1,1), 'E002', date(2000,1,1),[{'DATE': date(2000,1,1),'COND_01':'A01','COND_02':'A03'},
                                      {'DATE': date(2000,1,2),'COND_01':'A02','COND_02':None}]),
        ('F', date(2020,1,1), 'E002', date(2000,1,1),[{'DATE': date(2000,1,2),'COND_01':'A01','COND_02':'A03'}]),
        ('G', date(2020,1,1), 'E002', date(2000,1,1),[{'DATE': date(2000,1,2),'COND_01':'A01','COND_02':'A03'}]),
        ('G', date(2021,1,1), 'E002', date(2000,1,1),[{'DATE': date(2000,1,2),'COND_01':'A01','COND_02':'A03'}])

    ], data_schema)

    df_actual = cvdp_deduplicate_cohort_entries(
        df = df_input,
        fields_window = ['PID','DOB','EXTRACT'],
        field_extract_date = 'EXTRACT',
        field_journal_table = 'JOURNAL',
        field_journal_date = 'DATE',
        fields_null_drop = 'LSOA',
    )

    assert compare_results(df_actual, df_expected, join_columns = ['PID','DOB','EXTRACT'])

# COMMAND ----------

@suite.add_test
def test_preprocess_cvdp_cohort():
    
    ## Test Values
    valid_cohort_codes      = ['C01','C02']
    invalid_nhs_numbers     = ['0000000000','1111111111']
    invalid_nhs_prefixes    = ['8','9']

    ## Note: valid NHS Numbers are replaced with xxxxxxxxxn to avoid the posibility of PII being shared

    data_schema = T.StructType([
        T.StructField('PID', T.StringType(), True),
        T.StructField('DOB', T.DateType(), True),
        T.StructField('LSOA', T.StringType(), True),
        T.StructField('COHORT', T.StringType(), False),
        T.StructField('EXTRACT', T.DateType(), False),
        T.StructField('FOO', T.StringType(), True),
        T.StructField('JOURNAL', T.ArrayType(
            T.StructType([
                T.StructField('DATE', T.DateType(), False),
                T.StructField('COND_01', T.StringType(), True),
            ])), False),
    ])

    df_input_annual = spark.createDataFrame([
        ('xxxxxxxxx1', date(2000,1,1), 'E001', 'C01', date(2022,1,1), 'BAR_1A', [{'DATE': date(2022,1,1), 'COND_01':'A01'}]),       # Keep
        ('xxxxxxxxx2', date(2000,1,2), 'E001', 'C01', date(2022,1,1), 'BAR_2A_1', [{'DATE': date(2022,1,1), 'COND_01':'A01'}]),     # Keep
        ('xxxxxxxxx2', date(2000,1,2), 'E001', 'C01', date(2022,1,2), 'BAR_2A_2', [{'DATE': date(2022,1,1), 'COND_01':'A01'}]),     # Keep
        ('xxxxxxxxx3', date(2000,1,3), 'E001', 'C02', date(2022,1,1), 'BAR_3A', [{'DATE': date(2022,1,1), 'COND_01':'A01'}]),       # Keep
        ('0000000004', date(2000,1,4), 'E001', 'C03', date(2022,1,1), 'BAR_4A', [{'DATE': date(2022,1,1), 'COND_01':'A01'}]),       # Remove
        ('0000000000', date(2000,1,5), 'E001', 'C01', date(2022,1,1), 'BAR_5A', [{'DATE': date(2022,1,1), 'COND_01':'A01'}]),       # Remove
        ('8000000000', date(2000,1,6), 'E001', 'C01', date(2022,1,1), 'BAR_6A', [{'DATE': date(2022,1,1), 'COND_01':'A01'}]),       # Remove
        ('0000000007', None, 'E001', 'C01', date(2022,1,1), 'BAR_7A', [{'DATE': date(2022,1,1), 'COND_01':'A01'}]),                 # Remove
        (None, date(2000,1,8), 'E001', 'C01', date(2022,1,1), 'BAR_8A', [{'DATE': date(2022,1,1), 'COND_01':'A01'}]),               # Remove
        ('0000000009', date(2000,1,9), 'E001', 'C01', date(2022,1,1), 'BAR_9A', [{'DATE': date(2022,1,1), 'COND_01':'A01'}]),       # Remove
        ('xxxxxxxxx5', date(2000,1,10), 'E001', 'C01', date(2022,1,10), 'BAR_10A', [{'DATE': date(2022,1,1), 'COND_01':'A01'}]),    # Remove
        ('xxxxxxxxx6', date(2000,1,11), 'E001', 'C01', date(2022,1,11), 'BAR_11A', [{'DATE': date(2022,1,1), 'COND_01':'A01'}]),    # Remove
        ('xxxxxxxxx7', date(2000,1,12), 'E001', 'C01', date(2022,1,11), 'BAR_11A', [{'DATE': date(2022,1,1), 'COND_01':'A01'}]),    # Remove
        ('xxxxxxxxx7', date(2000,1,12), 'E001', 'C01', date(2022,1,11), 'BAR_11A', [{'DATE': date(2022,1,2), 'COND_01':'A01'}]),    # Keep
    ], data_schema)

    df_input_quarterly = spark.createDataFrame([
        ('xxxxxxxxx8', date(2000,1,9), 'E001', 'C01', date(2022,1,2), 'BAR_9Q', [{'DATE': date(2022,1,1), 'COND_01':'A01'}]),       # Keep
        ('1111111111', date(2000,1,10), 'E001', 'C01', date(2022,1,1), 'BAR_10Q', [{'DATE': date(2022,1,1), 'COND_01':'A01'}]),     # Remove
        ('9111111111', date(2000,1,11), 'E001', 'C01', date(2022,1,1), 'BAR_11Q', [{'DATE': date(2022,1,1), 'COND_01':'A01'}]),     # Remove
        (None, None, 'E001', 'C01', date(2022,1,1), 'BAR_12Q', [{'DATE': date(2022,1,1), 'COND_01':'A01'}]),                        # Remove
        ('xxxxxxxxx9', date(2000,1,13), 'E001', 'C01', date(2022,1,1), None, [{'DATE': date(2022,1,1), 'COND_01':'A01'}]),          # Keep
        ('xxxxxxxxx5', date(2000,1,10), 'E001', 'C01', date(2022,1,10), 'BAR_10A', [{'DATE': date(2022,1,1), 'COND_01':'A01'},
                                                                            {'DATE': date(2021,1,1),'COND_01':'A01'}]),             # Remove
        ('xxxxxxxxx6', date(2000,1,11), 'E001', 'C01', date(2022,1,11), 'BAR_11A', [{'DATE': date(2022,1,1), 'COND_01':'A01'}]),    # Keep
        ('xxxxxxxxx7', date(2000,1,12), None, 'C01', date(2022,1,11), 'BAR_11A', [{'DATE': date(2022,1,2), 'COND_01':'A01'}])       # Remove
    ], data_schema)


    df_expected = spark.createDataFrame([
        ('xxxxxxxxx1', date(2000,1,1), 'E001', 'C01', date(2022,1,1), 'BAR_1A', [{'DATE': date(2022,1,1), 'COND_01':'A01'}]),
        ('xxxxxxxxx2', date(2000,1,2), 'E001', 'C01', date(2022,1,2), 'BAR_2A_2', [{'DATE': date(2022,1,1), 'COND_01':'A01'}]),
        ('xxxxxxxxx3', date(2000,1,3), 'E001', 'C02', date(2022,1,1), 'BAR_3A', [{'DATE': date(2022,1,1), 'COND_01':'A01'}]),
        ('xxxxxxxxx8', date(2000,1,9), 'E001', 'C01', date(2022,1,2), 'BAR_9Q', [{'DATE': date(2022,1,1), 'COND_01':'A01'}]),
        ('xxxxxxxxx9', date(2000,1,13), 'E001', 'C01', date(2022,1,1), None, [{'DATE': date(2022,1,1), 'COND_01':'A01'}]),
        ('xxxxxxxxx5', date(2000,1,10), 'E001', 'C01', date(2022,1,10), 'BAR_10A', [{'DATE': date(2022,1,1), 'COND_01':'A01'},
                                                                            {'DATE': date(2021,1,1),'COND_01':'A01'}]),
        ('xxxxxxxxx6', date(2000,1,11), 'E001', 'C01', date(2022,1,11), 'BAR_11A', [{'DATE': date(2022,1,1), 'COND_01':'A01'}]),
        ('xxxxxxxxx7', date(2000,1,12), 'E001', 'C01', date(2022,1,11), 'BAR_11A', [{'DATE': date(2022,1,2), 'COND_01':'A01'}])
    ], data_schema)

    df_actual = preprocess_cvdp_cohort(
        cvdp_annual     = df_input_annual,
        cvdp_quarterly  = df_input_quarterly,
        field_cohort = 'COHORT',
        filter_cohort_codes = valid_cohort_codes,
        field_nhs_number = 'PID',
        invalid_nhs_numbers = invalid_nhs_numbers,
        invalid_nhs_prefix = invalid_nhs_prefixes,
        null_col = ['PID','DOB'],
        fields_window = ['PID','DOB'],
        field_extract_date = 'EXTRACT',
        field_journal_table = 'JOURNAL',
        field_journal_date  = 'DATE',
        fields_null_drop    = 'LSOA',
    )

    assert compare_results(df_actual, df_expected, join_columns = ['PID','DOB'])

# COMMAND ----------

suite.run()

# COMMAND ----------

dbutils.notebook.exit('pass')
