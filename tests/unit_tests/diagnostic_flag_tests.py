# Databricks notebook source
# MAGIC %run ../../src/test_helpers

# COMMAND ----------

PARAMS_PATH = 'default'

# COMMAND ----------

# MAGIC %run ../../params/params

# COMMAND ----------

# MAGIC %run ../../src/cvdp/diagnostic_flags_lib

# COMMAND ----------

from dsp.validation.validator import compare_results
from datetime import date
from uuid import uuid4
from datetime import datetime
import pyspark.sql.types as T

from pyspark.sql.types import StructType, StructField, TimestampType, IntegerType, StringType, DateType, ShortType, LongType, DoubleType, ArrayType

suite = FunctionTestSuite()

## The following are fake people and data created for test purposes

@suite.add_test
def test_map_diagnosis_codes ():
  
  df_input = spark.createDataFrame([
    ('1', date(2000,1,1),date(2022,1,1),'CVDPCX001','1',date(2020,1,1),'AFIB_COD'), #patient has AF
    ('2', date(2000,1,1),date(2022,1,1),'CVDPCX001','1',date(2020,1,1),'TIA_COD'), #patient has TIA
    ('3', date(2000,1,1),date(2022,1,1),'CVDPCX001','1',date(2020,1,1),'STRK_COD'), #patient has Stroke
    ('4', date(2000,1,1),date(2022,1,1),'CVDPCX001','1',date(2020,1,1),'AAA_COD'), #patient has AAA
    ('5', date(2000,1,1),date(2022,1,1),'CVDPCX001','445010006',date(2020,1,1),''), #patient only had a SNOMED code for FH
    ('6', date(2000,1,1),date(2022,1,1),'CVDPCX001','',date(2020,1,1),''), #patient has nothing
    ('7', date(2000,1,1),date(2022,1,1),'CVDPCX001','1',date(2020,1,1),'AFIBRES_COD'), #patient has AF
    ('8', date(2000,1,1),date(2022,1,1),'CVDPCX001','1',date(2020,1,1),'DULIPID_COD'), #patient has FHSCREEN
    ('9', date(2000,1,1),date(2022,1,1),'CVDPCX001','1',date(2020,1,1),'SBROOME_COD'), #patient has FHSCREEN
    ('10', date(2000,1,1),date(2022,1,1),'CVDPCX001','1',date(2020,1,1),'HF_COD'), #patinet has HF
    ('11', date(2000,1,1),date(2022,1,1),'CVDPCX001','1',date(2020,1,1),'NDH_COD'), #patient has NDH
    ('12', date(2000,1,1),date(2022,1,1),'CVDPCX001','1',date(2020,1,1),'IGT_COD'), #patient has NDH
    ('13', date(2000,1,1),date(2022,1,1),'CVDPCX001','1',date(2020,1,1),'PRD_COD'), #patient has NDH

    
  ],
  ['person_id','birth_date','extract_date','cohort','code','journal_date','Cluster_ID'])
  
  df_expected = spark.createDataFrame([
    ('1', date(2000,1,1),date(2022,1,1),'CVDPCX001','1',date(2020,1,1),'AFIB_COD', 'AF'), 
    ('2', date(2000,1,1),date(2022,1,1),'CVDPCX001','1',date(2020,1,1),'TIA_COD', 'TIA'), 
    ('3', date(2000,1,1),date(2022,1,1),'CVDPCX001','1',date(2020,1,1),'STRK_COD', 'STROKE'), 
    ('4', date(2000,1,1),date(2022,1,1),'CVDPCX001','1',date(2020,1,1),'AAA_COD', 'AAA'), 
    ('5', date(2000,1,1),date(2022,1,1),'CVDPCX001','445010006',date(2020,1,1),'', 'FH'), 
    ('7', date(2000,1,1),date(2022,1,1),'CVDPCX001','1',date(2020,1,1),'AFIBRES_COD', 'AFIBRES_COD'),
    ('8', date(2000,1,1),date(2022,1,1),'CVDPCX001','1',date(2020,1,1),'DULIPID_COD', 'FHSCREEN'),
    ('9', date(2000,1,1),date(2022,1,1),'CVDPCX001','1',date(2020,1,1),'SBROOME_COD', 'FHSCREEN'),
    ('10', date(2000,1,1),date(2022,1,1),'CVDPCX001','1',date(2020,1,1),'HF_COD', 'HF'),
    ('11', date(2000,1,1),date(2022,1,1),'CVDPCX001','1',date(2020,1,1),'NDH_COD', 'NDH'),
    ('12', date(2000,1,1),date(2022,1,1),'CVDPCX001','1',date(2020,1,1),'IGT_COD', 'NDH'),
    ('13', date(2000,1,1),date(2022,1,1),'CVDPCX001','1',date(2020,1,1),'PRD_COD', 'NDH'),
  ],
  ['person_id','birth_date','extract_date','cohort','code','journal_date','Cluster_ID', 'diagnosis'])
  
  df_actual = map_diagnosis_codes(df_input)
  
  assert compare_results(df_actual, df_expected, join_columns = ['person_id','birth_date'])
  
  
@suite.add_test
def test_get_max_date ():
    df_input = spark.createDataFrame([
    ('1', date(2000,1,1), date(2020,1,1),'AF'),
    ('1', date(2000,1,1), date(2021,1,1),'AF'), #patient 1 has a second later AF date
    ('3', date(2000,1,1), date(2020,1,1),'STROKE'), #patient has one code for Stroke
    ('4', date(2000,1,1), date(2020,1,1),'AAA'), #patient has AAA
    ('5', date(2000,1,1), date(2020,1,1),'FH'), #patient only had a SNOMED code for HF
    ('7', date(2000,1,1), date(2020,1,1),'AFIBRES_COD'), #patient has AF RES Code
    ('8', date(2000,1,1), date(2020,1,1), 'TIA'), #patient has one code for TIA
    ('9', date(2000,1,1), date(2020,1,1), 'NDH'),
    ('9', date(2000,1,1), date(2021,1,1), 'NDH'), #patient has a second later NDH date
  ],
  ['person_id','birth_date', 'journal_date','diagnosis'])
    
    df_expected = spark.createDataFrame([
    ('1', date(2000,1,1), date(2020,1,1),'AF', date(2021,1,1)), 
    ('1', date(2000,1,1), date(2021,1,1),'AF', date(2021,1,1)), #patient has two cases of AF and only the latest date is the max
    ('3', date(2000,1,1), date(2020,1,1),'STROKE', date(2020,1,1)), #patienthas one code for Stroke
    ('4', date(2000,1,1), date(2020,1,1),'AAA', date(2020,1,1)), #patient has AAA
    ('5', date(2000,1,1), date(2020,1,1),'FH', date(2020,1,1)), #patient only had a SNOMED code for HF
    ('7', date(2000,1,1), date(2020,1,1),'AFIBRES_COD', date(2020,1,1)), #patient has one resolve code which is classed as its own thing
    ('8', date(2000,1,1), date(2020,1,1),'TIA', date(2020,1,1)), #patient has one code for TIA
    ('9', date(2000,1,1), date(2020,1,1), 'NDH', date(2021,1,1)),
    ('9', date(2000,1,1), date(2021,1,1), 'NDH', date(2021,1,1)), #patient has a second later NDH date
  ],
  ['person_id','birth_date','journal_date','diagnosis', 'max_date'])
    
    df_actual = get_max_date(df_input)
    
    assert compare_results(df_actual, df_expected, join_columns = ['person_id','birth_date', 'journal_date'])
    
    
@suite.add_test
def test_create_parent_max_date():
  df_input = spark.createDataFrame([
    ('1', date(2000,1,1), date(2020,1,1),'AF', date(2021,1,1)), 
    ('1', date(2000,1,1), date(2021,1,1),'AF', date(2021,1,1)), #patient has two cases of AF and only the latest date is the max
    ('3', date(2000,1,1), date(2020,1,1),'STROKE', date(2020,1,1)), #patient has one code for Stroke
    ('1', date(2000,1,1), date(2022,1,1),'AFIBRES_COD', date(2022,1,1)) #patient has one resolve code which is classed as its own thing
  ],   ['person_id','birth_date','journal_date','diagnosis', 'max_date'])
  
  df_expected = spark.createDataFrame([
    ('1', date(2000,1,1), date(2020,1,1),'AF', date(2021,1,1), 'AF', date(2022,1,1)), 
    ('1', date(2000,1,1), date(2021,1,1),'AF', date(2021,1,1), 'AF', date(2022,1,1)), #patient has two cases of AF and only the latest date is the max
    ('3', date(2000,1,1), date(2020,1,1),'STROKE', date(2020,1,1), 'STROKE', date(2020,1,1)), #patient has one code for Stroke
    ('1', date(2000,1,1), date(2022,1,1),'AFIBRES_COD', date(2022,1,1), 'AF', date(2022,1,1)) #patient has one resolve code which is classed as its own thing
  ], ['person_id','birth_date','journal_date','diagnosis', 'max_date', 'parent', 'max_diagnosis_date'])
  
  df_actual = create_parent_max_date(df_input)
  
  assert compare_results(df_actual, df_expected, join_columns = ['person_id','birth_date', 'journal_date'])

    
@suite.add_test
def test_filter_res_codes():
    df_input = spark.createDataFrame([
    ('1', date(2000,1,1), date(2020,1,1),'1', 'AFIB_COD','AF', date(2021,1,1), 'AF', date(2022,1,1)), 
    ('1', date(2000,1,1), date(2021,1,1),'1','AFIB_COD','AF', date(2021,1,1), 'AF', date(2022,1,1)), #patient has two cases of AF and only the latest date is the max
    ('3', date(2000,1,1), date(2020,1,1),'1','STRK_COD','STROKE', date(2020,1,1), 'STROKE', date(2020,1,1)), #patient has one code for Stroke
    ('1', date(2000,1,1), date(2022,1,1),'1','AFIBRES_COD', 'AFIBRES_COD', date(2022,1,1), 'AF', date(2022,1,1)), #patient has one resolve code which is classed as its own thing
    ('2', date(2000,1,1), date(2020,1,1),'1', 'AFIB_COD','AF', date(2022,1,1), 'AF', date(2022,1,1)), 
    ('2', date(2000,1,1), date(2020,1,1),'1', 'AFIBRES_COD','AF', date(2022,1,1), 'AF', date(2022,1,1)), # AF Res and Diag date are on the same day
      
    
  ], ['person_id','birth_date','journal_date','code','Cluster_ID','diagnosis', 'max_date', 'parent', 'max_diagnosis_date'])

    
    df_expected = spark.createDataFrame([
    ('3', date(2000,1,1), date(2020,1,1), '1', 'STRK_COD','STROKE', date(2020,1,1), 'STROKE', date(2020,1,1)), #patient has one code for Stroke
    ('2', date(2000,1,1), date(2020,1,1),'1', 'AFIB_COD','AF', date(2022,1,1), 'AF', date(2022,1,1)), 
    ],  ['person_id','birth_date','journal_date','code','Cluster_ID','diagnosis', 'max_date', 'parent', 'max_diagnosis_date'])
    
    df_actual = filter_res_dates(df_input)
    
    assert compare_results(df_actual, df_expected, join_columns = ['person_id', 'birth_date'])

    
@suite.add_test
def test_split_diagnosis_dates():
    df_input = spark.createDataFrame([
    ('2', date(2000,1,1),date(2022,1,1),'CVDPCX001','1',date(2020,1,1),'CKD_COD', 'CKD', date(2019,1,1)), #patient has resolved AF
    ('2', date(2000,1,1),date(2022,1,1),'CVDPCX001','1',date(2020,1,1),'STRK_COD', 'STROKE', None), #patient has resolved AF
    ('3', date(2000,1,1),date(2022,1,1),'CVDPCX001','1',date(2020,1,1),'AF_COD', 'AF', None), #patient has resolved AF
  ],  ['person_id','birth_date','extract_date','cohort','code','journal_date','Cluster_ID', 'diagnosis', 'max(journal_date)'])
    
    df_expected = spark.createDataFrame([
      ('3', date(2000,1,1), date(2020,1,1), None, None),
      ('2', date(2000,1,1), None, date(2020,1,1), date(2020,1,1))
    ], ['person_id', 'birth_date', 'AF', 'CKD', 'STROKE'])
    
    df_actual = split_diagnosis_dates(df_input)
       
    assert compare_results(df_expected, df_actual, join_columns = ['person_id', 'birth_date'])

@suite.add_test
def test_ensure_columns_renamed():
  df_input = spark.createDataFrame([
    ('1', date(2020,1,1),date(2020,1,1),date(2020,1,1),date(2020,1,1),date(2020,1,1),date(2020,1,1),date(2020,1,1),date(2020,1,1),date(2020,1,1),date(2020,1,1),date(2020,1,1),date(2020,1,1),date(2020,1,1)),
  ], ['idx', 'AAA', 'AF', 'CKD', 'STROKE', 'DIABETES', 'PAD', 'FH', 'CHD', 'HTN', 'HF', 'TIA', 'FHSCREEN', 'NDH'])
  
  df_expected = spark.createDataFrame([
    ('1',date(2020,1,1),date(2020,1,1),date(2020,1,1),date(2020,1,1),date(2020,1,1),date(2020,1,1),date(2020,1,1),date(2020,1,1),date(2020,1,1),date(2020,1,1),date(2020,1,1),date(2020,1,1),date(2020,1,1)),
  ], ['idx','AAA_diagnosis_date', 'AF_diagnosis_date', 'CKD_diagnosis_date', 'STROKE_diagnosis_date',
      'DIABETES_diagnosis_date', 'PAD_diagnosis_date', 'FH_diagnosis_date', 'CHD_diagnosis_date', 
      'HTN_diagnosis_date', 'HF_diagnosis_date', 'TIA_diagnosis_date', 'FHSCREEN_diagnosis_date', 'NDH_diagnosis_date'])
  
  
  df_actual = ensure_cols(df_input)
  
  
  assert compare_results(df_expected, df_actual, join_columns = ['idx'])

  
@suite.add_test
def test_ensure_columns_adds():
  
  output_schema = T.StructType([
      T.StructField('idx', T.StringType(), True),
      T.StructField('AAA_diagnosis_date', T.DateType(), True),
      T.StructField('AF_diagnosis_date', T.DateType(), True),
      T.StructField('CKD_diagnosis_date', T.NullType(), True),
      T.StructField('STROKE_diagnosis_date', T.NullType(), True),
      T.StructField('DIABETES_diagnosis_date', T.NullType(), True),
      T.StructField('PAD_diagnosis_date', T.NullType(), True),
      T.StructField('FH_diagnosis_date', T.NullType(), True),
      T.StructField('CHD_diagnosis_date', T.NullType(), True),
      T.StructField('HTN_diagnosis_date', T.NullType(), True),
      T.StructField('HF_diagnosis_date', T.NullType(), True),
      T.StructField('TIA_diagnosis_date', T.NullType(), True),
      T.StructField('FHSCREEN_diagnosis_date', T.NullType(), True),
      T.StructField('NDH_diagnosis_date', T.NullType(), True),
    
    
  ])
  
  df_input = spark.createDataFrame([
    ('1', date(2020,1,1),date(2020,1,1)),
  ], ['idx', 'AAA', 'AF'])
  
  df_expected = spark.createDataFrame([
    ('1',date(2020,1,1),date(2020,1,1),None,None,None,None,None,None,None,None,None,None,None),
  ], output_schema
      )
  
  df_actual = ensure_cols(df_input)
    
  assert compare_results(df_actual, df_expected, join_columns = ['idx'])


# COMMAND ----------

suite.run()

# COMMAND ----------

dbutils.notebook.exit('pass')
