# Databricks notebook source
### Patient and Event Table Schemas
### Used for results checker

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, ArrayType, IntegerType, StringType, DateType

# COMMAND ----------

# Patients without META field
TABLE_PATIENTS_SCHEMA = StructType([
    StructField("person_id", StringType(), False),
    StructField("birth_date", DateType(), False),
    StructField("latest_extract_date", DateType(), False),
    StructField("latest_practice_identifier", StringType(), True),
    StructField("sex", StringType(), True),
    StructField("lsoa", StringType(), True),
    StructField("cohort", StringType(), True),
    StructField("ethnicity", StringType(), True),
    StructField("AAA_diagnosis_date", DateType(), True),
    StructField("AF_diagnosis_date", DateType(), True),
    StructField("CKD_diagnosis_date", DateType(), True),
    StructField("STROKE_diagnosis_date", DateType(), True),
    StructField("DIABETES_diagnosis_date", DateType(), True),
    StructField("PAD_diagnosis_date", DateType(), True),
    StructField("FH_diagnosis_date", DateType(), True),
    StructField("CHD_diagnosis_date", DateType(), True),
    StructField("HTN_diagnosis_date", DateType(), True),
    StructField("HF_diagnosis_date", DateType(), True),
    StructField("TIA_diagnosis_date", DateType(), True),
    StructField("FHSCREEN_diagnosis_date", DateType(), True),
    StructField("NDH_diagnosis_date", DateType(), True),
    StructField("date_of_death", DateType(), True),
    StructField("death_flag", StringType(), True),
    StructField("death_age_flag", StringType(), True),
    StructField("stroke_count", IntegerType(), True),
    StructField("max_stroke_date", DateType(), True),
    StructField("mi_count", IntegerType(), True),
    StructField("max_mi_date", DateType(), True),
    StructField("died_within_30_days_hospitalisation_flags", ArrayType(StringType(), True), True),
    StructField("hyp_risk_group", StringType(), True)
    ])
# Events without META field
TABLE_EVENTS_SCHEMA = StructType([
    StructField("person_id", StringType(), False),
    StructField("birth_date", DateType(), False),
    StructField("age", IntegerType(), True),
    StructField("sex", StringType(), True),
    StructField("dataset", StringType(), False),
    StructField("category", StringType(), False),
    StructField("record_id", IntegerType(), False),
    StructField("record_start_date", DateType(), False),
    StructField("record_end_date", DateType(), True),
    StructField("lsoa", StringType(), True),
    StructField("ethnicity", StringType(), True),
    StructField("code", StringType(), True),
    StructField("flag", StringType(), True),
    StructField("code_array", ArrayType(StringType(), True), True),
    StructField("flag_assoc", ArrayType(StringType(), True), True),
    StructField("record_id_assoc", ArrayType(StringType(), True), True)
    ])

# COMMAND ----------

