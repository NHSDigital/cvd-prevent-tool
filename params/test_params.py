# Databricks notebook source
params_json_string = '{"DATABASE_NAME": "test_database", "PID_FIELD": "test_patient_id"}'

# COMMAND ----------

dbutils.notebook.exit(params_json_string)
