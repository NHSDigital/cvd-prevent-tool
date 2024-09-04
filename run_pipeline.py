# Databricks notebook source
# Entry point for the main pipeline that creates various assets from combining source datasets.

# GIT_VERSION:              The most recent git hash from gitlab must be provided to run this script.
# PARAMS_PATH:  The default params is used unless a path to a non-default params is used
#               (see params/params_util).
# RUN_LOGGER:   Switch of if to run the pipeline loger stages.
# RUN_ARCHIVE:  Switch of if to archive the current assets so we have a copy before they are 
#               overwritten with the current pipeline run.
# DEV_MODE:     Switch of if to run the the pipeine in development mode. If true
#               will add a prefix 'dev' to the asset names.

# COMMAND ----------

# MAGIC %run ./src/util

# COMMAND ----------

# MAGIC %run ./pipeline/pipeline_util

# COMMAND ----------

PARAMS_PATH = 'default'
VERSION = ''

RUN_LOGGER  = True
RUN_ARCHIVE = False
DEV_MODE    = False

# COMMAND ----------

# MAGIC %run ./pipeline/default_pipeline

# COMMAND ----------

# Run pipelines and return status of pipeline run (pass if successful)
run_pipeline(
  VERSION, 
  params, 
  get_default_pipeline_stages(
    run_logger_stages = RUN_LOGGER, 
    run_archive_stages = RUN_ARCHIVE, 
    pipeline_dev_mode = DEV_MODE
  )
)

# COMMAND ----------

# Return status of notebook run
dbutils.notebook.exit('pass')