# Databricks notebook source
# Entry point for the main pipeline that creates various assets from combining source datasets.

# cohort_table:             The default is do not use a previous cohort table (widget is blank). If
#                           a previous table needs to be selected, the most recent is avaliable in 
#                           the widget dropdown (or can be specified manually)
# git_version:              The most recent git hash from gitlab must be provided to run this script. 
# params_path:              The deafult params is used unless a path to a non-default params is used 
#                           (see params/params_util). 
# prepare_pseudo_assets:    Switch (bool) for the pipeline to run without saving curated and 
#                           desensitised assets (False) or to run the desensitise stage 
#                           post-pipeline (True). Defaults to False.

# COMMAND ----------

# MAGIC %run ../src/util

# COMMAND ----------

dbutils.widgets.text('params_path', 'default')
dbutils.widgets.text('git_version', '')
dbutils.widgets.combobox('cohort_table', '', [find_latest_cohort_table('prevent_tool_collab')])
dbutils.widgets.dropdown('prepare_pseudo_assets','False', ['False','True'])

# COMMAND ----------

# MAGIC %run ../pipeline/pipeline_util

# COMMAND ----------

PARAMS_PATH = dbutils.widgets.get('params_path')
GIT_VERSION = dbutils.widgets.get('git_version')
COHORT_OPT  = dbutils.widgets.get('cohort_table')
PSEUDO_SAVE  = dbutils.widgets.get('prepare_pseudo_assets') == "True"

# COMMAND ----------

assert GIT_VERSION != ''
assert type(COHORT_OPT) == str
VERSION = GIT_VERSION[:6]
assert type(PSEUDO_SAVE) == bool

# COMMAND ----------

# MAGIC %run ../pipeline/default_pipeline

# COMMAND ----------

# Run pipelines and return status of pipeline run (pass if successful)
run_pipeline(VERSION, params, get_default_pipeline_stages(cohort_opt = COHORT_OPT, prepare_pseudo_assets = PSEUDO_SAVE))

# COMMAND ----------

dbutils.widgets.removeAll()

# COMMAND ----------

# Return status of notebook run
dbutils.notebook.exit('pass')