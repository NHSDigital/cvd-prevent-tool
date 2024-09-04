# Databricks notebook source
# Import this file into any script requiring the params values.

# This script is the singular method for choosing which params to use and setting up the 'params' object.

# The global paramter 'PARAMS_PATH' must be provided. Set it to 'default' to read the default params as
# in params/params_util.

# To read a different params file set 'PARAMS_PATH' to the relative path of the notebook to be used as the
# params. There are multiple ways to create such a notebook, see params_util for more details.

# After running this script all params values are available from the created 'params' object, such as
# 'params.DATABASE_NAME'.

# COMMAND ----------

# MAGIC %run ./params_util

# COMMAND ----------

if not PARAMS_PATH:
  raise AssertionError("Parameter PARAMS_PATH must be set. Set it to 'default' to access the default params values.")

# COMMAND ----------

if PARAMS_PATH == 'default':
  new_params = DEFAULT_PARAMS
else:
  new_params_json_string = dbutils.notebook.run(PARAMS_PATH, 0)
  new_params = DEFAULT_PARAMS.from_json(new_params_json_string)
  new_params.set_params_path(PARAMS_PATH)

# COMMAND ----------

if 'params' in globals() and params.params_path != new_params.params_path:
  raise AssertionError(f'The PARAMS_PATH ({PARAMS_PATH}) has changed. Stored params path has changed '
                       f'from {params.params_path} to {new_params.params_path}. The defined params values '
                       f'must not change while the pipeline is running.')

# COMMAND ----------

params = new_params