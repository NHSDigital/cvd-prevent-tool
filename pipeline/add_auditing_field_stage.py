# Databricks notebook source
# MAGIC %run ./pipeline_util

# COMMAND ----------

from pyspark.sql import Row
from pyspark.sql.functions import col, lit
import pyspark.sql.functions as F

# COMMAND ----------

class AddAuditingFieldStage(PipelineStage):
  '''
  Adds an auditing column called 'META' to the given asset. This stage should be run on all data
  assets just before writing the data to a database.

  The META column is a struct containing: git version, params version, date, a struct describing
  the pipeline definition, the database currently being used, the params values, an index for
  the current stage in which this META column is added, and finally a uniformly random number
  between 0 and 1 that can be used for sampling.

  See the schema in pipeline/PipelineContext.get_auditing_schema. The random sampling value is a
  double added separately to this schema.

  The given asset is a passthrough asset, meaning it is the input and output dataframe, and the
  output has the same context key as the input.

  Most of the values in the META field are available from the PipelineContext, thus this class calls
  to the PipelineContext instance to get the values and the base schema of the META field is part
  of the PipelineContext.

  '''
  META_FIELD = 'META'

  def __init__(self, passthrough_asset_add_meta_column: str):
    self._passthrough_asset_add_meta_column = passthrough_asset_add_meta_column
    super(AddAuditingFieldStage, self).__init__({self._passthrough_asset_add_meta_column},
                                                {self._passthrough_asset_add_meta_column})

  def _run(self, context):

    df = context[self._passthrough_asset_add_meta_column].df

    if self.META_FIELD in df.columns:
      raise ValueError(f'ERROR: The dataframe of PipelineAsset key {self._passthrough_asset_add_meta_column} '
                       f'already contains a column called {self.META_FIELD}.')

    meta_struct = context.get_auditing_struct()
    meta_schema = context.get_auditing_schema()

    struct_col = F.struct(*(lit(v).alias(k) for k, v in meta_struct.items()))
    df_with_meta_column = df.withColumn(self.META_FIELD, struct_col.cast(meta_schema))

    struct_fields = df_with_meta_column.schema[self.META_FIELD].dataType.names
    df_with_meta_column = df_with_meta_column.withColumn(self.META_FIELD,
                               F.struct(*([col(self.META_FIELD)[c].alias(c) for c in struct_fields]\
                            + [lit(F.rand()).astype('double').alias('sampling')])))

    return {self._passthrough_asset_add_meta_column: df_with_meta_column}