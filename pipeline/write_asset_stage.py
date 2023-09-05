# Databricks notebook source
# MAGIC %run ./pipeline_util

# COMMAND ----------

class WriteAssetStage(PipelineStage):
  '''
  Pipeline stage for saving an asset to a databricks table. 
  
  The 'input_asset_to_save' is a key representing a PipelineAsset in the pipeline context. The data 
  in the pipeline asset will be saved to disk at the location defined by the PipelineAsset instance. 
  The data will then be read from that location into a new pipeline asset. The 'output' parameter is 
  an output key which determines where in the pipeline context the data loaded from disk will be 
  stored. Using the data from disk is preferable as it acts as a hard cache.

  If db and table_base_name are given, the data will be saved to that location instead of the location 
  defined by the given pipeline asset PipelineAsset.
  '''
  
  def __init__(self, input_asset_to_save, output, db=None, table_base_name=None, overwrite=False):
    self._input_asset_to_save = input_asset_to_save
    self._output = output
    self._db = db
    self._table_base_name = table_base_name
    self._overwrite = overwrite
    super(WriteAssetStage, self).__init__({self._input_asset_to_save}, {self._output})
    
  def _run(self, context, log):
    
    log._add_stage(self.name + '_' + self._input_asset_to_save)
        
    input_asset = context[self._input_asset_to_save]
    
    self._log_metadata(context, log)
      
    log._timer(self.name + '_' + self._input_asset_to_save)
    
    db_name, table_base_name = input_asset.write(self._db, self._table_base_name, overwrite=self._overwrite)
    
    log._timer(self.name + '_' + self._input_asset_to_save, end=True)

    # Conditional - Internal data(overwrite == False) or external data(overwrite == True)
    if self._overwrite == False:
      return {
        self._output: PipelineAsset(
          self._output,
          context,
          db=db_name,
          base_name=table_base_name
          )
        }
    else:
      return {
        self._output: PipelineAsset(
          key = self._output,
          context = context,
          df = context[self._input_asset_to_save].df,
          db = db_name,
          table = table_base_name
          )
        }
  
  def _log_metadata(self, context, log):
    if ('logger' not in self._input_asset_to_save):
      log._get_dates(self.name + '_' + self._input_asset_to_save, context[self._input_asset_to_save].df)
      
      if (('eligible_cohort' not in self._input_asset_to_save) & ('cvdp_linkage' not in self._input_asset_to_save) & ('results_checker_table' not in self._input_asset_to_save)):
        log._get_counts(self.name + '_' + self._input_asset_to_save, context[self._input_asset_to_save].df)

# COMMAND ----------

