# Databricks notebook source
# MAGIC %run ./pipeline_util

# COMMAND ----------

class WriteAssetStage(PipelineStage):
    """WriteAssetStage
    Pipeline stage for saving an asset to a databricks table.

    The 'input_asset_to_save' is a key representing a PipelineAsset in the pipeline context. The data
    in the pipeline asset will be saved to disk at the location defined by the PipelineAsset instance.
    The data will then be read from that location into a new pipeline asset. The 'output' parameter is
    an output key which determines where in the pipeline context the data loaded from disk will be
    stored. Using the data from disk is preferable as it acts as a hard cache.

    If db and table_base_name are given, the data will be saved to that location instead of the location
    defined by the given pipeline asset PipelineAsset.

    If the PipelineAsset was created as a delta table asset (delta_table = True) then the stage will
    perform the delta table version of writing a dataframe. Unless specified as a pipeline stage
    parameter, the delta table merge columns will be used that were specified when the PipelineAsset
    was created.
    """
    def __init__(
        self,
        input_asset_to_save,
        output,
        db=None,
        table_base_name=None,
        overwrite=False,
        delta_merge_columns=None
        ):
        self._input_asset_to_save = input_asset_to_save
        self._output = output
        self._db = db
        self._table_base_name = table_base_name
        self._overwrite = overwrite
        self._delta_merge_columns = delta_merge_columns
        super(WriteAssetStage, self).__init__(
            {self._input_asset_to_save}, {self._output})

    def _run(self, context, log):
        # Initiate logger
        log._add_stage(self.name + '_' + self._input_asset_to_save)
        # Define input asset
        input_asset = context[self._input_asset_to_save]
        # Update logger
        if input_asset.check_delta() == False:
          self._log_metadata(context, log)
        log._timer(self.name + '_' + self._input_asset_to_save)

        # Write PipelineAsset - Conditional
        # Standard Asset
        if input_asset.check_delta() == False:
            db_name, table_base_name = input_asset.write(
                self._db, self._table_base_name, overwrite=self._overwrite)
        # Delta Table
        elif input_asset.check_delta() == True:
            # Merge delta table
            db_name, table_base_name = input_asset.write_delta(
                self._db, self._table_base_name, self._delta_merge_columns, overwrite=self._overwrite)
            # Optimise delta table
            input_asset.optimise_delta()
        # Error
        else:
            raise ValueError(
                f'ERROR: PipelineAsset {self._input_asset_to_save} missing delta_table flag when asset initiated')

        # Stop logger
        log._timer(self.name + '_' + self._input_asset_to_save, end=True)

        # Conditional - Delta Table Asset
        if input_asset.check_delta() == True:
          return {
                self._output: PipelineAsset(
                    self._output,
                    context,
                    db=db_name,
                    base_name=table_base_name,
                    delta_table=True
                )
            }
        else:
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
                      key=self._output,
                      context=context,
                      df=context[self._input_asset_to_save].df,
                      db=db_name,
                      table=table_base_name
                  )
              }

    # Supporting Methods
    ## Logger Metadata
    def _log_metadata(self, context, log):
        if ('logger' not in self._input_asset_to_save):
            log._get_dates(self.name + '_' + self._input_asset_to_save, context[self._input_asset_to_save].df)

            if (('eligible_cohort' not in self._input_asset_to_save) & ('cvdp_linkage' not in self._input_asset_to_save) & ('results_checker_table' not in self._input_asset_to_save)):
                log._get_counts(self.name + '_' + self._input_asset_to_save,
                                context[self._input_asset_to_save].df)
