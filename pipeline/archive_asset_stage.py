# Databricks notebook source
# MAGIC %run ./pipeline_util

# COMMAND ----------

class ArchiveAssetStage(PipelineStage):
    """ArchiveAssetStage

    Pipeline stage responsible for cloning pre-existing [delta] tables, source, into
    a new archived table (target). The cloned table contains versioning information,
    including the date of the pipeline run, the params version and gitlab commit hash
    version.

    Associated Notebooks:
        pipeline/pipeline_util: Notebook containing abstract base pipeline classes
    """
    def __init__(
        self,
        input_asset_to_archive: str,
        db: str=None,
        table_base_name: str=None,
        table_clone_name: str=None,
        run_archive_stage: bool=True
        ):
        """__init__

        Initialisation of ArchiveAssetStage class.

        Args:
            input_asset_to_archive (str): Pipeline context key for source table for
                cloning [input].
            db (str, optional): Database path. When None, uses PARAMS definition.
                Defaults to None.
            table_base_name (str, optional): Source table path (original table). When
                None, uses the table name defined when PipelineAsset was created.
                Defaults to None.
            table_clone_name (str, optional): Target table path (cloned table). When
                None, uses the table name defined when PipelineAsset was created along
                with the version and date information from the context/params.
                Defaults to None.
            run_archive_stage (bool, optional): Switch of if to run the stage. Is passed 
                as a keyword argument to the parent class PiplineStage init method.
                Defaults to True.
        """
        self._input_asset_to_archive = input_asset_to_archive
        self._db = db
        self._table_base_name = table_base_name
        self._table_clone_name = table_clone_name
        self._run_archive_stage = run_archive_stage
        super(ArchiveAssetStage, self).__init__(
            {self._input_asset_to_archive}, set(), run_stage = self._run_archive_stage
        )

    # Main Run
    def _run(self, context, log):
        # Initiate logger
        log._add_stage(self.name + '_' + self._input_asset_to_archive)
        # Define input asset
        input_asset = context[self._input_asset_to_archive]
        # Update logger
        log._timer(self.name + '_' + self._input_asset_to_archive)
        # Clone the asset
        input_asset.clone_delta(
            self._db,
            self._table_base_name,
            self._table_clone_name
        )
        # Stop Logger
        log._timer(self.name + '_' + self._input_asset_to_archive, end=True)