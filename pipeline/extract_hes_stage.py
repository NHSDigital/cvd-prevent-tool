# Databricks notebook source
# extract_hes_stage

# COMMAND ----------

from pyspark.sql import DataFrame

# COMMAND ----------

# MAGIC %run ../src/util

# COMMAND ----------

# MAGIC %run ./pipeline_util

# COMMAND ----------

# MAGIC %run ../params/params

# COMMAND ----------

# MAGIC %run ../src/hes/extract_hes_data

# COMMAND ----------

class ExtractHESDataStage(PipelineStage):
    """ExtractHESDataStage

    Pipeline stage associated with extracting and combining the HES (AE, APC, OP) tables, across a
    range of years, into a single, flattened table. This process also includes joining the HES tables
    to their repsective sensitive versions (yielding the NHS Numbers and dates of births for patients).
    In the case of HES APC, the OTR table is also joined to yield the hospital episode-spell associated,
    used in calculating HES Spell statistics.

    Associated Notebooks:
        pipeline/pipeline_util: Provides abstract/base Pipeline classes and methods
        src/hes/extract_hes_data: Provides processing function(s) for HES AE, APC and OP extraction
    """
    # Class Initialisation
    def __init__(
        self,
        extracted_cvdp_table_input: str,
        extracted_hes_apc_table_output: str,
        extracted_hes_ae_table_output: str,
        extracted_hes_op_table_output: str,
        ):
        self._extracted_cvdp_table_input = extracted_cvdp_table_input
        self._extracted_hes_apc_table_output = extracted_hes_apc_table_output
        self._extracted_hes_ae_table_output = extracted_hes_ae_table_output
        self._extracted_hes_op_table_output = extracted_hes_op_table_output
        self._data_holder: Dict[str, DataFrame] = {}
        super(ExtractHESDataStage, self).__init__(
            {
                self._extracted_cvdp_table_input
                },
            {
                self._extracted_hes_apc_table_output,
                self._extracted_hes_ae_table_output,
                self._extracted_hes_op_table_output
                }
            )

    # Stage Run
    def _run(self, context, log, limit: bool = False):
        ## Initialise Logger
        log._add_stage(self.name)
        log._timer(self.name)
        ## Process HES Extracts
        self._process_hes_extracts_apc()
        self._process_hes_extracts_ae()
        self._process_hes_extracts_op()
        ## Optional: If limit is specified
        if limit:
            print('INFO: Pipeline Stage being Ran in Limit Mode')
            self._load_cvdp_data(context)
            self._filter_hes_data()
        ## Check unique identifiers
        self._check_unique_identifiers()
        ## Stop Logger
        log._timer(self.name, end=True)
        ## Return key assignment (delta table)
        return {
            ## HES APC
            self._extracted_hes_apc_table_output: PipelineAsset(
                key = self._extracted_hes_apc_table_output,
                context = context,
                db = params.DATABASE_NAME,
                df = self._data_holder['hes_apc'],
                cache = False,
                delta_table = True,
                delta_columns = params.HES_APC_DELTA_MERGE_COLUMNS
                ),
            ## HES AE
            self._extracted_hes_ae_table_output: PipelineAsset(
                key = self._extracted_hes_ae_table_output,
                context = context,
                db = params.DATABASE_NAME,
                df = self._data_holder['hes_ae'],
                cache = False,
                delta_table = True,
                delta_columns = params.HES_AE_DELTA_MERGE_COLUMNS
                ),
            ## HES OP
            self._extracted_hes_op_table_output: PipelineAsset(
                key = self._extracted_hes_op_table_output,
                context = context,
                db = params.DATABASE_NAME,
                df = self._data_holder['hes_op'],
                cache = False,
                delta_table = True,
                delta_columns = params.HES_OP_DELTA_MERGE_COLUMNS
                )
            }

    # Supporting Methods

    ## HES Extract Data Processing
    ### HES APC Processing
    def _process_hes_extracts_apc(self):
        self._data_holder['hes_apc'] = process_hes_extract(
            hes_dataset_name='hes_apc',
            start_year=params.HES_START_YEAR
        )
    ### HES AE Processing
    def _process_hes_extracts_ae(self):
        self._data_holder['hes_ae'] = process_hes_extract(
            hes_dataset_name='hes_ae',
            start_year=params.HES_START_YEAR
        )
    ### HES OP Processing
    def _process_hes_extracts_op(self):
        self._data_holder['hes_op'] = process_hes_extract(
            hes_dataset_name='hes_op',
            start_year=params.HES_5YR_START_YEAR
        )

    ## Filter (limit) HES data
    ### Load Extracted CVDP data
    def _load_cvdp_data(self,context):
        self._data_holder['cvdp'] = context[self._extracted_cvdp_table_input].df

    ### Filtering HES Dataframes
    def _filter_hes_data(self):
        # HES APC Limit
        self._data_holder['hes_apc'] = filter_by_dataframe(
            df_input = self._data_holder['hes_apc'],
            df_filter = self._data_holder['cvdp'],
            filter_to_input_map = {
                params.CVDP_PID_FIELD: params.HES_S_APC_PID_FIELD,
                params.CVDP_DOB_FIELD: params.HES_S_APC_DOB_FIELD,
            }
        )
        # HES AE Limit
        self._data_holder['hes_ae'] = filter_by_dataframe(
            df_input = self._data_holder['hes_ae'],
            df_filter = self._data_holder['cvdp'],
            filter_to_input_map = {
                params.CVDP_PID_FIELD: params.HES_S_AE_PID_FIELD,
                params.CVDP_DOB_FIELD: params.HES_S_AE_DOB_FIELD,
            }
        )
        # HES OP Limit
        self._data_holder['hes_op'] = filter_by_dataframe(
            df_input = self._data_holder['hes_op'],
            df_filter = self._data_holder['cvdp'],
            filter_to_input_map = {
                params.CVDP_PID_FIELD: params.HES_S_OP_PID_FIELD,
                params.CVDP_DOB_FIELD: params.HES_S_OP_DOB_FIELD,
            }
        )

    ## Unique Identifier Check
    def _check_unique_identifiers(self):
        """_check_unique_identifiers

        Checks each of the HES dataframes in self._data_holder for unique values in
        the params defined linkage keys (unique for each HES dataset)

        Raises:
            Exception: Raised if any of the HES dataframes contain non-unique values
                in the HES linkage key.
        """
        hes_check_list = []
        # HES APC
        if check_dictionary_key(self._data_holder,'hes_apc'):
            if check_unique_values(self._data_holder['hes_apc'], params.HES_S_APC_LINK_KEY):
                pass
            else:
                hes_check_list.append('hes_apc')
        # HES AE
        if check_dictionary_key(self._data_holder,'hes_ae'):
            if check_unique_values(self._data_holder['hes_ae'], params.HES_S_AE_LINK_KEY):
                pass
            else:
                hes_check_list.append('hes_ae')
        # HES OP
        if check_dictionary_key(self._data_holder,'hes_op'):
            if check_unique_values(self._data_holder['hes_op'], params.HES_S_OP_LINK_KEY):
                pass
            else:
                hes_check_list.append('hes_op')
        # Final Check
        if len(hes_check_list) > 0:
            raise Exception(f"ERROR: Non-unique identifiers found in HES datasets: {', '.join(hes_check_list)}")