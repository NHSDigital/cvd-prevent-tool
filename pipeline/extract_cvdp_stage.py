# Databricks notebook source
# extract_cvdp_stage

# COMMAND ----------

from pyspark.sql import DataFrame

# COMMAND ----------

# MAGIC %run ./pipeline_util

# COMMAND ----------

# MAGIC %run ../params/params

# COMMAND ----------

# MAGIC %run ../src/cvdp/extract_cvdp_data

# COMMAND ----------

class ExtractCVDPDataStage(PipelineStage):
    """ExtractCVDPDataStage

    Pipeline stage associated with extracting and combining the CVDP Store annual and
    quarterly data, producing the extracted CVDP asset. This process also includes
    filtering the combined table to the cohort code list specified in params. A primary
    key is added to the combined dataset, that is a SHA 256 hash key from a set of
    predefined columns (in params).

    This stage will error if the hash keys generated are not unique.

    Associated Notebooks:
        pipeline/pipeline_util: Provides abstract/base Pipeline classes and methods
        src/cvdp/extract_cvdp_data: Provides processing function(s) for CVDP extraction
    """
    # Class Initialisation
    def __init__(self,extracted_cvdp_table_output: str):
        self._extracted_cvdp_table_output: str = extracted_cvdp_table_output
        self._extract_table_presence: bool = False
        self._data_holder: Dict[str, DataFrame] = {}
        super(ExtractCVDPDataStage, self).__init__(set(), {self._extracted_cvdp_table_output})

    # Stage Run
    def _run(self, context, log, limit: bool = False):
        ## Initialise Logger
        log._add_stage(self.name)
        log._timer(self.name)
        ## Process CVDP Extract
        self._load_source_cvdp_data()
        self._process_cvdp_extract()
        ## Optional: Cohort Limit
        if limit == True:
            print('INFO: Pipeline Stage being Ran in Limit Mode')
            self._limit_cohort()
        ## Check hash collisions in the extracted dataframe
        self._check_hash_collisions()
        ## Stop Logger
        log._timer(self.name, end=True)
        ## Return key assignment (delta table)
        return {
            self._extracted_cvdp_table_output: PipelineAsset(
                key = self._extracted_cvdp_table_output,
                context = context,
                db = params.DATABASE_NAME,
                df = self._data_holder['cohort_target'],
                cache = False,
                delta_table = True,
                delta_columns = params.CVDP_DELTA_MERGE_COLUMNS
                )
            }

    # Supporting Methods
    ## Load cvdp source data
    def _load_source_cvdp_data(self):
        self._data_holder['cohort_source_qtr'] = spark.table(f'{params.CVDP_STORE_DATABASE}.{params.CVDP_STORE_QUARTERLY_TABLE}')
        self._data_holder['cohort_source_ann'] = spark.table(f'{params.CVDP_STORE_DATABASE}.{params.CVDP_STORE_ANNUAL_TABLE}')

    ## CVDP Extract Data Processsing
    ### Full table update
    def _process_cvdp_extract(self):
        self._data_holder['cohort_target'] = process_cvdp_extract(
            cvdp_annual = self._data_holder['cohort_source_ann'],
            cvdp_quarterly = self._data_holder['cohort_source_qtr']
        )

    ## Limit Cohort Records
    def _limit_cohort(self,
        columns: list = [params.CVDP_PID_FIELD,params.CVDP_DOB_FIELD],
        lim_int: int = params.INTEGRATION_TEST_LIMIT):
        """
        Selects a random selection of 'lim_int' rows from the combined CVDP extract table,
        to be used in integration testing.
        """
        # Select a random selection of lim_int rows
        nhs_dob        = (self._data_holder['cohort_target']).select(columns).distinct()
        cohort_count   = nhs_dob.count()
        fraction = lim_int / cohort_count
        #check fraction is less than 1
        if fraction > 1:
            fraction = 1.0
            warnings.warn('Number of rows given to limit the cohort table by is greater than the total number of rows in' \
                        'the cohort table. Running integration test with full cohort table. To change this please change' \
                        f'INTEGRATION_TEST_LIMIT in params_util to less than {cohort_count}')
        nhs_dob_random = nhs_dob.sample(fraction = fraction).limit(lim_int)
        # Return random rows
        self._data_holder['cohort_target'] = nhs_dob_random.join(
            self._data_holder['cohort_target'],
            on = columns,
            how = "left")

    ## Hash Collision Check
    ### Check combined table for non-unique hash values
    def _check_hash_collisions(self):
        hash_check = check_unique_values(
            df = self._data_holder['cohort_target'],
            field_name = params.CVDP_PRIMARY_KEY)
        if hash_check == True:
            pass
        else:
            raise Exception(f'ERROR: Non-unique hash values in extracted CVDP table. Pipeline stopped, check hashable fields.')