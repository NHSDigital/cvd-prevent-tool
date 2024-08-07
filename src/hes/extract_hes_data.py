# Databricks notebook source
# extract_hes_data

# COMMAND ----------

# MAGIC %run ../../params/params

# COMMAND ----------

# MAGIC %run ../util

# COMMAND ----------

# Base HES Extraction
def get_hes_dataframes_from_years(
    hes_dataset_name: str,
    start_year: int = params.HES_START_YEAR,
    end_year: int = params.HES_END_YEAR,
    db_hes: str = params.HES_DATABASE,
    db_ahas: str = params.HES_AHAS_DATABASE,
    field_pseudo_id: str = params.HES_PSEUDO_ID,
    field_token_id: str = params.HES_TOKEN_ID,
    field_person_id: str = params.HES_PERSON_ID,
    field_hes_id: str = params.HES_HES_ID,
    join_filter_columns: Optional[List[str]] = None,
    drop_columns: Optional[List[str]] = None) -> DataFrame:
    """get_hes_dataframes_from_years

    Read the various hes tables in the given database and search for all tables matching hes_dataset_name.
    Filter the years based on the two digit integers start_year (inclusive) and end_year (exclusive).
    If the first_year is greater than or equal to 21, use the ahas data instead of the hes data, as
    specified by the optional arguments.
    This function can be used to read the non-sensistive, sensitive and mps tables by altering the db_hes and db_ahas variables

    Args:
        hes_dataset_name (str): Name (prefix) of the hes datasets to process (e.g. hes_apc)
        start_year (int, optional): Abbreviated (YY) starting year to obtain hes data from.
            Defaults to params.HES_START_YEAR.
        end_year (int, optional): Abbreviated (YY) ending year to obtain hes data to.
            Defaults to params.HES_END_YEAR.
        db_hes (str, optional): Database name containing hes tables (exclusive prior to 2021-2022).
            Defaults to params.HES_DATABASE.
        db_ahas (str, optional): Database name containing hes tables (inclusive from 2021-2022).
            Defaults to params.HES_AHAS_DATABASE.
        field_pseudo_id (str, optional): Field name of HES pseudo id column to be renamed.
            Defaults to params.HES_PSEUDO_ID.
        field_token_id (str, optional): Field name for HES pseudo id column to be renamed to.
            Defaults to params.HES_TOKEN_ID.
        field_person_id (str, optional): Field name of HES person id column to be renamed.
            Defaults to params.HES_PERSON_ID.
        field_hes_id (str, optional): Field name for HES person id column to be renamed to.
            Defaults to params.HES_HES_ID.
        join_filter_columns (Optional[List[str]], optional): List of columns to select prior to HES table union.
            Defaults to None.
        drop_columns (Optional[List[str]], optional): List of columns to drop from final flattened HES table.
            Defaults to None.

    Raises:
        ValueError: Raised if incorrect type for join_filter_columns is supplied (list)
        Exception: Raise if flattened HES table is None (prior to return)

    Returns:
        DataFrame: Combined and flattened HES table containing HES from start_year to end_year
    """
    ## NAME FORMAT
    hes_dataset_name = (f'{hes_dataset_name}_' if hes_dataset_name[-1] != '_' else hes_dataset_name)

    ## HES_YEAR TABLE MATCHING
    extract_pattern = f'({hes_dataset_name})([0-9][0-9]{{3}})'
    old_year_suffixes = [row._year for row in spark.sql(f'SHOW TABLES IN {db_hes}') \
                                                    .withColumn('_year', F.regexp_extract(F.col('tableName'), extract_pattern, 2)) \
                                                    .select('_year').where(F.col('_year') != '').distinct() \
                                                    .sort(F.col('_year').desc()).collect()]
    new_year_suffixes = [row._year for row in spark.sql(f'SHOW TABLES IN {db_ahas}') \
                                                    .withColumn('_year', F.regexp_extract(F.col('tableName'), extract_pattern, 2)) \
                                                    .select('_year').where(F.col('_year') != '').distinct() \
                                                    .sort(F.col('_year').desc()).collect()]
    all_year_suffixes = list(set(old_year_suffixes+new_year_suffixes))
    all_year_suffixes.sort()
    year_suffixes = [year_pattern for year_pattern in all_year_suffixes \
                    if int(year_pattern[:2]) >= start_year and int(year_pattern[:2]) < end_year \
                    and int(year_pattern[2:]) <= end_year]

    ## HES YEARS DATAFRAMES
    df_flat = None
    for year_suffix in year_suffixes:
        _year   = int(year_suffix[:2])
        _db     = (db_hes if _year < 21 else db_ahas)
        if join_filter_columns is None:
            df_hes  = spark.table(f'{_db}.{hes_dataset_name}{year_suffix}')
        elif type(join_filter_columns) == list:
            df_hes  = spark.table(f'{_db}.{hes_dataset_name}{year_suffix}').select(join_filter_columns)
        else:
            raise ValueError('[ERROR] VARIABLE JOIN_FILTER_COLUMNS IS OF NON-SUPPORTED TYPE (REQUIRES NONETYPE OR LIST)')
        # Fix due to 2024 HES schema update
        if field_pseudo_id in df_hes.columns:
          df_hes = df_hes.withColumnRenamed(field_pseudo_id, field_token_id)
        if field_person_id in df_hes.columns:
          df_hes = df_hes.withColumnRenamed(field_person_id, field_hes_id)
        # Union dataframes
        df_flat = df_hes if df_flat is None else df_flat.unionByName(df_hes, allowMissingColumns = False)

    ## Conditional - Drop Columns Specified
    if drop_columns is not None:
        if not isinstance(drop_columns, list):
            drop_columns = [drop_columns]
        df_flat = df_flat.drop(*drop_columns)

    ## Return
    if df_flat is None:
        raise Exception(f"ERROR: Flatten HES dataframe for {hes_dataset_name} is empty.")
    else:
        return df_flat

# COMMAND ----------

def join_to_hes_sensitive(
    df: DataFrame, 
    dataset_name: str, 
    link_key_map: Dict[str,str] = params.HES_SENSITIVE_LINK_KEY_MAP,
    hes_5yr_start: int = params.HES_5YR_START_YEAR,
    start_year: int = params.HES_START_YEAR,
    hes_end_year: int = params.HES_END_YEAR,
    db_hes: str = params.HES_S_DATABASE,
    db_ahas: str = params.HES_AHAS_S_DATABASE,
    hes_year_field: str = params.HES_YEAR_FIELD,
    field_pseudo_id: str = params.HES_PSEUDO_ID,
    field_token_id: str = params.HES_TOKEN_ID,
    field_person_id: str = params.HES_PERSON_ID,
    field_hes_id: str = params.HES_HES_ID,
    limit_col: Optional[bool] = False,
    hes_preprocessing_cols_map: Dict[str,str] = params.HES_PREPROCESSING_COLS_MAP,
) -> DataFrame:
    """join_to_hes_sensitive

    Join the HES and HES Sensitive (HES_S) tables together for a specified HES dataset (AE, APC, OP). The
    supplied flattened HES table is joined to the associated HES sensitive tables (inner) to yield the
    associated NHS Numbers and dates of birth for each hospital episode.

    Args:
        df (DataFrame): HES non-sensitive dataframe
        dataset_name (str): Name of hes table (e.g. hes_apc)
        link_key_map (Dict[str,str]): Dictionary of mapping for sensitive link keys.
            Defaults to params.HES_SENSITIVE_LINK_KEY_MAP.
        hes_5yr_start (int, optional): Abbreviated (YY) starting year to obtain hes data from when limiting to 5 years.
            Defaults to params.HES_START_YEAR.
        start_year (int, optional): Abbreviated (YY) starting year to obtain hes data from.
            Defaults to params.HES_START_YEAR.
        hes_end_year (int, optional): Abbreviated (YY) ending year to obtain hes data to.
            Defaults to params.HES_END_YEAR.
        db_hes (str, optional): Database name containing hes sensitive tables (exclusive prior to 2021-2022).
            Defaults to params.HES_S_DATABASE.
        db_ahas (str, optional): Database name containing hes sensitive tables (inclusive from 2021-2022).
            Defaults to params.HES_AHAS_S_DATABASE.
        hes_year_field (str, optional): Field name of HES year column to be dropped.
            Defaults to params.HES_YEAR_FIELD.
        field_pseudo_id (str, optional): Field name of HES pseudo id column to be renamed.
            Defaults to params.HES_PSEUDO_ID.
        field_token_id (str, optional): Field name for HES pseudo id column to be renamed to.
            Defaults to params.HES_TOKEN_ID.
        field_person_id (str, optional): Field name of HES person id column to be renamed.
            Defaults to params.HES_PERSON_ID.
        field_hes_id (str, optional): Field name for HES person id column to be renamed to.
            Defaults to params.HES_HES_ID.
        limit_col (Optional[bool], optional): Limit columns to those defined in params. 
            Defaults to False.
        hes_preprocessing_cols_map (Dict[str,str]): Dictionary of hes columns mapping.
            Defaults to params.HES_PREPROCESSING_COLS_MAP

    Returns:
        DataFrame: HES joined dataframe (non-sensitive:sensitive) - [optional] limited columns
    """

    ## NON-SENSITIVE <> SENSITIVE LINKAGE KEY
    link_key = link_key_map[dataset_name]

    ## SENSITIVE HES
    if dataset_name in ['hes_op']:
        hes_start_year = hes_5yr_start # only look at previous 5 years for use in demographic table
    else:
        hes_start_year = start_year
    df_hes_s = get_hes_dataframes_from_years(
        hes_dataset_name = dataset_name,
        start_year = hes_start_year,
        end_year = hes_end_year,
        db_hes = db_hes,
        db_ahas = db_ahas,
        field_pseudo_id = field_pseudo_id,
        field_token_id = field_token_id,
        field_person_id = field_person_id,
        field_hes_id = field_hes_id,
        drop_columns = hes_year_field)

    ## CONDITIONAL - LIMIT COLUMNS IN HES
    if limit_col == True:
        hes_ns_cols = hes_preprocessing_cols_map[f'{dataset_name}_ns']
        hes_s_cols  = hes_preprocessing_cols_map[f'{dataset_name}_s']
        # HES NON-SENSITIVE
        df = df.select(hes_ns_cols)
        # HES SENSITIVE
        df_hes_s = df_hes_s.select(hes_s_cols)

    ## JOIN NON-SENSITIVE AND SENSITIVE HES
    df = df.join(df_hes_s, [link_key], 'inner')

    return df

# COMMAND ----------

def join_to_hes_apc_otr(
    df: DataFrame,
    dataset_name: str = params.HES_APC_OTR_TABLE, 
    otr_link_key: str = params.HES_OTR_LINK_KEY,
    otr_spell_id: str = params.HES_OTR_SPELL_ID,
    start_year: int = params.HES_START_YEAR,
    hes_end_year: int = params.HES_END_YEAR,
    db_hes: str = params.HES_DATABASE,
    db_ahas: str = params.HES_AHAS_DATABASE,
    hes_year_field: str = params.HES_YEAR_FIELD,
    field_pseudo_id: str = params.HES_PSEUDO_ID,
    field_token_id: str = params.HES_TOKEN_ID,
    field_person_id: str = params.HES_PERSON_ID,
    field_hes_id: str = params.HES_HES_ID,
) -> DataFrame:
    """join_to_hes_apc_otr

    Joins the HES APC OTR table to the HES APC (joined: non-sensitive + sensitive) dataframe
    to obtain the SUSSPELLID column for each episode

    Args:
        df (DataFrame): HES APC dataframe.
        dataset_name (str, optional): Name of otr table.
            Defaults to params.HES_APC_OTR_TABLE.
        otr_link_key (str, optional): Name of field to join tables on.
            Defaults to params.HES_OTR_LINK_KEY.
        otr_spell_id (str, optional): Name of field containing spell id.
            Defaults to params.HES_OTR_SPELL_ID.
        start_year (int, optional): Abbreviated (YY) starting year to obtain hes data from.
            Defaults to params.HES_START_YEAR.
        hes_end_year (int, optional): Abbreviated (YY) ending year to obtain hes data to.
            Defaults to params.HES_END_YEAR.
        db_hes (str, optional): Database name containing hes tables (exclusive prior to 2021-2022).
            Defaults to params.HES_DATABASE.
        db_ahas (str, optional): Database name containing hes tables (inclusive from 2021-2022).
            Defaults to params.HES_AHAS_DATABASE.
        hes_year_field (str, optional): Field name of HES year column to be dropped.
            Defaults to params.HES_YEAR_FIELD.
        field_pseudo_id (str, optional): Field name of HES pseudo id column to be renamed.
            Defaults to params.HES_PSEUDO_ID.
        field_token_id (str, optional): Field name for HES pseudo id column to be renamed to.
            Defaults to params.HES_TOKEN_ID.
        field_person_id (str, optional): Field name of HES person id column to be renamed.
            Defaults to params.HES_PERSON_ID.
        field_hes_id (str, optional): Field name for HES person id column to be renamed to.
            Defaults to params.HES_HES_ID.
        

    Returns:
        DataFrame: HES APC dataframe with SUSSPELLID column
    """

    ## HES OTR COLUMNS FOR EXTRACTION
    HES_OTR_COLS = [otr_link_key,otr_spell_id]

    ## HES OTR
    df_hes_otr = get_hes_dataframes_from_years(
        hes_dataset_name = dataset_name,
        start_year = start_year,
        end_year = hes_end_year,
        db_hes = db_hes,
        db_ahas = db_ahas,
        join_filter_columns = HES_OTR_COLS,
        field_pseudo_id = field_pseudo_id,
        field_token_id = field_token_id,
        field_person_id = field_person_id,
        field_hes_id = field_hes_id
    )

    ## SELECT: LINK KEY AND SPELL ID ONLY
    df_hes_otr = df_hes_otr.select(otr_link_key,otr_spell_id)

    ## JOIN DF AND OTR HES
    df_joined = df.join(df_hes_otr, [otr_link_key], 'left_outer')

    ## CHECK: RECORD NUMBERS
    try:
        assert df.count() == df_joined.count()
    except:
        print(f'[WARNING] HES APC-OTR JOIN - NON-UNIQUE KEYS FOUND: ROW COUNT RECEIVED {df_joined.count()} ROW COUNT EXPECTED {df.count()}')

    return df_joined

# COMMAND ----------

def process_hes_extract(
    hes_dataset_name: str,
    otr_dataset_name: str = params.HES_APC_OTR_TABLE,
    start_year: int = 0,
    end_year: int = params.HES_END_YEAR,
    db_hes: str = params.HES_DATABASE,
    db_s_hes: str = params.HES_S_DATABASE,
    db_ahas: str = params.HES_AHAS_DATABASE,
    db_s_ahas: str = params.HES_AHAS_S_DATABASE,
    field_pseudo_id: str = params.HES_PSEUDO_ID,
    field_token_id: str = params.HES_TOKEN_ID,
    field_person_id: str = params.HES_PERSON_ID,
    field_hes_id: str = params.HES_HES_ID,
    limit_col: Optional[bool] = False,
    link_key_map: Dict[str,str] = params.HES_SENSITIVE_LINK_KEY_MAP,
    otr_link_key: str = params.HES_OTR_LINK_KEY,
    otr_spell_id: str = params.HES_OTR_SPELL_ID,
    hes_5yr_start: int = params.HES_5YR_START_YEAR,
    hes_year_field: str = params.HES_YEAR_FIELD,
    hes_preprocessing_cols_map: Dict[str,str] = params.HES_PREPROCESSING_COLS_MAP
    ) -> DataFrame:
    """process_hes_extract

    Generates the combined and flattened HES dataframe from start to end years. Dataframe is
    joined to sensitive HES.
    For HES APC, table also contains information from joining to the HES APC OTR table.

    Args:
        hes_dataset_name (str): Name (prefix) of the hes datasets to process (e.g. hes_apc)
        otr_dataset_name (str, optional): Name of otr table.
            Defaults to params.HES_APC_OTR_TABLE.
        start_year (int, optional): Abbreviated (YY) starting year to obtain hes data from.
            Defaults to 0.
        end_year (int, optional): Abbreviated (YY) ending year to obtain hes data to.
            Defaults to params.HES_END_YEAR.
        db_hes (str, optional): Database name containing hes tables (exclusive prior to 2021-2022).
            Defaults to params.HES_DATABASE.
        db_s_hes (str, optional): Database name containing hes sensitive tables (exclusive prior to 2021-2022).
            Defaults to params.HES_S_DATABASE.
        db_ahas (str, optional): Database name containing hes tables (inclusive from 2021-2022).
            Defaults to params.HES_AHAS_DATABASE.
        db_s_ahas (str, optional): Database name containing hes sensitive tables (inclusive from 2021-2022).
            Defaults to params.HES_AHAS_S_DATABASE.
        field_pseudo_id (str, optional): Field name of HES pseudo id column to be renamed.
            Defaults to params.HES_PSEUDO_ID.
        field_token_id (str, optional): Field name for HES pseudo id column to be renamed to.
            Defaults to params.HES_TOKEN_ID.
        field_person_id (str, optional): Field name of HES person id column to be renamed.
            Defaults to params.HES_PERSON_ID.
        field_hes_id (str, optional): Field name for HES person id column to be renamed to.
            Defaults to params.HES_HES_ID.
        limit_col (Optional[bool], optional): Limit columns to those defined in params. 
            Defaults to False.
        link_key_map (Dict[str,str]): Dictionary of mapping for sensitive link keys.
            Defaults to params.HES_SENSITIVE_LINK_KEY_MAP.
        otr_link_key (str, optional): Name of field to join tables on.
            Defaults to params.HES_OTR_LINK_KEY.
        otr_spell_id (str, optional): Name of field containing spell id.
            Defaults to params.HES_OTR_SPELL_ID.
        hes_5yr_start (int, optional): Abbreviated (YY) starting year to obtain hes data from when limiting to 5 years.
            Defaults to params.HES_START_YEAR.
        hes_year_field (str, optional): Field name of HES year column to be dropped.
            Defaults to params.HES_YEAR_FIELD.
        hes_preprocessing_cols_map (Dict[str,str]): Dictionary of hes columns mapping.
            Defaults to params.HES_PREPROCESSING_COLS_MAP

    Raises:
        ValueError: Raised if the start_year is set to default value of 0.

    Returns:
        DataFrame: Combined and flattened HES data extract.
    """
    ## Error if start_year not supplied
    if start_year == 0:
        raise ValueError('ERROR: HES Start year not supplied or auto-assigned...')
    ## Create flat version of HES tables
    df_hes = get_hes_dataframes_from_years(
        hes_dataset_name = hes_dataset_name,
        start_year = start_year,
        end_year = end_year,
        db_hes = db_hes,
        db_ahas = db_ahas,
        field_pseudo_id = field_pseudo_id,
        field_token_id = field_token_id,
        field_person_id = field_person_id,
        field_hes_id = field_hes_id,
    )
    ## Join to sensitive HES
    df_hes = join_to_hes_sensitive(
        df = df_hes, 
        dataset_name = hes_dataset_name, 
        link_key_map = link_key_map,
        hes_5yr_start = hes_5yr_start,
        start_year = start_year,
        hes_end_year = end_year,
        db_hes = db_s_hes,
        db_ahas = db_s_ahas,
        hes_year_field = hes_year_field,
        field_pseudo_id = field_pseudo_id,
        field_token_id = field_token_id,
        field_person_id = field_person_id,
        field_hes_id = field_hes_id,
        limit_col = limit_col,
        hes_preprocessing_cols_map = hes_preprocessing_cols_map,
    ) 
    ## Conditional: HES APC join to APC OTR
    if hes_dataset_name == 'hes_apc':
        df_hes = join_to_hes_apc_otr(
            df = df_hes,
            dataset_name = otr_dataset_name, 
            otr_link_key = otr_link_key,
            otr_spell_id = otr_spell_id,
            start_year = start_year,
            hes_end_year = end_year,
            db_hes = db_hes,
            db_ahas = db_ahas,
            hes_year_field = hes_year_field,
            field_pseudo_id = field_pseudo_id,
            field_token_id = field_token_id,
            field_person_id = field_person_id,
            field_hes_id = field_hes_id,
        )
    
    ## Return
    return df_hes