# Databricks notebook source
## Overview

# There are three ways to create a params. A DEFAULT_PARAMS is already instantiated with the default values. Any values not given in the methods below are populated with the default values.

# 1) Overwrite values with keyword arguments:
#     params = Params(DATABASE_NAME='fake_database', start_date=date(2021, 1, 1))

# 2) Pass in a dictionary of values:
#     params = Params.from_dict({'DATABASE_NAME': 'fake_database', 'start_date': date(2021, 1, 1)})

# 3) Pass in a json formatted string:
#     params = Params.from_json('{"DATABASE_NAME": "fake_database", "start_date": "2021-01-01"}')

# Only keys that are defined in the Params class can be set. So passing in an unknown key in
# any of the above methods will raise an error.

## Notes
# Dataclasses can't have mutable default values. default_factory functions must be used to
# provide mutable data types as defaults. This is the exact same issue as when you define a
# normal class __init__ function with a mutable default arg, like my_list = []. It uses the
# same list for all instances of the class.

## Imported Parameter Notebooks
# List of parameter notebooks imported into params_util and their definition/usage
# * params_diagnostic_codes: Provides the diagnostic and inclusion codes (ICD10, SNOMED) for data filtering
# * params_pipeline_assets: Defines the pipeline assets (key:value) for pipeline running
# * params_table_schemas: Defines the table schema checks for pipeline results checker

# COMMAND ----------

# MAGIC %run ./params_diagnostic_codes

# COMMAND ----------

# MAGIC %run ./params_pipeline_assets

# COMMAND ----------

# MAGIC %run ./params_table_schemas

# COMMAND ----------

# MAGIC %run ../src/util

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, ArrayType, IntegerType
from dataclasses import dataclass, asdict, field
from datetime import date, datetime
import json
from typing import Dict, Tuple, List, Any
from abc import ABC
import hashlib

# COMMAND ----------

def _json_to_date_hook(json_dict):
  for k, v in json_dict.items():
    if isinstance(v, str):
      try:
        json_dict[k] = datetime.strptime(v, '%Y-%m-%d').date()
      except:
        pass
  return json_dict

# COMMAND ----------

def _calc_latest_hes_year(db:str,prefix_match:str):
  """_calc_latest_hes_year
  
  Params function to calculate latest HES year. This value is pulled directly
  from tables present in the (user defined - db) HES database. The value
  returned is an integer NN that represents the latest year suffix of the HES
  tables (defined as table_Y1Y1Y2Y2, where NN is Y2Y2).
  
  Note: This function assumes HES tables have a naming convention of
  hes_XXX_Y1Y1Y2Y2 (where XXX is information about the specific table, e.g., 
  hes_apc and Y1Y1Y2Y2 are the financial year start Y1Y1 and end Y2Y2 values)
  
  Warning: Given current HES table naming, this function will cease to work 
  correctly past the year 2099 (financial year suffix 9899).

  Args:
      db (str): HES database name to pull table information from.
      prefix_match (str): Table prefix to match in HES database, used to identify
        tables to calculate year suffix from.

  Raises:
      AssertionError: Raised when calculated year suffix is not equal to current year or year + 1.
      TypeError: Raised when calculated year suffix is not returned as integer type.
      RuntimeError: Raised when profiling HES database fails (unspecified).
      TypeError: Raised when database argument is not string type.

  Returns:
      end_year_value: Integer of latest HES year value (Y2Y2)
  """
  # Ensure arguments are supplied and are strings
  if (isinstance(db,str) and isinstance(prefix_match,str)):
    ## Try: Pull table information from HES database
    try:
      desc_tables = (
        spark.sql(f'SHOW TABLES FROM {db}')
        .select('tableName')
        .filter(F.col('tableName').contains(prefix_match))
        .withColumn('_yr', F.substring(F.col('tableName'),-2,2).cast(IntegerType()))
        .select('_yr').distinct()
        .agg({'_yr':'max'})
      )
      end_year_value =  desc_tables.select('max(_yr)').first()[0]
      ## Ensure returned year value is an integer
      if isinstance(end_year_value, int):
        ## Ensure year value is current year (y) or y+1
        curr_year_value = int(datetime.today().strftime('%y'))
        next_year_value = curr_year_value + 1
        if curr_year_value <= end_year_value <= next_year_value:
          return end_year_value
        else:
          raise AssertionError(f'ERROR: Output of params._calc_latest_hes_year expected value between {curr_year_value} and {next_year_value} but receieved {end_year_value}.')
      else:
        raise TypeError(f'ERROR: Output of params._calc_latest_hes_year expected int type but received {type(end_year_value)}.')
    ## Except: Try statement failed, return generic runtime error
    except:
      raise RuntimeError('ERROR: Error in params._calc_latest_hes_year function.')
  # Argument are not supplied/not string type
  else:
    raise TypeError('ERROR: Database and prefix string type required for params._calc_latest_hes_year.')

# COMMAND ----------

@dataclass(frozen=True)
class ParamsBase(ABC):

  params_date: date = date.today()
  params_path: str = None

  @property
  def version(self):
    return hashlib.md5(self.to_json().encode()).hexdigest()[:6]

  @classmethod
  def from_dict(cls, val_dict: Dict):
    return cls(**val_dict)

  @classmethod
  def from_json(cls, json_string: str):
    val_dict = json.loads(json_string, object_hook=_json_to_date_hook)
    return cls.from_dict(val_dict)

  def set_params_path(self, path):
    object.__setattr__(self, 'params_path', path)

  def to_dict(self):
    return asdict(self)

  def to_json(self, sort_keys=True, indent=None):
    params_dict = self.to_dict()

    for key in params_dict.keys():
      if isinstance(params_dict[key], date):
        params_dict[key] = str(params_dict[key])

    del params_dict['params_date']
    del params_dict['params_path']

    return json.dumps(params_dict, sort_keys=sort_keys, indent=indent)

# COMMAND ----------

@dataclass(frozen=True)
class Params(ParamsBase):
  #------------------------------------------------------
  # project initialisation 
  #------------------------------------------------------
  ## Database where the intermediate assets should be saved
  DATABASE_NAME: str = 'prevent_tool_collab'
  ## Assets where the final pseudonymised (cvdp_linkage) tables should be saved
  PSEUDO_DB_PATH: str = 'prevent_tool_collab'
  
  #------------------------------------------------------
  # age filtering 
  #------------------------------------------------------
  PATIENT_MAX_AGE: int = 120
  PATIENT_MIN_AGE: int = 16
  HTN_PATIENT_MIN_AGE: int = 18
  
  #------------------------------------------------------
  # pipeline + function switches (bool)
  #------------------------------------------------------
  ## Use to switch functionality on or off
  SWITCH_CVDP_JOURNAL_ORDER_DESC: bool = True
  SWITCH_CVDP_JOURNAL_ADD_REF_DATA: bool = True
  SWITCH_HES_LIMIT_COLUMNS: bool = True
  SWITCH_HES_SELECT_CODED_EVENTS: bool = False
  SWITCH_PATIENT_ENHANCE_DEMOGRAPHICS: bool = True
  
  #------------------------------------------------------
  # raw database and tables
  #------------------------------------------------------
  DSS_CORPORATE_DATABASE: str = 'dss_corporate'
  REF_CODES_TABLE: str = 'gpdata_cluster_refset'
  REF_OTHER_TABLE: str = 'snomed_sct2_refset_full'
  REF_ETHNICITY_MAPPING: str = 'gdppr_ethnicity_mappings'
  CVDP_STORE_DATABASE: str = 'cvdp_store'
  CVDP_STORE_QUARTERLY_TABLE: str = 'cvdp_store_quarterly'
  CVDP_STORE_ANNUAL_TABLE: str = 'cvdp_store_annual'
  DARS_DATABASE: str = 'dars_bird_deaths'
  DARS_DEATHS_TABLE: str = 'dars_bird_deaths'
  HES_DATABASE: str = 'hes'
  HES_S_DATABASE: str = 'flat_hes_s'
  HES_AHAS_DATABASE: str = 'hes_ahas'
  HES_AHAS_S_DATABASE: str = 'hes_ahas_s'
  HES_APC_TABLE: str = 'hes_apc'
  HES_APC_OTR_TABLE: str = 'hes_apc_otr'
  HES_OP_TABLE: str = 'hes_op'
  HES_AE_TABLE: str = 'hes_ae'
  
  #------------------------------------------------------
  # date filtering 
  #------------------------------------------------------
  ## Main Pipeline Dates
  start_date: date = date(1990,1,1)
  end_date: date = date.today()
  ## CVDP - BLOOD PRESSURE READINGS
  CVDP_BP_START_DATE: date = date(2018,1,1)
  ## HES - DATE RANGE
  HES_TABLE_PREFIX: str = 'hes_'
  HES_START_YEAR: int = 11 # Reading HES tables
  HES_END_YEAR: int = _calc_latest_hes_year(HES_AHAS_DATABASE,HES_TABLE_PREFIX) # Reading HES tables (calculated)
  HES_5YR_START_YEAR: int = HES_END_YEAR - 5 # Filter for ethnicity enhancement (calculated)
  
  #------------------------------------------------------
  # Pipeline Asset Definitions
  #------------------------------------------------------
  # Prefix for tables in development pipeline run
  ASSET_PREFIX_DEV: str = '_dev'
  # Dictionary containing pipeline assets
  PIPELINE_ASSETS: Dict[str,str] = PARAMS_PIPELINE_ASSET_MAPPING
  
  #------------------------------------------------------
  # Pipeline Unique Keys
  #------------------------------------------------------
  CVDP_PRIMARY_KEY: str = 'hash_id_cvdp'
  CVDP_COHORT_PRIMARY_KEY: str= 'PK_cohort'
  CVDP_JOURNAL_PRIMARY_KEY: str = 'PK_journal'
  PSEUDO_PATIENT_PRIMARY_KEY: str = 'PK_pseudo_patient'
  
  #------------------------------------------------------
  # CVDP column names
  #------------------------------------------------------
  ## Raw Data Fields
  CVDP_EXTRACT_DATE: str = 'extract_date'
  CVDP_PID_FIELD: str = 'nhs_number'
  CVDP_COHORT_FIELD: str = 'cohort'
  CVDP_DOB_FIELD: str = 'date_of_birth'
  CVDP_JOURNAL_FIELD: str = 'journals_table'
  CVDP_JOURNAL_DATE_FIELD: str = 'journal_date'
  CVDP_CODE_FIELD: str = 'code'
  CVDP_CODE_VALUE1_FIELD: str = 'value1_condition'
  CVDP_CODE_VALUE2_FIELD: str = 'value2_condition'
  CVDP_PRACTICE_FIELD: str = 'patient_practice'
  CVDP_SEX_FIELD: str = 'sex'
  CVDP_POSTCODE_FIELD: str = 'postcode'
  CVDP_TIMESTAMP_FIELD: str = 'time_stamp'
  CVDP_AGE_FIELD: str = 'age'
  CVDP_META_PRACTICE_ID_FIELD: str = 'meta.practice_id'
  
  #------------------------------------------------------
  # CVDP Diagnostic Flag fields
  #------------------------------------------------------
  CVDP_MAX_RESOLVED_DATE: str = 'max_resolved_date'
  CVDP_MAX_DIAGNOSIS_DATE: str = 'max_diagnosis_date'
  CVDP_MAX_BP_DATE: str = 'last_bp_date'
  CVDP_AGE_AT_EVENT: str = 'age_at_event'
  CVDP_DIASTOLIC_BP_READING: str = 'diastolic_blood_pressure'
  CVDP_SYSTOLIC_BP_READING: str = 'systolic_blood_pressure'
  CVDP_HYP_RISK_FIELD: str = 'hyp_risk_group'
  CVDP_CALCULATE_AGE_AT: str = 'extract_date'

  #------------------------------------------------------
  # REF column names
  #------------------------------------------------------
  REF_CLUSTER_FIELD: str = 'Cluster_ID'
  REF_ACTIVE_IN_REFSET: str = 'active_in_refset'
  REF_CODE_FIELD: str = 'ConceptId'
  REF_ETHNICITY_CODE_FIELD: str = 'PrimaryCode'
  REF_VMP_PACK_FIELD: str = 'VPPID'
  REF_VMP_FIELD: str = 'VPID'
  REF_AMP_PACK_FIELD: str = 'APPID'
  REF_AMP_FIELD: str = 'APID'
  PACK_CODE_FIELD: str = 'pack_code_field'

  #------------------------------------------------------
  # DARS column names
  #------------------------------------------------------
  DARS_ID_FIELD: str = 'DARS_BIRD_DEATHS_ID'
  DARS_DOB_FIELD: str = 'DEC_DATE_OF_BIRTH'
  DARS_DOD_FIELD: str = 'REG_DATE_OF_DEATH'
  DARS_PID_ORIGIN_FIELD_1: str = 'DEC_CONF_NHS_NUMBER'
  DARS_PID_ORIGIN_FIELD_2: str = 'DEC_NHS_NUMBER'
  DARS_UNDERLYING_CODE_FIELD: str = 'S_UNDERLYING_COD_ICD10'
  DARS_COMORBS_CODES_FIELD: str = 'S_COD_CODE_CONCAT'
  DARS_RESIDENCE_FIELD: str = 'LSOAR'
  DARS_LOCATION_FIELD: str = 'LSOA11_POD'
  DARS_CURRENT_FIELD: str = 'IS_CURRENT'
  DARS_CANCELLED_FIELD: str = 'CANCELLED_FLAG'
  DARS_SEX_FIELD: str = 'DEC_SEX'
  DARS_REG_FIELD: str = 'REG_DATE'
  DARS_POSTCODE: str = 'DECAD_STAT_POSTCODE'
  ### Added
  DARS_PID_FIELD: str = 'nhs_number'
  ### Renamed
  DARS_LSOA_RESIDENCE: str = 'lsoa_residence'
  DARS_LSOA_LOCATION: str = 'lsoa_death_location'

  #------------------------------------------------------
  # HES fields
  #------------------------------------------------------
  ## Generic
  HES_YEAR_FIELD: str     = 'FYEAR'
  ## APC
  ### Non-sensitive
  HES_APC_ID_FIELD: str         = 'EPIKEY'
  HES_APC_STARTDATE_FIELD: str  = 'EPISTART'
  HES_APC_ENDDATE_FIELD: str    = 'EPIEND'
  HES_APC_LSOA_FIELD: str       = 'LSOA11'
  HES_APC_SEX_FIELD: str        = 'SEX'
  HES_APC_ETHNICITY_FIELD: str  = 'ETHNOS'
  HES_APC_CODE_FIELD: str       = 'DIAG_4_01'
  HES_APC_CODE_LIST_FIELD: str  = 'DIAG_4_CONCAT'
  HES_APC_SPELL_STARTDATE_FIELD: str = 'ADMIDATE'
  HES_APC_ADMIMETH_FIELD: str   = 'ADMIMETH'
  HES_APC_SPELL_ENDDATE_FIELD: str = 'DISDATE'
  HES_APC_SPELL_END_INDICATOR_FIELD: str = 'SPELEND'
  HES_APC_SPELL_DUR_FIELD: str = 'SPELDUR_CALC'
  HES_APC_SPELL_BEGIN_FIELD: str = 'SPELBGIN'
  ### Sensitive
  HES_S_APC_LINK_KEY: str       = 'EPIKEY'
  HES_S_APC_HESID_FIELD: str    = 'HESID'
  HES_S_APC_PID_FIELD: str      = 'NEWNHSNO'
  HES_S_APC_SEX_FIELD: str      = 'SEX'
  HES_S_APC_DOB_FIELD: str      = 'DOB'
  HES_S_APC_HOMEADD_FIELD: str  = 'HOMEADD'
  ### Renamed
  HES_ID_FIELD: str             = 'hes_id'
  HES_SPELL_ID_FIELD: str       = 'hes_spell_id'
  HES_STARTDATE_FIELD: str      = 'hes_spell_admidate'
  HES_ENDDATE_FIELD: str        = 'hes_spell_disdate'
  HES_SPELL_BEGIN_FIELD: str    = 'hes_spell_begin'
  HES_SPELL_ENDFLAG_FIELD: str  = 'hes_spell_end'
  HES_SPELL_DUR_FIELD: str      = 'hes_spell_duration'
  ### OTR
  HES_OTR_LINK_KEY: str = 'EPIKEY'
  HES_OTR_SPELL_ID: str = 'SUSSPELLID'
  ### Added
  HES_FLAG_FIELD: str = 'flag'
  HES_SPELL_START_FIELD: str = 'hes_spell_start_date'
  HES_SPELL_END_FIELD: str = 'hes_spell_end_date'
  ## AE
  ### Non-sensitive
  HES_AE_ID_FIELD: str = 'AEKEY'
  HES_AE_STARTDATE_FIELD: str = 'ARRIVALDATE'
  HES_AE_LSOA_FIELD: str       = 'LSOA11'
  HES_AE_SEX_FIELD: str        = 'SEX'
  HES_AE_ETHNICITY_FIELD: str  = 'ETHNOS'
  HES_AE_CODE_PREFIX: str = 'DIAG3_'
  HES_AE_CODE_FIELD: str = 'DIAG3_01'
  HES_AE_UNPLANNED_FLAG: str = 'AEATTEND_EXC_PLANNED'
  HES_AE_APC_LINK_KEY: str = 'EPIKEY'
  ### Sensitive
  HES_S_AE_LINK_KEY: str       = 'AEKEY'
  HES_S_AE_HESID_FIELD: str    = 'HESID'
  HES_S_AE_PID_FIELD: str      = 'NEWNHSNO'
  HES_S_AE_DOB_FIELD: str      = 'DOB'
  HES_S_AE_SEX_FIELD: str      = 'SEX'
  HES_S_AE_HOMEADD_FIELD: str  = 'HOMEADD'
  ### Added
  HES_AE_CODE_LIST_FIELD: str = 'DIAG3_CONCAT'
   ### Renamed
  ## OP
  ### Non-sensitive
  HES_OP_ID_FIELD: str         = 'ATTENDKEY'
  HES_OP_CODE_FIELD: str       = 'DIAG_3_01'
  HES_OP_TRETSPEF_FIELD: str   = 'TRETSPEF'
  HES_OP_CODE_LIST_FIELD: str  = 'DIAG_3_CONCAT'
  HES_OP_STARTDATE_FIELD: str  = 'APPTDATE'
  HES_OP_ETHNICITY_FIELD: str  = 'ETHNOS'
  HES_OP_ATTENDANCE_FIELD: str = 'ATTENDED'
  HES_OP_OUTCOME_FIELD: str    = 'OUTCOME'
  HES_OP_LSOA_FIELD: str       = 'LSOA11'
  HES_OP_REQDATE_FIELD: str    = 'REQDATE'
  HES_OP_SEX_FIELD: str        = 'SEX'
  ### Sensitive
  HES_S_OP_LINK_KEY: str       = 'ATTENDKEY'
  HES_S_OP_HESID_FIELD: str    = 'HESID'
  HES_S_OP_PID_FIELD: str      = 'NEWNHSNO'
  HES_S_OP_DOB_FIELD: str      = 'DOB'
  HES_S_OP_SEX_FIELD: str      = 'SEX'
  HES_S_OP_HOMEADD_FIELD: str  = 'HOMEADD'
  HES_S_OP_CONSULT: str        = 'CONSULT'
  ## Fix for 2024 HES schema update
  HES_PERSON_ID: str = 'PERSON_ID'
  HES_HES_ID: str = 'HESID'
  HES_TOKEN_ID: str = 'TOKEN_PERSON_ID'
  HES_PSEUDO_ID: str = 'PSEUDO_HESID'

  #------------------------------------------------------
  # pipeline output table columns
  #------------------------------------------------------
  # Dataset filter list - must be one of these
  DATASETS: List[str] = field(default_factory = lambda: [
    'cvdp_store_quarterly','dars_bird_deaths','hes_apc',
    'hes_op','hes_ae','cvdp_diag_flags','cvdp_cohort',
    'cvdp_cohort_journal','cvdp_htn',])
  # Datasets mandatory presence in final Events table
  DATASETS_RESULTS_CHECKER: List[str] = field(default_factory = lambda: [
    'dars_bird_deaths','hes_apc',
    'cvdp_cohort','cvdp_htn',])
  # Cohorts mandatory presence in final Events table
  COHORTS_RESULTS_CHECKER: List[str] = field(default_factory = lambda: [
    'CVDPCX001',
    'CVDPCX002'])
  ## SHARED
  PID_FIELD: str          = 'person_id'
  PRACTICE_FIELD: str     = 'practice_identifier'
  AGE_FIELD: str          = 'age'
  DOB_FIELD: str          = 'birth_date'
  DOD_FIELD: str          = 'death_date'
  DATASET_FIELD: str      = 'dataset'
  ## RECORD FIELDS
  RECORD_ID_FIELD         = 'record_id'
  RECORD_STARTDATE_FIELD  = 'record_start_date'
  RECORD_ENDDATE_FIELD    = 'record_end_date'
  ## COHORT TABLE
  EXTRACT_DATE_FIELD: str = 'extract_date'
  JOURNAL_DATE_FIELD: str = 'journal_date'
  ## PATIENT TABLE
  SEX_FIELD: str            = 'sex'
  LSOA_FIELD: str           = 'lsoa'
  COHORT_FIELD: str         = 'cohort'
  ETHNICITY_FIELD: str      = 'ethnicity'
  ETHNICITY_CODE_FIELD:str  = 'ethnicity_code'
  COHORT_ENTRY_FIELD: str   = 'cohort_entry'
  LATEST_EXTRACT_DATE: str  = 'latest_extract_date'
  LATEST_PRACTICE_ID: str   = 'latest_practice_identifier'
  ## DIAGNOSTIC
  CODE_FIELD: str         = 'code'
  CODE_ARRAY_FIELD: str   = 'code_array'
  FLAG_FIELD: str         = 'flag'
  ASSOC_FLAG_FIELD: str   = 'flag_assoc'
  ## EVENTS TABLE
  CATEGORY_FIELD: str     = 'category'
  ASSOC_REC_ID_FIELD: str = 'record_id_assoc'

  #------------------------------------------------------
  # Ground truth schemas for Patients and Events table
  #------------------------------------------------------
  # Patients without META field
  EXPECTED_PATIENTS_SCHEMA = TABLE_PATIENTS_SCHEMA
  # Events without META field
  EXPECTED_EVENTS_SCHEMA = TABLE_EVENTS_SCHEMA
  
  #------------------------------------------------------
  # Generate Events Table
  #------------------------------------------------------
  # CVDP COHORT EVENTS
  EVENTS_CVDP_COHORT_DATASET: str = 'cvdp_cohort'
  EVENTS_CVDP_COHORT_CATEGORY: str = 'cohort_extract'
  # CVDP HYPERTENSION EVENTS
  EVENTS_CVDP_HTN_DATASET: str = 'cvdp_htn'
  EVENTS_CVDP_HTN_CATEGORY: str = 'bp'
  # DARS MORTALITY EVENTS
  EVENTS_DARS_DATASET: str = 'dars_bird_deaths'
  EVENTS_DARS_CATEGORY: str = 'death'
  # HES CATEGORIES
  EVENTS_HES_APC_SPELL_CATEGORY: str = 'spell'
  EVENTS_HES_APC_EPISODE_CATEGORY: str = 'episode'
  # HES SPELL PREFIX
  EVENTS_HES_APC_SPELL_PREFIX: str = 'S_'

  #------------------------------------------------------
  # Generate Demographics Table
  #------------------------------------------------------
  DEMOG_CVDP_COHORT_JOURNAL_DATASET: str = 'cvdp_cohort_journal'

  #------------------------------------------------------
  # Generate Patient Table
  #------------------------------------------------------
  # DISEASE FLAGS
  STROKE_FLAG: str                    = 'STROKE'
  HEARTATTACK_FLAG: str               = 'HEARTATTACK'
  COUNT_FLAG: str                     = 'count'
  AGE_AT_DEATH: str                   = 'age_at_death'
  DEATH_AGE_FLAG: str                 = 'death_age_flag'
  UNDER_75_FLAG: str                  = 'DIED_UNDER_75'
  MAX_HOSPITALISATION_ENTRY: str      = 'max_hospitalisation_entry'
  MAX_DATE_AFTER_HOSPITALISATION: str = 'max_after_hospitalisation'
  #Patient Table Column Names
  STROKE_COUNT: str                   = 'stroke_count'
  MI_COUNT: str                       = 'mi_count'
  MAX_STROKE_DATE: str                = 'max_stroke_date'
  MAX_MI_DATE: str                    = 'max_mi_date'
  DATE_OF_DEATH: str                  = 'date_of_death'
  DEATH_FLAG: str                     = 'death_flag'
  JOURNAL_EXTRACT_DATE: str           = 'journal_extract_date'
  DEATH_30_HOSPITALISATION            = 'died_within_30_days_hospitalisation_flags'
  #Death Flags
  NON_CVD_DEATH: str                  = 'NON_CVD'
  #Associated Death Flags
  CVD_OTHER_FLAG: str                 = 'flag_assoc_cvd-other'
  CVD_OTHER_FLAG_RENAMED: str         = 'death_flag_assoc_cvd-other'
  CVD_STROKE_FLAG: str                = 'flag_assoc_stroke'
  CVD_STROKE_FLAG_RENAMED: str        = 'death_flag_assoc_stroke'
  CVD_HEARTATTACK_FLAG: str           = 'flag_assoc_heartattack'
  CVD_HEARTATTACK_FLAG_RENAMED: str   = 'death_flag_assoc_mi'
  #Columns to Drop
  PATIENT_TABLE_COLUMNS_TO_DROP       = (CVD_OTHER_FLAG_RENAMED, CVD_STROKE_FLAG_RENAMED, CVD_HEARTATTACK_FLAG_RENAMED, MAX_HOSPITALISATION_ENTRY,JOURNAL_EXTRACT_DATE)

  #------------------------------------------------------
  # diagnostic and inclusion codes
  #------------------------------------------------------
  ## see ./params/params_diagnostic_codes
  ALL_CVD_ICD10_CODES: List[str] = field(default_factory = lambda: ALL_CVD_CODES)
  STROKE_ICD10_CODES: List[str] = field(default_factory = lambda: STROKE_CODES)
  HEARTATTACK_ICD10_CODES: List[str]= field(default_factory = lambda: HEARTATTACK_CODES)
  CVDP_COHORT_CODES: List[str] = field(default_factory = lambda: CVDP_COHORT_CODES)
  DICT_DIAG_CODE_FIELD: str = 'diag_code'
  DICT_RES_CODE_FIELD: str = 'res_code'
  DICT_EXTRACT_WITH_FIELD: str = 'extract_with'
  DICT_FLAG_SUFFIX: str = 'diagnosis_date'
  HYPERTENSION_FLAG_PREFIX: str = 'HTN'
  ATRIAL_FIBRILLATION_FLAG_PREFIX: str = 'AF'
  STROKE_FLAG_PREFIX: str = 'STROKE'
  TIA_FLAG_PREFIX: str = 'TIA'
  CHRONIC_KIDNEY_DISEASE_FLAG_PREFIX: str = 'CKD'
  DIABETES_FLAG_PREFIX: str = 'DIABETES'
  ABDOMINAL_AORTIC_ANEURYSM_FLAG_PREFIX: str = 'AAA'
  PERIPHERAL_ARTERIAL_DISEASE_FLAG_PREFIX: str = 'PAD'
  FAMILIAL_HYPERCHOLESTEROLAEMIA_FLAG_PREFIX: str = 'FH'
  CORONARY_HEART_DISEASE_FLAG_PREFIX: str = 'CHD'
  HEART_FAILURE_FLAG_PREFIX: str = 'HF'
  FAMILIAL_HYPERCHOLESTEROLAEMIA_SCREEN_FLAG_PREFIX: str = 'FHSCREEN'
  NON_DIABETIC_HYPERGLYCAEMIA_FLAG_PREFIX: str = 'NDH'
  DIASTOLIC_BP_SNOMED_CODES: List[str] = field(default_factory = lambda: DIASTOLIC_BP_CODES)
  SYSTOLIC_BP_SNOMED_CODES: List[str] = field(default_factory = lambda: SYSTOLIC_BP_CODES)
  BP_SNOMED_CLUSTER: List[str] = field(default_factory = lambda: BP_CODE)
  MINIMUM_DIASTOLIC_BP: int = 20
  MINIMUM_SYSTOLIC_BP: int = 50
  ETHNICITY_UNKNOWN_CODES: List[str] = field(default_factory = lambda: ["Z","z","X","x","99","9", ""," "])

  #------------------------------------------------------
  # dataset filter values
  #------------------------------------------------------
  HES_APC_OTR_ADMIDATE_REPLACE_VALUE: str = '9999-01-01'
  HES_APC_OTR_DATE_FILTERS: List[str] = field(default_factory = lambda: ["1800-01-01", "1801-01-01","9999-01-01"])
  HES_APC_OTR_SPELL_ID_FILTER: str = '-1'
  HES_CODE_FILTER: str = 'R69X'

  #------------------------------------------------------
  # constants
  #------------------------------------------------------
  UNMAPPABLE_INT_MAPPING: int = -1
  UNMAPPABLE_STRING_MAPPING: str = 'invalid_map_value'
  EVENTS_HES_MULTIPLE_FLAGS: str = 'MULTIPLE'
  CVDP_DIAG_FLAGS_MAX_AGE: int = 120
  INVALID_NHS_NUMBER_FULL: List[str] = field(default_factory = lambda: ['0000000000', '1111111111', '2222222222', '3333333333', '4444444444', '5555555555', '6666666666', '7777777777', '8888888888'])
  INVALID_NHS_NUMBERS_PREFIX: str = '999'

  #------------------------------------------------------
  # Test Params
  #------------------------------------------------------
  INTEGRATION_TEST_LIMIT: int = 1000

  #------------------------------------------------------
  # pseudonymised asset params
  #------------------------------------------------------
  PSEUDO_DOB_FORMAT: str = 'y' # Value formats
  PSEUDO_HES_FLAG_REMOVAL_VALUE: str = 'NO_CVD' # Flag values to drop during filtering
  PSEUDO_DOB_FIELD: str = 'birth_year' # Column names
  PSEUDO_TABLE_PREFIX: str = 'cvdp_linkage' # Asset values
  PSEUDO_EVENTS_INCLUSION_DATASETS: List[str] = field(default_factory = lambda: [
    'cvdp_cohort','cvdp_htn','hes_apc','dars_bird_deaths'
  ]) # Events Filter: Inclusion Values
  PSEUDO_EVENTS_INCLUSION_CATEGORIES: List[str] = field(default_factory = lambda: [
    'cohort_extract','bp','episode','death','spell'
  ]) # Events Filter: Inclusion Values
  PSEUDO_SAVE_OVERWRITE: bool = True # Save Mode: Overwrite
  
  #------------------------------------------------------
  # member variable mapping/dict objects
  #------------------------------------------------------
  ## used to create mappings from member variables
  def __post_init__(self):
    ## Primary global join key(s) for all patient data
    self.GLOBAL_JOIN_KEY: List[str] = [self.PID_FIELD, self.DOB_FIELD]
    ## CVDP
    # Final deduplication columns for the CVDP cohort preprocessing
    self.CVDP_FIELDS_DEDUPE_COHORT: List[str] = [
      self.CVDP_PRACTICE_FIELD, self.SEX_FIELD, self.LSOA_FIELD, self.ETHNICITY_CODE_FIELD, self.CVDP_TIMESTAMP_FIELD,
      self.CVDP_META_PRACTICE_ID_FIELD
    ]
    # Columns to initially select from the cohort table -> journal table
    self.CVDP_JOURNAL_INPUT_COLUMNS: List[str] = [
      self.CVDP_PID_FIELD, self.CVDP_DOB_FIELD, self.CVDP_COHORT_FIELD, self.CVDP_EXTRACT_DATE,
      self.SEX_FIELD, self.LSOA_FIELD, self.ETHNICITY_CODE_FIELD, self.CVDP_PRIMARY_KEY
    ]
    # Columns to select from the journal_table array
    self.CVDP_JOURNAL_EXPLODE_COLUMNS: List[str] = [
      self.CVDP_JOURNAL_DATE_FIELD, self.CVDP_CODE_FIELD, self.CVDP_CODE_VALUE1_FIELD,
      self.CVDP_CODE_VALUE2_FIELD,
    ]
    # Columns to use as window partition fields for journal table deduplication
    self.CVDP_JOURNAL_DEDUPLICATION_FIELDS: List[str] = [
      self.CVDP_PID_FIELD, self.CVDP_DOB_FIELD, self.CVDP_JOURNAL_DATE_FIELD,
      self.CVDP_CODE_FIELD, self.CODE_ARRAY_FIELD,
    ]
    ## MAPPING FOR CVD HES AND DARS ICD10 CODES
    self.ALL_NON_STROKE_MI_ICD10_CODES: List[str] = [code for code in self.ALL_CVD_ICD10_CODES if code not in self.STROKE_ICD10_CODES and code not in self.HEARTATTACK_ICD10_CODES]
    self.DARS_ICD10_CODES_MAP: Dict[str, List[str]] = {
      'CVD_OTHER': self.ALL_NON_STROKE_MI_ICD10_CODES,
      'STROKE': self.STROKE_ICD10_CODES,
      'HEARTATTACK': self.HEARTATTACK_ICD10_CODES
    }
    self.HES_ICD10_CODES_MAP: Dict[str,str]  = {
      'STROKE': self.STROKE_ICD10_CODES,'HEARTATTACK': self.HEARTATTACK_ICD10_CODES,
      'CVD_OTHER': self.ALL_NON_STROKE_MI_ICD10_CODES
    }
    ## COHORT STAGE - CVDP COHORT MAPPING
    self.COHORT_TRANSFORM_MAPPING: Dict[str, str] = {
      self.CVDP_PID_FIELD: self.PID_FIELD,self.CVDP_DOB_FIELD: self.DOB_FIELD, 
      self.CVDP_COHORT_FIELD: self.COHORT_FIELD,self.CVDP_EXTRACT_DATE: self.EXTRACT_DATE_FIELD,
      self.CVDP_PRACTICE_FIELD: self.PRACTICE_FIELD,self.CVDP_PRIMARY_KEY: self.CVDP_COHORT_PRIMARY_KEY
    }
    ## COHORT STAGE - CVDP JOURNAL MAPPING
    self.JOURNAL_TRANSFORM_MAPPING: Dict[str, str] = {
      self.CVDP_PID_FIELD: self.PID_FIELD,self.CVDP_DOB_FIELD: self.DOB_FIELD,
      self.CVDP_COHORT_FIELD: self.COHORT_FIELD,self.CVDP_EXTRACT_DATE: self.EXTRACT_DATE_FIELD,
      self.CVDP_JOURNAL_DATE_FIELD: self.JOURNAL_DATE_FIELD,self.CVDP_CODE_FIELD: self.CODE_FIELD,
      self.CODE_ARRAY_FIELD: self.CODE_ARRAY_FIELD,self.REF_CLUSTER_FIELD: self.REF_CLUSTER_FIELD,
    }
    ## COHORT STAGE - CVDP COHORT FINAL COLUMNS
    self.COHORT_STAGE_COHORT_COLUMNS: List[str] = [
      self.PID_FIELD,self.DOB_FIELD, self.PRACTICE_FIELD,
      self.COHORT_FIELD, self.EXTRACT_DATE_FIELD, self.AGE_FIELD,
      self.SEX_FIELD, self.LSOA_FIELD, self.ETHNICITY_CODE_FIELD,
      self.CVDP_COHORT_PRIMARY_KEY,
    ]
    ## COHORT STAGE - CVDP JOURNAL HASHABLE COLUMNS
    self.CVDP_JOURNAL_HASHABLE_FIELDS: List[str] = [
      self.COHORT_FIELD,self.EXTRACT_DATE_FIELD,self.CODE_FIELD,
      self.JOURNAL_DATE_FIELD,self.CODE_ARRAY_FIELD,self.REF_CLUSTER_FIELD,
      self.CVDP_PRIMARY_KEY
    ]
    ## COHORT STAGE - CVDP JOURNAL FINAL COLUMNS
    self.COHORT_STAGE_JOURNAL_COLUMNS: List[str] = [
      self.PID_FIELD,self.DOB_FIELD,self.COHORT_FIELD,
      self.EXTRACT_DATE_FIELD,self.CODE_FIELD,
      self.JOURNAL_DATE_FIELD,self.SEX_FIELD,
      self.LSOA_FIELD,self.ETHNICITY_CODE_FIELD,
      self.CODE_ARRAY_FIELD,self.REF_CLUSTER_FIELD,
      self.CVDP_JOURNAL_PRIMARY_KEY,
    ]
    ## CVDP - HYPERTENSION (HTN)
    # > HASHABLE FIELDS FOR PRIMARY KEY
    self.CVDP_HTN_HASHABLE_FIELDS: List[str] = [
      self.PID_FIELD,self.DOB_FIELD,self.JOURNAL_DATE_FIELD
    ]
    # > FINAL OUTPUT COLUMNS
    self.CVDP_HTN_OUTPUT_COLUMNS: List[str] = [
      self.PID_FIELD,self.DOB_FIELD,self.AGE_FIELD,
      self.COHORT_FIELD,self.EXTRACT_DATE_FIELD,self.CVDP_SYSTOLIC_BP_READING,
      self.CVDP_DIASTOLIC_BP_READING,self.FLAG_FIELD,self.JOURNAL_DATE_FIELD,
      self.SEX_FIELD,self.LSOA_FIELD,self.ETHNICITY_CODE_FIELD,
    ]
    ## DARS
    # > BASE COLUMNS - MINIMUM REQUIRED COLUMNS
    selected_fields = [
      self.DARS_PID_FIELD, self.DARS_DOB_FIELD, self.DARS_SEX_FIELD,
      self.DARS_DOD_FIELD, self.FLAG_FIELD, self.DARS_UNDERLYING_CODE_FIELD,
      self.DARS_COMORBS_CODES_FIELD, self.DARS_RESIDENCE_FIELD, self.DARS_LOCATION_FIELD, 
      self.RECORD_ID_FIELD, self.ASSOC_REC_ID_FIELD
      ]
    # > BASE COLUMNS - ADDITIONAL ICD10 CODED COLUMNS
    selected_fields.extend([f'{self.FLAG_FIELD}_assoc_{code.lower()}' for code in self.DARS_ICD10_CODES_MAP.keys()])
    # > BASE COLUMNS - PREPROCESS DARS
    self.DARS_PREPROCESS_COLUMNS: List[str] = selected_fields
    ## HES
    ### APC
    self.HES_APC_PREPROCESS_COLUMNS: List[str] = [
      self.HES_APC_ID_FIELD, self.HES_APC_STARTDATE_FIELD,
      self.HES_APC_ENDDATE_FIELD, self.HES_APC_CODE_FIELD, self.HES_APC_CODE_LIST_FIELD,
      self.HES_APC_SEX_FIELD, self.HES_APC_LSOA_FIELD, self.HES_APC_ETHNICITY_FIELD,
      self.HES_APC_SPELL_STARTDATE_FIELD, self.HES_APC_SPELL_ENDDATE_FIELD,
      self.HES_APC_SPELL_END_INDICATOR_FIELD, self.HES_APC_SPELL_DUR_FIELD,
      self.HES_APC_SPELL_BEGIN_FIELD, self.HES_APC_ADMIMETH_FIELD
      ]
    self.HES_APC_S_PREPROCESS_COLUMNS: List[str] = [
      self.HES_S_APC_LINK_KEY, self.HES_S_APC_PID_FIELD, self.HES_S_APC_HESID_FIELD,
      self.HES_S_APC_DOB_FIELD
      ]
    ### OP
    self.HES_OP_PREPROCESS_COLUMNS: List[str] = [
      self.HES_OP_ID_FIELD, self.HES_OP_STARTDATE_FIELD, self.HES_OP_CODE_FIELD,
      self.HES_OP_CODE_LIST_FIELD, self.HES_OP_ETHNICITY_FIELD,
      ]
    self.HES_OP_S_PREPROCESS_COLUMNS: List[str] = [
      self.HES_S_OP_LINK_KEY, self.HES_S_OP_PID_FIELD, self.HES_S_OP_HESID_FIELD,
      self.HES_S_OP_DOB_FIELD
      ]
    ### AE
    self.HES_AE_PREPROCESS_COLUMNS: List[str] = []
    self.HES_AE_S_PREPROCESS_COLUMNS: List[str] = []
    self.HES_AE_PREPROCESS_COLUMNS: List[str] = [
      self.HES_AE_ID_FIELD, self.HES_AE_STARTDATE_FIELD,self.HES_AE_CODE_FIELD, self.HES_AE_CODE_LIST_FIELD,
      self.HES_AE_SEX_FIELD, self.HES_AE_LSOA_FIELD, self.HES_AE_ETHNICITY_FIELD,self.HES_AE_APC_LINK_KEY
      ]
    self.HES_AE_S_PREPROCESS_COLUMNS: List[str] = [
      self.HES_S_AE_LINK_KEY, self.HES_S_AE_PID_FIELD, self.HES_S_AE_HESID_FIELD,
      self.HES_S_AE_DOB_FIELD
      ]
    # HES MAPS
    self.HES_SENSITIVE_LINK_KEY_MAP: Dict[str, str] = {
                            'hes_apc': self.HES_S_APC_LINK_KEY,
                            'hes_op':  self.HES_S_OP_LINK_KEY,
                            'hes_ae':  self.HES_S_AE_LINK_KEY
                            }
    self.HES_PERSON_ID_FIELD_MAP: Dict[str, str] = {
                            'hes_apc': self.HES_S_APC_PID_FIELD,
                            'hes_op':  self.HES_S_OP_PID_FIELD,
                            'hes_ae': self.HES_S_AE_PID_FIELD
                            }
    self.HES_CODE_FIELD_MAP: Dict[str,str] = {
                            'hes_apc': self.HES_APC_CODE_FIELD,
                            'hes_op':  self.HES_OP_CODE_FIELD,
                            'hes_ae':  self.HES_AE_CODE_FIELD,
                            }
    self.HES_CODE_LIST_FIELD_MAP: Dict[str,str] = {
                            'hes_apc': self.HES_APC_CODE_LIST_FIELD,
                            'hes_op':  self.HES_OP_CODE_LIST_FIELD,
                            'hes_ae':  self.HES_AE_CODE_LIST_FIELD
    }
    self.HES_PREPROCESSING_COLS_MAP: Dict[str, str] = {
                            'hes_apc_ns': self.HES_APC_PREPROCESS_COLUMNS,
                            'hes_apc_s':  self.HES_APC_S_PREPROCESS_COLUMNS,
                            'hes_op_ns': self.HES_OP_PREPROCESS_COLUMNS,
                            'hes_op_s':  self.HES_OP_S_PREPROCESS_COLUMNS,
                            'hes_ae_ns': self.HES_AE_PREPROCESS_COLUMNS,
                            'hes_ae_s':  self.HES_AE_S_PREPROCESS_COLUMNS
                            }
    ## DEMOGRAPHIC
    self.DEMOGRAPHIC_OUTPUT_FIELDS: List[str] = [
      self.PID_FIELD, self.DOB_FIELD, self.DATASET_FIELD, 
      self.RECORD_STARTDATE_FIELD, self.ETHNICITY_FIELD
    ]
    self.DEMOGRAPHIC_FIELDS_TO_ENHANCE: List[str] = [self.ETHNICITY_FIELD]
    ## EVENTS TABLE COLUMNS
    self.HES_SPLIT_EPISODES_COLS: List[str] = [
      ## EPISODE SPECIFIC
      self.PID_FIELD, self.DOB_FIELD, self.SEX_FIELD, self.DATASET_FIELD, self.RECORD_ID_FIELD,
      self.RECORD_STARTDATE_FIELD, self.RECORD_ENDDATE_FIELD, self.LSOA_FIELD, self.ETHNICITY_FIELD,
      self.CODE_FIELD, self.FLAG_FIELD, self.CODE_ARRAY_FIELD, self.HES_APC_ADMIMETH_FIELD,
      ## SPELL ASSOCIATED
      self.HES_SPELL_ID_FIELD
    ]
    self.HES_SPLIT_SPELL_COLS: List[str] = [
      ## SPELL SPECIFIC
      self.PID_FIELD, self.DOB_FIELD, self.SEX_FIELD, self.DATASET_FIELD, self.HES_SPELL_ID_FIELD,
      self.HES_SPELL_START_FIELD, self.HES_SPELL_END_FIELD, self.LSOA_FIELD, self.ETHNICITY_FIELD,
      self.CODE_FIELD, self.FLAG_FIELD, self.CODE_ARRAY_FIELD, self.RECORD_STARTDATE_FIELD,
      ## EPISODE ASSOCIATED
      self.RECORD_ID_FIELD
    ]
    self.EVENTS_OUTPUT_FIELDS: List[str] = [
      self.PID_FIELD, self.DOB_FIELD, self.AGE_FIELD, self.SEX_FIELD,
      self.DATASET_FIELD, self.CATEGORY_FIELD, self.RECORD_ID_FIELD,
      self.RECORD_STARTDATE_FIELD, self.RECORD_ENDDATE_FIELD,
      self.LSOA_FIELD, self.ETHNICITY_FIELD, self.CODE_FIELD,
      self.FLAG_FIELD, self.CODE_ARRAY_FIELD, self.ASSOC_FLAG_FIELD,
      self.ASSOC_REC_ID_FIELD
    ]
    ## DISEASES
    self.FLAGS_TO_PROCESS: List[str] = [
      self.ABDOMINAL_AORTIC_ANEURYSM_FLAG_PREFIX, self.ATRIAL_FIBRILLATION_FLAG_PREFIX,
      self.CHRONIC_KIDNEY_DISEASE_FLAG_PREFIX, self.STROKE_FLAG_PREFIX, self.DIABETES_FLAG_PREFIX,
      self.PERIPHERAL_ARTERIAL_DISEASE_FLAG_PREFIX, self.FAMILIAL_HYPERCHOLESTEROLAEMIA_FLAG_PREFIX,
      self.CORONARY_HEART_DISEASE_FLAG_PREFIX, self.HYPERTENSION_FLAG_PREFIX, self.HEART_FAILURE_FLAG_PREFIX,
      self.TIA_FLAG_PREFIX, self.FAMILIAL_HYPERCHOLESTEROLAEMIA_SCREEN_FLAG_PREFIX, self.NON_DIABETIC_HYPERGLYCAEMIA_FLAG_PREFIX,
    ]
    self.DISEASE_FLAG_DICTIONARY: Dict[str, Dict[str,str]] = {
      self.ABDOMINAL_AORTIC_ANEURYSM_FLAG_PREFIX: {
        self.DICT_DIAG_CODE_FIELD: AAA_CODE,
        self.DICT_RES_CODE_FIELD: None,
        self.DICT_EXTRACT_WITH_FIELD: self.REF_CLUSTER_FIELD
      },
      self.ATRIAL_FIBRILLATION_FLAG_PREFIX: {
        self.DICT_DIAG_CODE_FIELD: AF_CODE,
        self.DICT_RES_CODE_FIELD: AF_RES_CODE,
        self.DICT_EXTRACT_WITH_FIELD: self.REF_CLUSTER_FIELD
      },
      self.CHRONIC_KIDNEY_DISEASE_FLAG_PREFIX: {
        self.DICT_DIAG_CODE_FIELD: CKD_CODE,
        self.DICT_RES_CODE_FIELD: CKD_RES_CODE,
        self.DICT_EXTRACT_WITH_FIELD: self.REF_CLUSTER_FIELD
      },
      self.STROKE_FLAG_PREFIX: {
        self.DICT_DIAG_CODE_FIELD: STROKE_CODE,
        self.DICT_RES_CODE_FIELD: None,
        self.DICT_EXTRACT_WITH_FIELD: self.REF_CLUSTER_FIELD
      },
      self.DIABETES_FLAG_PREFIX: {
        self.DICT_DIAG_CODE_FIELD: DIABETES_CODE,
        self.DICT_RES_CODE_FIELD: None,
        self.DICT_EXTRACT_WITH_FIELD: self.REF_CLUSTER_FIELD
      },
      self.PERIPHERAL_ARTERIAL_DISEASE_FLAG_PREFIX: {
        self.DICT_DIAG_CODE_FIELD: PAD_CODE,
        self.DICT_RES_CODE_FIELD: None,
        self.DICT_EXTRACT_WITH_FIELD: self.REF_CLUSTER_FIELD
      },
      self.FAMILIAL_HYPERCHOLESTEROLAEMIA_FLAG_PREFIX: {
        self.DICT_DIAG_CODE_FIELD: FH_SNOMED_CODES,
        self.DICT_RES_CODE_FIELD: None,
        self.DICT_EXTRACT_WITH_FIELD: self.CODE_FIELD
      },
      self.CORONARY_HEART_DISEASE_FLAG_PREFIX: {
        self.DICT_DIAG_CODE_FIELD: CHD_CODE,
        self.DICT_RES_CODE_FIELD: None,
        self.DICT_EXTRACT_WITH_FIELD: self.REF_CLUSTER_FIELD
      },
      self.HYPERTENSION_FLAG_PREFIX: {
        self.DICT_DIAG_CODE_FIELD: HYP_CODE,
        self.DICT_RES_CODE_FIELD: HYP_RES_CODE,
        self.DICT_EXTRACT_WITH_FIELD: self.REF_CLUSTER_FIELD
      },
      self.HEART_FAILURE_FLAG_PREFIX: {
        self.DICT_DIAG_CODE_FIELD: HF_CODE,
        self.DICT_RES_CODE_FIELD: None,
        self.DICT_EXTRACT_WITH_FIELD: self.REF_CLUSTER_FIELD
      },
      self.TIA_FLAG_PREFIX: {
        self.DICT_DIAG_CODE_FIELD: TIA_CODE,
        self.DICT_RES_CODE_FIELD: None,
        self.DICT_EXTRACT_WITH_FIELD: self.REF_CLUSTER_FIELD
      },
      self.FAMILIAL_HYPERCHOLESTEROLAEMIA_SCREEN_FLAG_PREFIX: {
        self.DICT_DIAG_CODE_FIELD: FH_SCREEN_CODE,
        self.DICT_RES_CODE_FIELD: None,
        self.DICT_EXTRACT_WITH_FIELD: self.REF_CLUSTER_FIELD
      },
      self.NON_DIABETIC_HYPERGLYCAEMIA_FLAG_PREFIX: {
        self.DICT_DIAG_CODE_FIELD: NDH_CODE,
        self.DICT_RES_CODE_FIELD: None,
        self.DICT_EXTRACT_WITH_FIELD: self.REF_CLUSTER_FIELD
      },
    }
    ## PATIENT TABLE COLUMNS
    ### BASE PATIENT TABLE FIELDS
    self.PATIENT_TABLE_BASE_FIELDS: List[str] = [
      self.PID_FIELD, self.DOB_FIELD, self.CODE_FIELD, self.AGE_FIELD, self.SEX_FIELD,
      self.ETHNICITY_FIELD, self.LSOA_FIELD, self.RECORD_STARTDATE_FIELD, self.CODE_ARRAY_FIELD,
    ]
    ### BASE PATIENT TABLE MAPPING
    self.PATIENT_TABLE_BASE_MAPPING: Dict[str, str] = {
      self.CODE_FIELD: self.COHORT_FIELD,
      self.RECORD_STARTDATE_FIELD: self.LATEST_EXTRACT_DATE,
      self.CODE_ARRAY_FIELD: self.LATEST_PRACTICE_ID,
    }
    ### OUTPUT DIAGNOSTIC COLUMNS
    self.PATIENT_TABLE_DIAGNOSTIC_FIELDS: List[str] = [
      field + '_' + self.DICT_FLAG_SUFFIX for field in self.FLAGS_TO_PROCESS
    ]
    ### FINAL OUTPUT FIELDS
    self.PATIENT_TABLE_OUTPUT_FIELDS: List[str] = [
      self.PID_FIELD, self.DOB_FIELD, self.LATEST_EXTRACT_DATE, self.LATEST_PRACTICE_ID,
      self.SEX_FIELD, self.LSOA_FIELD, self.COHORT_FIELD, self.ETHNICITY_FIELD,
    ] + self.PATIENT_TABLE_DIAGNOSTIC_FIELDS + [
      self.DATE_OF_DEATH, self.DEATH_FLAG, self.DEATH_AGE_FLAG,
      self.STROKE_COUNT,self.MAX_STROKE_DATE, self.MI_COUNT,
      self.MAX_MI_DATE, self.DEATH_30_HOSPITALISATION, self.CVDP_HYP_RISK_FIELD
    ]
    # Pseudo-Preparation Stage
    ## Columns to drop: Events Table
    self.PSEUDO_EVENTS_COLUMNS_DROPPED: tuple = (self.DOB_FIELD)
    ## Columns to drop: Patient Table
    self.PSEUDO_PATIENT_COLUMNS_DROPPED: tuple = ()
    # CVDP Extraction Fields
    ## Hashable Fields
    self.CVDP_HASHABLE_FIELDS: List[str] = [
      self.CVDP_PID_FIELD, self.CVDP_DOB_FIELD, self.CVDP_EXTRACT_DATE, self.CVDP_SEX_FIELD,
      self.CVDP_POSTCODE_FIELD, self.CVDP_PRACTICE_FIELD, self.CVDP_TIMESTAMP_FIELD,
      self.CVDP_AGE_FIELD, self.CVDP_META_PRACTICE_ID_FIELD
    ]
    ## Delta merge columns
    self.CVDP_DELTA_MERGE_COLUMNS: List[str] = [self.CVDP_PRIMARY_KEY]
    # HES Extraction Fields
    self.HES_APC_DELTA_MERGE_COLUMNS: List[str] = [self.HES_S_APC_LINK_KEY]
    self.HES_AE_DELTA_MERGE_COLUMNS: List[str] = [self.HES_S_AE_LINK_KEY]
    self.HES_OP_DELTA_MERGE_COLUMNS: List[str] = [self.HES_S_OP_LINK_KEY]
class DefaultParams(Params):
  pass

# COMMAND ----------

DEFAULT_PARAMS = DefaultParams()