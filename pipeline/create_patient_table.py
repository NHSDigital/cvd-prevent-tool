# Databricks notebook source
# create_patient_table

## Overview
# Stage for creating the patient table from the events table

# COMMAND ----------

# MAGIC %run ../params/params

# COMMAND ----------

# MAGIC %run ./pipeline_util

# COMMAND ----------

# MAGIC %run ./create_patient_table_lib

# COMMAND ----------

import pyspark.sql.functions as F

from pyspark.sql import DataFrame

# COMMAND ----------


class CreatePatientTableStage(PipelineStage):
    """
    A pipeline stage that processes the events table to generate the one-row-per-patient table.
    This table is enriched with data from the cohort and journal table

    The format of this stage is as follows:
    * _run(): Contains all the patient table processing functions
    * _create_base_table(): Creates the base patient table from the events table
    """

    # Final patient table format (defined in params)
    _output_diag_fields = params.PATIENT_TABLE_DIAGNOSTIC_FIELDS
    _output_fields      = params.PATIENT_TABLE_OUTPUT_FIELDS

    ### MAIN STAGE-CLASS DEFINITIONS ###
    def __init__(
        self,
        indicators_table_input: str,
        events_table_input: str,
        demographic_table_input: str,
        patient_table_output: str,
    ):
        self._indicators_table_input = indicators_table_input
        self._events_table_input = events_table_input
        self._demographic_table_input = demographic_table_input
        self._patient_table_output = patient_table_output
        self._data_holder: Dict[str, DataFrame] = {}
        self._data_check_rec: Dict[str, int] = {}
        super(CreatePatientTableStage, self).__init__(
            {
                self._indicators_table_input,
                self._events_table_input,
                self._demographic_table_input,
            },
            {self._patient_table_output},
        )

    # Public run method
    def _run(self, context, log):

        log._add_stage(self.name)

        log._timer(self.name)
        # Load the required stage assets
        self._load_data_assets(context)
        # Create patient base table from latest cohort events
        self._create_base_table()
        # Add demographic data
        if params.SWITCH_PATIENT_ENHANCE_DEMOGRAPHICS:
            self._add_demographic_data()
        # Add death events
        self._add_deaths_data()
        # Add hospitalisation information
        self._add_hospitalisation_data()
        # add diagnostic flags and create some patient flags
        self._add_patient_flags()
        # Add hypertension data
        self._add_hypertension_data()
        # Format patient table
        self._format_patient_table()
        # Check formatting and data quality of patient table
        self._check_patient_table()

        log._timer(self.name, end=True)

        # Return pipeline asset
        return {
            self._patient_table_output: PipelineAsset(
                key = self._patient_table_output,
                context = context,
                db = params.DATABASE_NAME,
                df = self._data_holder["patient"],
                cache = False,
                delta_table = True,
                delta_columns = [params.PID_FIELD,params.DOB_FIELD]
            )
        }

    # Load data assets into stage
    def _load_data_assets(self, context):
        """_load_data_assets
        Loads the events table and any additional enrichment data. Stores into _data_holder.
        """
        self._data_holder["events"] = context[self._events_table_input].df.drop("META")
        self._data_holder["diag_flags"] = context[self._indicators_table_input].df.drop("META")
        if params.SWITCH_PATIENT_ENHANCE_DEMOGRAPHICS:
            self._data_holder["demographics"] = context[self._demographic_table_input].df.drop("META")

    # Create base table from events table
    def _create_base_table(self):
        """create_base_table
        Creates the patient table from the events table, cohort based events.
        (dataset = cvdp_cohort; category = cohort_extract).

        Functions:
            pipeline/create_patient_table_lib::extract_patient_events
            pipeline/create_patient_table_lib::format_base_patient_table_schema
            src/util::filter_latest
        """
        # Load the events table
        df = self._data_holder["events"]
        # Extract CVDP cohort events from the events table and filter to keep latest only
        df = extract_patient_events(
            df=df,
            value_dataset=params.EVENTS_CVDP_COHORT_DATASET,
            value_category=params.EVENTS_CVDP_COHORT_CATEGORY,
            )
        df = filter_latest(
            df=df,
            window_fields=[params.PID_FIELD, params.DOB_FIELD],
            date_field=params.RECORD_STARTDATE_FIELD
            )
        # Select columns that form the base patient table
        df = df.select(params.PATIENT_TABLE_BASE_FIELDS)
        if params.SWITCH_PATIENT_ENHANCE_DEMOGRAPHICS:
            df = df.drop(*params.DEMOGRAPHIC_FIELDS_TO_ENHANCE)
        # Map base columns to final base table schema (column renaming)
        df = format_base_patient_table_schema(
            df = df,
            mapping_cols = params.PATIENT_TABLE_BASE_MAPPING,
            field_practice_identifier = params.CODE_ARRAY_FIELD
            )
        # Save patient table
        self._data_holder["patient"] = df

    # Add demographic information
    def _add_demographic_data(self):
        """_add_demographic_data
        Extracts demographic data from the demographic table and joins to the patient table.
        Currently this just includes ethnicity.
        """
        df = self._data_holder["patient"]

        pid_fields = [params.PID_FIELD, params.DOB_FIELD]
        demographic_fields = params.DEMOGRAPHIC_FIELDS_TO_ENHANCE

        df_demographics = self._data_holder["demographics"].select(*pid_fields, *demographic_fields)

        df = df.join(df_demographics, on=pid_fields, how='left')

        self._data_holder["patient"] = df

    # Events Table Data: Add death information
    def _add_deaths_data(self):
        """_add_deaths_data
        Extracts death events from the events table and joins them to the patient table
        """
        # Load and filter deaths data
        df_death = self._data_holder["events"]
        df_death = extract_patient_events(
            df=df_death,
            value_dataset=params.EVENTS_DARS_DATASET,
            value_category=params.EVENTS_DARS_CATEGORY,
        )
        df_death = df_death.select(
            F.col(params.PID_FIELD),
            F.col(params.DOB_FIELD),
            F.col(params.RECORD_STARTDATE_FIELD).alias(params.DATE_OF_DEATH),
            F.col(params.FLAG_FIELD).alias(params.DEATH_FLAG),
        )
        # Join to patient table
        df_patient = self._data_holder["patient"]
        df_patient = df_patient.join(
            df_death, params.GLOBAL_JOIN_KEY, "left"
        )
        # Return
        self._data_holder["patient"] = df_patient

    # Events Table Data: Add hospitalisation data
    def _add_hospitalisation_data(self):
        """
        SUMMARY: adds counts per patient for Strokes and Heart Attacks
                    Also adds the most recent hospitalisation date for both
                    Updates the patient table with the given columns
        """

        df = self._data_holder["patient"]
        df_events = self._data_holder["events"]

        df_events = extract_patient_events(
            df=df_events, value_dataset=params.HES_APC_TABLE, value_category=params.EVENTS_HES_APC_SPELL_CATEGORY)

        self._data_holder["patient"] = add_stroke_mi_information(
            patient_table=df, events_table=df_events, keep_nulls=True, keep_inclusive=True)

    # Events Table Data: Add patient flag information
    def _add_patient_flags(self):
        """
        SUMMARY: Joins on the diagnostic flags table to the current patient table
                    Also adds a flag column if the patient died under 75,
                    Also adds a flag column if the patient died within 30 days of a hospitalisation
                    Updates the patient table with the given columns
        """

        df_patient = self._data_holder["patient"]
        df_events = self._data_holder["events"]

        df_patient = df_patient.join(self._data_holder["diag_flags"], params.GLOBAL_JOIN_KEY, "left")

        # FILTER ONLY EPISODES
        df_events = extract_patient_events(df = df_events, value_dataset = params.HES_APC_TABLE, value_category = params.EVENTS_HES_APC_EPISODE_CATEGORY)

        df_indicators = create_patient_table_flags(
        df=df_patient, events=df_events
        )

        self._data_holder["patient"] = df_indicators

    # Events Table Data: Add hypertension data
    def _add_hypertension_data(self):
        """
        SUMMARY: adds the hypertension risk group information from the blood pressure events.
                    Also adds a flag for non-hypertensive patients (-1) and calculation for
                    instances where the most recent blood pressure measurement is not valid.
        """

        # Data Loading
        df = self._data_holder["patient"]
        df_events = extract_patient_events(
            df=self._data_holder["events"],
            value_dataset=params.EVENTS_CVDP_HTN_DATASET,
            value_category=params.EVENTS_CVDP_HTN_CATEGORY
        )

        # Risk group assignment
        df = assign_htn_risk_groups(
            patient_table = df,
            events_table = df_events
        )

        self._data_holder["patient"] = df

    # Format the patient table to the final output schema
    def _format_patient_table(self):
        """_format_patient_table
        Selects desired columns from the patient table and orders correctly
        """
        df = self._data_holder["patient"]
        self._data_holder["patient"] = df.select(self._output_fields)

    # Data quality check for non-nullable fields
    def _check_patient_table(self):
        """_check_patient_table
        Perform checks on the patient identifier, date of birth and cohort fields to ensure there are no
        null values in these columns (should not be possible as these are linkage and inclusion critera
        fields)
        """
        df = self._data_holder["patient"]
        df_null_pid = df.select("*").where(F.col(params.PID_FIELD).isNull()).limit(1)
        df_null_dob = df.select("*").where(F.col(params.DOB_FIELD).isNull()).limit(1)
        df_null_cohort = (
            df.select("*").where(F.col(params.COHORT_FIELD).isNull()).limit(1)
        )

        if df_null_pid.count() > 0:
            print(f"[WARNING] NULLS IDENTIFIED IN COL {params.PID_FIELD}")
            self._data_check_rec["pid"] = 1
        else:
            self._data_check_rec["pid"] = 0

        if df_null_dob.count() > 0:
            print(f"[WARNING] NULLS IDENTIFIED IN COL {params.DOB_FIELD}")
            self._data_check_rec["dob"] = 1
        else:
            self._data_check_rec["dob"] = 0

        if df_null_cohort.count() > 0:
            print(f"[WARNING] NULLS IDENTIFIED IN COL {params.COHORT_FIELD}")
            self._data_check_rec["cohort"] = 1
        else:
            self._data_check_rec["cohort"] = 0

# COMMAND ----------

