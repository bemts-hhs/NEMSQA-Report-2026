# IOWA NEMSQA REPORT TRAUMA-04 2026 ------------------------------------

###_____________________________________________________________________________
# this script will contain all reporting calculations for Trauma-04
# use nemsqa_report_prep_2026.R to get critical functions into memory
###_____________________________________________________________________________
# assume that nemsqa_report_prep_2026.R was already ran to load needed packages
# and project-specific custom functions in the project
###_____________________________________________________________________________
# For any section that includes parallel processing, the intent is to run the
# tictoc chunks together with the nemsqar / mirai parallel processing chunks so
# that function time benchmarking can happen. This is not required, but will
# help check how parallel processing is performing, and if parallel processing
# should be used at all for certain NEMSQA measure analyses.
###___________________________________________________________________________

###___________________________________________________________________________
# Note on parallel processing - due to the heavy overhead involved with this
# function, we will only use nemsqar's built in grouping capabilities and will
# not leverage mori. mirai will only be used here for data ingestion.
###___________________________________________________________________________

# DATA -----------------------------------------------------------------------

# tables imported in alphabetical order
# tables do not need to be loaded again if already in memory

## disposition tables -----------------------------------------------------
disposition_table <- load_nemsqa_parallel(
  table = "disposition",
  years = 2021:2025,
  cores = 13,
  exclude = "DISPOSITION_DESTINATION_US_NATIONAL_GRID_COORDINATES_E_DISPOSITION_10"
)

# share the disposition table
disposition_table_s <- mori::share(disposition_table)

## exam tables ------------------------------------------------------------

# Parallel process
exam_table <- load_nemsqa_parallel(
  table = "exam",
  years = c(
    "2021",
    "2022",
    "2023",
    "2024_1",
    "2024_2",
    "2024_3",
    "2024_4",
    "2024_5",
    "2024_6",
    "2025_1",
    "2025_2",
    "2025_3",
    "2025_4",
    "2025_5",
    "2025_6",
    "2025_7"
  ),
  cores = 13
)


## injury tables ----------------------------------------------------------

# Parallel process
injury_table <- load_nemsqa_parallel(
  table = "injury",
  years = 2021:2025,
  cores = 13
)

## medications tables -----------------------------------------------------
medications_table <- load_nemsqa_parallel(
  table = "medications",
  year = 2021:2025,
  cores = 13
)

## patient tables ---------------------------------------------------------
# Utilize mirai for asynchronous loading
# automatically bind rows
patient_scene_clean <- load_nemsqa_parallel(
  table = "patient_scene",
  years = 2021:2025,
  cores = 13
)

### final manipulations on the patient/scene table ----
# handle multiple issues with location using external data sources with
# consistent names

patient_scene_table <- patient_scene_clean |>
  dplyr::left_join(
    zipcodes,
    by = c("SCENE_INCIDENT_POSTAL_CODE_E_SCENE_19" = "new_zipcode")
  ) |>
  dplyr::left_join(
    location,
    by = c("SCENE_INCIDENT_COUNTY_NAME_E_SCENE_21" = "County")
  ) |>
  dplyr::relocate(new_county, .after = SCENE_INCIDENT_COUNTY_NAME_E_SCENE_21) |>
  dplyr::relocate(new_state, .after = SCENE_INCIDENT_STATE_NAME_E_SCENE_18) |>
  dplyr::relocate(`Region: Preparedness`, .after = new_county) |>
  dplyr::relocate(Pop, .after = `Region: Preparedness`) |>
  dplyr::relocate(State, .after = SCENE_INCIDENT_STATE_NAME_E_SCENE_18) |>
  dplyr::relocate(Country, .after = new_state) |>
  dplyr::relocate(Designation, .after = new_county) |>
  dplyr::mutate(
    SCENE_INCIDENT_COUNTY_NAME_E_SCENE_21 = dplyr::if_else(
      is.na(SCENE_INCIDENT_COUNTY_NAME_E_SCENE_21) &
        !is.na(new_county),
      new_county,
      SCENE_INCIDENT_COUNTY_NAME_E_SCENE_21
    ),
    SCENE_INCIDENT_STATE_NAME_E_SCENE_18 = dplyr::if_else(
      is.na(SCENE_INCIDENT_STATE_NAME_E_SCENE_18) &
        !is.na(new_state),
      new_state,
      SCENE_INCIDENT_STATE_NAME_E_SCENE_18
    )
  ) |>
  clean_county_names_1(
    county_column = SCENE_INCIDENT_COUNTY_NAME_E_SCENE_21,
    city_column = SCENE_INCIDENT_CITY_NAME_E_SCENE_17,
    zip_column = SCENE_INCIDENT_POSTAL_CODE_E_SCENE_19
  ) |>
  clean_county_names_1(
    county_column = PATIENT_HOME_COUNTY_NAME_E_PATIENT_07,
    city_column = PATIENT_HOME_CITY_NAME_E_PATIENT_06,
    zip_column = PATIENT_HOME_POSTAL_CODE_E_PATIENT_09
  ) |>
  clean_county_names_2(
    county_column = SCENE_INCIDENT_COUNTY_NAME_E_SCENE_21,
    zip_column = SCENE_INCIDENT_POSTAL_CODE_E_SCENE_19
  ) |>
  clean_county_names_2(
    county_column = PATIENT_HOME_COUNTY_NAME_E_PATIENT_07,
    zip_column = PATIENT_HOME_POSTAL_CODE_E_PATIENT_09
  ) |>
  fix_county_region(
    city_col = SCENE_INCIDENT_CITY_NAME_E_SCENE_17,
    county_col = SCENE_INCIDENT_COUNTY_NAME_E_SCENE_21,
    region_col = `Region: Preparedness`,
    external_city = Iowa_Data_Final$name_city,
    external_county = county_data$County,
    external_region = county_data$`Region: Preparedness`
  ) |>
  dplyr::mutate(
    State_Iowa = grepl(
      "(?:iowa$|^ia.*$|^ia$)",
      SCENE_INCIDENT_STATE_NAME_E_SCENE_18,
      ignore.case = TRUE
    )
  ) |>
  dplyr::mutate(
    SCENE_INCIDENT_COUNTY_NAME_E_SCENE_21 = factor(
      SCENE_INCIDENT_COUNTY_NAME_E_SCENE_21
    )
  )

### remove patient_scene_clean to preserve memory
rm(patient_scene_clean)
gc()

## procedures tables ------------------------------------------------------
procedures_table <- load_nemsqa_parallel(
  table = "procedures",
  years = 2021:2025,
  cores = 13
)

## response tables --------------------------------------------------------
response_table <- load_nemsqa_parallel(
  table = "response",
  years = 2021:2025,
  cores = 13
)

## situation tables -------------------------------------------------------
# set up situation table for manipulations
situation_table <- load_nemsqa_parallel(
  table = "situation",
  years = 2021:2025,
  cores = 13
)

## vitals tables ----------------------------------------------------------
vitals_table <- load_nemsqa_parallel(
  table = "vitals",
  years = 2021:2025,
  cores = 13
)

# CALCULATIONS ---------------------------------------------------------------

## Trauma-04 ==================================================================

## trauma-04 populations ######################################################

### populations over all years 2021-2025 -----------------------------------
trauma_04_pop <- nemsqar::trauma_04_population(
  df = NULL,
  patient_scene_table = patient_scene_table,
  response_table = response_table,
  situation_table = situation_table,
  vitals_table = vitals_table,
  exam_table = exam_table,
  procedures_table = procedures_table,
  injury_table = injury_table,
  disposition_table = disposition_table,
  erecord_01_col = FACT_INCIDENT_PK,
  incident_date_col = INCIDENT_DATE,
  patient_DOB_col = PATIENT_DATE_OF_BIRTH_E_PATIENT_17,
  epatient_15_col = PATIENT_AGE_E_PATIENT_15,
  epatient_16_col = PATIENT_AGE_UNITS_E_PATIENT_16,
  esituation_02_col = SITUATION_POSSIBLE_INJURY_WITH_CODE_E_SITUATION_02,
  eresponse_05_col = RESPONSE_TYPE_OF_SERVICE_REQUESTED_WITH_CODE_E_RESPONSE_05,
  eresponse_10_col = RESPONSE_TYPE_OF_SCENE_DELAY_LIST_E_RESPONSE_10,
  transport_disposition_col = c(
    DISPOSITION_INCIDENT_PATIENT_DISPOSITION_WITH_CODE_3_4_E_DISPOSITION_12_3_5_IT_DISPOSITION_112,
    TRANSPORT_DISPOSITION_3_4_IT_DISPOSITION_102_3_5_E_DISPOSITION_30
  ),
  edisposition_02_col = DISPOSITION_DESTINATION_CODE_DELIVERED_TRANSFERRED_TO_E_DISPOSITION_02,
  trauma_center_facility_IDs = facilities$`Facility State ID`,
  evitals_06_col = VITALS_SYSTOLIC_BLOOD_PRESSURE_SBP_E_VITALS_06,
  evitals_10_col = VITALS_HEART_RATE_E_VITALS_10,
  evitals_12_col = VITALS_PULSE_OXIMETRY_E_VITALS_12,
  evitals_14_col = VITALS_RESPIRATORY_RATE_E_VITALS_14,
  evitals_15_col = VITALS_RESPIRATORY_EFFORT_E_VITALS_15,
  evitals_21_col = VITALS_GLASGOW_COMA_SCORE_GCS_MOTOR_E_VITALS_21,
  eexam_16_col = PATIENT_EXTREMITY_ASSESSMENT_FINDINGS_LIST_E_EXAM_16,
  eexam_20_col = PATIENT_NEUROLOGICAL_ASSESSMENT_FINDINGS_LIST_E_EXAM_20,
  eexam_23_col = PATIENT_LUNG_ASSESSMENT_FINDINGS_LIST_3_4_IT_EXAM_100_3_5_E_EXAM_23,
  eexam_25_col = PATIENT_CHEST_EXCLUSIVE_ASSESSMENT_FINDINGS_LIST_3_4_IT_EXAM_102_3_5_E_EXAM_25,
  eprocedures_03_col = PATIENT_ATTEMPTED_PROCEDURE_DESCRIPTIONS_AND_CODES_LIST_E_PROCEDURES_03,
  einjury_01_col = INJURY_CAUSE_OF_INJURY_DESCRIPTION_AND_CODE_LIST_E_INJURY_01,
  einjury_03_col = INJURY_TRAUMA_CENTER_TRIAGE_CRITERIA_STEPS_1_AND_2_LIST_E_INJURY_03,
  einjury_04_col = INJURY_VEHICULAR_PEDESTRIAN_OR_OTHER_INJURY_RISK_FACTOR_TRIAGE_CRITERIA_STEPS_3_AND_4_LIST_E_INJURY_04,
  einjury_09_col = INJURY_HEIGHT_OF_FALL_IN_FEET_E_INJURY_09
)

# population results for 2021-2025
trauma_04_pop_filter_process <- trauma_04_pop$filter_process

# population missingness for 2021-2025
trauma_04_missings <- trauma_04_pop$missingness

### get trauma_04 population data for each year using mirai and mori ------

# track progress
tictoc::tic(msg = "trauma_04_pop_years_init")

trauma_04_pop_years_init <- purrr::map(
  .x = report_years,
  \(yr) {
    # Dynamic message inside the loop
    cli::cli_alert_info("Running year: {yr}.")

    # filter by year
    ps_y <- patient_scene_table |> dplyr::filter(INCIDENT_YEAR == yr)
    rsp_y <- response_table |> dplyr::filter(INCIDENT_YEAR == yr)
    dis_y <- disposition_table |> dplyr::filter(INCIDENT_YEAR == yr)
    sit_y <- situation_table |> dplyr::filter(INCIDENT_YEAR == yr)
    vit_y <- vitals_table |> dplyr::filter(INCIDENT_YEAR == yr)
    ex_y <- exam_table |> dplyr::filter(INCIDENT_YEAR == yr)
    pro_y <- procedures_table |> dplyr::filter(INCIDENT_YEAR == yr)
    inj_y <- injury_table |> dplyr::filter(INCIDENT_YEAR == yr)

    # run the function
    nemsqar::trauma_04_population(
      df = NULL,
      patient_scene_table = ps_y,
      response_table = rsp_y,
      situation_table = sit_y,
      vitals_table = vit_y,
      exam_table = ex_y,
      procedures_table = pro_y,
      injury_table = inj_y,
      disposition_table = dis_y,
      erecord_01_col = FACT_INCIDENT_PK,
      incident_date_col = INCIDENT_DATE,
      patient_DOB_col = PATIENT_DATE_OF_BIRTH_E_PATIENT_17,
      epatient_15_col = PATIENT_AGE_E_PATIENT_15,
      epatient_16_col = PATIENT_AGE_UNITS_E_PATIENT_16,
      esituation_02_col = SITUATION_POSSIBLE_INJURY_WITH_CODE_E_SITUATION_02,
      eresponse_05_col = RESPONSE_TYPE_OF_SERVICE_REQUESTED_WITH_CODE_E_RESPONSE_05,
      eresponse_10_col = RESPONSE_TYPE_OF_SCENE_DELAY_LIST_E_RESPONSE_10,
      transport_disposition_col = c(
        DISPOSITION_INCIDENT_PATIENT_DISPOSITION_WITH_CODE_3_4_E_DISPOSITION_12_3_5_IT_DISPOSITION_112,
        TRANSPORT_DISPOSITION_3_4_IT_DISPOSITION_102_3_5_E_DISPOSITION_30
      ),
      edisposition_02_col = DISPOSITION_DESTINATION_CODE_DELIVERED_TRANSFERRED_TO_E_DISPOSITION_02,
      trauma_center_facility_IDs = facilities$`Facility State ID`,
      evitals_06_col = VITALS_SYSTOLIC_BLOOD_PRESSURE_SBP_E_VITALS_06,
      evitals_10_col = VITALS_HEART_RATE_E_VITALS_10,
      evitals_12_col = VITALS_PULSE_OXIMETRY_E_VITALS_12,
      evitals_14_col = VITALS_RESPIRATORY_RATE_E_VITALS_14,
      evitals_15_col = VITALS_RESPIRATORY_EFFORT_E_VITALS_15,
      evitals_21_col = VITALS_GLASGOW_COMA_SCORE_GCS_MOTOR_E_VITALS_21,
      eexam_16_col = PATIENT_EXTREMITY_ASSESSMENT_FINDINGS_LIST_E_EXAM_16,
      eexam_20_col = PATIENT_NEUROLOGICAL_ASSESSMENT_FINDINGS_LIST_E_EXAM_20,
      eexam_23_col = PATIENT_LUNG_ASSESSMENT_FINDINGS_LIST_3_4_IT_EXAM_100_3_5_E_EXAM_23,
      eexam_25_col = PATIENT_CHEST_EXCLUSIVE_ASSESSMENT_FINDINGS_LIST_3_4_IT_EXAM_102_3_5_E_EXAM_25,
      eprocedures_03_col = PATIENT_ATTEMPTED_PROCEDURE_DESCRIPTIONS_AND_CODES_LIST_E_PROCEDURES_03,
      einjury_01_col = INJURY_CAUSE_OF_INJURY_DESCRIPTION_AND_CODE_LIST_E_INJURY_01,
      einjury_03_col = INJURY_TRAUMA_CENTER_TRIAGE_CRITERIA_STEPS_1_AND_2_LIST_E_INJURY_03,
      einjury_04_col = INJURY_VEHICULAR_PEDESTRIAN_OR_OTHER_INJURY_RISK_FACTOR_TRIAGE_CRITERIA_STEPS_3_AND_4_LIST_E_INJURY_04,
      einjury_09_col = INJURY_HEIGHT_OF_FALL_IN_FEET_E_INJURY_09
    )
  }
)

# Get total time
time <- tictoc::toc()

#### append years to the population files ----
trauma_04_pop_years <- add_year_to_nested(
  x = trauma_04_pop_years_init,
  file = "filter_process",
  years = 2021:2025
)

#### append years to the missingness files ----
trauma_04_missingness_years <- add_year_to_nested(
  x = trauma_04_pop_years_init,
  file = "missingness",
  years = 2021:2025
)

# plot population trends over time
trauma_04_pop_years |>
  plot_nemsqa_pops(
    type = "col",
    wrap_width = 25,
    plot_title = "Trauma-04"
  )

# create a gt table from population trends over time
trauma_04_pop_years |>
  prepare_population_statistical_file() |>
  population_statistical_file_gt(measure = "Trauma-04") |>
  tab_style_hhs(message_text = "via R package {nemsqar}", border_cols = 2)


## trauma-04 results ##########################################################

### results years ----------------------------------------------------------

#### year ----
trauma_04_result_year <- nemsqar::trauma_04(
  df = NULL,
  patient_scene_table = patient_scene_table,
  response_table = response_table,
  situation_table = situation_table,
  vitals_table = vitals_table,
  exam_table = exam_table,
  procedures_table = procedures_table,
  injury_table = injury_table,
  disposition_table = disposition_table,
  erecord_01_col = FACT_INCIDENT_PK,
  incident_date_col = INCIDENT_DATE,
  patient_DOB_col = PATIENT_DATE_OF_BIRTH_E_PATIENT_17,
  epatient_15_col = PATIENT_AGE_E_PATIENT_15,
  epatient_16_col = PATIENT_AGE_UNITS_E_PATIENT_16,
  esituation_02_col = SITUATION_POSSIBLE_INJURY_WITH_CODE_E_SITUATION_02,
  eresponse_05_col = RESPONSE_TYPE_OF_SERVICE_REQUESTED_WITH_CODE_E_RESPONSE_05,
  eresponse_10_col = RESPONSE_TYPE_OF_SCENE_DELAY_LIST_E_RESPONSE_10,
  transport_disposition_col = c(
    DISPOSITION_INCIDENT_PATIENT_DISPOSITION_WITH_CODE_3_4_E_DISPOSITION_12_3_5_IT_DISPOSITION_112,
    TRANSPORT_DISPOSITION_3_4_IT_DISPOSITION_102_3_5_E_DISPOSITION_30
  ),
  edisposition_02_col = DISPOSITION_DESTINATION_CODE_DELIVERED_TRANSFERRED_TO_E_DISPOSITION_02,
  trauma_center_facility_IDs = facilities$`Facility State ID`,
  evitals_06_col = VITALS_SYSTOLIC_BLOOD_PRESSURE_SBP_E_VITALS_06,
  evitals_10_col = VITALS_HEART_RATE_E_VITALS_10,
  evitals_12_col = VITALS_PULSE_OXIMETRY_E_VITALS_12,
  evitals_14_col = VITALS_RESPIRATORY_RATE_E_VITALS_14,
  evitals_15_col = VITALS_RESPIRATORY_EFFORT_E_VITALS_15,
  evitals_21_col = VITALS_GLASGOW_COMA_SCORE_GCS_MOTOR_E_VITALS_21,
  eexam_16_col = PATIENT_EXTREMITY_ASSESSMENT_FINDINGS_LIST_E_EXAM_16,
  eexam_20_col = PATIENT_NEUROLOGICAL_ASSESSMENT_FINDINGS_LIST_E_EXAM_20,
  eexam_23_col = PATIENT_LUNG_ASSESSMENT_FINDINGS_LIST_3_4_IT_EXAM_100_3_5_E_EXAM_23,
  eexam_25_col = PATIENT_CHEST_EXCLUSIVE_ASSESSMENT_FINDINGS_LIST_3_4_IT_EXAM_102_3_5_E_EXAM_25,
  eprocedures_03_col = PATIENT_ATTEMPTED_PROCEDURE_DESCRIPTIONS_AND_CODES_LIST_E_PROCEDURES_03,
  einjury_01_col = INJURY_CAUSE_OF_INJURY_DESCRIPTION_AND_CODE_LIST_E_INJURY_01,
  einjury_03_col = INJURY_TRAUMA_CENTER_TRIAGE_CRITERIA_STEPS_1_AND_2_LIST_E_INJURY_03,
  einjury_04_col = INJURY_VEHICULAR_PEDESTRIAN_OR_OTHER_INJURY_RISK_FACTOR_TRIAGE_CRITERIA_STEPS_3_AND_4_LIST_E_INJURY_04,
  einjury_09_col = INJURY_HEIGHT_OF_FALL_IN_FEET_E_INJURY_09,
  confidence_interval = TRUE,
  method = "w",
  conf.level = 0.95,
  correct = TRUE,
  .by = INCIDENT_YEAR
)

### results regions and years ----------------------------------------------

#### regions and years ----
trauma_04_result_regions_years <- nemsqar::trauma_04(
  df = NULL,
  patient_scene_table = patient_scene_table,
  response_table = response_table,
  situation_table = situation_table,
  vitals_table = vitals_table,
  exam_table = exam_table,
  procedures_table = procedures_table,
  injury_table = injury_table,
  disposition_table = disposition_table,
  erecord_01_col = FACT_INCIDENT_PK,
  incident_date_col = INCIDENT_DATE,
  patient_DOB_col = PATIENT_DATE_OF_BIRTH_E_PATIENT_17,
  epatient_15_col = PATIENT_AGE_E_PATIENT_15,
  epatient_16_col = PATIENT_AGE_UNITS_E_PATIENT_16,
  esituation_02_col = SITUATION_POSSIBLE_INJURY_WITH_CODE_E_SITUATION_02,
  eresponse_05_col = RESPONSE_TYPE_OF_SERVICE_REQUESTED_WITH_CODE_E_RESPONSE_05,
  eresponse_10_col = RESPONSE_TYPE_OF_SCENE_DELAY_LIST_E_RESPONSE_10,
  transport_disposition_col = c(
    DISPOSITION_INCIDENT_PATIENT_DISPOSITION_WITH_CODE_3_4_E_DISPOSITION_12_3_5_IT_DISPOSITION_112,
    TRANSPORT_DISPOSITION_3_4_IT_DISPOSITION_102_3_5_E_DISPOSITION_30
  ),
  edisposition_02_col = DISPOSITION_DESTINATION_CODE_DELIVERED_TRANSFERRED_TO_E_DISPOSITION_02,
  trauma_center_facility_IDs = facilities$`Facility State ID`,
  evitals_06_col = VITALS_SYSTOLIC_BLOOD_PRESSURE_SBP_E_VITALS_06,
  evitals_10_col = VITALS_HEART_RATE_E_VITALS_10,
  evitals_12_col = VITALS_PULSE_OXIMETRY_E_VITALS_12,
  evitals_14_col = VITALS_RESPIRATORY_RATE_E_VITALS_14,
  evitals_15_col = VITALS_RESPIRATORY_EFFORT_E_VITALS_15,
  evitals_21_col = VITALS_GLASGOW_COMA_SCORE_GCS_MOTOR_E_VITALS_21,
  eexam_16_col = PATIENT_EXTREMITY_ASSESSMENT_FINDINGS_LIST_E_EXAM_16,
  eexam_20_col = PATIENT_NEUROLOGICAL_ASSESSMENT_FINDINGS_LIST_E_EXAM_20,
  eexam_23_col = PATIENT_LUNG_ASSESSMENT_FINDINGS_LIST_3_4_IT_EXAM_100_3_5_E_EXAM_23,
  eexam_25_col = PATIENT_CHEST_EXCLUSIVE_ASSESSMENT_FINDINGS_LIST_3_4_IT_EXAM_102_3_5_E_EXAM_25,
  eprocedures_03_col = PATIENT_ATTEMPTED_PROCEDURE_DESCRIPTIONS_AND_CODES_LIST_E_PROCEDURES_03,
  einjury_01_col = INJURY_CAUSE_OF_INJURY_DESCRIPTION_AND_CODE_LIST_E_INJURY_01,
  einjury_03_col = INJURY_TRAUMA_CENTER_TRIAGE_CRITERIA_STEPS_1_AND_2_LIST_E_INJURY_03,
  einjury_04_col = INJURY_VEHICULAR_PEDESTRIAN_OR_OTHER_INJURY_RISK_FACTOR_TRIAGE_CRITERIA_STEPS_3_AND_4_LIST_E_INJURY_04,
  einjury_09_col = INJURY_HEIGHT_OF_FALL_IN_FEET_E_INJURY_09,
  confidence_interval = TRUE,
  method = "w",
  conf.level = 0.95,
  correct = TRUE,
  .by = c(INCIDENT_YEAR, `Region: Preparedness`)
) |>
  dplyr::mutate(
    `Region: Preparedness` = dplyr::if_else(
      is.na(`Region: Preparedness`),
      "Missing",
      `Region: Preparedness`
    )
  ) |>
  tidyr::complete(
    INCIDENT_YEAR,
    `Region: Preparedness`,
    measure,
    pop,
    fill = list(
      numerator = 0,
      denominator = 0,
      prop = NA_real_,
      prop_label = NA_character_,
      lower_ci = NA_real_,
      upper_ci = NA_real_
    )
  )

### results regions --------------------------------------------------------

#### regions ----
trauma_04_result_regions <- nemsqar::trauma_04(
  df = NULL,
  patient_scene_table = patient_scene_table,
  response_table = response_table,
  situation_table = situation_table,
  vitals_table = vitals_table,
  exam_table = exam_table,
  procedures_table = procedures_table,
  injury_table = injury_table,
  disposition_table = disposition_table,
  erecord_01_col = FACT_INCIDENT_PK,
  incident_date_col = INCIDENT_DATE,
  patient_DOB_col = PATIENT_DATE_OF_BIRTH_E_PATIENT_17,
  epatient_15_col = PATIENT_AGE_E_PATIENT_15,
  epatient_16_col = PATIENT_AGE_UNITS_E_PATIENT_16,
  esituation_02_col = SITUATION_POSSIBLE_INJURY_WITH_CODE_E_SITUATION_02,
  eresponse_05_col = RESPONSE_TYPE_OF_SERVICE_REQUESTED_WITH_CODE_E_RESPONSE_05,
  eresponse_10_col = RESPONSE_TYPE_OF_SCENE_DELAY_LIST_E_RESPONSE_10,
  transport_disposition_col = c(
    DISPOSITION_INCIDENT_PATIENT_DISPOSITION_WITH_CODE_3_4_E_DISPOSITION_12_3_5_IT_DISPOSITION_112,
    TRANSPORT_DISPOSITION_3_4_IT_DISPOSITION_102_3_5_E_DISPOSITION_30
  ),
  edisposition_02_col = DISPOSITION_DESTINATION_CODE_DELIVERED_TRANSFERRED_TO_E_DISPOSITION_02,
  trauma_center_facility_IDs = facilities$`Facility State ID`,
  evitals_06_col = VITALS_SYSTOLIC_BLOOD_PRESSURE_SBP_E_VITALS_06,
  evitals_10_col = VITALS_HEART_RATE_E_VITALS_10,
  evitals_12_col = VITALS_PULSE_OXIMETRY_E_VITALS_12,
  evitals_14_col = VITALS_RESPIRATORY_RATE_E_VITALS_14,
  evitals_15_col = VITALS_RESPIRATORY_EFFORT_E_VITALS_15,
  evitals_21_col = VITALS_GLASGOW_COMA_SCORE_GCS_MOTOR_E_VITALS_21,
  eexam_16_col = PATIENT_EXTREMITY_ASSESSMENT_FINDINGS_LIST_E_EXAM_16,
  eexam_20_col = PATIENT_NEUROLOGICAL_ASSESSMENT_FINDINGS_LIST_E_EXAM_20,
  eexam_23_col = PATIENT_LUNG_ASSESSMENT_FINDINGS_LIST_3_4_IT_EXAM_100_3_5_E_EXAM_23,
  eexam_25_col = PATIENT_CHEST_EXCLUSIVE_ASSESSMENT_FINDINGS_LIST_3_4_IT_EXAM_102_3_5_E_EXAM_25,
  eprocedures_03_col = PATIENT_ATTEMPTED_PROCEDURE_DESCRIPTIONS_AND_CODES_LIST_E_PROCEDURES_03,
  einjury_01_col = INJURY_CAUSE_OF_INJURY_DESCRIPTION_AND_CODE_LIST_E_INJURY_01,
  einjury_03_col = INJURY_TRAUMA_CENTER_TRIAGE_CRITERIA_STEPS_1_AND_2_LIST_E_INJURY_03,
  einjury_04_col = INJURY_VEHICULAR_PEDESTRIAN_OR_OTHER_INJURY_RISK_FACTOR_TRIAGE_CRITERIA_STEPS_3_AND_4_LIST_E_INJURY_04,
  einjury_09_col = INJURY_HEIGHT_OF_FALL_IN_FEET_E_INJURY_09,
  confidence_interval = TRUE,
  method = "w",
  conf.level = 0.95,
  correct = TRUE,
  .by = `Region: Preparedness`
) |>
  dplyr::mutate(
    `Region: Preparedness` = dplyr::if_else(
      is.na(`Region: Preparedness`),
      "Missing",
      `Region: Preparedness`
    )
  ) |>
  tidyr::complete(
    `Region: Preparedness`,
    measure,
    pop,
    fill = list(
      numerator = 0,
      denominator = 0,
      prop = NA_real_,
      prop_label = NA_character_,
      lower_ci = NA_real_,
      upper_ci = NA_real_
    )
  )

### results counties -------------------------------------------------------

#### counties ----
trauma_04_result_counties <- nemsqar::trauma_04(
  df = NULL,
  patient_scene_table = patient_scene_table |>
    dplyr::mutate(
      SCENE_INCIDENT_COUNTY_NAME_E_SCENE_21 = factor(
        SCENE_INCIDENT_COUNTY_NAME_E_SCENE_21
      )
    ),
  response_table = response_table,
  situation_table = situation_table,
  vitals_table = vitals_table,
  exam_table = exam_table,
  procedures_table = procedures_table,
  injury_table = injury_table,
  disposition_table = disposition_table,
  erecord_01_col = FACT_INCIDENT_PK,
  incident_date_col = INCIDENT_DATE,
  patient_DOB_col = PATIENT_DATE_OF_BIRTH_E_PATIENT_17,
  epatient_15_col = PATIENT_AGE_E_PATIENT_15,
  epatient_16_col = PATIENT_AGE_UNITS_E_PATIENT_16,
  esituation_02_col = SITUATION_POSSIBLE_INJURY_WITH_CODE_E_SITUATION_02,
  eresponse_05_col = RESPONSE_TYPE_OF_SERVICE_REQUESTED_WITH_CODE_E_RESPONSE_05,
  eresponse_10_col = RESPONSE_TYPE_OF_SCENE_DELAY_LIST_E_RESPONSE_10,
  transport_disposition_col = c(
    DISPOSITION_INCIDENT_PATIENT_DISPOSITION_WITH_CODE_3_4_E_DISPOSITION_12_3_5_IT_DISPOSITION_112,
    TRANSPORT_DISPOSITION_3_4_IT_DISPOSITION_102_3_5_E_DISPOSITION_30
  ),
  edisposition_02_col = DISPOSITION_DESTINATION_CODE_DELIVERED_TRANSFERRED_TO_E_DISPOSITION_02,
  trauma_center_facility_IDs = facilities$`Facility State ID`,
  evitals_06_col = VITALS_SYSTOLIC_BLOOD_PRESSURE_SBP_E_VITALS_06,
  evitals_10_col = VITALS_HEART_RATE_E_VITALS_10,
  evitals_12_col = VITALS_PULSE_OXIMETRY_E_VITALS_12,
  evitals_14_col = VITALS_RESPIRATORY_RATE_E_VITALS_14,
  evitals_15_col = VITALS_RESPIRATORY_EFFORT_E_VITALS_15,
  evitals_21_col = VITALS_GLASGOW_COMA_SCORE_GCS_MOTOR_E_VITALS_21,
  eexam_16_col = PATIENT_EXTREMITY_ASSESSMENT_FINDINGS_LIST_E_EXAM_16,
  eexam_20_col = PATIENT_NEUROLOGICAL_ASSESSMENT_FINDINGS_LIST_E_EXAM_20,
  eexam_23_col = PATIENT_LUNG_ASSESSMENT_FINDINGS_LIST_3_4_IT_EXAM_100_3_5_E_EXAM_23,
  eexam_25_col = PATIENT_CHEST_EXCLUSIVE_ASSESSMENT_FINDINGS_LIST_3_4_IT_EXAM_102_3_5_E_EXAM_25,
  eprocedures_03_col = PATIENT_ATTEMPTED_PROCEDURE_DESCRIPTIONS_AND_CODES_LIST_E_PROCEDURES_03,
  einjury_01_col = INJURY_CAUSE_OF_INJURY_DESCRIPTION_AND_CODE_LIST_E_INJURY_01,
  einjury_03_col = INJURY_TRAUMA_CENTER_TRIAGE_CRITERIA_STEPS_1_AND_2_LIST_E_INJURY_03,
  einjury_04_col = INJURY_VEHICULAR_PEDESTRIAN_OR_OTHER_INJURY_RISK_FACTOR_TRIAGE_CRITERIA_STEPS_3_AND_4_LIST_E_INJURY_04,
  einjury_09_col = INJURY_HEIGHT_OF_FALL_IN_FEET_E_INJURY_09,
  confidence_interval = TRUE,
  method = "w",
  conf.level = 0.95,
  correct = TRUE,
  .by = SCENE_INCIDENT_COUNTY_NAME_E_SCENE_21
) |>
  tidyr::complete(
    SCENE_INCIDENT_COUNTY_NAME_E_SCENE_21,
    measure,
    pop,
    fill = list(
      numerator = 0,
      denominator = 0,
      prop = NA_real_,
      prop_label = NA_character_,
      lower_ci = NA_real_,
      upper_ci = NA_real_
    )
  )

### results counties years -------------------------------------------------

#### counties years ----
trauma_04_result_counties_years <- nemsqar::trauma_04(
  df = NULL,
  patient_scene_table = patient_scene_table,
  response_table = response_table,
  situation_table = situation_table,
  vitals_table = vitals_table,
  exam_table = exam_table,
  procedures_table = procedures_table,
  injury_table = injury_table,
  disposition_table = disposition_table,
  erecord_01_col = FACT_INCIDENT_PK,
  incident_date_col = INCIDENT_DATE,
  patient_DOB_col = PATIENT_DATE_OF_BIRTH_E_PATIENT_17,
  epatient_15_col = PATIENT_AGE_E_PATIENT_15,
  epatient_16_col = PATIENT_AGE_UNITS_E_PATIENT_16,
  esituation_02_col = SITUATION_POSSIBLE_INJURY_WITH_CODE_E_SITUATION_02,
  eresponse_05_col = RESPONSE_TYPE_OF_SERVICE_REQUESTED_WITH_CODE_E_RESPONSE_05,
  eresponse_10_col = RESPONSE_TYPE_OF_SCENE_DELAY_LIST_E_RESPONSE_10,
  transport_disposition_col = c(
    DISPOSITION_INCIDENT_PATIENT_DISPOSITION_WITH_CODE_3_4_E_DISPOSITION_12_3_5_IT_DISPOSITION_112,
    TRANSPORT_DISPOSITION_3_4_IT_DISPOSITION_102_3_5_E_DISPOSITION_30
  ),
  edisposition_02_col = DISPOSITION_DESTINATION_CODE_DELIVERED_TRANSFERRED_TO_E_DISPOSITION_02,
  trauma_center_facility_IDs = facilities$`Facility State ID`,
  evitals_06_col = VITALS_SYSTOLIC_BLOOD_PRESSURE_SBP_E_VITALS_06,
  evitals_10_col = VITALS_HEART_RATE_E_VITALS_10,
  evitals_12_col = VITALS_PULSE_OXIMETRY_E_VITALS_12,
  evitals_14_col = VITALS_RESPIRATORY_RATE_E_VITALS_14,
  evitals_15_col = VITALS_RESPIRATORY_EFFORT_E_VITALS_15,
  evitals_21_col = VITALS_GLASGOW_COMA_SCORE_GCS_MOTOR_E_VITALS_21,
  eexam_16_col = PATIENT_EXTREMITY_ASSESSMENT_FINDINGS_LIST_E_EXAM_16,
  eexam_20_col = PATIENT_NEUROLOGICAL_ASSESSMENT_FINDINGS_LIST_E_EXAM_20,
  eexam_23_col = PATIENT_LUNG_ASSESSMENT_FINDINGS_LIST_3_4_IT_EXAM_100_3_5_E_EXAM_23,
  eexam_25_col = PATIENT_CHEST_EXCLUSIVE_ASSESSMENT_FINDINGS_LIST_3_4_IT_EXAM_102_3_5_E_EXAM_25,
  eprocedures_03_col = PATIENT_ATTEMPTED_PROCEDURE_DESCRIPTIONS_AND_CODES_LIST_E_PROCEDURES_03,
  einjury_01_col = INJURY_CAUSE_OF_INJURY_DESCRIPTION_AND_CODE_LIST_E_INJURY_01,
  einjury_03_col = INJURY_TRAUMA_CENTER_TRIAGE_CRITERIA_STEPS_1_AND_2_LIST_E_INJURY_03,
  einjury_04_col = INJURY_VEHICULAR_PEDESTRIAN_OR_OTHER_INJURY_RISK_FACTOR_TRIAGE_CRITERIA_STEPS_3_AND_4_LIST_E_INJURY_04,
  einjury_09_col = INJURY_HEIGHT_OF_FALL_IN_FEET_E_INJURY_09,
  confidence_interval = TRUE,
  method = "w",
  conf.level = 0.95,
  correct = TRUE,
  .by = c(INCIDENT_YEAR, SCENE_INCIDENT_COUNTY_NAME_E_SCENE_21)
) |>
  tidyr::complete(
    INCIDENT_YEAR,
    SCENE_INCIDENT_COUNTY_NAME_E_SCENE_21,
    measure,
    pop,
    fill = list(
      numerator = 0,
      denominator = 0,
      prop = NA_real_,
      prop_label = NA_character_,
      lower_ci = NA_real_,
      upper_ci = NA_real_
    )
  )

### results overall --------------------------------------------------------

#### overall ----
trauma_04_result_overall <- nemsqar::trauma_04(
  df = NULL,
  patient_scene_table = patient_scene_table,
  response_table = response_table,
  situation_table = situation_table,
  vitals_table = vitals_table,
  exam_table = exam_table,
  procedures_table = procedures_table,
  injury_table = injury_table,
  disposition_table = disposition_table,
  erecord_01_col = FACT_INCIDENT_PK,
  incident_date_col = INCIDENT_DATE,
  patient_DOB_col = PATIENT_DATE_OF_BIRTH_E_PATIENT_17,
  epatient_15_col = PATIENT_AGE_E_PATIENT_15,
  epatient_16_col = PATIENT_AGE_UNITS_E_PATIENT_16,
  esituation_02_col = SITUATION_POSSIBLE_INJURY_WITH_CODE_E_SITUATION_02,
  eresponse_05_col = RESPONSE_TYPE_OF_SERVICE_REQUESTED_WITH_CODE_E_RESPONSE_05,
  eresponse_10_col = RESPONSE_TYPE_OF_SCENE_DELAY_LIST_E_RESPONSE_10,
  transport_disposition_col = c(
    DISPOSITION_INCIDENT_PATIENT_DISPOSITION_WITH_CODE_3_4_E_DISPOSITION_12_3_5_IT_DISPOSITION_112,
    TRANSPORT_DISPOSITION_3_4_IT_DISPOSITION_102_3_5_E_DISPOSITION_30
  ),
  edisposition_02_col = DISPOSITION_DESTINATION_CODE_DELIVERED_TRANSFERRED_TO_E_DISPOSITION_02,
  trauma_center_facility_IDs = facilities$`Facility State ID`,
  evitals_06_col = VITALS_SYSTOLIC_BLOOD_PRESSURE_SBP_E_VITALS_06,
  evitals_10_col = VITALS_HEART_RATE_E_VITALS_10,
  evitals_12_col = VITALS_PULSE_OXIMETRY_E_VITALS_12,
  evitals_14_col = VITALS_RESPIRATORY_RATE_E_VITALS_14,
  evitals_15_col = VITALS_RESPIRATORY_EFFORT_E_VITALS_15,
  evitals_21_col = VITALS_GLASGOW_COMA_SCORE_GCS_MOTOR_E_VITALS_21,
  eexam_16_col = PATIENT_EXTREMITY_ASSESSMENT_FINDINGS_LIST_E_EXAM_16,
  eexam_20_col = PATIENT_NEUROLOGICAL_ASSESSMENT_FINDINGS_LIST_E_EXAM_20,
  eexam_23_col = PATIENT_LUNG_ASSESSMENT_FINDINGS_LIST_3_4_IT_EXAM_100_3_5_E_EXAM_23,
  eexam_25_col = PATIENT_CHEST_EXCLUSIVE_ASSESSMENT_FINDINGS_LIST_3_4_IT_EXAM_102_3_5_E_EXAM_25,
  eprocedures_03_col = PATIENT_ATTEMPTED_PROCEDURE_DESCRIPTIONS_AND_CODES_LIST_E_PROCEDURES_03,
  einjury_01_col = INJURY_CAUSE_OF_INJURY_DESCRIPTION_AND_CODE_LIST_E_INJURY_01,
  einjury_03_col = INJURY_TRAUMA_CENTER_TRIAGE_CRITERIA_STEPS_1_AND_2_LIST_E_INJURY_03,
  einjury_04_col = INJURY_VEHICULAR_PEDESTRIAN_OR_OTHER_INJURY_RISK_FACTOR_TRIAGE_CRITERIA_STEPS_3_AND_4_LIST_E_INJURY_04,
  einjury_09_col = INJURY_HEIGHT_OF_FALL_IN_FEET_E_INJURY_09,
  confidence_interval = TRUE,
  method = "w",
  conf.level = 0.95,
  correct = TRUE
)

### results services -------------------------------------------------------

#### services ----
trauma_04_result_services <- nemsqar::trauma_04(
  df = NULL,
  patient_scene_table = patient_scene_table,
  response_table = response_table,
  situation_table = situation_table,
  vitals_table = vitals_table,
  exam_table = exam_table,
  procedures_table = procedures_table,
  injury_table = injury_table,
  disposition_table = disposition_table,
  erecord_01_col = FACT_INCIDENT_PK,
  incident_date_col = INCIDENT_DATE,
  patient_DOB_col = PATIENT_DATE_OF_BIRTH_E_PATIENT_17,
  epatient_15_col = PATIENT_AGE_E_PATIENT_15,
  epatient_16_col = PATIENT_AGE_UNITS_E_PATIENT_16,
  esituation_02_col = SITUATION_POSSIBLE_INJURY_WITH_CODE_E_SITUATION_02,
  eresponse_05_col = RESPONSE_TYPE_OF_SERVICE_REQUESTED_WITH_CODE_E_RESPONSE_05,
  eresponse_10_col = RESPONSE_TYPE_OF_SCENE_DELAY_LIST_E_RESPONSE_10,
  transport_disposition_col = c(
    DISPOSITION_INCIDENT_PATIENT_DISPOSITION_WITH_CODE_3_4_E_DISPOSITION_12_3_5_IT_DISPOSITION_112,
    TRANSPORT_DISPOSITION_3_4_IT_DISPOSITION_102_3_5_E_DISPOSITION_30
  ),
  edisposition_02_col = DISPOSITION_DESTINATION_CODE_DELIVERED_TRANSFERRED_TO_E_DISPOSITION_02,
  trauma_center_facility_IDs = facilities$`Facility State ID`,
  evitals_06_col = VITALS_SYSTOLIC_BLOOD_PRESSURE_SBP_E_VITALS_06,
  evitals_10_col = VITALS_HEART_RATE_E_VITALS_10,
  evitals_12_col = VITALS_PULSE_OXIMETRY_E_VITALS_12,
  evitals_14_col = VITALS_RESPIRATORY_RATE_E_VITALS_14,
  evitals_15_col = VITALS_RESPIRATORY_EFFORT_E_VITALS_15,
  evitals_21_col = VITALS_GLASGOW_COMA_SCORE_GCS_MOTOR_E_VITALS_21,
  eexam_16_col = PATIENT_EXTREMITY_ASSESSMENT_FINDINGS_LIST_E_EXAM_16,
  eexam_20_col = PATIENT_NEUROLOGICAL_ASSESSMENT_FINDINGS_LIST_E_EXAM_20,
  eexam_23_col = PATIENT_LUNG_ASSESSMENT_FINDINGS_LIST_3_4_IT_EXAM_100_3_5_E_EXAM_23,
  eexam_25_col = PATIENT_CHEST_EXCLUSIVE_ASSESSMENT_FINDINGS_LIST_3_4_IT_EXAM_102_3_5_E_EXAM_25,
  eprocedures_03_col = PATIENT_ATTEMPTED_PROCEDURE_DESCRIPTIONS_AND_CODES_LIST_E_PROCEDURES_03,
  einjury_01_col = INJURY_CAUSE_OF_INJURY_DESCRIPTION_AND_CODE_LIST_E_INJURY_01,
  einjury_03_col = INJURY_TRAUMA_CENTER_TRIAGE_CRITERIA_STEPS_1_AND_2_LIST_E_INJURY_03,
  einjury_04_col = INJURY_VEHICULAR_PEDESTRIAN_OR_OTHER_INJURY_RISK_FACTOR_TRIAGE_CRITERIA_STEPS_3_AND_4_LIST_E_INJURY_04,
  einjury_09_col = INJURY_HEIGHT_OF_FALL_IN_FEET_E_INJURY_09,
  confidence_interval = TRUE,
  method = "w",
  conf.level = 0.95,
  correct = TRUE,
  .by = c(INCIDENT_YEAR, AGENCY_NAME_D_AGENCY_03)
) |>
  tidyr::complete(
    INCIDENT_YEAR,
    AGENCY_NAME_D_AGENCY_03,
    measure,
    pop,
    fill = list(
      numerator = 0,
      denominator = 0,
      prop = NA_real_,
      prop_label = NA_character_,
      lower_ci = NA_real_,
      upper_ci = NA_real_
    )
  )

# EXPORT =====================================================================

## population exports #########################################################

export_nemsqa_data(
  pattern = "trauma_04_pop",
  measure = "Trauma-04",
  folder = "population"
)

## results exports ############################################################

export_nemsqa_data(
  pattern = "trauma_04_result",
  measure = "Trauma-04",
  folder = "result"
)

## missingness exports ########################################################

export_nemsqa_data(
  pattern = "trauma_04_(?:missings|missingness)",
  measure = "Trauma-04",
  folder = "missings"
)
