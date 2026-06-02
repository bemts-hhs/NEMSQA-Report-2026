# IOWA NEMSQA REPORT AIRWAY-18 2026 ------------------------------------------

###_____________________________________________________________________________
# this script will contain all reporting calculations for Airway-18
# use nemsqa_report_prep_2025.R to get critical functions into memory
###_____________________________________________________________________________
# assume that nemsqa_report_prep_2025.R was already ran to load needed packages
# and project-specific custom functions in the project
###_____________________________________________________________________________

###___________________________________________________________________________
# Note on parallel processing - due to the heavy overhead involved with this
# function, we will only use nemsqar's built in grouping capabilities and will
# not leverage mori. mirai will only be used here for data ingestion.
###___________________________________________________________________________

# DATA -----------------------------------------------------------------------

# tables imported in alphabetical order
# tables do not need to be loaded again if already in memory

## airway tables ----------------------------------------------------------
# Utilize mirai for asynchronous loading
# automatically bind rows
airway_table <- load_nemsqa_parallel(
  table = "airway",
  years = 2021:2025,
  cores = 13
)

# share the arrest table
airway_table_s <- mori::share(airway_table)

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

# share the patient_scene_table
patient_scene_table_s <- mori::share(patient_scene_table)

## procedures tables ------------------------------------------------------
procedures_table <- load_nemsqa_parallel(
  table = "procedures",
  years = 2021:2025,
  cores = 13
)

# share the procedures table
procedures_table_s <- mori::share(procedures_table)

## response tables --------------------------------------------------------
response_table <- load_nemsqa_parallel(
  table = "response",
  years = 2021:2025,
  cores = 13
)

# share the response table
response_table_s <- mori::share(response_table)

## vitals tables ----------------------------------------------------------
vitals_table <- load_nemsqa_parallel(
  table = "vitals",
  years = 2021:2025,
  cores = 13
)

# share the vitals table
vitals_table_s <- mori::share(vitals_table)

# CALCULATIONS ---------------------------------------------------------------

## Airway-18 ==================================================================

# airway-18 populations ######################################################

## populations over all years 2021-2025 -----------------------------------

airway_18_pop <- nemsqar::airway_18_population(
  df = NULL,
  patient_scene_table = patient_scene_table,
  response_table = response_table,
  procedures_table = procedures_table,
  vitals_table = vitals_table,
  airway_table = airway_table,
  erecord_01_col = FACT_INCIDENT_PK,
  incident_date_col = INCIDENT_DATE,
  patient_DOB_col = PATIENT_DATE_OF_BIRTH_E_PATIENT_17,
  epatient_15_col = PATIENT_AGE_E_PATIENT_15,
  epatient_16_col = PATIENT_AGE_UNITS_E_PATIENT_16,
  eresponse_05_col = RESPONSE_TYPE_OF_SERVICE_REQUESTED_WITH_CODE_E_RESPONSE_05,
  eprocedures_01_col = PROCEDURE_PERFORMED_DATE_TIME_E_PROCEDURES_01,
  eprocedures_02_col = PROCEDURE_PERFORMED_PRIOR_TO_EMS_CARE_E_PROCEDURES_02,
  eprocedures_03_col = PROCEDURE_PERFORMED_DESCRIPTION_AND_CODE_E_PROCEDURES_03,
  eprocedures_06_col = PROCEDURE_SUCCESSFUL_E_PROCEDURES_06,
  eairway_02_col = AIRWAY_DEVICE_PLACEMENT_CONFIRMATION_DATE_TIME_E_AIRWAY_02,
  eairway_04_col = PATIENT_AIRWAY_DEVICE_PLACEMENT_CONFIRMED_METHOD_LIST_E_AIRWAY_04,
  evitals_01_col = VITALS_SIGNS_TAKEN_DATE_TIME_E_VITALS_01,
  evitals_16_col = VITALS_CARBON_DIOXIDE_CO2_E_VITALS_16
)

### population results for 2021-2025 ----
airway_18_pop_filter_process <- airway_18_pop$filter_process

### missingness results for 2021-2025 ----
airway_18_missings <- airway_18_pop$missingness

## get airway_18_population data for each year -------

### 2021 ----
airway_18_pop_2021 <- nemsqar::airway_18_population(
  df = NULL,
  patient_scene_table = patient_scene_table |>
    dplyr::filter(INCIDENT_YEAR == 2021),
  response_table = response_table |> dplyr::filter(INCIDENT_YEAR == 2021),
  procedures_table = procedures_table |> dplyr::filter(INCIDENT_YEAR == 2021),
  vitals_table = vitals_table |> dplyr::filter(INCIDENT_YEAR == 2021),
  airway_table = airway_table |> dplyr::filter(INCIDENT_YEAR == 2021),
  erecord_01_col = FACT_INCIDENT_PK,
  incident_date_col = INCIDENT_DATE,
  patient_DOB_col = PATIENT_DATE_OF_BIRTH_E_PATIENT_17,
  epatient_15_col = PATIENT_AGE_E_PATIENT_15,
  epatient_16_col = PATIENT_AGE_UNITS_E_PATIENT_16,
  eresponse_05_col = RESPONSE_TYPE_OF_SERVICE_REQUESTED_WITH_CODE_E_RESPONSE_05,
  eprocedures_01_col = PROCEDURE_PERFORMED_DATE_TIME_E_PROCEDURES_01,
  eprocedures_02_col = PROCEDURE_PERFORMED_PRIOR_TO_EMS_CARE_E_PROCEDURES_02,
  eprocedures_03_col = PROCEDURE_PERFORMED_DESCRIPTION_AND_CODE_E_PROCEDURES_03,
  eprocedures_06_col = PROCEDURE_SUCCESSFUL_E_PROCEDURES_06,
  eairway_02_col = AIRWAY_DEVICE_PLACEMENT_CONFIRMATION_DATE_TIME_E_AIRWAY_02,
  eairway_04_col = PATIENT_AIRWAY_DEVICE_PLACEMENT_CONFIRMED_METHOD_LIST_E_AIRWAY_04,
  evitals_01_col = VITALS_SIGNS_TAKEN_DATE_TIME_E_VITALS_01,
  evitals_16_col = VITALS_CARBON_DIOXIDE_CO2_E_VITALS_16
)

### population results 2021 ----
airway_18_pop_filter_process_2021 <- airway_18_pop_2021$filter_process |>
  dplyr::mutate(YEAR = 2021)

### missingness results 2021 ----
airway_18_pop_missingness_2021 <- airway_18_pop_2021$missingness |>
  dplyr::mutate(YEAR = 2021)

## 2022 ----
airway_18_pop_2022 <- nemsqar::airway_18_population(
  df = NULL,
  patient_scene_table = patient_scene_table |>
    dplyr::filter(INCIDENT_YEAR == 2022),
  response_table = response_table |> dplyr::filter(INCIDENT_YEAR == 2022),
  procedures_table = procedures_table |> dplyr::filter(INCIDENT_YEAR == 2022),
  vitals_table = vitals_table |> dplyr::filter(INCIDENT_YEAR == 2022),
  airway_table = airway_table |> dplyr::filter(INCIDENT_YEAR == 2022),
  erecord_01_col = FACT_INCIDENT_PK,
  incident_date_col = INCIDENT_DATE,
  patient_DOB_col = PATIENT_DATE_OF_BIRTH_E_PATIENT_17,
  epatient_15_col = PATIENT_AGE_E_PATIENT_15,
  epatient_16_col = PATIENT_AGE_UNITS_E_PATIENT_16,
  eresponse_05_col = RESPONSE_TYPE_OF_SERVICE_REQUESTED_WITH_CODE_E_RESPONSE_05,
  eprocedures_01_col = PROCEDURE_PERFORMED_DATE_TIME_E_PROCEDURES_01,
  eprocedures_02_col = PROCEDURE_PERFORMED_PRIOR_TO_EMS_CARE_E_PROCEDURES_02,
  eprocedures_03_col = PROCEDURE_PERFORMED_DESCRIPTION_AND_CODE_E_PROCEDURES_03,
  eprocedures_06_col = PROCEDURE_SUCCESSFUL_E_PROCEDURES_06,
  eairway_02_col = AIRWAY_DEVICE_PLACEMENT_CONFIRMATION_DATE_TIME_E_AIRWAY_02,
  eairway_04_col = PATIENT_AIRWAY_DEVICE_PLACEMENT_CONFIRMED_METHOD_LIST_E_AIRWAY_04,
  evitals_01_col = VITALS_SIGNS_TAKEN_DATE_TIME_E_VITALS_01,
  evitals_16_col = VITALS_CARBON_DIOXIDE_CO2_E_VITALS_16
)

### population results 2022 ----
airway_18_pop_filter_process_2022 <- airway_18_pop_2022$filter_process |>
  dplyr::mutate(YEAR = 2022)

### missingness results 2022 ----
airway_18_pop_missingness_2022 <- airway_18_pop_2022$missingness |>
  dplyr::mutate(YEAR = 2022)

## 2023 ----
airway_18_pop_2023 <- nemsqar::airway_18_population(
  df = NULL,
  patient_scene_table = patient_scene_table |>
    dplyr::filter(INCIDENT_YEAR == 2023),
  response_table = response_table |> dplyr::filter(INCIDENT_YEAR == 2023),
  procedures_table = procedures_table |> dplyr::filter(INCIDENT_YEAR == 2023),
  vitals_table = vitals_table |> dplyr::filter(INCIDENT_YEAR == 2023),
  airway_table = airway_table |> dplyr::filter(INCIDENT_YEAR == 2023),
  erecord_01_col = FACT_INCIDENT_PK,
  incident_date_col = INCIDENT_DATE,
  patient_DOB_col = PATIENT_DATE_OF_BIRTH_E_PATIENT_17,
  epatient_15_col = PATIENT_AGE_E_PATIENT_15,
  epatient_16_col = PATIENT_AGE_UNITS_E_PATIENT_16,
  eresponse_05_col = RESPONSE_TYPE_OF_SERVICE_REQUESTED_WITH_CODE_E_RESPONSE_05,
  eprocedures_01_col = PROCEDURE_PERFORMED_DATE_TIME_E_PROCEDURES_01,
  eprocedures_02_col = PROCEDURE_PERFORMED_PRIOR_TO_EMS_CARE_E_PROCEDURES_02,
  eprocedures_03_col = PROCEDURE_PERFORMED_DESCRIPTION_AND_CODE_E_PROCEDURES_03,
  eprocedures_06_col = PROCEDURE_SUCCESSFUL_E_PROCEDURES_06,
  eairway_02_col = AIRWAY_DEVICE_PLACEMENT_CONFIRMATION_DATE_TIME_E_AIRWAY_02,
  eairway_04_col = PATIENT_AIRWAY_DEVICE_PLACEMENT_CONFIRMED_METHOD_LIST_E_AIRWAY_04,
  evitals_01_col = VITALS_SIGNS_TAKEN_DATE_TIME_E_VITALS_01,
  evitals_16_col = VITALS_CARBON_DIOXIDE_CO2_E_VITALS_16
)

### population results 2023 ----
airway_18_pop_filter_process_2023 <- airway_18_pop_2023$filter_process |>
  dplyr::mutate(YEAR = 2023)

### missingness results 2023 ----
airway_18_pop_missingness_2023 <- airway_18_pop_2023$missingness |>
  dplyr::mutate(YEAR = 2023)

## 2024 ----
airway_18_pop_2024 <- nemsqar::airway_18_population(
  df = NULL,
  patient_scene_table = patient_scene_table |>
    dplyr::filter(INCIDENT_YEAR == 2024),
  response_table = response_table |> dplyr::filter(INCIDENT_YEAR == 2024),
  procedures_table = procedures_table |> dplyr::filter(INCIDENT_YEAR == 2024),
  vitals_table = vitals_table |> dplyr::filter(INCIDENT_YEAR == 2024),
  airway_table = airway_table |> dplyr::filter(INCIDENT_YEAR == 2024),
  erecord_01_col = FACT_INCIDENT_PK,
  incident_date_col = INCIDENT_DATE,
  patient_DOB_col = PATIENT_DATE_OF_BIRTH_E_PATIENT_17,
  epatient_15_col = PATIENT_AGE_E_PATIENT_15,
  epatient_16_col = PATIENT_AGE_UNITS_E_PATIENT_16,
  eresponse_05_col = RESPONSE_TYPE_OF_SERVICE_REQUESTED_WITH_CODE_E_RESPONSE_05,
  eprocedures_01_col = PROCEDURE_PERFORMED_DATE_TIME_E_PROCEDURES_01,
  eprocedures_02_col = PROCEDURE_PERFORMED_PRIOR_TO_EMS_CARE_E_PROCEDURES_02,
  eprocedures_03_col = PROCEDURE_PERFORMED_DESCRIPTION_AND_CODE_E_PROCEDURES_03,
  eprocedures_06_col = PROCEDURE_SUCCESSFUL_E_PROCEDURES_06,
  eairway_02_col = AIRWAY_DEVICE_PLACEMENT_CONFIRMATION_DATE_TIME_E_AIRWAY_02,
  eairway_04_col = PATIENT_AIRWAY_DEVICE_PLACEMENT_CONFIRMED_METHOD_LIST_E_AIRWAY_04,
  evitals_01_col = VITALS_SIGNS_TAKEN_DATE_TIME_E_VITALS_01,
  evitals_16_col = VITALS_CARBON_DIOXIDE_CO2_E_VITALS_16
)

### population results 2024 ----
airway_18_pop_filter_process_2024 <- airway_18_pop_2024$filter_process |>
  dplyr::mutate(YEAR = 2024)

### missingness results 2024 ----
airway_18_pop_missingness_2024 <- airway_18_pop_2024$missingness |>
  dplyr::mutate(YEAR = 2024)

## 2025 ----
airway_18_pop_2025 <- nemsqar::airway_18_population(
  df = NULL,
  patient_scene_table = patient_scene_table |>
    dplyr::filter(INCIDENT_YEAR == 2025),
  response_table = response_table |> dplyr::filter(INCIDENT_YEAR == 2025),
  procedures_table = procedures_table |> dplyr::filter(INCIDENT_YEAR == 2025),
  vitals_table = vitals_table |> dplyr::filter(INCIDENT_YEAR == 2025),
  airway_table = airway_table |> dplyr::filter(INCIDENT_YEAR == 2025),
  erecord_01_col = FACT_INCIDENT_PK,
  incident_date_col = INCIDENT_DATE,
  patient_DOB_col = PATIENT_DATE_OF_BIRTH_E_PATIENT_17,
  epatient_15_col = PATIENT_AGE_E_PATIENT_15,
  epatient_16_col = PATIENT_AGE_UNITS_E_PATIENT_16,
  eresponse_05_col = RESPONSE_TYPE_OF_SERVICE_REQUESTED_WITH_CODE_E_RESPONSE_05,
  eprocedures_01_col = PROCEDURE_PERFORMED_DATE_TIME_E_PROCEDURES_01,
  eprocedures_02_col = PROCEDURE_PERFORMED_PRIOR_TO_EMS_CARE_E_PROCEDURES_02,
  eprocedures_03_col = PROCEDURE_PERFORMED_DESCRIPTION_AND_CODE_E_PROCEDURES_03,
  eprocedures_06_col = PROCEDURE_SUCCESSFUL_E_PROCEDURES_06,
  eairway_02_col = AIRWAY_DEVICE_PLACEMENT_CONFIRMATION_DATE_TIME_E_AIRWAY_02,
  eairway_04_col = PATIENT_AIRWAY_DEVICE_PLACEMENT_CONFIRMED_METHOD_LIST_E_AIRWAY_04,
  evitals_01_col = VITALS_SIGNS_TAKEN_DATE_TIME_E_VITALS_01,
  evitals_16_col = VITALS_CARBON_DIOXIDE_CO2_E_VITALS_16
)

### population results 2025 ----
airway_18_pop_filter_process_2025 <- airway_18_pop_2025$filter_process |>
  dplyr::mutate(YEAR = 2025)

### missingness results 2025 ----
airway_18_pop_missingness_2025 <- airway_18_pop_2025$missingness |>
  dplyr::mutate(YEAR = 2025)

### airway-18 populations over the years ----
airway_18_pop_years <- dplyr::bind_rows(
  airway_18_pop_filter_process_2021,
  airway_18_pop_filter_process_2022,
  airway_18_pop_filter_process_2023,
  airway_18_pop_filter_process_2024,
  airway_18_pop_filter_process_2025
)

### airway-18 populations over the years ----
airway_18_missingness_years <- dplyr::bind_rows(
  airway_18_pop_missingness_2021,
  airway_18_pop_missingness_2022,
  airway_18_pop_missingness_2023,
  airway_18_pop_missingness_2024,
  airway_18_pop_missingness_2025
)

### plot population trends over time ----
airway_18_pop_years |>
  plot_nemsqa_pops(
    type = "col",
    wrap_width = 25,
    plot_title = "Airway-18"
  )

# airway-18 results ##########################################################

## results years ----------------------------------------------------------

### year ----
airway_18_result_year <- nemsqar::airway_18(
  df = NULL,
  patient_scene_table = patient_scene_table,
  response_table = response_table,
  procedures_table = procedures_table,
  vitals_table = vitals_table,
  airway_table = airway_table,
  erecord_01_col = FACT_INCIDENT_PK,
  incident_date_col = INCIDENT_DATE,
  patient_DOB_col = PATIENT_DATE_OF_BIRTH_E_PATIENT_17,
  epatient_15_col = PATIENT_AGE_E_PATIENT_15,
  epatient_16_col = PATIENT_AGE_UNITS_E_PATIENT_16,
  eresponse_05_col = RESPONSE_TYPE_OF_SERVICE_REQUESTED_WITH_CODE_E_RESPONSE_05,
  eprocedures_01_col = PROCEDURE_PERFORMED_DATE_TIME_E_PROCEDURES_01,
  eprocedures_02_col = PROCEDURE_PERFORMED_PRIOR_TO_EMS_CARE_E_PROCEDURES_02,
  eprocedures_03_col = PROCEDURE_PERFORMED_DESCRIPTION_AND_CODE_E_PROCEDURES_03,
  eprocedures_06_col = PROCEDURE_SUCCESSFUL_E_PROCEDURES_06,
  eairway_02_col = AIRWAY_DEVICE_PLACEMENT_CONFIRMATION_DATE_TIME_E_AIRWAY_02,
  eairway_04_col = PATIENT_AIRWAY_DEVICE_PLACEMENT_CONFIRMED_METHOD_LIST_E_AIRWAY_04,
  evitals_01_col = VITALS_SIGNS_TAKEN_DATE_TIME_E_VITALS_01,
  evitals_16_col = VITALS_CARBON_DIOXIDE_CO2_E_VITALS_16,
  confidence_interval = TRUE,
  method = "w",
  conf.level = 0.95,
  correct = TRUE,
  .by = INCIDENT_YEAR
)


## results regions and years ----------------------------------------------

### regions and years ----
airway_18_result_regions_years <- nemsqar::airway_18(
  df = NULL,
  patient_scene_table = patient_scene_table,
  response_table = response_table,
  procedures_table = procedures_table,
  vitals_table = vitals_table,
  airway_table = airway_table,
  erecord_01_col = FACT_INCIDENT_PK,
  incident_date_col = INCIDENT_DATE,
  patient_DOB_col = PATIENT_DATE_OF_BIRTH_E_PATIENT_17,
  epatient_15_col = PATIENT_AGE_E_PATIENT_15,
  epatient_16_col = PATIENT_AGE_UNITS_E_PATIENT_16,
  eresponse_05_col = RESPONSE_TYPE_OF_SERVICE_REQUESTED_WITH_CODE_E_RESPONSE_05,
  eprocedures_01_col = PROCEDURE_PERFORMED_DATE_TIME_E_PROCEDURES_01,
  eprocedures_02_col = PROCEDURE_PERFORMED_PRIOR_TO_EMS_CARE_E_PROCEDURES_02,
  eprocedures_03_col = PROCEDURE_PERFORMED_DESCRIPTION_AND_CODE_E_PROCEDURES_03,
  eprocedures_06_col = PROCEDURE_SUCCESSFUL_E_PROCEDURES_06,
  eairway_02_col = AIRWAY_DEVICE_PLACEMENT_CONFIRMATION_DATE_TIME_E_AIRWAY_02,
  eairway_04_col = PATIENT_AIRWAY_DEVICE_PLACEMENT_CONFIRMED_METHOD_LIST_E_AIRWAY_04,
  evitals_01_col = VITALS_SIGNS_TAKEN_DATE_TIME_E_VITALS_01,
  evitals_16_col = VITALS_CARBON_DIOXIDE_CO2_E_VITALS_16,
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


## results regions --------------------------------------------------------

### regions ----
airway_18_result_regions <- nemsqar::airway_18(
  df = NULL,
  patient_scene_table = patient_scene_table,
  response_table = response_table,
  procedures_table = procedures_table,
  vitals_table = vitals_table,
  airway_table = airway_table,
  erecord_01_col = FACT_INCIDENT_PK,
  incident_date_col = INCIDENT_DATE,
  patient_DOB_col = PATIENT_DATE_OF_BIRTH_E_PATIENT_17,
  epatient_15_col = PATIENT_AGE_E_PATIENT_15,
  epatient_16_col = PATIENT_AGE_UNITS_E_PATIENT_16,
  eresponse_05_col = RESPONSE_TYPE_OF_SERVICE_REQUESTED_WITH_CODE_E_RESPONSE_05,
  eprocedures_01_col = PROCEDURE_PERFORMED_DATE_TIME_E_PROCEDURES_01,
  eprocedures_02_col = PROCEDURE_PERFORMED_PRIOR_TO_EMS_CARE_E_PROCEDURES_02,
  eprocedures_03_col = PROCEDURE_PERFORMED_DESCRIPTION_AND_CODE_E_PROCEDURES_03,
  eprocedures_06_col = PROCEDURE_SUCCESSFUL_E_PROCEDURES_06,
  eairway_02_col = AIRWAY_DEVICE_PLACEMENT_CONFIRMATION_DATE_TIME_E_AIRWAY_02,
  eairway_04_col = PATIENT_AIRWAY_DEVICE_PLACEMENT_CONFIRMED_METHOD_LIST_E_AIRWAY_04,
  evitals_01_col = VITALS_SIGNS_TAKEN_DATE_TIME_E_VITALS_01,
  evitals_16_col = VITALS_CARBON_DIOXIDE_CO2_E_VITALS_16,
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


## results counties -------------------------------------------------------

### counties ----
airway_18_result_counties <- nemsqar::airway_18(
  df = NULL,
  patient_scene_table = patient_scene_table,
  response_table = response_table,
  procedures_table = procedures_table,
  vitals_table = vitals_table,
  airway_table = airway_table,
  erecord_01_col = FACT_INCIDENT_PK,
  incident_date_col = INCIDENT_DATE,
  patient_DOB_col = PATIENT_DATE_OF_BIRTH_E_PATIENT_17,
  epatient_15_col = PATIENT_AGE_E_PATIENT_15,
  epatient_16_col = PATIENT_AGE_UNITS_E_PATIENT_16,
  eresponse_05_col = RESPONSE_TYPE_OF_SERVICE_REQUESTED_WITH_CODE_E_RESPONSE_05,
  eprocedures_01_col = PROCEDURE_PERFORMED_DATE_TIME_E_PROCEDURES_01,
  eprocedures_02_col = PROCEDURE_PERFORMED_PRIOR_TO_EMS_CARE_E_PROCEDURES_02,
  eprocedures_03_col = PROCEDURE_PERFORMED_DESCRIPTION_AND_CODE_E_PROCEDURES_03,
  eprocedures_06_col = PROCEDURE_SUCCESSFUL_E_PROCEDURES_06,
  eairway_02_col = AIRWAY_DEVICE_PLACEMENT_CONFIRMATION_DATE_TIME_E_AIRWAY_02,
  eairway_04_col = PATIENT_AIRWAY_DEVICE_PLACEMENT_CONFIRMED_METHOD_LIST_E_AIRWAY_04,
  evitals_01_col = VITALS_SIGNS_TAKEN_DATE_TIME_E_VITALS_01,
  evitals_16_col = VITALS_CARBON_DIOXIDE_CO2_E_VITALS_16,
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


## results counties years -------------------------------------------------

### counties ----
airway_18_result_counties_years <- nemsqar::airway_18(
  df = NULL,
  patient_scene_table = patient_scene_table,
  response_table = response_table,
  procedures_table = procedures_table,
  vitals_table = vitals_table,
  airway_table = airway_table,
  erecord_01_col = FACT_INCIDENT_PK,
  incident_date_col = INCIDENT_DATE,
  patient_DOB_col = PATIENT_DATE_OF_BIRTH_E_PATIENT_17,
  epatient_15_col = PATIENT_AGE_E_PATIENT_15,
  epatient_16_col = PATIENT_AGE_UNITS_E_PATIENT_16,
  eresponse_05_col = RESPONSE_TYPE_OF_SERVICE_REQUESTED_WITH_CODE_E_RESPONSE_05,
  eprocedures_01_col = PROCEDURE_PERFORMED_DATE_TIME_E_PROCEDURES_01,
  eprocedures_02_col = PROCEDURE_PERFORMED_PRIOR_TO_EMS_CARE_E_PROCEDURES_02,
  eprocedures_03_col = PROCEDURE_PERFORMED_DESCRIPTION_AND_CODE_E_PROCEDURES_03,
  eprocedures_06_col = PROCEDURE_SUCCESSFUL_E_PROCEDURES_06,
  eairway_02_col = AIRWAY_DEVICE_PLACEMENT_CONFIRMATION_DATE_TIME_E_AIRWAY_02,
  eairway_04_col = PATIENT_AIRWAY_DEVICE_PLACEMENT_CONFIRMED_METHOD_LIST_E_AIRWAY_04,
  evitals_01_col = VITALS_SIGNS_TAKEN_DATE_TIME_E_VITALS_01,
  evitals_16_col = VITALS_CARBON_DIOXIDE_CO2_E_VITALS_16,
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


## results overall --------------------------------------------------------

### overall ----
airway_18_result_overall <- nemsqar::airway_18(
  df = NULL,
  patient_scene_table = patient_scene_table,
  response_table = response_table,
  procedures_table = procedures_table,
  vitals_table = vitals_table,
  airway_table = airway_table,
  erecord_01_col = FACT_INCIDENT_PK,
  incident_date_col = INCIDENT_DATE,
  patient_DOB_col = PATIENT_DATE_OF_BIRTH_E_PATIENT_17,
  epatient_15_col = PATIENT_AGE_E_PATIENT_15,
  epatient_16_col = PATIENT_AGE_UNITS_E_PATIENT_16,
  eresponse_05_col = RESPONSE_TYPE_OF_SERVICE_REQUESTED_WITH_CODE_E_RESPONSE_05,
  eprocedures_01_col = PROCEDURE_PERFORMED_DATE_TIME_E_PROCEDURES_01,
  eprocedures_02_col = PROCEDURE_PERFORMED_PRIOR_TO_EMS_CARE_E_PROCEDURES_02,
  eprocedures_03_col = PROCEDURE_PERFORMED_DESCRIPTION_AND_CODE_E_PROCEDURES_03,
  eprocedures_06_col = PROCEDURE_SUCCESSFUL_E_PROCEDURES_06,
  eairway_02_col = AIRWAY_DEVICE_PLACEMENT_CONFIRMATION_DATE_TIME_E_AIRWAY_02,
  eairway_04_col = PATIENT_AIRWAY_DEVICE_PLACEMENT_CONFIRMED_METHOD_LIST_E_AIRWAY_04,
  evitals_01_col = VITALS_SIGNS_TAKEN_DATE_TIME_E_VITALS_01,
  evitals_16_col = VITALS_CARBON_DIOXIDE_CO2_E_VITALS_16,
  confidence_interval = TRUE,
  method = "w",
  conf.level = 0.95,
  correct = TRUE
)

## results services -------------------------------------------------------

### services ----
airway_18_result_services <- nemsqar::airway_18(
  df = NULL,
  patient_scene_table = patient_scene_table,
  response_table = response_table,
  procedures_table = procedures_table,
  vitals_table = vitals_table,
  airway_table = airway_table,
  erecord_01_col = FACT_INCIDENT_PK,
  incident_date_col = INCIDENT_DATE,
  patient_DOB_col = PATIENT_DATE_OF_BIRTH_E_PATIENT_17,
  epatient_15_col = PATIENT_AGE_E_PATIENT_15,
  epatient_16_col = PATIENT_AGE_UNITS_E_PATIENT_16,
  eresponse_05_col = RESPONSE_TYPE_OF_SERVICE_REQUESTED_WITH_CODE_E_RESPONSE_05,
  eprocedures_01_col = PROCEDURE_PERFORMED_DATE_TIME_E_PROCEDURES_01,
  eprocedures_02_col = PROCEDURE_PERFORMED_PRIOR_TO_EMS_CARE_E_PROCEDURES_02,
  eprocedures_03_col = PROCEDURE_PERFORMED_DESCRIPTION_AND_CODE_E_PROCEDURES_03,
  eprocedures_06_col = PROCEDURE_SUCCESSFUL_E_PROCEDURES_06,
  eairway_02_col = AIRWAY_DEVICE_PLACEMENT_CONFIRMATION_DATE_TIME_E_AIRWAY_02,
  eairway_04_col = PATIENT_AIRWAY_DEVICE_PLACEMENT_CONFIRMED_METHOD_LIST_E_AIRWAY_04,
  evitals_01_col = VITALS_SIGNS_TAKEN_DATE_TIME_E_VITALS_01,
  evitals_16_col = VITALS_CARBON_DIOXIDE_CO2_E_VITALS_16,
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

# EXPORT -----------------------------------------------------------------

## population exports #########################################################

export_nemsqa_data(
  pattern = "(airway_18_pop_filter_process$|airway_18_pop_years)",
  measure = "Airway-18",
  folder = "population"
)

## results exports ############################################################

export_nemsqa_data(
  pattern = "airway_18_result",
  measure = "Airway-18",
  folder = "result"
)

## missingness exports ########################################################

export_nemsqa_data(
  pattern = "airway_18_missingness|airway_18_missings",
  measure = "Airway-18",
  folder = "missings"
)
