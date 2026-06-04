# IOWA NEMSQA REPORT PEDIATRICS-03B 2026 -------------------------------------

###_____________________________________________________________________________
# this script will contain all reporting calculations for Pediatrics-03b
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
# In this specific measure, we will only use mirai to ingest the tables, but
# then run the functions as usual without parallel processing given that the
# exam tables are too large, and as of mori 0.2.0, it does not seem to handle
# sharing dataframes as large as 99 million rows or so.
###___________________________________________________________________________

# DATA -----------------------------------------------------------------------

# tables imported in alphabetical order
# tables do not need to be loaded again if already in memory

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

## response tables --------------------------------------------------------
response_table <- load_nemsqa_parallel(
  table = "response",
  years = 2021:2025,
  cores = 13
)

# CALCULATIONS ---------------------------------------------------------------

## Pediatrics-03b =============================================================

## pediatrics-03b populations #################################################

### populations over all years 2021-2025 -----------------------------------

pediatrics_03b_pop <- nemsqar::pediatrics_03b_population(
  df = NULL,
  patient_scene_table = patient_scene_table,
  response_table = response_table,
  exam_table = exam_table,
  medications_table = medications_table,
  erecord_01_col = FACT_INCIDENT_PK,
  incident_date_col = INCIDENT_DATE,
  patient_DOB_col = PATIENT_DATE_OF_BIRTH_E_PATIENT_17,
  epatient_15_col = PATIENT_AGE_E_PATIENT_15,
  epatient_16_col = PATIENT_AGE_UNITS_E_PATIENT_16,
  eresponse_05_col = RESPONSE_TYPE_OF_SERVICE_REQUESTED_WITH_CODE_E_RESPONSE_05,
  eexam_01_col = PATIENT_WEIGHT_IN_KILOGRAMS_E_EXAM_01,
  eexam_02_col = PATIENT_LENGTH_BASED_COLOR_E_EXAM_02,
  emedications_03_col = MEDICATION_GIVEN_OR_ADMINISTERED_DESCRIPTION_AND_RXCUI_CODE_E_MEDICATIONS_03,
  emedications_04_col = MEDICATION_ADMINISTERED_ROUTE_E_MEDICATIONS_04
)

#### population results for 2021-2025
pediatrics_03b_pop_filter_process <- pediatrics_03b_pop$filter_process

#### population missingness results for 2021-2025
pediatrics_03b_missings <- pediatrics_03b_pop$missingness


### get pediatrics-03b population data for each year -----------------------

#### 2021 -------------------------------------------------------------------

pediatrics_03b_pop_2021 <- nemsqar::pediatrics_03b_population(
  df = NULL,
  patient_scene_table = patient_scene_table |>
    dplyr::filter(INCIDENT_YEAR == 2021),
  response_table = response_table |> dplyr::filter(INCIDENT_YEAR == 2021),
  exam_table = exam_table |> dplyr::filter(INCIDENT_YEAR == 2021),
  medications_table = medications_table |> dplyr::filter(INCIDENT_YEAR == 2021),
  erecord_01_col = FACT_INCIDENT_PK,
  incident_date_col = INCIDENT_DATE,
  patient_DOB_col = PATIENT_DATE_OF_BIRTH_E_PATIENT_17,
  epatient_15_col = PATIENT_AGE_E_PATIENT_15,
  epatient_16_col = PATIENT_AGE_UNITS_E_PATIENT_16,
  eresponse_05_col = RESPONSE_TYPE_OF_SERVICE_REQUESTED_WITH_CODE_E_RESPONSE_05,
  eexam_01_col = PATIENT_WEIGHT_IN_KILOGRAMS_E_EXAM_01,
  eexam_02_col = PATIENT_LENGTH_BASED_COLOR_E_EXAM_02,
  emedications_03_col = MEDICATION_GIVEN_OR_ADMINISTERED_DESCRIPTION_AND_RXCUI_CODE_E_MEDICATIONS_03,
  emedications_04_col = MEDICATION_ADMINISTERED_ROUTE_E_MEDICATIONS_04
)

##### population results 2021 ----
pediatrics_03b_pop_filter_process_2021 <- pediatrics_03b_pop_2021$filter_process |>
  dplyr::mutate(YEAR = 2021)

##### population missingness 2021 ----
pediatrics_03b_missingness_2021 <- pediatrics_03b_pop_2021$missingness |>
  dplyr::mutate(YEAR = 2021)


#### 2022 -------------------------------------------------------------------

pediatrics_03b_pop_2022 <- nemsqar::pediatrics_03b_population(
  df = NULL,
  patient_scene_table = patient_scene_table |>
    dplyr::filter(INCIDENT_YEAR == 2022),
  response_table = response_table |> dplyr::filter(INCIDENT_YEAR == 2022),
  exam_table = exam_table |> dplyr::filter(INCIDENT_YEAR == 2022),
  medications_table = medications_table |> dplyr::filter(INCIDENT_YEAR == 2022),
  erecord_01_col = FACT_INCIDENT_PK,
  incident_date_col = INCIDENT_DATE,
  patient_DOB_col = PATIENT_DATE_OF_BIRTH_E_PATIENT_17,
  epatient_15_col = PATIENT_AGE_E_PATIENT_15,
  epatient_16_col = PATIENT_AGE_UNITS_E_PATIENT_16,
  eresponse_05_col = RESPONSE_TYPE_OF_SERVICE_REQUESTED_WITH_CODE_E_RESPONSE_05,
  eexam_01_col = PATIENT_WEIGHT_IN_KILOGRAMS_E_EXAM_01,
  eexam_02_col = PATIENT_LENGTH_BASED_COLOR_E_EXAM_02,
  emedications_03_col = MEDICATION_GIVEN_OR_ADMINISTERED_DESCRIPTION_AND_RXCUI_CODE_E_MEDICATIONS_03,
  emedications_04_col = MEDICATION_ADMINISTERED_ROUTE_E_MEDICATIONS_04
)

##### population results 2022 ----
pediatrics_03b_pop_filter_process_2022 <- pediatrics_03b_pop_2022$filter_process |>
  dplyr::mutate(YEAR = 2022)

##### population missingness 2022 ----
pediatrics_03b_missingness_2022 <- pediatrics_03b_pop_2022$missingness |>
  dplyr::mutate(YEAR = 2022)


#### 2023 -------------------------------------------------------------------

pediatrics_03b_pop_2023 <- nemsqar::pediatrics_03b_population(
  df = NULL,
  patient_scene_table = patient_scene_table |>
    dplyr::filter(INCIDENT_YEAR == 2023),
  response_table = response_table |> dplyr::filter(INCIDENT_YEAR == 2023),
  exam_table = exam_table |> dplyr::filter(INCIDENT_YEAR == 2023),
  medications_table = medications_table |> dplyr::filter(INCIDENT_YEAR == 2023),
  erecord_01_col = FACT_INCIDENT_PK,
  incident_date_col = INCIDENT_DATE,
  patient_DOB_col = PATIENT_DATE_OF_BIRTH_E_PATIENT_17,
  epatient_15_col = PATIENT_AGE_E_PATIENT_15,
  epatient_16_col = PATIENT_AGE_UNITS_E_PATIENT_16,
  eresponse_05_col = RESPONSE_TYPE_OF_SERVICE_REQUESTED_WITH_CODE_E_RESPONSE_05,
  eexam_01_col = PATIENT_WEIGHT_IN_KILOGRAMS_E_EXAM_01,
  eexam_02_col = PATIENT_LENGTH_BASED_COLOR_E_EXAM_02,
  emedications_03_col = MEDICATION_GIVEN_OR_ADMINISTERED_DESCRIPTION_AND_RXCUI_CODE_E_MEDICATIONS_03,
  emedications_04_col = MEDICATION_ADMINISTERED_ROUTE_E_MEDICATIONS_04
)

##### population results 2023
pediatrics_03b_pop_filter_process_2023 <- pediatrics_03b_pop_2023$filter_process |>
  dplyr::mutate(YEAR = 2023)

##### population missingness 2023 ----
pediatrics_03b_missingness_2023 <- pediatrics_03b_pop_2023$missingness |>
  dplyr::mutate(YEAR = 2023)


#### 2024 -------------------------------------------------------------------

pediatrics_03b_pop_2024 <- nemsqar::pediatrics_03b_population(
  df = NULL,
  patient_scene_table = patient_scene_table |>
    dplyr::filter(INCIDENT_YEAR == 2024),
  response_table = response_table |> dplyr::filter(INCIDENT_YEAR == 2024),
  exam_table = exam_table |> dplyr::filter(INCIDENT_YEAR == 2024),
  medications_table = medications_table |> dplyr::filter(INCIDENT_YEAR == 2024),
  erecord_01_col = FACT_INCIDENT_PK,
  incident_date_col = INCIDENT_DATE,
  patient_DOB_col = PATIENT_DATE_OF_BIRTH_E_PATIENT_17,
  epatient_15_col = PATIENT_AGE_E_PATIENT_15,
  epatient_16_col = PATIENT_AGE_UNITS_E_PATIENT_16,
  eresponse_05_col = RESPONSE_TYPE_OF_SERVICE_REQUESTED_WITH_CODE_E_RESPONSE_05,
  eexam_01_col = PATIENT_WEIGHT_IN_KILOGRAMS_E_EXAM_01,
  eexam_02_col = PATIENT_LENGTH_BASED_COLOR_E_EXAM_02,
  emedications_03_col = MEDICATION_GIVEN_OR_ADMINISTERED_DESCRIPTION_AND_RXCUI_CODE_E_MEDICATIONS_03,
  emedications_04_col = MEDICATION_ADMINISTERED_ROUTE_E_MEDICATIONS_04
)

##### population results 2024
pediatrics_03b_pop_filter_process_2024 <- pediatrics_03b_pop_2024$filter_process |>
  dplyr::mutate(YEAR = 2024)

##### population missingness 2024 ----
pediatrics_03b_missingness_2024 <- pediatrics_03b_pop_2024$missingness |>
  dplyr::mutate(YEAR = 2024)

#### 2025 -------------------------------------------------------------------

pediatrics_03b_pop_2025 <- nemsqar::pediatrics_03b_population(
  df = NULL,
  patient_scene_table = patient_scene_table |>
    dplyr::filter(INCIDENT_YEAR == 2025),
  response_table = response_table |> dplyr::filter(INCIDENT_YEAR == 2025),
  exam_table = exam_table |> dplyr::filter(INCIDENT_YEAR == 2025),
  medications_table = medications_table |> dplyr::filter(INCIDENT_YEAR == 2025),
  erecord_01_col = FACT_INCIDENT_PK,
  incident_date_col = INCIDENT_DATE,
  patient_DOB_col = PATIENT_DATE_OF_BIRTH_E_PATIENT_17,
  epatient_15_col = PATIENT_AGE_E_PATIENT_15,
  epatient_16_col = PATIENT_AGE_UNITS_E_PATIENT_16,
  eresponse_05_col = RESPONSE_TYPE_OF_SERVICE_REQUESTED_WITH_CODE_E_RESPONSE_05,
  eexam_01_col = PATIENT_WEIGHT_IN_KILOGRAMS_E_EXAM_01,
  eexam_02_col = PATIENT_LENGTH_BASED_COLOR_E_EXAM_02,
  emedications_03_col = MEDICATION_GIVEN_OR_ADMINISTERED_DESCRIPTION_AND_RXCUI_CODE_E_MEDICATIONS_03,
  emedications_04_col = MEDICATION_ADMINISTERED_ROUTE_E_MEDICATIONS_04
)

##### population results 2025
pediatrics_03b_pop_filter_process_2025 <- pediatrics_03b_pop_2025$filter_process |>
  dplyr::mutate(YEAR = 2025)

##### population missingness 2025 ----
pediatrics_03b_missingness_2025 <- pediatrics_03b_pop_2025$missingness |>
  dplyr::mutate(YEAR = 2025)

### pediatrics_03b populations over the years ----
pediatrics_03b_pop_years <- dplyr::bind_rows(
  pediatrics_03b_pop_filter_process_2021,
  pediatrics_03b_pop_filter_process_2022,
  pediatrics_03b_pop_filter_process_2023,
  pediatrics_03b_pop_filter_process_2024,
  pediatrics_03b_pop_filter_process_2025
)

### pediatrics_03b missingness over the years ----
pediatrics_03b_missingness <- dplyr::bind_rows(
  pediatrics_03b_missingness_2021,
  pediatrics_03b_missingness_2022,
  pediatrics_03b_missingness_2023,
  pediatrics_03b_missingness_2024,
  pediatrics_03b_missingness_2025
)

# plot population trends over time
pediatrics_03b_pop_years |>
  plot_nemsqa_pops(
    type = "col",
    wrap_width = 25,
    plot_title = "Pediatrics-03b"
  )

## pediatrics-03b results #####################################################

### results years ----------------------------------------------------------

pediatrics_03b_result_year <- nemsqar::pediatrics_03b(
  df = NULL,
  patient_scene_table = patient_scene_table,
  response_table = response_table,
  exam_table = exam_table,
  medications_table = medications_table,
  erecord_01_col = FACT_INCIDENT_PK,
  incident_date_col = INCIDENT_DATE,
  patient_DOB_col = PATIENT_DATE_OF_BIRTH_E_PATIENT_17,
  epatient_15_col = PATIENT_AGE_E_PATIENT_15,
  epatient_16_col = PATIENT_AGE_UNITS_E_PATIENT_16,
  eresponse_05_col = RESPONSE_TYPE_OF_SERVICE_REQUESTED_WITH_CODE_E_RESPONSE_05,
  eexam_01_col = PATIENT_WEIGHT_IN_KILOGRAMS_E_EXAM_01,
  eexam_02_col = PATIENT_LENGTH_BASED_COLOR_E_EXAM_02,
  emedications_03_col = MEDICATION_GIVEN_OR_ADMINISTERED_DESCRIPTION_AND_RXCUI_CODE_E_MEDICATIONS_03,
  emedications_04_col = MEDICATION_ADMINISTERED_ROUTE_E_MEDICATIONS_04,
  confidence_interval = TRUE,
  method = "w",
  conf.level = 0.95,
  correct = TRUE,
  .by = INCIDENT_YEAR
)

### results regions and years ----------------------------------------------

pediatrics_03b_result_regions_years <- nemsqar::pediatrics_03b(
  df = NULL,
  patient_scene_table = patient_scene_table,
  response_table = response_table,
  exam_table = exam_table,
  medications_table = medications_table,
  erecord_01_col = FACT_INCIDENT_PK,
  incident_date_col = INCIDENT_DATE,
  patient_DOB_col = PATIENT_DATE_OF_BIRTH_E_PATIENT_17,
  epatient_15_col = PATIENT_AGE_E_PATIENT_15,
  epatient_16_col = PATIENT_AGE_UNITS_E_PATIENT_16,
  eresponse_05_col = RESPONSE_TYPE_OF_SERVICE_REQUESTED_WITH_CODE_E_RESPONSE_05,
  eexam_01_col = PATIENT_WEIGHT_IN_KILOGRAMS_E_EXAM_01,
  eexam_02_col = PATIENT_LENGTH_BASED_COLOR_E_EXAM_02,
  emedications_03_col = MEDICATION_GIVEN_OR_ADMINISTERED_DESCRIPTION_AND_RXCUI_CODE_E_MEDICATIONS_03,
  emedications_04_col = MEDICATION_ADMINISTERED_ROUTE_E_MEDICATIONS_04,
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

pediatrics_03b_result_regions <- nemsqar::pediatrics_03b(
  df = NULL,
  patient_scene_table = patient_scene_table,
  response_table = response_table,
  exam_table = exam_table,
  medications_table = medications_table,
  erecord_01_col = FACT_INCIDENT_PK,
  incident_date_col = INCIDENT_DATE,
  patient_DOB_col = PATIENT_DATE_OF_BIRTH_E_PATIENT_17,
  epatient_15_col = PATIENT_AGE_E_PATIENT_15,
  epatient_16_col = PATIENT_AGE_UNITS_E_PATIENT_16,
  eresponse_05_col = RESPONSE_TYPE_OF_SERVICE_REQUESTED_WITH_CODE_E_RESPONSE_05,
  eexam_01_col = PATIENT_WEIGHT_IN_KILOGRAMS_E_EXAM_01,
  eexam_02_col = PATIENT_LENGTH_BASED_COLOR_E_EXAM_02,
  emedications_03_col = MEDICATION_GIVEN_OR_ADMINISTERED_DESCRIPTION_AND_RXCUI_CODE_E_MEDICATIONS_03,
  emedications_04_col = MEDICATION_ADMINISTERED_ROUTE_E_MEDICATIONS_04,
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

pediatrics_03b_result_counties <- nemsqar::pediatrics_03b(
  df = NULL,
  patient_scene_table = patient_scene_table |>
    dplyr::mutate(
      SCENE_INCIDENT_COUNTY_NAME_E_SCENE_21 = factor(
        SCENE_INCIDENT_COUNTY_NAME_E_SCENE_21
      )
    ),
  response_table = response_table,
  exam_table = exam_table,
  medications_table = medications_table,
  erecord_01_col = FACT_INCIDENT_PK,
  incident_date_col = INCIDENT_DATE,
  patient_DOB_col = PATIENT_DATE_OF_BIRTH_E_PATIENT_17,
  epatient_15_col = PATIENT_AGE_E_PATIENT_15,
  epatient_16_col = PATIENT_AGE_UNITS_E_PATIENT_16,
  eresponse_05_col = RESPONSE_TYPE_OF_SERVICE_REQUESTED_WITH_CODE_E_RESPONSE_05,
  eexam_01_col = PATIENT_WEIGHT_IN_KILOGRAMS_E_EXAM_01,
  eexam_02_col = PATIENT_LENGTH_BASED_COLOR_E_EXAM_02,
  emedications_03_col = MEDICATION_GIVEN_OR_ADMINISTERED_DESCRIPTION_AND_RXCUI_CODE_E_MEDICATIONS_03,
  emedications_04_col = MEDICATION_ADMINISTERED_ROUTE_E_MEDICATIONS_04,
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


### results counties years ----------------------------------------------

pediatrics_03b_result_counties_years <- nemsqar::pediatrics_03b(
  df = NULL,
  patient_scene_table = patient_scene_table |>
    dplyr::mutate(
      SCENE_INCIDENT_COUNTY_NAME_E_SCENE_21 = factor(
        SCENE_INCIDENT_COUNTY_NAME_E_SCENE_21
      )
    ),
  response_table = response_table,
  exam_table = exam_table,
  medications_table = medications_table,
  erecord_01_col = FACT_INCIDENT_PK,
  incident_date_col = INCIDENT_DATE,
  patient_DOB_col = PATIENT_DATE_OF_BIRTH_E_PATIENT_17,
  epatient_15_col = PATIENT_AGE_E_PATIENT_15,
  epatient_16_col = PATIENT_AGE_UNITS_E_PATIENT_16,
  eresponse_05_col = RESPONSE_TYPE_OF_SERVICE_REQUESTED_WITH_CODE_E_RESPONSE_05,
  eexam_01_col = PATIENT_WEIGHT_IN_KILOGRAMS_E_EXAM_01,
  eexam_02_col = PATIENT_LENGTH_BASED_COLOR_E_EXAM_02,
  emedications_03_col = MEDICATION_GIVEN_OR_ADMINISTERED_DESCRIPTION_AND_RXCUI_CODE_E_MEDICATIONS_03,
  emedications_04_col = MEDICATION_ADMINISTERED_ROUTE_E_MEDICATIONS_04,
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

pediatrics_03b_result_overall <- nemsqar::pediatrics_03b(
  df = NULL,
  patient_scene_table = patient_scene_table,
  response_table = response_table,
  exam_table = exam_table,
  medications_table = medications_table,
  erecord_01_col = FACT_INCIDENT_PK,
  incident_date_col = INCIDENT_DATE,
  patient_DOB_col = PATIENT_DATE_OF_BIRTH_E_PATIENT_17,
  epatient_15_col = PATIENT_AGE_E_PATIENT_15,
  epatient_16_col = PATIENT_AGE_UNITS_E_PATIENT_16,
  eresponse_05_col = RESPONSE_TYPE_OF_SERVICE_REQUESTED_WITH_CODE_E_RESPONSE_05,
  eexam_01_col = PATIENT_WEIGHT_IN_KILOGRAMS_E_EXAM_01,
  eexam_02_col = PATIENT_LENGTH_BASED_COLOR_E_EXAM_02,
  emedications_03_col = MEDICATION_GIVEN_OR_ADMINISTERED_DESCRIPTION_AND_RXCUI_CODE_E_MEDICATIONS_03,
  emedications_04_col = MEDICATION_ADMINISTERED_ROUTE_E_MEDICATIONS_04,
  confidence_interval = TRUE,
  method = "w",
  conf.level = 0.95,
  correct = TRUE
)


### results services -------------------------------------------------------

pediatrics_03b_result_services <- nemsqar::pediatrics_03b(
  df = NULL,
  patient_scene_table = patient_scene_table,
  response_table = response_table,
  exam_table = exam_table,
  medications_table = medications_table,
  erecord_01_col = FACT_INCIDENT_PK,
  incident_date_col = INCIDENT_DATE,
  patient_DOB_col = PATIENT_DATE_OF_BIRTH_E_PATIENT_17,
  epatient_15_col = PATIENT_AGE_E_PATIENT_15,
  epatient_16_col = PATIENT_AGE_UNITS_E_PATIENT_16,
  eresponse_05_col = RESPONSE_TYPE_OF_SERVICE_REQUESTED_WITH_CODE_E_RESPONSE_05,
  eexam_01_col = PATIENT_WEIGHT_IN_KILOGRAMS_E_EXAM_01,
  eexam_02_col = PATIENT_LENGTH_BASED_COLOR_E_EXAM_02,
  emedications_03_col = MEDICATION_GIVEN_OR_ADMINISTERED_DESCRIPTION_AND_RXCUI_CODE_E_MEDICATIONS_03,
  emedications_04_col = MEDICATION_ADMINISTERED_ROUTE_E_MEDICATIONS_04,
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
  pattern = "pediatrics_03b_pop",
  measure = "Pediatrics-03b",
  folder = "population"
)

## results exports ############################################################

export_nemsqa_data(
  pattern = "pediatrics_03b_result",
  measure = "Pediatrics-03b",
  folder = "result"
)

## missingness exports ########################################################

export_nemsqa_data(
  pattern = "pediatrics_03b_(?:missings|missingness)",
  measure = "Pediatrics-03b",
  folder = "missings"
)
