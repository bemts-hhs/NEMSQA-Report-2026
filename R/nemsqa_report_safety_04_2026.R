### IOWA NEMSQA REPORT SAFETY-04 2026 ------------------------------------

###_____________________________________________________________________________
# this script will contain all reporting calculations for Safety-04
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

# DATA -----------------------------------------------------------------------

# tables imported in alphabetical order
# tables do not need to be loaded again if already in memory

## arrest tables ----------------------------------------------------------
# Utilize mirai for asynchronous loading
# automatically bind rows
arrest_table <- load_nemsqa_parallel(
  table = "arrest",
  years = 2021:2025,
  cores = 13
)

# share the arrest table
arrest_table_s <- mori::share(arrest_table)


## disposition tables -----------------------------------------------------
disposition_table <- load_nemsqa_parallel(
  table = "disposition",
  years = 2021:2025,
  cores = 13,
  exclude = "DISPOSITION_DESTINATION_US_NATIONAL_GRID_COORDINATES_E_DISPOSITION_10"
)

# share the disposition table
disposition_table_s <- mori::share(disposition_table)

## injury tables ----------------------------------------------------------
# set up the injury table for manipulations
injury_table <- load_nemsqa_parallel(
  table = "injury",
  years = 2021:2025,
  cores = 13
)

# share the injury table
injury_table_s <- mori::share(injury_table)

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

# share the patient table
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

# CALCULATIONS ---------------------------------------------------------------

## Safety-04 ==================================================================

## safety-04 populations ######################################################

### get safety_04 populations over all years 2021-2025 ---------------------

safety_04_pop <- nemsqar::safety_04_population(
  df = NULL,
  patient_scene_table = patient_scene_table,
  response_table = response_table,
  arrest_table = arrest_table,
  injury_table = injury_table,
  procedures_table = procedures_table,
  disposition_table = disposition_table,
  erecord_01_col = FACT_INCIDENT_PK,
  incident_date_col = INCIDENT_DATE,
  patient_DOB_col = PATIENT_DATE_OF_BIRTH_E_PATIENT_17,
  epatient_15_col = PATIENT_AGE_E_PATIENT_15,
  epatient_16_col = PATIENT_AGE_UNITS_E_PATIENT_16,
  eresponse_05_col = RESPONSE_TYPE_OF_SERVICE_REQUESTED_WITH_CODE_E_RESPONSE_05,
  earrest_01_col = CARDIAC_ARREST_DURING_EMS_EVENT_WITH_CODE_E_ARREST_01,
  einjury_03_col = INJURY_TRAUMA_CENTER_TRIAGE_CRITERIA_STEPS_1_AND_2_LIST_E_INJURY_03,
  eprocedures_03_col = PROCEDURE_PERFORMED_DESCRIPTION_AND_CODE_E_PROCEDURES_03,
  edisposition_14_col = DISPOSITION_POSITION_OF_PATIENT_DURING_TRANSPORT_LIST_E_DISPOSITION_14,
  transport_disposition_col = c(
    DISPOSITION_INCIDENT_PATIENT_DISPOSITION_WITH_CODE_3_4_E_DISPOSITION_12_3_5_IT_DISPOSITION_112,
    TRANSPORT_DISPOSITION_3_4_IT_DISPOSITION_102_3_5_E_DISPOSITION_30
  )
)

#### population results for 2021-2025 ----
safety_04_pop_filter_process <- safety_04_pop$filter_process

#### population results for 2021-2025 ----
safety_04_missings <- safety_04_pop$missingness

# set up daemons
mirai::daemons(n = 13)

### get safety_04 population data for each year using mirai and mori -------

# track progress
tictoc::tic(msg = "safety_04_pop_years_init")

safety_04_pop_years_init <- purrr::map(
  report_years,
  \(yr) {
    # Dynamic message inside the loop
    cli::cli_alert_info("Running year: {yr}.")

    # loop over years
    ps_y <- patient_scene_table |> dplyr::filter(INCIDENT_YEAR == yr)
    rsp_y <- response_table |> dplyr::filter(INCIDENT_YEAR == yr)
    arr_y <- arrest_table |> dplyr::filter(INCIDENT_YEAR == yr)
    inj_y <- injury_table |> dplyr::filter(INCIDENT_YEAR == yr)
    pro_y <- procedures_table |> dplyr::filter(INCIDENT_YEAR == yr)
    dis_y <- disposition_table |> dplyr::filter(INCIDENT_YEAR == yr)

    # run function in parallel
    nemsqar::safety_04_population(
      df = NULL,
      patient_scene_table = ps_y,
      response_table = rsp_y,
      arrest_table = arr_y,
      injury_table = inj_y,
      procedures_table = pro_y,
      disposition_table = dis_y,
      erecord_01_col = FACT_INCIDENT_PK,
      incident_date_col = INCIDENT_DATE,
      patient_DOB_col = PATIENT_DATE_OF_BIRTH_E_PATIENT_17,
      epatient_15_col = PATIENT_AGE_E_PATIENT_15,
      epatient_16_col = PATIENT_AGE_UNITS_E_PATIENT_16,
      eresponse_05_col = RESPONSE_TYPE_OF_SERVICE_REQUESTED_WITH_CODE_E_RESPONSE_05,
      earrest_01_col = CARDIAC_ARREST_DURING_EMS_EVENT_WITH_CODE_E_ARREST_01,
      einjury_03_col = INJURY_TRAUMA_CENTER_TRIAGE_CRITERIA_STEPS_1_AND_2_LIST_E_INJURY_03,
      eprocedures_03_col = PROCEDURE_PERFORMED_DESCRIPTION_AND_CODE_E_PROCEDURES_03,
      edisposition_14_col = DISPOSITION_POSITION_OF_PATIENT_DURING_TRANSPORT_LIST_E_DISPOSITION_14,
      transport_disposition_col = c(
        DISPOSITION_INCIDENT_PATIENT_DISPOSITION_WITH_CODE_3_4_E_DISPOSITION_12_3_5_IT_DISPOSITION_112,
        TRANSPORT_DISPOSITION_3_4_IT_DISPOSITION_102_3_5_E_DISPOSITION_30
      )
    )
  }
)

# Get total time
time <- tictoc::toc()

#### append years to the population files ----
safety_04_pop_years <- add_year_to_nested(
  x = safety_04_pop_years_init,
  file = "filter_process",
  years = 2021:2025
)

#### append years to the missingness files ----
safety_04_missingness_years <- add_year_to_nested(
  x = safety_04_pop_years_init,
  file = "missingness",
  years = 2021:2025
)

# plot population trends over time
safety_04_pop_years |>
  plot_nemsqa_pops(
    type = "col",
    wrap_width = 25,
    plot_title = "Safety-04"
  )

## safety-04 results ##########################################################

### results years ----------------------------------------------------------

#### year ----
safety_04_result_year <- nemsqar::safety_04(
  df = NULL,
  patient_scene_table = patient_scene_table,
  response_table = response_table,
  arrest_table = arrest_table,
  injury_table = injury_table,
  procedures_table = procedures_table,
  disposition_table = disposition_table,
  erecord_01_col = FACT_INCIDENT_PK,
  incident_date_col = INCIDENT_DATE,
  patient_DOB_col = PATIENT_DATE_OF_BIRTH_E_PATIENT_17,
  epatient_15_col = PATIENT_AGE_E_PATIENT_15,
  epatient_16_col = PATIENT_AGE_UNITS_E_PATIENT_16,
  eresponse_05_col = RESPONSE_TYPE_OF_SERVICE_REQUESTED_WITH_CODE_E_RESPONSE_05,
  earrest_01_col = CARDIAC_ARREST_DURING_EMS_EVENT_WITH_CODE_E_ARREST_01,
  einjury_03_col = INJURY_TRAUMA_CENTER_TRIAGE_CRITERIA_STEPS_1_AND_2_LIST_E_INJURY_03,
  eprocedures_03_col = PROCEDURE_PERFORMED_DESCRIPTION_AND_CODE_E_PROCEDURES_03,
  edisposition_14_col = DISPOSITION_POSITION_OF_PATIENT_DURING_TRANSPORT_LIST_E_DISPOSITION_14,
  transport_disposition_col = c(
    DISPOSITION_INCIDENT_PATIENT_DISPOSITION_WITH_CODE_3_4_E_DISPOSITION_12_3_5_IT_DISPOSITION_112,
    TRANSPORT_DISPOSITION_3_4_IT_DISPOSITION_102_3_5_E_DISPOSITION_30
  ),
  confidence_interval = TRUE,
  method = "w",
  conf.level = 0.95,
  correct = TRUE,
  .by = INCIDENT_YEAR
)

### results regions and years ----------------------------------------------

#### regions and years ----
safety_04_result_regions_years <- nemsqar::safety_04(
  df = NULL,
  patient_scene_table = patient_scene_table,
  response_table = response_table,
  arrest_table = arrest_table,
  injury_table = injury_table,
  procedures_table = procedures_table,
  disposition_table = disposition_table,
  erecord_01_col = FACT_INCIDENT_PK,
  incident_date_col = INCIDENT_DATE,
  patient_DOB_col = PATIENT_DATE_OF_BIRTH_E_PATIENT_17,
  epatient_15_col = PATIENT_AGE_E_PATIENT_15,
  epatient_16_col = PATIENT_AGE_UNITS_E_PATIENT_16,
  eresponse_05_col = RESPONSE_TYPE_OF_SERVICE_REQUESTED_WITH_CODE_E_RESPONSE_05,
  earrest_01_col = CARDIAC_ARREST_DURING_EMS_EVENT_WITH_CODE_E_ARREST_01,
  einjury_03_col = INJURY_TRAUMA_CENTER_TRIAGE_CRITERIA_STEPS_1_AND_2_LIST_E_INJURY_03,
  eprocedures_03_col = PROCEDURE_PERFORMED_DESCRIPTION_AND_CODE_E_PROCEDURES_03,
  edisposition_14_col = DISPOSITION_POSITION_OF_PATIENT_DURING_TRANSPORT_LIST_E_DISPOSITION_14,
  transport_disposition_col = c(
    DISPOSITION_INCIDENT_PATIENT_DISPOSITION_WITH_CODE_3_4_E_DISPOSITION_12_3_5_IT_DISPOSITION_112,
    TRANSPORT_DISPOSITION_3_4_IT_DISPOSITION_102_3_5_E_DISPOSITION_30
  ),
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
safety_04_result_regions <- nemsqar::safety_04(
  df = NULL,
  patient_scene_table = patient_scene_table,
  response_table = response_table,
  arrest_table = arrest_table,
  injury_table = injury_table,
  procedures_table = procedures_table,
  disposition_table = disposition_table,
  erecord_01_col = FACT_INCIDENT_PK,
  incident_date_col = INCIDENT_DATE,
  patient_DOB_col = PATIENT_DATE_OF_BIRTH_E_PATIENT_17,
  epatient_15_col = PATIENT_AGE_E_PATIENT_15,
  epatient_16_col = PATIENT_AGE_UNITS_E_PATIENT_16,
  eresponse_05_col = RESPONSE_TYPE_OF_SERVICE_REQUESTED_WITH_CODE_E_RESPONSE_05,
  earrest_01_col = CARDIAC_ARREST_DURING_EMS_EVENT_WITH_CODE_E_ARREST_01,
  einjury_03_col = INJURY_TRAUMA_CENTER_TRIAGE_CRITERIA_STEPS_1_AND_2_LIST_E_INJURY_03,
  eprocedures_03_col = PROCEDURE_PERFORMED_DESCRIPTION_AND_CODE_E_PROCEDURES_03,
  edisposition_14_col = DISPOSITION_POSITION_OF_PATIENT_DURING_TRANSPORT_LIST_E_DISPOSITION_14,
  transport_disposition_col = c(
    DISPOSITION_INCIDENT_PATIENT_DISPOSITION_WITH_CODE_3_4_E_DISPOSITION_12_3_5_IT_DISPOSITION_112,
    TRANSPORT_DISPOSITION_3_4_IT_DISPOSITION_102_3_5_E_DISPOSITION_30
  ),
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
safety_04_result_counties <- nemsqar::safety_04(
  df = NULL,
  patient_scene_table = patient_scene_table |>
    dplyr::mutate(
      SCENE_INCIDENT_COUNTY_NAME_E_SCENE_21 = factor(
        SCENE_INCIDENT_COUNTY_NAME_E_SCENE_21
      )
    ),
  response_table = response_table,
  arrest_table = arrest_table,
  injury_table = injury_table,
  procedures_table = procedures_table,
  disposition_table = disposition_table,
  erecord_01_col = FACT_INCIDENT_PK,
  incident_date_col = INCIDENT_DATE,
  patient_DOB_col = PATIENT_DATE_OF_BIRTH_E_PATIENT_17,
  epatient_15_col = PATIENT_AGE_E_PATIENT_15,
  epatient_16_col = PATIENT_AGE_UNITS_E_PATIENT_16,
  eresponse_05_col = RESPONSE_TYPE_OF_SERVICE_REQUESTED_WITH_CODE_E_RESPONSE_05,
  earrest_01_col = CARDIAC_ARREST_DURING_EMS_EVENT_WITH_CODE_E_ARREST_01,
  einjury_03_col = INJURY_TRAUMA_CENTER_TRIAGE_CRITERIA_STEPS_1_AND_2_LIST_E_INJURY_03,
  eprocedures_03_col = PROCEDURE_PERFORMED_DESCRIPTION_AND_CODE_E_PROCEDURES_03,
  edisposition_14_col = DISPOSITION_POSITION_OF_PATIENT_DURING_TRANSPORT_LIST_E_DISPOSITION_14,
  transport_disposition_col = c(
    DISPOSITION_INCIDENT_PATIENT_DISPOSITION_WITH_CODE_3_4_E_DISPOSITION_12_3_5_IT_DISPOSITION_112,
    TRANSPORT_DISPOSITION_3_4_IT_DISPOSITION_102_3_5_E_DISPOSITION_30
  ),
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

### results counties and years ---------------------------------------------

#### counties and years ----
safety_04_result_counties_years <- nemsqar::safety_04(
  df = NULL,
  patient_scene_table = patient_scene_table,
  response_table = response_table,
  arrest_table = arrest_table,
  injury_table = injury_table,
  procedures_table = procedures_table,
  disposition_table = disposition_table,
  erecord_01_col = FACT_INCIDENT_PK,
  incident_date_col = INCIDENT_DATE,
  patient_DOB_col = PATIENT_DATE_OF_BIRTH_E_PATIENT_17,
  epatient_15_col = PATIENT_AGE_E_PATIENT_15,
  epatient_16_col = PATIENT_AGE_UNITS_E_PATIENT_16,
  eresponse_05_col = RESPONSE_TYPE_OF_SERVICE_REQUESTED_WITH_CODE_E_RESPONSE_05,
  earrest_01_col = CARDIAC_ARREST_DURING_EMS_EVENT_WITH_CODE_E_ARREST_01,
  einjury_03_col = INJURY_TRAUMA_CENTER_TRIAGE_CRITERIA_STEPS_1_AND_2_LIST_E_INJURY_03,
  eprocedures_03_col = PROCEDURE_PERFORMED_DESCRIPTION_AND_CODE_E_PROCEDURES_03,
  edisposition_14_col = DISPOSITION_POSITION_OF_PATIENT_DURING_TRANSPORT_LIST_E_DISPOSITION_14,
  transport_disposition_col = c(
    DISPOSITION_INCIDENT_PATIENT_DISPOSITION_WITH_CODE_3_4_E_DISPOSITION_12_3_5_IT_DISPOSITION_112,
    TRANSPORT_DISPOSITION_3_4_IT_DISPOSITION_102_3_5_E_DISPOSITION_30
  ),
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
safety_04_result_overall <- nemsqar::safety_04(
  df = NULL,
  patient_scene_table = patient_scene_table,
  response_table = response_table,
  arrest_table = arrest_table,
  injury_table = injury_table,
  procedures_table = procedures_table,
  disposition_table = disposition_table,
  erecord_01_col = FACT_INCIDENT_PK,
  incident_date_col = INCIDENT_DATE,
  patient_DOB_col = PATIENT_DATE_OF_BIRTH_E_PATIENT_17,
  epatient_15_col = PATIENT_AGE_E_PATIENT_15,
  epatient_16_col = PATIENT_AGE_UNITS_E_PATIENT_16,
  eresponse_05_col = RESPONSE_TYPE_OF_SERVICE_REQUESTED_WITH_CODE_E_RESPONSE_05,
  earrest_01_col = CARDIAC_ARREST_DURING_EMS_EVENT_WITH_CODE_E_ARREST_01,
  einjury_03_col = INJURY_TRAUMA_CENTER_TRIAGE_CRITERIA_STEPS_1_AND_2_LIST_E_INJURY_03,
  eprocedures_03_col = PROCEDURE_PERFORMED_DESCRIPTION_AND_CODE_E_PROCEDURES_03,
  edisposition_14_col = DISPOSITION_POSITION_OF_PATIENT_DURING_TRANSPORT_LIST_E_DISPOSITION_14,
  transport_disposition_col = c(
    DISPOSITION_INCIDENT_PATIENT_DISPOSITION_WITH_CODE_3_4_E_DISPOSITION_12_3_5_IT_DISPOSITION_112,
    TRANSPORT_DISPOSITION_3_4_IT_DISPOSITION_102_3_5_E_DISPOSITION_30
  ),
  confidence_interval = TRUE,
  method = "w",
  conf.level = 0.95,
  correct = TRUE
)

### results services  ------------------------------------------------------

#### services ----
safety_04_result_services <- nemsqar::safety_04(
  df = NULL,
  patient_scene_table = patient_scene_table,
  response_table = response_table,
  arrest_table = arrest_table,
  injury_table = injury_table,
  procedures_table = procedures_table,
  disposition_table = disposition_table,
  erecord_01_col = FACT_INCIDENT_PK,
  incident_date_col = INCIDENT_DATE,
  patient_DOB_col = PATIENT_DATE_OF_BIRTH_E_PATIENT_17,
  epatient_15_col = PATIENT_AGE_E_PATIENT_15,
  epatient_16_col = PATIENT_AGE_UNITS_E_PATIENT_16,
  eresponse_05_col = RESPONSE_TYPE_OF_SERVICE_REQUESTED_WITH_CODE_E_RESPONSE_05,
  earrest_01_col = CARDIAC_ARREST_DURING_EMS_EVENT_WITH_CODE_E_ARREST_01,
  einjury_03_col = INJURY_TRAUMA_CENTER_TRIAGE_CRITERIA_STEPS_1_AND_2_LIST_E_INJURY_03,
  eprocedures_03_col = PROCEDURE_PERFORMED_DESCRIPTION_AND_CODE_E_PROCEDURES_03,
  edisposition_14_col = DISPOSITION_POSITION_OF_PATIENT_DURING_TRANSPORT_LIST_E_DISPOSITION_14,
  transport_disposition_col = c(
    DISPOSITION_INCIDENT_PATIENT_DISPOSITION_WITH_CODE_3_4_E_DISPOSITION_12_3_5_IT_DISPOSITION_112,
    TRANSPORT_DISPOSITION_3_4_IT_DISPOSITION_102_3_5_E_DISPOSITION_30
  ),
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
  pattern = "safety_04_pop",
  measure = "Safety-04",
  folder = "population"
)

## results exports ############################################################

export_nemsqa_data(
  pattern = "safety_04_result",
  measure = "Safety-04",
  folder = "result"
)

## results missingness ########################################################

export_nemsqa_data(
  pattern = "safety_04_(?:missings|missingness)",
  measure = "Safety-04",
  folder = "missings"
)
