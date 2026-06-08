# IOWA NEMSQA REPORT AIRWAY-05 2026 ------------------------------------------

###_____________________________________________________________________________
# this script will contain all reporting calculations for Airway-05
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

## patient tables ---------------------------------------------------------
# Utilize mirai for asynchronous loading
# automatically bind rows
patient_scene_clean <- load_nemsqa_parallel(
  table = "patient_scene",
  years = 2021:2025,
  cores = 13
)

### final manipulations on the patient/scene table
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

## Airway-05 ==================================================================

## airway-05 populations ########################################################

### populations over all years 2021-2025 -----------------------------------

airway_05_pop <- nemsqar::airway_05_population(
  df = NULL,
  patient_scene_table = patient_scene_table,
  response_table = response_table,
  arrest_table = arrest_table,
  procedures_table = procedures_table,
  vitals_table = vitals_table,
  erecord_01_col = FACT_INCIDENT_PK,
  incident_date_col = INCIDENT_DATE,
  patient_DOB_col = PATIENT_DATE_OF_BIRTH_E_PATIENT_17,
  epatient_15_col = PATIENT_AGE_E_PATIENT_15,
  epatient_16_col = PATIENT_AGE_UNITS_E_PATIENT_16,
  earrest_01_col = CARDIAC_ARREST_DURING_EMS_EVENT_WITH_CODE_E_ARREST_01,
  eresponse_05_col = RESPONSE_TYPE_OF_SERVICE_REQUESTED_WITH_CODE_E_RESPONSE_05,
  evitals_01_col = VITALS_SIGNS_TAKEN_DATE_TIME_E_VITALS_01,
  evitals_12_col = VITALS_PULSE_OXIMETRY_E_VITALS_12,
  eprocedures_01_col = PROCEDURE_PERFORMED_DATE_TIME_E_PROCEDURES_01,
  eprocedures_02_col = PROCEDURE_PERFORMED_PRIOR_TO_EMS_CARE_E_PROCEDURES_02,
  eprocedures_03_col = PROCEDURE_PERFORMED_DESCRIPTION_AND_CODE_E_PROCEDURES_03
)

#### population results for 2021-2025 ----
airway_05_pop_filter_process <- airway_05_pop$filter_process

#### missingness results for 2021-2025 ----
airway_05_missings <- airway_05_pop$missingness

# set up daemons
mirai::daemons(n = 13)

### get airway_05_population data for each year using mirai and mori -------

# track progress
tictoc::tic(msg = "airway_05_pop_years_init")

airway_05_pop_years_init <- mirai::mirai_map(
  report_years,
  \(yr, ps, rsp, arr, pro, vit) {
    # parallelize by year
    ps_y <- ps |> dplyr::filter(INCIDENT_YEAR == yr)
    rsp_y <- rsp |> dplyr::filter(INCIDENT_YEAR == yr)
    arr_y <- arr |> dplyr::filter(INCIDENT_YEAR == yr)
    pro_y <- pro |> dplyr::filter(INCIDENT_YEAR == yr)
    vit_y <- vit |> dplyr::filter(INCIDENT_YEAR == yr)

    # run function in parallel
    nemsqar::airway_05_population(
      df = NULL,
      patient_scene_table = ps_y,
      response_table = rsp_y,
      arrest_table = arr_y,
      procedures_table = pro_y,
      vitals_table = vit_y,
      erecord_01_col = FACT_INCIDENT_PK,
      incident_date_col = INCIDENT_DATE,
      patient_DOB_col = PATIENT_DATE_OF_BIRTH_E_PATIENT_17,
      epatient_15_col = PATIENT_AGE_E_PATIENT_15,
      epatient_16_col = PATIENT_AGE_UNITS_E_PATIENT_16,
      earrest_01_col = CARDIAC_ARREST_DURING_EMS_EVENT_WITH_CODE_E_ARREST_01,
      eresponse_05_col = RESPONSE_TYPE_OF_SERVICE_REQUESTED_WITH_CODE_E_RESPONSE_05,
      evitals_01_col = VITALS_SIGNS_TAKEN_DATE_TIME_E_VITALS_01,
      evitals_12_col = VITALS_PULSE_OXIMETRY_E_VITALS_12,
      eprocedures_01_col = PROCEDURE_PERFORMED_DATE_TIME_E_PROCEDURES_01,
      eprocedures_02_col = PROCEDURE_PERFORMED_PRIOR_TO_EMS_CARE_E_PROCEDURES_02,
      eprocedures_03_col = PROCEDURE_PERFORMED_DESCRIPTION_AND_CODE_E_PROCEDURES_03
    )
  },
  .args = list(
    ps = patient_scene_table_s,
    rsp = response_table_s,
    arr = arrest_table_s,
    pro = procedures_table_s,
    vit = vitals_table_s
  )
)[.progress]

# Get total time
time <- tictoc::toc()

#### append years to the population files ----
airway_05_pop_years <- add_year_to_nested(
  x = airway_05_pop_years_init,
  file = "filter_process",
  years = 2021:2025
)

#### append years to the missingness files ----
airway_05_missingness_years <- add_year_to_nested(
  x = airway_05_pop_years_init,
  file = "missingness",
  years = 2021:2025
)

#### plot population trends over time ----
airway_05_pop_years |>
  plot_nemsqa_pops(
    type = "col",
    wrap_width = 25,
    plot_title = "Airway-05"
  )

# airway-05 results ############################################################

### results years ----------------------------------------------------------

# get start time
tictoc::tic(msg = "airway_05_result_year")

#### year ----
airway_05_result_year <- mirai::mirai_map(
  report_years,
  \(yr, ps, rsp, arr, pro, vit) {
    # parallelize region work
    ps_y <- ps |> dplyr::filter(INCIDENT_YEAR == yr)
    rsp_y <- rsp |> dplyr::filter(INCIDENT_YEAR == yr)
    arr_y <- arr |> dplyr::filter(INCIDENT_YEAR == yr)
    pro_y <- pro |> dplyr::filter(INCIDENT_YEAR == yr)
    vit_y <- vit |> dplyr::filter(INCIDENT_YEAR == yr)

    # run the function in parallel
    nemsqar::airway_05(
      df = NULL,
      patient_scene_table = ps_y,
      response_table = rsp_y,
      arrest_table = arr_y,
      procedures_table = pro_y,
      vitals_table = vit_y,
      erecord_01_col = UNIQUE_RUN_ID,
      incident_date_col = INCIDENT_DATE,
      patient_DOB_col = PATIENT_DATE_OF_BIRTH_E_PATIENT_17,
      epatient_15_col = PATIENT_AGE_E_PATIENT_15,
      epatient_16_col = PATIENT_AGE_UNITS_E_PATIENT_16,
      earrest_01_col = CARDIAC_ARREST_DURING_EMS_EVENT_WITH_CODE_E_ARREST_01,
      eresponse_05_col = RESPONSE_TYPE_OF_SERVICE_REQUESTED_WITH_CODE_E_RESPONSE_05,
      evitals_01_col = VITALS_SIGNS_TAKEN_DATE_TIME_E_VITALS_01,
      evitals_12_col = VITALS_PULSE_OXIMETRY_E_VITALS_12,
      eprocedures_01_col = PROCEDURE_PERFORMED_DATE_TIME_E_PROCEDURES_01,
      eprocedures_02_col = PROCEDURE_PERFORMED_PRIOR_TO_EMS_CARE_E_PROCEDURES_02,
      eprocedures_03_col = PROCEDURE_PERFORMED_DESCRIPTION_AND_CODE_E_PROCEDURES_03,
      confidence_interval = TRUE,
      method = "w",
      conf.level = 0.95,
      correct = TRUE,
      .by = INCIDENT_YEAR
    )
  },
  .args = list(
    ps = patient_scene_table_s,
    rsp = response_table_s,
    arr = arrest_table_s,
    pro = procedures_table_s,
    vit = vitals_table_s
  )
)[.progress] |>
  dplyr::bind_rows()

# total time
time_result_year <- tictoc::toc()


### results regions and years ----------------------------------------------

# get start time
tictoc::tic(msg = "airway_05_result_regions_year")

#### regions and years ----
airway_05_result_regions_years <- mirai::mirai_map(
  report_years,
  \(yr, ps, rsp, arr, pro, vit) {
    # parallelize region work
    ps_y <- ps |> dplyr::filter(INCIDENT_YEAR == yr)
    rsp_y <- rsp |> dplyr::filter(INCIDENT_YEAR == yr)
    arr_y <- arr |> dplyr::filter(INCIDENT_YEAR == yr)
    pro_y <- pro |> dplyr::filter(INCIDENT_YEAR == yr)
    vit_y <- vit |> dplyr::filter(INCIDENT_YEAR == yr)

    # run the function in parallel
    nemsqar::airway_05(
      df = NULL,
      patient_scene_table = ps_y,
      response_table = rsp_y,
      arrest_table = arr_y,
      procedures_table = pro_y,
      vitals_table = vit_y,
      erecord_01_col = FACT_INCIDENT_PK,
      incident_date_col = INCIDENT_DATE,
      patient_DOB_col = PATIENT_DATE_OF_BIRTH_E_PATIENT_17,
      epatient_15_col = PATIENT_AGE_E_PATIENT_15,
      epatient_16_col = PATIENT_AGE_UNITS_E_PATIENT_16,
      earrest_01_col = CARDIAC_ARREST_DURING_EMS_EVENT_WITH_CODE_E_ARREST_01,
      eresponse_05_col = RESPONSE_TYPE_OF_SERVICE_REQUESTED_WITH_CODE_E_RESPONSE_05,
      evitals_01_col = VITALS_SIGNS_TAKEN_DATE_TIME_E_VITALS_01,
      evitals_12_col = VITALS_PULSE_OXIMETRY_E_VITALS_12,
      eprocedures_01_col = PROCEDURE_PERFORMED_DATE_TIME_E_PROCEDURES_01,
      eprocedures_02_col = PROCEDURE_PERFORMED_PRIOR_TO_EMS_CARE_E_PROCEDURES_02,
      eprocedures_03_col = PROCEDURE_PERFORMED_DESCRIPTION_AND_CODE_E_PROCEDURES_03,
      confidence_interval = TRUE,
      method = "w",
      conf.level = 0.95,
      correct = TRUE,
      .by = c(INCIDENT_YEAR, `Region: Preparedness`)
    )
  },
  .args = list(
    ps = patient_scene_table_s,
    rsp = response_table_s,
    arr = arrest_table_s,
    pro = procedures_table_s,
    vit = vitals_table_s
  )
)[.progress] |>
  dplyr::bind_rows() |>
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

# total time
time_result_regions_year <- tictoc::toc()

### results regions --------------------------------------------------------

# get start time
tictoc::tic(msg = "airway_05_result_regions")

#### regions ----
airway_05_result_regions <- mirai::mirai_map(
  report_regions,
  \(reg, ps, rsp, arr, pro, vit) {
    # get tables
    ps_r <- ps |> dplyr::filter(`Region: Preparedness` == reg)

    # run function in parallel
    nemsqar::airway_05(
      df = NULL,
      patient_scene_table = ps_r,
      response_table = rsp,
      arrest_table = arr,
      procedures_table = pro,
      vitals_table = vit,
      erecord_01_col = FACT_INCIDENT_PK,
      incident_date_col = INCIDENT_DATE,
      patient_DOB_col = PATIENT_DATE_OF_BIRTH_E_PATIENT_17,
      epatient_15_col = PATIENT_AGE_E_PATIENT_15,
      epatient_16_col = PATIENT_AGE_UNITS_E_PATIENT_16,
      earrest_01_col = CARDIAC_ARREST_DURING_EMS_EVENT_WITH_CODE_E_ARREST_01,
      eresponse_05_col = RESPONSE_TYPE_OF_SERVICE_REQUESTED_WITH_CODE_E_RESPONSE_05,
      evitals_01_col = VITALS_SIGNS_TAKEN_DATE_TIME_E_VITALS_01,
      evitals_12_col = VITALS_PULSE_OXIMETRY_E_VITALS_12,
      eprocedures_01_col = PROCEDURE_PERFORMED_DATE_TIME_E_PROCEDURES_01,
      eprocedures_02_col = PROCEDURE_PERFORMED_PRIOR_TO_EMS_CARE_E_PROCEDURES_02,
      eprocedures_03_col = PROCEDURE_PERFORMED_DESCRIPTION_AND_CODE_E_PROCEDURES_03,
      confidence_interval = TRUE,
      method = "w",
      conf.level = 0.95,
      correct = TRUE,
      .by = `Region: Preparedness`
    )
  },
  .args = list(
    ps = patient_scene_table_s,
    rsp = response_table_s,
    arr = arrest_table_s,
    pro = procedures_table_s,
    vit = vitals_table_s
  )
)[.progress] |>
  dplyr::bind_rows() |>
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

# total time
time_result_regions <- tictoc::toc()

### results counties -------------------------------------------------------

#### counties ----
airway_05_result_counties <- nemsqar::airway_05(
  df = NULL,
  patient_scene_table = patient_scene_table |>
    dplyr::mutate(
      SCENE_INCIDENT_COUNTY_NAME_E_SCENE_21 = factor(
        SCENE_INCIDENT_COUNTY_NAME_E_SCENE_21
      )
    ),
  response_table = response_table,
  arrest_table = arrest_table,
  procedures_table = procedures_table,
  vitals_table = vitals_table,
  erecord_01_col = FACT_INCIDENT_PK,
  incident_date_col = INCIDENT_DATE,
  patient_DOB_col = PATIENT_DATE_OF_BIRTH_E_PATIENT_17,
  epatient_15_col = PATIENT_AGE_E_PATIENT_15,
  epatient_16_col = PATIENT_AGE_UNITS_E_PATIENT_16,
  earrest_01_col = CARDIAC_ARREST_DURING_EMS_EVENT_WITH_CODE_E_ARREST_01,
  eresponse_05_col = RESPONSE_TYPE_OF_SERVICE_REQUESTED_WITH_CODE_E_RESPONSE_05,
  evitals_01_col = VITALS_SIGNS_TAKEN_DATE_TIME_E_VITALS_01,
  evitals_12_col = VITALS_PULSE_OXIMETRY_E_VITALS_12,
  eprocedures_01_col = PROCEDURE_PERFORMED_DATE_TIME_E_PROCEDURES_01,
  eprocedures_02_col = PROCEDURE_PERFORMED_PRIOR_TO_EMS_CARE_E_PROCEDURES_02,
  eprocedures_03_col = PROCEDURE_PERFORMED_DESCRIPTION_AND_CODE_E_PROCEDURES_03,
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

# start time counties / years
tictoc::tic(msg = "airway_05_result_counties_years")

#### counties years ----
airway_05_result_counties_years <- mirai::mirai_map(
  report_years,
  \(yr, ps, rsp, arr, pro, vit) {
    # parallelize over years
    ps_y <- ps |> dplyr::filter(INCIDENT_YEAR == yr)
    rsp_y <- rsp |> dplyr::filter(INCIDENT_YEAR == yr)
    arr_y <- arr |> dplyr::filter(INCIDENT_YEAR == yr)
    pro_y <- pro |> dplyr::filter(INCIDENT_YEAR == yr)
    vit_y <- vit |> dplyr::filter(INCIDENT_YEAR == yr)

    # run the function
    nemsqar::airway_05(
      df = NULL,
      patient_scene_table = ps_y,
      response_table = rsp_y,
      arrest_table = arr_y,
      procedures_table = pro_y,
      vitals_table = vit_y,
      erecord_01_col = FACT_INCIDENT_PK,
      incident_date_col = INCIDENT_DATE,
      patient_DOB_col = PATIENT_DATE_OF_BIRTH_E_PATIENT_17,
      epatient_15_col = PATIENT_AGE_E_PATIENT_15,
      epatient_16_col = PATIENT_AGE_UNITS_E_PATIENT_16,
      earrest_01_col = CARDIAC_ARREST_DURING_EMS_EVENT_WITH_CODE_E_ARREST_01,
      eresponse_05_col = RESPONSE_TYPE_OF_SERVICE_REQUESTED_WITH_CODE_E_RESPONSE_05,
      evitals_01_col = VITALS_SIGNS_TAKEN_DATE_TIME_E_VITALS_01,
      evitals_12_col = VITALS_PULSE_OXIMETRY_E_VITALS_12,
      eprocedures_01_col = PROCEDURE_PERFORMED_DATE_TIME_E_PROCEDURES_01,
      eprocedures_02_col = PROCEDURE_PERFORMED_PRIOR_TO_EMS_CARE_E_PROCEDURES_02,
      eprocedures_03_col = PROCEDURE_PERFORMED_DESCRIPTION_AND_CODE_E_PROCEDURES_03,
      confidence_interval = TRUE,
      method = "w",
      conf.level = 0.95,
      correct = TRUE,
      .by = c(INCIDENT_YEAR, SCENE_INCIDENT_COUNTY_NAME_E_SCENE_21)
    )
  },
  .args = list(
    ps = patient_scene_table_s,
    rsp = response_table_s,
    arr = arrest_table_s,
    pro = procedures_table_s,
    vit = vitals_table_s
  )
)[.progress] |>
  dplyr::bind_rows() |>
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

# total time
time_result_counties_years <- tictoc::toc()

### results overall --------------------------------------------------------

#### overall ----
airway_05_result_overall <- nemsqar::airway_05(
  df = NULL,
  patient_scene_table = patient_scene_table,
  response_table = response_table,
  arrest_table = arrest_table,
  procedures_table = procedures_table,
  vitals_table = vitals_table,
  erecord_01_col = FACT_INCIDENT_PK,
  incident_date_col = INCIDENT_DATE,
  patient_DOB_col = PATIENT_DATE_OF_BIRTH_E_PATIENT_17,
  epatient_15_col = PATIENT_AGE_E_PATIENT_15,
  epatient_16_col = PATIENT_AGE_UNITS_E_PATIENT_16,
  earrest_01_col = CARDIAC_ARREST_DURING_EMS_EVENT_WITH_CODE_E_ARREST_01,
  eresponse_05_col = RESPONSE_TYPE_OF_SERVICE_REQUESTED_WITH_CODE_E_RESPONSE_05,
  evitals_01_col = VITALS_SIGNS_TAKEN_DATE_TIME_E_VITALS_01,
  evitals_12_col = VITALS_PULSE_OXIMETRY_E_VITALS_12,
  eprocedures_01_col = PROCEDURE_PERFORMED_DATE_TIME_E_PROCEDURES_01,
  eprocedures_02_col = PROCEDURE_PERFORMED_PRIOR_TO_EMS_CARE_E_PROCEDURES_02,
  eprocedures_03_col = PROCEDURE_PERFORMED_DESCRIPTION_AND_CODE_E_PROCEDURES_03,
  confidence_interval = TRUE,
  method = "w",
  conf.level = 0.95,
  correct = TRUE
)

### results services -------------------------------------------------------

# get start time
tictoc::tic(msg = "airway_05_result_services")

#### services ----
airway_05_result_services <- mirai::mirai_map(
  report_years,
  \(yr, ps, rsp, arr, pro, vit) {
    # parallelize over years
    ps_y <- ps |> dplyr::filter(INCIDENT_YEAR == yr)
    rsp_y <- rsp |> dplyr::filter(INCIDENT_YEAR == yr)
    arr_y <- arr |> dplyr::filter(INCIDENT_YEAR == yr)
    pro_y <- pro |> dplyr::filter(INCIDENT_YEAR == yr)
    vit_y <- vit |> dplyr::filter(INCIDENT_YEAR == yr)

    # run the function
    nemsqar::airway_05(
      df = NULL,
      patient_scene_table = ps_y,
      response_table = rsp_y,
      arrest_table = arr_y,
      procedures_table = pro_y,
      vitals_table = vit_y,
      erecord_01_col = FACT_INCIDENT_PK,
      incident_date_col = INCIDENT_DATE,
      patient_DOB_col = PATIENT_DATE_OF_BIRTH_E_PATIENT_17,
      epatient_15_col = PATIENT_AGE_E_PATIENT_15,
      epatient_16_col = PATIENT_AGE_UNITS_E_PATIENT_16,
      earrest_01_col = CARDIAC_ARREST_DURING_EMS_EVENT_WITH_CODE_E_ARREST_01,
      eresponse_05_col = RESPONSE_TYPE_OF_SERVICE_REQUESTED_WITH_CODE_E_RESPONSE_05,
      evitals_01_col = VITALS_SIGNS_TAKEN_DATE_TIME_E_VITALS_01,
      evitals_12_col = VITALS_PULSE_OXIMETRY_E_VITALS_12,
      eprocedures_01_col = PROCEDURE_PERFORMED_DATE_TIME_E_PROCEDURES_01,
      eprocedures_02_col = PROCEDURE_PERFORMED_PRIOR_TO_EMS_CARE_E_PROCEDURES_02,
      eprocedures_03_col = PROCEDURE_PERFORMED_DESCRIPTION_AND_CODE_E_PROCEDURES_03,
      confidence_interval = TRUE,
      method = "w",
      conf.level = 0.95,
      correct = TRUE,
      .by = c(INCIDENT_YEAR, AGENCY_NAME_D_AGENCY_03)
    )
  },
  .args = list(
    ps = patient_scene_table_s,
    rsp = response_table_s,
    arr = arrest_table_s,
    pro = procedures_table_s,
    vit = vitals_table_s
  )
)[.progress] |>
  dplyr::bind_rows() |>
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

# total time
time_result_services <- tictoc::toc()

# unburden daemons
mirai::daemons(n = 0)

# EXPORT -----------------------------------------------------------------

## population exports #########################################################

export_nemsqa_data(
  pattern = "airway_05_pop",
  measure = "Airway-05",
  folder = "population"
)

## results exports ############################################################

export_nemsqa_data(
  pattern = "airway_05_result",
  measure = "Airway-05",
  folder = "result"
)

## missingness exports ########################################################

export_nemsqa_data(
  pattern = "airway_05_missingness|airway_05_missings",
  measure = "Airway-05",
  folder = "missings"
)
