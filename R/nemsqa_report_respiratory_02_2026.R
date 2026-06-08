# IOWA NEMSQA REPORT RESPIRATORY-02 2026 ------------------------------------

###_____________________________________________________________________________
# this script will contain all reporting calculations for Respiratory-02
# use nemsqa_report_prep_2025.R to get critical functions into memory
###_____________________________________________________________________________
# assume that nemsqa_report_prep_2025.R was already ran to load needed packages
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

## medications tables -----------------------------------------------------
medications_table <- load_nemsqa_parallel(
  table = "medications",
  year = 2021:2025,
  cores = 13
)

# share the medications table
medications_table_s <- mori::share(medications_table)

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

## Respiratory-02 =============================================================

## respiratory-02 populations ##################################################

### get respiratory-02 populations over all years 2021-2025 ------------

respiratory_02_pop <- nemsqar::respiratory_02_population(
  df = NULL,
  patient_scene_table = patient_scene_table,
  response_table = response_table,
  vitals_table = vitals_table,
  medications_table = medications_table,
  procedures_table = procedures_table,
  erecord_01_col = INCIDENT_PATIENT_CARE_REPORT_NUMBER_PCR_E_RECORD_01,
  incident_date_col = INCIDENT_DATE,
  patient_DOB_col = PATIENT_DATE_OF_BIRTH_E_PATIENT_17,
  epatient_15_col = PATIENT_AGE_E_PATIENT_15,
  epatient_16_col = PATIENT_AGE_UNITS_E_PATIENT_16,
  eresponse_05_col = RESPONSE_TYPE_OF_SERVICE_REQUESTED_WITH_CODE_E_RESPONSE_05,
  evitals_12_col = VITALS_PULSE_OXIMETRY_E_VITALS_12,
  emedications_03_col = MEDICATION_GIVEN_OR_ADMINISTERED_DESCRIPTION_AND_RXCUI_CODE_E_MEDICATIONS_03,
  eprocedures_03_col = PROCEDURE_PERFORMED_DESCRIPTION_AND_CODE_E_PROCEDURES_03
)

#### population results for 2021-2025 ----
respiratory_02_pop_filter_process <- respiratory_02_pop$filter_process

#### population results for 2021-2025 ----
respiratory_02_missings <- respiratory_02_pop$missingness

# set up daemons
mirai::daemons(n = 13)

### get respiratory_02 population data for each year using mirai and mori ----

# track progress
tictoc::tic(msg = "respiratory_02_pop_years_init")

respiratory_02_pop_years_init <- mirai::mirai_map(
  report_years,
  \(yr, ps, rsp, vit, med, pro) {
    # parallelize by year
    ps_y <- ps |> dplyr::filter(INCIDENT_YEAR == yr)
    rsp_y <- rsp |> dplyr::filter(INCIDENT_YEAR == yr)
    med_y <- med |> dplyr::filter(INCIDENT_YEAR == yr)
    vit_y <- vit |> dplyr::filter(INCIDENT_YEAR == yr)
    pro_y <- pro |> dplyr::filter(INCIDENT_YEAR == yr)

    # run function in parallel
    nemsqar::respiratory_02_population(
      df = NULL,
      patient_scene_table = ps_y,
      response_table = rsp_y,
      vitals_table = vit_y,
      medications_table = med_y,
      procedures_table = pro_y,
      erecord_01_col = FACT_INCIDENT_PK,
      incident_date_col = INCIDENT_DATE,
      patient_DOB_col = PATIENT_DATE_OF_BIRTH_E_PATIENT_17,
      epatient_15_col = PATIENT_AGE_E_PATIENT_15,
      epatient_16_col = PATIENT_AGE_UNITS_E_PATIENT_16,
      eresponse_05_col = RESPONSE_TYPE_OF_SERVICE_REQUESTED_WITH_CODE_E_RESPONSE_05,
      evitals_12_col = VITALS_PULSE_OXIMETRY_E_VITALS_12,
      emedications_03_col = MEDICATION_GIVEN_OR_ADMINISTERED_DESCRIPTION_AND_RXCUI_CODE_E_MEDICATIONS_03,
      eprocedures_03_col = PROCEDURE_PERFORMED_DESCRIPTION_AND_CODE_E_PROCEDURES_03
    )
  },
  .args = list(
    ps = patient_scene_table_s,
    rsp = response_table_s,
    vit = vitals_table_s,
    med = medications_table_s,
    pro = procedures_table_s
  )
)[.progress]

# Get total time
time <- tictoc::toc()

#### append years to the population files ----
respiratory_02_pop_years <- add_year_to_nested(
  x = respiratory_02_pop_years_init,
  file = "filter_process",
  years = 2021:2025
)

#### append years to the missingness files ----
respiratory_02_missingness_years <- add_year_to_nested(
  x = respiratory_02_pop_years_init,
  file = "missingness",
  years = 2021:2025
)

#### plot population trends over time ----
respiratory_02_pop_years |>
  plot_nemsqa_pops(
    type = "col",
    wrap_width = 25,
    plot_title = "Respiratory-02"
  )

## respiratory-02 results #####################################################

### results years ----------------------------------------------------------

# benchmark time - start
tictoc::tic(msg = "respiratory_02_result_year")

#### year ----
respiratory_02_result_year <- mirai::mirai_map(
  report_years,
  \(yr, ps, rsp, vit, med, pro) {
    # parallelize by year
    ps_y <- ps |> dplyr::filter(INCIDENT_YEAR == yr)
    rsp_y <- rsp |> dplyr::filter(INCIDENT_YEAR == yr)
    med_y <- med |> dplyr::filter(INCIDENT_YEAR == yr)
    vit_y <- vit |> dplyr::filter(INCIDENT_YEAR == yr)
    pro_y <- pro |> dplyr::filter(INCIDENT_YEAR == yr)

    # run function in parallel
    nemsqar::respiratory_02(
      df = NULL,
      patient_scene_table = ps_y,
      response_table = rsp_y,
      vitals_table = vit_y,
      medications_table = med_y,
      procedures_table = pro_y,
      erecord_01_col = FACT_INCIDENT_PK,
      incident_date_col = INCIDENT_DATE,
      patient_DOB_col = PATIENT_DATE_OF_BIRTH_E_PATIENT_17,
      epatient_15_col = PATIENT_AGE_E_PATIENT_15,
      epatient_16_col = PATIENT_AGE_UNITS_E_PATIENT_16,
      eresponse_05_col = RESPONSE_TYPE_OF_SERVICE_REQUESTED_WITH_CODE_E_RESPONSE_05,
      evitals_12_col = VITALS_PULSE_OXIMETRY_E_VITALS_12,
      emedications_03_col = MEDICATION_GIVEN_OR_ADMINISTERED_DESCRIPTION_AND_RXCUI_CODE_E_MEDICATIONS_03,
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
    vit = vitals_table_s,
    med = medications_table_s,
    pro = procedures_table_s
  )
)[.progress] |>
  dplyr::bind_rows()

# benchmark time diff
time_result_year <- tictoc::toc()

### results regions and years ----------------------------------------------

# benchmark time - start
tictoc::tic(msg = "respiratory_02_result_regions_years")

#### regions and years ----
respiratory_02_result_regions_years <- mirai::mirai_map(
  report_years,
  \(yr, ps, rsp, vit, med, pro) {
    # parallelize by year
    ps_y <- ps |> dplyr::filter(INCIDENT_YEAR == yr)
    rsp_y <- rsp |> dplyr::filter(INCIDENT_YEAR == yr)
    med_y <- med |> dplyr::filter(INCIDENT_YEAR == yr)
    vit_y <- vit |> dplyr::filter(INCIDENT_YEAR == yr)
    pro_y <- pro |> dplyr::filter(INCIDENT_YEAR == yr)

    # run function in parallel
    nemsqar::respiratory_02(
      df = NULL,
      patient_scene_table = ps_y,
      response_table = rsp_y,
      vitals_table = vit_y,
      medications_table = med_y,
      procedures_table = pro_y,
      erecord_01_col = FACT_INCIDENT_PK,
      incident_date_col = INCIDENT_DATE,
      patient_DOB_col = PATIENT_DATE_OF_BIRTH_E_PATIENT_17,
      epatient_15_col = PATIENT_AGE_E_PATIENT_15,
      epatient_16_col = PATIENT_AGE_UNITS_E_PATIENT_16,
      eresponse_05_col = RESPONSE_TYPE_OF_SERVICE_REQUESTED_WITH_CODE_E_RESPONSE_05,
      evitals_12_col = VITALS_PULSE_OXIMETRY_E_VITALS_12,
      emedications_03_col = MEDICATION_GIVEN_OR_ADMINISTERED_DESCRIPTION_AND_RXCUI_CODE_E_MEDICATIONS_03,
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
    vit = vitals_table_s,
    med = medications_table_s,
    pro = procedures_table_s
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

# benchmark time diff
time_result_regions_year <- tictoc::toc()

### results regions --------------------------------------------------------

# get start time
tictoc::tic(msg = "respiratory_02_result_regions")

#### regions ----
respiratory_02_result_regions <- mirai::mirai_map(
  report_regions,
  \(reg, ps, rsp, vit, med, pro) {
    # parallelize by year
    ps_r <- ps |> dplyr::filter(`Region: Preparedness` == reg)

    # run function in parallel
    nemsqar::respiratory_02(
      df = NULL,
      patient_scene_table = ps_r,
      response_table = rsp,
      vitals_table = vit,
      medications_table = med,
      procedures_table = pro,
      erecord_01_col = FACT_INCIDENT_PK,
      incident_date_col = INCIDENT_DATE,
      patient_DOB_col = PATIENT_DATE_OF_BIRTH_E_PATIENT_17,
      epatient_15_col = PATIENT_AGE_E_PATIENT_15,
      epatient_16_col = PATIENT_AGE_UNITS_E_PATIENT_16,
      eresponse_05_col = RESPONSE_TYPE_OF_SERVICE_REQUESTED_WITH_CODE_E_RESPONSE_05,
      evitals_12_col = VITALS_PULSE_OXIMETRY_E_VITALS_12,
      emedications_03_col = MEDICATION_GIVEN_OR_ADMINISTERED_DESCRIPTION_AND_RXCUI_CODE_E_MEDICATIONS_03,
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
    vit = vitals_table_s,
    med = medications_table_s,
    pro = procedures_table_s
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

# get total time
time_result_regions <- tictoc::toc()

### results counties -------------------------------------------------------

#### counties ----
respiratory_02_result_counties <- nemsqar::respiratory_02(
  df = NULL,
  patient_scene_table = patient_scene_table_s,
  response_table = response_table_s,
  vitals_table = vitals_table_s,
  medications_table = medications_table_s,
  procedures_table = procedures_table_s,
  erecord_01_col = FACT_INCIDENT_PK,
  incident_date_col = INCIDENT_DATE,
  patient_DOB_col = PATIENT_DATE_OF_BIRTH_E_PATIENT_17,
  epatient_15_col = PATIENT_AGE_E_PATIENT_15,
  epatient_16_col = PATIENT_AGE_UNITS_E_PATIENT_16,
  eresponse_05_col = RESPONSE_TYPE_OF_SERVICE_REQUESTED_WITH_CODE_E_RESPONSE_05,
  evitals_12_col = VITALS_PULSE_OXIMETRY_E_VITALS_12,
  emedications_03_col = MEDICATION_GIVEN_OR_ADMINISTERED_DESCRIPTION_AND_RXCUI_CODE_E_MEDICATIONS_03,
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

### results counties and years ---------------------------------------------

# get start time
tictoc::tic(msg = "respiratory_02_result_counties_years")

#### counties and years ----
respiratory_02_result_counties_years <- mirai::mirai_map(
  report_years,
  \(yr, ps, rsp, vit, med, pro) {
    # parallelize by year
    ps_y <- ps |> dplyr::filter(INCIDENT_YEAR == yr)
    rsp_y <- rsp |> dplyr::filter(INCIDENT_YEAR == yr)
    med_y <- med |> dplyr::filter(INCIDENT_YEAR == yr)
    vit_y <- vit |> dplyr::filter(INCIDENT_YEAR == yr)
    pro_y <- pro |> dplyr::filter(INCIDENT_YEAR == yr)

    # run function in parallel
    nemsqar::respiratory_02(
      df = NULL,
      patient_scene_table = ps_y,
      response_table = rsp_y,
      vitals_table = vit_y,
      medications_table = med_y,
      procedures_table = pro_y,
      erecord_01_col = FACT_INCIDENT_PK,
      incident_date_col = INCIDENT_DATE,
      patient_DOB_col = PATIENT_DATE_OF_BIRTH_E_PATIENT_17,
      epatient_15_col = PATIENT_AGE_E_PATIENT_15,
      epatient_16_col = PATIENT_AGE_UNITS_E_PATIENT_16,
      eresponse_05_col = RESPONSE_TYPE_OF_SERVICE_REQUESTED_WITH_CODE_E_RESPONSE_05,
      evitals_12_col = VITALS_PULSE_OXIMETRY_E_VITALS_12,
      emedications_03_col = MEDICATION_GIVEN_OR_ADMINISTERED_DESCRIPTION_AND_RXCUI_CODE_E_MEDICATIONS_03,
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
    vit = vitals_table_s,
    med = medications_table_s,
    pro = procedures_table_s
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

# get total time
time_result_counties_years <- tictoc::toc()

### results overall --------------------------------------------------------

#### overall ----
respiratory_02_result_overall <- nemsqar::respiratory_02(
  df = NULL,
  patient_scene_table = patient_scene_table,
  response_table = response_table,
  vitals_table = vitals_table,
  medications_table = medications_table,
  procedures_table = procedures_table,
  erecord_01_col = FACT_INCIDENT_PK,
  incident_date_col = INCIDENT_DATE,
  patient_DOB_col = PATIENT_DATE_OF_BIRTH_E_PATIENT_17,
  epatient_15_col = PATIENT_AGE_E_PATIENT_15,
  epatient_16_col = PATIENT_AGE_UNITS_E_PATIENT_16,
  eresponse_05_col = RESPONSE_TYPE_OF_SERVICE_REQUESTED_WITH_CODE_E_RESPONSE_05,
  evitals_12_col = VITALS_PULSE_OXIMETRY_E_VITALS_12,
  emedications_03_col = MEDICATION_GIVEN_OR_ADMINISTERED_DESCRIPTION_AND_RXCUI_CODE_E_MEDICATIONS_03,
  eprocedures_03_col = PROCEDURE_PERFORMED_DESCRIPTION_AND_CODE_E_PROCEDURES_03,
  confidence_interval = TRUE,
  method = "w",
  conf.level = 0.95,
  correct = TRUE
)

### results services  ------------------------------------------------------

# get start time
tictoc::tic(msg = "respiratory_02_result_services")

#### services ----
respiratory_02_result_services <- mirai::mirai_map(
  report_years,
  \(yr, ps, rsp, vit, med, pro) {
    # parallelize by year
    ps_y <- ps |> dplyr::filter(INCIDENT_YEAR == yr)
    rsp_y <- rsp |> dplyr::filter(INCIDENT_YEAR == yr)
    med_y <- med |> dplyr::filter(INCIDENT_YEAR == yr)
    vit_y <- vit |> dplyr::filter(INCIDENT_YEAR == yr)
    pro_y <- pro |> dplyr::filter(INCIDENT_YEAR == yr)

    # run function in parallel
    nemsqar::respiratory_02(
      df = NULL,
      patient_scene_table = ps_y,
      response_table = rsp_y,
      vitals_table = vit_y,
      medications_table = med_y,
      procedures_table = pro_y,
      erecord_01_col = FACT_INCIDENT_PK,
      incident_date_col = INCIDENT_DATE,
      patient_DOB_col = PATIENT_DATE_OF_BIRTH_E_PATIENT_17,
      epatient_15_col = PATIENT_AGE_E_PATIENT_15,
      epatient_16_col = PATIENT_AGE_UNITS_E_PATIENT_16,
      eresponse_05_col = RESPONSE_TYPE_OF_SERVICE_REQUESTED_WITH_CODE_E_RESPONSE_05,
      evitals_12_col = VITALS_PULSE_OXIMETRY_E_VITALS_12,
      emedications_03_col = MEDICATION_GIVEN_OR_ADMINISTERED_DESCRIPTION_AND_RXCUI_CODE_E_MEDICATIONS_03,
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
    vit = vitals_table_s,
    med = medications_table_s,
    pro = procedures_table_s
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

# get total time
time_result_services <- tictoc::toc()

# unburden daemons
mirai::daemons(n = 0)

# EXPORT =====================================================================

## population exports #########################################################

export_nemsqa_data(
  pattern = "respiratory_02_pop",
  measure = "Respiratory-02",
  folder = "population"
)

## results exports ############################################################

export_nemsqa_data(
  pattern = "respiratory_02_result",
  measure = "Respiratory-02",
  folder = "result"
)

## missingness exports ########################################################

export_nemsqa_data(
  pattern = "respiratory_02_(?:missings|missingness)",
  measure = "Respiratory-02",
  folder = "missings"
)
