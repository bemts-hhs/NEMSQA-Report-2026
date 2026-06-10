# IOWA NEMSQA REPORT SEIZURE-02 2026 ------------------------------------

###_____________________________________________________________________________
# this script will contain all reporting calculations for Seizure-02
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

# share the patient table
patient_scene_table_s <- mori::share(patient_scene_table)

## response tables --------------------------------------------------------
response_table <- load_nemsqa_parallel(
  table = "response",
  years = 2021:2025,
  cores = 13
)

# share the response table
response_table_s <- mori::share(response_table)

## situation tables -------------------------------------------------------
# set up situation table for manipulations
situation_table <- load_nemsqa_parallel(
  table = "situation",
  years = 2021:2025,
  cores = 13
)

# Share the situation_table
situation_table_s <- mori::share(situation_table)

# CALCULATIONS ---------------------------------------------------------------

# remove intermediary table objects
rm(list = ls(pattern = "_table$"))

# garbage collection
gc()

## Seizure-02 =================================================================

## seizure-02 populations #####################################################

### get seizure_02 populations over all years 2021-2025 --------------------

seizure_02_pop <- nemsqar::seizure_02_population(
  df = NULL,
  patient_scene_table = patient_scene_table_s,
  response_table = response_table_s,
  situation_table = situation_table_s,
  medications_table = medications_table_s,
  erecord_01_col = FACT_INCIDENT_PK,
  incident_date_col = INCIDENT_DATE,
  patient_DOB_col = PATIENT_DATE_OF_BIRTH_E_PATIENT_17,
  epatient_15_col = PATIENT_AGE_E_PATIENT_15,
  epatient_16_col = PATIENT_AGE_UNITS_E_PATIENT_16,
  eresponse_05_col = RESPONSE_TYPE_OF_SERVICE_REQUESTED_WITH_CODE_E_RESPONSE_05,
  esituation_11_col = SITUATION_PROVIDER_PRIMARY_IMPRESSION_CODE_AND_DESCRIPTION_E_SITUATION_11,
  esituation_12_col = SITUATION_PROVIDER_SECONDARY_IMPRESSION_DESCRIPTION_AND_CODE_LIST_E_SITUATION_12,
  emedications_03_col = MEDICATION_GIVEN_OR_ADMINISTERED_DESCRIPTION_AND_RXCUI_CODE_E_MEDICATIONS_03
)

#### population results for 2021-2025 ----
seizure_02_pop_filter_process <- seizure_02_pop$filter_process

#### population missingness for 2021-2025 ----
seizure_02_missings <- seizure_02_pop$missingness

# set up daemons
mirai::daemons(n = 13)

### get seizure_02 population data for each year using mirai and mori ------

# track progress
tictoc::tic(msg = "seizure_02_pop_years_init")

seizure_02_pop_years_init <- mirai::mirai_map(
  report_years,
  \(yr, ps, rsp, sit, med) {
    # parallelize by year
    ps_y <- ps |> dplyr::filter(INCIDENT_YEAR == yr)
    rsp_y <- rsp |> dplyr::filter(INCIDENT_YEAR == yr)
    sit_y <- sit |> dplyr::filter(INCIDENT_YEAR == yr)
    med_y <- med |> dplyr::filter(INCIDENT_YEAR == yr)

    # run function in parallel
    nemsqar::seizure_02_population(
      df = NULL,
      patient_scene_table = ps_y,
      response_table = rsp_y,
      situation_table = sit_y,
      medications_table = med_y,
      erecord_01_col = FACT_INCIDENT_PK,
      incident_date_col = INCIDENT_DATE,
      patient_DOB_col = PATIENT_DATE_OF_BIRTH_E_PATIENT_17,
      epatient_15_col = PATIENT_AGE_E_PATIENT_15,
      epatient_16_col = PATIENT_AGE_UNITS_E_PATIENT_16,
      eresponse_05_col = RESPONSE_TYPE_OF_SERVICE_REQUESTED_WITH_CODE_E_RESPONSE_05,
      esituation_11_col = SITUATION_PROVIDER_PRIMARY_IMPRESSION_CODE_AND_DESCRIPTION_E_SITUATION_11,
      esituation_12_col = SITUATION_PROVIDER_SECONDARY_IMPRESSION_DESCRIPTION_AND_CODE_LIST_E_SITUATION_12,
      emedications_03_col = MEDICATION_GIVEN_OR_ADMINISTERED_DESCRIPTION_AND_RXCUI_CODE_E_MEDICATIONS_03
    )
  },
  .args = list(
    ps = patient_scene_table_s,
    rsp = response_table_s,
    sit = situation_table_s,
    med = medications_table_s
  )
)[.progress]

# Get total time
time <- tictoc::toc()

# unburden daemons
mirai::daemons(n = 0)

#### append years to the population files ----
seizure_02_pop_years <- add_year_to_nested(
  x = seizure_02_pop_years_init,
  file = "filter_process",
  years = 2021:2025
)

#### append years to the missingness files ----
seizure_02_missingness_years <- add_year_to_nested(
  x = seizure_02_pop_years_init,
  file = "missingness",
  years = 2021:2025
)


# plot population trends over time
seizure_02_pop_years |>
  plot_nemsqa_pops(
    type = "col",
    wrap_width = 25,
    plot_title = "Seizure-02"
  )

## seizure-02 results #########################################################

### results years ----------------------------------------------------------

# set up daemons
mirai::daemons(n = 13)

# get start time
tictoc::tic(msg = "seizure_02_result_year")

#### year ----
seizure_02_result_year <- mirai::mirai_map(
  report_years,
  \(yr, ps, rsp, sit, med) {
    # parallelize by year
    ps_y <- ps |> dplyr::filter(INCIDENT_YEAR == yr)
    rsp_y <- rsp |> dplyr::filter(INCIDENT_YEAR == yr)
    sit_y <- sit |> dplyr::filter(INCIDENT_YEAR == yr)
    med_y <- med |> dplyr::filter(INCIDENT_YEAR == yr)

    # run function in parallel
    nemsqar::seizure_02(
      df = NULL,
      patient_scene_table = ps_y,
      response_table = rsp_y,
      situation_table = sit_y,
      medications_table = med_y,
      erecord_01_col = INCIDENT_PATIENT_CARE_REPORT_NUMBER_PCR_E_RECORD_01,
      incident_date_col = INCIDENT_DATE,
      patient_DOB_col = PATIENT_DATE_OF_BIRTH_E_PATIENT_17,
      epatient_15_col = PATIENT_AGE_E_PATIENT_15,
      epatient_16_col = PATIENT_AGE_UNITS_E_PATIENT_16,
      eresponse_05_col = RESPONSE_TYPE_OF_SERVICE_REQUESTED_WITH_CODE_E_RESPONSE_05,
      esituation_11_col = SITUATION_PROVIDER_PRIMARY_IMPRESSION_CODE_AND_DESCRIPTION_E_SITUATION_11,
      esituation_12_col = SITUATION_PROVIDER_SECONDARY_IMPRESSION_DESCRIPTION_AND_CODE_LIST_E_SITUATION_12,
      emedications_03_col = MEDICATION_GIVEN_OR_ADMINISTERED_DESCRIPTION_AND_RXCUI_CODE_E_MEDICATIONS_03,
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
    sit = situation_table_s,
    med = medications_table_s
  )
)[.progress] |>
  dplyr::bind_rows()

# total time
time_result_year <- tictoc::toc()

# unburden daemons
mirai::daemons(n = 0)

### results regions and years ----------------------------------------------

# set up daemons
mirai::daemons(n = 13)

# get start time
tictoc::tic(msg = "seizure_02_result_regions_year")

#### regions and years ----
seizure_02_result_regions_years <- mirai::mirai_map(
  report_years,
  \(yr, ps, rsp, sit, med) {
    # parallelize by year
    ps_y <- ps |> dplyr::filter(INCIDENT_YEAR == yr)
    rsp_y <- rsp |> dplyr::filter(INCIDENT_YEAR == yr)
    sit_y <- sit |> dplyr::filter(INCIDENT_YEAR == yr)
    med_y <- med |> dplyr::filter(INCIDENT_YEAR == yr)

    # run function in parallel
    nemsqar::seizure_02(
      df = NULL,
      patient_scene_table = ps_y,
      response_table = rsp_y,
      situation_table = sit_y,
      medications_table = med_y,
      erecord_01_col = INCIDENT_PATIENT_CARE_REPORT_NUMBER_PCR_E_RECORD_01,
      incident_date_col = INCIDENT_DATE,
      patient_DOB_col = PATIENT_DATE_OF_BIRTH_E_PATIENT_17,
      epatient_15_col = PATIENT_AGE_E_PATIENT_15,
      epatient_16_col = PATIENT_AGE_UNITS_E_PATIENT_16,
      eresponse_05_col = RESPONSE_TYPE_OF_SERVICE_REQUESTED_WITH_CODE_E_RESPONSE_05,
      esituation_11_col = SITUATION_PROVIDER_PRIMARY_IMPRESSION_CODE_AND_DESCRIPTION_E_SITUATION_11,
      esituation_12_col = SITUATION_PROVIDER_SECONDARY_IMPRESSION_DESCRIPTION_AND_CODE_LIST_E_SITUATION_12,
      emedications_03_col = MEDICATION_GIVEN_OR_ADMINISTERED_DESCRIPTION_AND_RXCUI_CODE_E_MEDICATIONS_03,
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
    sit = situation_table_s,
    med = medications_table_s
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

# unburden daemons
mirai::daemons(n = 0)

### results regions --------------------------------------------------------

#### regions ----
seizure_02_result_regions <- nemsqar::seizure_02(
  df = NULL,
  patient_scene_table = patient_scene_table_s,
  response_table = response_table_s,
  situation_table = situation_table_s,
  medications_table = medications_table_s,
  erecord_01_col = INCIDENT_PATIENT_CARE_REPORT_NUMBER_PCR_E_RECORD_01,
  incident_date_col = INCIDENT_DATE,
  patient_DOB_col = PATIENT_DATE_OF_BIRTH_E_PATIENT_17,
  epatient_15_col = PATIENT_AGE_E_PATIENT_15,
  epatient_16_col = PATIENT_AGE_UNITS_E_PATIENT_16,
  eresponse_05_col = RESPONSE_TYPE_OF_SERVICE_REQUESTED_WITH_CODE_E_RESPONSE_05,
  esituation_11_col = SITUATION_PROVIDER_PRIMARY_IMPRESSION_CODE_AND_DESCRIPTION_E_SITUATION_11,
  esituation_12_col = SITUATION_PROVIDER_SECONDARY_IMPRESSION_DESCRIPTION_AND_CODE_LIST_E_SITUATION_12,
  emedications_03_col = MEDICATION_GIVEN_OR_ADMINISTERED_DESCRIPTION_AND_RXCUI_CODE_E_MEDICATIONS_03,
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
seizure_02_result_counties <- nemsqar::seizure_02(
  df = NULL,
  patient_scene_table = patient_scene_table_s,
  response_table = response_table_s,
  situation_table = situation_table_s,
  medications_table = medications_table_s,
  erecord_01_col = INCIDENT_PATIENT_CARE_REPORT_NUMBER_PCR_E_RECORD_01,
  incident_date_col = INCIDENT_DATE,
  patient_DOB_col = PATIENT_DATE_OF_BIRTH_E_PATIENT_17,
  epatient_15_col = PATIENT_AGE_E_PATIENT_15,
  epatient_16_col = PATIENT_AGE_UNITS_E_PATIENT_16,
  eresponse_05_col = RESPONSE_TYPE_OF_SERVICE_REQUESTED_WITH_CODE_E_RESPONSE_05,
  esituation_11_col = SITUATION_PROVIDER_PRIMARY_IMPRESSION_CODE_AND_DESCRIPTION_E_SITUATION_11,
  esituation_12_col = SITUATION_PROVIDER_SECONDARY_IMPRESSION_DESCRIPTION_AND_CODE_LIST_E_SITUATION_12,
  emedications_03_col = MEDICATION_GIVEN_OR_ADMINISTERED_DESCRIPTION_AND_RXCUI_CODE_E_MEDICATIONS_03,
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

# set up daemons
mirai::daemons(n = 13)

# start time counties / years
tictoc::tic(msg = "seizure_02_result_counties_years")

#### counties years ----
seizure_02_result_counties_years <- mirai::mirai_map(
  report_years,
  \(yr, ps, rsp, sit, med) {
    # parallelize by year
    ps_y <- ps |> dplyr::filter(INCIDENT_YEAR == yr)
    rsp_y <- rsp |> dplyr::filter(INCIDENT_YEAR == yr)
    sit_y <- sit |> dplyr::filter(INCIDENT_YEAR == yr)
    med_y <- med |> dplyr::filter(INCIDENT_YEAR == yr)

    # run function in parallel
    nemsqar::seizure_02(
      df = NULL,
      patient_scene_table = ps_y,
      response_table = rsp_y,
      situation_table = sit_y,
      medications_table = med_y,
      erecord_01_col = INCIDENT_PATIENT_CARE_REPORT_NUMBER_PCR_E_RECORD_01,
      incident_date_col = INCIDENT_DATE,
      patient_DOB_col = PATIENT_DATE_OF_BIRTH_E_PATIENT_17,
      epatient_15_col = PATIENT_AGE_E_PATIENT_15,
      epatient_16_col = PATIENT_AGE_UNITS_E_PATIENT_16,
      eresponse_05_col = RESPONSE_TYPE_OF_SERVICE_REQUESTED_WITH_CODE_E_RESPONSE_05,
      esituation_11_col = SITUATION_PROVIDER_PRIMARY_IMPRESSION_CODE_AND_DESCRIPTION_E_SITUATION_11,
      esituation_12_col = SITUATION_PROVIDER_SECONDARY_IMPRESSION_DESCRIPTION_AND_CODE_LIST_E_SITUATION_12,
      emedications_03_col = MEDICATION_GIVEN_OR_ADMINISTERED_DESCRIPTION_AND_RXCUI_CODE_E_MEDICATIONS_03,
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
    sit = situation_table_s,
    med = medications_table_s
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

# unburden daemons
mirai::daemons(n = 0)

### results overall --------------------------------------------------------

#### overall ----
seizure_02_result_overall <- nemsqar::seizure_02(
  df = NULL,
  patient_scene_table = patient_scene_table_s,
  response_table = response_table_s,
  situation_table = situation_table_s,
  medications_table = medications_table_s,
  erecord_01_col = INCIDENT_PATIENT_CARE_REPORT_NUMBER_PCR_E_RECORD_01,
  incident_date_col = INCIDENT_DATE,
  patient_DOB_col = PATIENT_DATE_OF_BIRTH_E_PATIENT_17,
  epatient_15_col = PATIENT_AGE_E_PATIENT_15,
  epatient_16_col = PATIENT_AGE_UNITS_E_PATIENT_16,
  eresponse_05_col = RESPONSE_TYPE_OF_SERVICE_REQUESTED_WITH_CODE_E_RESPONSE_05,
  esituation_11_col = SITUATION_PROVIDER_PRIMARY_IMPRESSION_CODE_AND_DESCRIPTION_E_SITUATION_11,
  esituation_12_col = SITUATION_PROVIDER_SECONDARY_IMPRESSION_DESCRIPTION_AND_CODE_LIST_E_SITUATION_12,
  emedications_03_col = MEDICATION_GIVEN_OR_ADMINISTERED_DESCRIPTION_AND_RXCUI_CODE_E_MEDICATIONS_03,
  confidence_interval = TRUE,
  method = "w",
  conf.level = 0.95,
  correct = TRUE
)

### results services -------------------------------------------------------

# set up daemons
mirai::daemons(n = 13)

# get start time
tictoc::tic(msg = "seizure_02_result_services")

#### services ----
seizure_02_result_services <- mirai::mirai_map(
  report_years,
  \(yr, ps, rsp, sit, med) {
    # parallelize by year
    ps_y <- ps |> dplyr::filter(INCIDENT_YEAR == yr)
    rsp_y <- rsp |> dplyr::filter(INCIDENT_YEAR == yr)
    sit_y <- sit |> dplyr::filter(INCIDENT_YEAR == yr)
    med_y <- med |> dplyr::filter(INCIDENT_YEAR == yr)

    # run function in parallel
    nemsqar::seizure_02(
      df = NULL,
      patient_scene_table = ps_y,
      response_table = rsp_y,
      situation_table = sit_y,
      medications_table = med_y,
      erecord_01_col = INCIDENT_PATIENT_CARE_REPORT_NUMBER_PCR_E_RECORD_01,
      incident_date_col = INCIDENT_DATE,
      patient_DOB_col = PATIENT_DATE_OF_BIRTH_E_PATIENT_17,
      epatient_15_col = PATIENT_AGE_E_PATIENT_15,
      epatient_16_col = PATIENT_AGE_UNITS_E_PATIENT_16,
      eresponse_05_col = RESPONSE_TYPE_OF_SERVICE_REQUESTED_WITH_CODE_E_RESPONSE_05,
      esituation_11_col = SITUATION_PROVIDER_PRIMARY_IMPRESSION_CODE_AND_DESCRIPTION_E_SITUATION_11,
      esituation_12_col = SITUATION_PROVIDER_SECONDARY_IMPRESSION_DESCRIPTION_AND_CODE_LIST_E_SITUATION_12,
      emedications_03_col = MEDICATION_GIVEN_OR_ADMINISTERED_DESCRIPTION_AND_RXCUI_CODE_E_MEDICATIONS_03,
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
    sit = situation_table_s,
    med = medications_table_s
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

# EXPORT =====================================================================

## population exports #########################################################

export_nemsqa_data(
  pattern = "seizure_02_pop",
  measure = "Seizure-02",
  folder = "population"
)

## results exports ############################################################

export_nemsqa_data(
  pattern = "seizure_02_result",
  measure = "Seizure-02",
  folder = "result"
)

## missingness exports ########################################################

export_nemsqa_data(
  pattern = "seizure_02_(?:missings|missingness)",
  measure = "Seizure-02",
  folder = "missings"
)
