# IOWA NEMSQA REPORT STROKE-01 2026 ------------------------------------

###_____________________________________________________________________________
# this script will contain all reporting calculations for Stroke-01
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

## vitals tables ----------------------------------------------------------
vitals_table <- load_nemsqa_parallel(
  table = "vitals",
  years = 2021:2025,
  cores = 13
)

# share the vitals table
vitals_table_s <- mori::share(vitals_table)

# CALCULATIONS ---------------------------------------------------------------

# remove intermediary table objects
rm(list = ls(pattern = "_table$"))

# garbage collection
gc()

## Stroke-01 ==================================================================

## stroke-01 populations ######################################################

### get stroke_01 populations over all years 2021-2025 ---------------------

stroke_01_pop <- nemsqar::stroke_01_population(
  df = NULL,
  patient_scene_table = patient_scene_table_s,
  response_table = response_table_s,
  situation_table = situation_table_s,
  vitals_table = vitals_table_s,
  erecord_01_col = FACT_INCIDENT_PK,
  eresponse_05_col = RESPONSE_TYPE_OF_SERVICE_REQUESTED_WITH_CODE_E_RESPONSE_05,
  esituation_11_col = SITUATION_PROVIDER_PRIMARY_IMPRESSION_CODE_AND_DESCRIPTION_E_SITUATION_11,
  esituation_12_col = SITUATION_PROVIDER_SECONDARY_IMPRESSION_DESCRIPTION_AND_CODE_LIST_E_SITUATION_12,
  evitals_23_col = VITALS_TOTAL_GLASGOW_COMA_SCORE_GCS_E_VITALS_23,
  evitals_26_col = VITALS_LEVEL_OF_RESPONSIVENESS_AVPU_E_VITALS_26,
  evitals_29_col = VITALS_STROKE_SCALE_SCORE_E_VITALS_29,
  evitals_30_col = VITALS_STROKE_SCALE_TYPE_E_VITALS_30
)

# population results for 2021-2025
stroke_01_pop_filter_process <- stroke_01_pop$filter_process

# population results for 2021-2025
stroke_01_missings <- stroke_01_pop$missingness

# set up daemons
mirai::daemons(n = 13)

### get stroke_01 population data for each year using mirai and mori -------

# track progress
tictoc::tic(msg = "stroke_01_pop_years_init")

stroke_01_pop_years_init <- mirai::mirai_map(
  report_years,
  \(yr, ps, rsp, sit, vit) {
    # parallelize by year
    ps_y <- ps |> dplyr::filter(INCIDENT_YEAR == yr)
    rsp_y <- rsp |> dplyr::filter(INCIDENT_YEAR == yr)
    sit_y <- sit |> dplyr::filter(INCIDENT_YEAR == yr)
    vit_y <- vit |> dplyr::filter(INCIDENT_YEAR == yr)

    # run function in parallel
    nemsqar::stroke_01_population(
      df = NULL,
      patient_scene_table = ps_y,
      response_table = rsp_y,
      situation_table = sit_y,
      vitals_table = vit_y,
      erecord_01_col = FACT_INCIDENT_PK,
      eresponse_05_col = RESPONSE_TYPE_OF_SERVICE_REQUESTED_WITH_CODE_E_RESPONSE_05,
      esituation_11_col = SITUATION_PROVIDER_PRIMARY_IMPRESSION_CODE_AND_DESCRIPTION_E_SITUATION_11,
      esituation_12_col = SITUATION_PROVIDER_SECONDARY_IMPRESSION_DESCRIPTION_AND_CODE_LIST_E_SITUATION_12,
      evitals_23_col = VITALS_TOTAL_GLASGOW_COMA_SCORE_GCS_E_VITALS_23,
      evitals_26_col = VITALS_LEVEL_OF_RESPONSIVENESS_AVPU_E_VITALS_26,
      evitals_29_col = VITALS_STROKE_SCALE_SCORE_E_VITALS_29,
      evitals_30_col = VITALS_STROKE_SCALE_TYPE_E_VITALS_30
    )
  },
  .args = list(
    ps = patient_scene_table_s,
    rsp = response_table_s,
    sit = situation_table_s,
    vit = vitals_table_s
  )
)[.progress]

# Get total time
time <- tictoc::toc()

#### append years to the population files ----
stroke_01_pop_years <- add_year_to_nested(
  x = stroke_01_pop_years_init,
  file = "filter_process",
  years = 2021:2025
)

#### append years to the missingness files ----
stroke_01_missingness_years <- add_year_to_nested(
  x = stroke_01_pop_years_init,
  file = "missingness",
  years = 2021:2025
)

# unburden daemons
mirai::daemons(n = 0)

# plot population trends over time
stroke_01_pop_years |>
  plot_nemsqa_pops(
    type = "col",
    wrap_width = 25,
    plot_title = "Stroke-01"
  )

### stroke-01 results ##########################################################

### results years ----------------------------------------------------------

#### year ----
stroke_01_result_year <- nemsqar::stroke_01(
  df = NULL,
  patient_scene_table = patient_scene_table_s,
  response_table = response_table_s,
  situation_table = situation_table_s,
  vitals_table = vitals_table_s,
  erecord_01_col = FACT_INCIDENT_PK,
  eresponse_05_col = RESPONSE_TYPE_OF_SERVICE_REQUESTED_WITH_CODE_E_RESPONSE_05,
  esituation_11_col = SITUATION_PROVIDER_PRIMARY_IMPRESSION_CODE_AND_DESCRIPTION_E_SITUATION_11,
  esituation_12_col = SITUATION_PROVIDER_SECONDARY_IMPRESSION_DESCRIPTION_AND_CODE_LIST_E_SITUATION_12,
  evitals_23_col = VITALS_TOTAL_GLASGOW_COMA_SCORE_GCS_E_VITALS_23,
  evitals_26_col = VITALS_LEVEL_OF_RESPONSIVENESS_AVPU_E_VITALS_26,
  evitals_29_col = VITALS_STROKE_SCALE_SCORE_E_VITALS_29,
  evitals_30_col = VITALS_STROKE_SCALE_TYPE_E_VITALS_30,
  confidence_interval = TRUE,
  method = "w",
  conf.level = 0.95,
  correct = TRUE,
  .by = INCIDENT_YEAR
)

### results regions and years ----------------------------------------------

#### regions and years ----
stroke_01_result_regions_years <- nemsqar::stroke_01(
  df = NULL,
  patient_scene_table = patient_scene_table_s,
  response_table = response_table_s,
  situation_table = situation_table_s,
  vitals_table = vitals_table_s,
  erecord_01_col = FACT_INCIDENT_PK,
  eresponse_05_col = RESPONSE_TYPE_OF_SERVICE_REQUESTED_WITH_CODE_E_RESPONSE_05,
  esituation_11_col = SITUATION_PROVIDER_PRIMARY_IMPRESSION_CODE_AND_DESCRIPTION_E_SITUATION_11,
  esituation_12_col = SITUATION_PROVIDER_SECONDARY_IMPRESSION_DESCRIPTION_AND_CODE_LIST_E_SITUATION_12,
  evitals_23_col = VITALS_TOTAL_GLASGOW_COMA_SCORE_GCS_E_VITALS_23,
  evitals_26_col = VITALS_LEVEL_OF_RESPONSIVENESS_AVPU_E_VITALS_26,
  evitals_29_col = VITALS_STROKE_SCALE_SCORE_E_VITALS_29,
  evitals_30_col = VITALS_STROKE_SCALE_TYPE_E_VITALS_30,
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
stroke_01_result_regions <- nemsqar::stroke_01(
  df = NULL,
  patient_scene_table = patient_scene_table_s,
  response_table = response_table_s,
  situation_table = situation_table_s,
  vitals_table = vitals_table_s,
  erecord_01_col = FACT_INCIDENT_PK,
  eresponse_05_col = RESPONSE_TYPE_OF_SERVICE_REQUESTED_WITH_CODE_E_RESPONSE_05,
  esituation_11_col = SITUATION_PROVIDER_PRIMARY_IMPRESSION_CODE_AND_DESCRIPTION_E_SITUATION_11,
  esituation_12_col = SITUATION_PROVIDER_SECONDARY_IMPRESSION_DESCRIPTION_AND_CODE_LIST_E_SITUATION_12,
  evitals_23_col = VITALS_TOTAL_GLASGOW_COMA_SCORE_GCS_E_VITALS_23,
  evitals_26_col = VITALS_LEVEL_OF_RESPONSIVENESS_AVPU_E_VITALS_26,
  evitals_29_col = VITALS_STROKE_SCALE_SCORE_E_VITALS_29,
  evitals_30_col = VITALS_STROKE_SCALE_TYPE_E_VITALS_30,
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
stroke_01_result_counties <- nemsqar::stroke_01(
  df = NULL,
  patient_scene_table = patient_scene_table_s,
  response_table = response_table_s,
  situation_table = situation_table_s,
  vitals_table = vitals_table_s,
  erecord_01_col = FACT_INCIDENT_PK,
  eresponse_05_col = RESPONSE_TYPE_OF_SERVICE_REQUESTED_WITH_CODE_E_RESPONSE_05,
  esituation_11_col = SITUATION_PROVIDER_PRIMARY_IMPRESSION_CODE_AND_DESCRIPTION_E_SITUATION_11,
  esituation_12_col = SITUATION_PROVIDER_SECONDARY_IMPRESSION_DESCRIPTION_AND_CODE_LIST_E_SITUATION_12,
  evitals_23_col = VITALS_TOTAL_GLASGOW_COMA_SCORE_GCS_E_VITALS_23,
  evitals_26_col = VITALS_LEVEL_OF_RESPONSIVENESS_AVPU_E_VITALS_26,
  evitals_29_col = VITALS_STROKE_SCALE_SCORE_E_VITALS_29,
  evitals_30_col = VITALS_STROKE_SCALE_TYPE_E_VITALS_30,
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
stroke_01_result_counties_years <- nemsqar::stroke_01(
  df = NULL,
  patient_scene_table = patient_scene_table_s,
  response_table = response_table_s,
  situation_table = situation_table_s,
  vitals_table = vitals_table_s,
  erecord_01_col = FACT_INCIDENT_PK,
  eresponse_05_col = RESPONSE_TYPE_OF_SERVICE_REQUESTED_WITH_CODE_E_RESPONSE_05,
  esituation_11_col = SITUATION_PROVIDER_PRIMARY_IMPRESSION_CODE_AND_DESCRIPTION_E_SITUATION_11,
  esituation_12_col = SITUATION_PROVIDER_SECONDARY_IMPRESSION_DESCRIPTION_AND_CODE_LIST_E_SITUATION_12,
  evitals_23_col = VITALS_TOTAL_GLASGOW_COMA_SCORE_GCS_E_VITALS_23,
  evitals_26_col = VITALS_LEVEL_OF_RESPONSIVENESS_AVPU_E_VITALS_26,
  evitals_29_col = VITALS_STROKE_SCALE_SCORE_E_VITALS_29,
  evitals_30_col = VITALS_STROKE_SCALE_TYPE_E_VITALS_30,
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
stroke_01_result_overall <- nemsqar::stroke_01(
  df = NULL,
  patient_scene_table = patient_scene_table_s,
  response_table = response_table_s,
  situation_table = situation_table_s,
  vitals_table = vitals_table_s,
  erecord_01_col = FACT_INCIDENT_PK,
  eresponse_05_col = RESPONSE_TYPE_OF_SERVICE_REQUESTED_WITH_CODE_E_RESPONSE_05,
  esituation_11_col = SITUATION_PROVIDER_PRIMARY_IMPRESSION_CODE_AND_DESCRIPTION_E_SITUATION_11,
  esituation_12_col = SITUATION_PROVIDER_SECONDARY_IMPRESSION_DESCRIPTION_AND_CODE_LIST_E_SITUATION_12,
  evitals_23_col = VITALS_TOTAL_GLASGOW_COMA_SCORE_GCS_E_VITALS_23,
  evitals_26_col = VITALS_LEVEL_OF_RESPONSIVENESS_AVPU_E_VITALS_26,
  evitals_29_col = VITALS_STROKE_SCALE_SCORE_E_VITALS_29,
  evitals_30_col = VITALS_STROKE_SCALE_TYPE_E_VITALS_30,
  confidence_interval = TRUE,
  method = "w",
  conf.level = 0.95,
  correct = TRUE
)

### results services -------------------------------------------------------

#### services ----
stroke_01_result_services <- nemsqar::stroke_01(
  df = NULL,
  patient_scene_table = patient_scene_table_s,
  response_table = response_table_s,
  situation_table = situation_table_s,
  vitals_table = vitals_table_s,
  erecord_01_col = FACT_INCIDENT_PK,
  eresponse_05_col = RESPONSE_TYPE_OF_SERVICE_REQUESTED_WITH_CODE_E_RESPONSE_05,
  esituation_11_col = SITUATION_PROVIDER_PRIMARY_IMPRESSION_CODE_AND_DESCRIPTION_E_SITUATION_11,
  esituation_12_col = SITUATION_PROVIDER_SECONDARY_IMPRESSION_DESCRIPTION_AND_CODE_LIST_E_SITUATION_12,
  evitals_23_col = VITALS_TOTAL_GLASGOW_COMA_SCORE_GCS_E_VITALS_23,
  evitals_26_col = VITALS_LEVEL_OF_RESPONSIVENESS_AVPU_E_VITALS_26,
  evitals_29_col = VITALS_STROKE_SCALE_SCORE_E_VITALS_29,
  evitals_30_col = VITALS_STROKE_SCALE_TYPE_E_VITALS_30,
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
  pattern = "stroke_01_pop",
  measure = "Stroke-01",
  folder = "population"
)

## results exports ############################################################

export_nemsqa_data(
  pattern = "stroke_01_result",
  measure = "Stroke-01",
  folder = "result"
)

## missingness exports ########################################################

export_nemsqa_data(
  pattern = "stroke_01_(missings|missingness)",
  measure = "Stroke-01",
  folder = "missings"
)
