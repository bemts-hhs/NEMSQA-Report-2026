### IOWA NEMSQAR REPORT PREP 2026 ----------------------------------------------

# This script prepares for the analyses using the `nemsqar` package v1.2.0
# For the shapefiles, it is assumed that the files are downloaded from
# https://www.census.gov/cgi-bin/geo/shapefiles/index.php using the year 2025
# and then utilizing the Counties (and equivalent), States (and equivalent), and
# Zip Code Tabulation Areas (ZCTAS) options in the dropdown dialogue to download
# the files manually and put them in the directory used here.

# report years
report_years <- 2021:2025

# report regions
report_regions <- c("1A", "1C", as.character(c(2:7)))

### PACKAGES -------------------------------------------------------------------

# CRAN versions ================================================================

# install these packages if not already

# install.packages("renv")
# renv::init()

# renv::install(c(
#   "tidyverse",
#   "traumar",
#   "devtools",
#   "remotes",
#   "janitor",
#   "gt",
#   "gtsummary",
#   "gtExtras",
#   "zipcodeR",
#   "naniar",
#   "nemsqar",
#   "showtext",
#   "extrafont",
#   "flextable",
#   "mirai",
#   "mori",
#   "ggthemes"
# ))

# showtext setup ----

# # run showtext auto, use throughout project
# showtext::showtext_auto()

# # get 300 dpi with showtext
# showtext::showtext_opts(dpi = 300)

# # get work sans fonts of interest
# all_fonts <- systemfonts::system_fonts()

# # regular
# work_sans <- all_fonts |>
#   dplyr::filter(name == "WorkSans-Regular") |>
#   dplyr::pull(path)

# # semibold
# work_sans_semibold <- all_fonts |>
#   dplyr::filter(name == "WorkSans-SemiBold") |>
#   dplyr::pull(path)

# # extrabold
# work_sans_extrabold <- all_fonts |>
#   dplyr::filter(name == "WorkSans-ExtraBold") |>
#   dplyr::pull(path)

# # use sysfonts to load the fonts
# sysfonts::font_add(
#   family = "Work Sans",
#   regular = work_sans,
#   bold = work_sans_semibold
# )

# sysfonts::font_add(
#   family = "Work Sans",
#   regular = work_sans,
#   bold = work_sans_extrabold
# )

###___________________________________________________________________________
# Get location data
###___________________________________________________________________________

# Get the secure county data file path
county_data_path <- Sys.getenv("COUNTY_FILE_PATH")

# get location data
# Iowa county data
county_data <- readxl::read_excel(
  path = county_data_path
)

# essential service counties
essential_counties <- county_data |>
  dplyr::filter(`EMS Essential Service` == TRUE) |>
  dplyr::pull(County)

# helper object for manipulations
location <-
  county_data |>
  dplyr::select(
    County,
    `Region: Preparedness`,
    Pop:Designation,
    `EMS Essential Service`
  ) |>
  dplyr::mutate(
    `EMS Essential Service` = dplyr::if_else(
      County %in% essential_counties,
      TRUE,
      FALSE
    )
  )

# counties
report_counties <- c(unique(location$County), "OOS or Missing")

# zipcode level data
zipcodes <- zipcodeR::zip_code_db |>
  dplyr::mutate(
    county = stringr::str_remove_all(county, pattern = "(?:\\sCounty)")
  ) |>
  dplyr::select(major_city, state, county, zipcode, lat, lng) |>
  dplyr::mutate(
    county = dplyr::if_else(
      state == "IA" &
        county == "",
      "Polk",
      county
    )
  ) |>
  dplyr::rename("new_city" = "major_city") |>
  dplyr::rename("new_state" = "state") |>
  dplyr::rename("new_county" = "county") |>
  dplyr::rename("new_zipcode" = "zipcode")

# unzip the larger US data from Geonames
# create a temp file to hold the download as a container
temp <- tempfile()

# download the temp file
download.file("https://download.geonames.org/export/dump/US.zip", temp)

# unzip the zip file and pull the specific flat file
con <- unz(temp, "US.txt")

# read in the file of interest and specify column names and types using readr
US <-
  readr::read_delim(
    file = con,
    col_names = FALSE,
    col_types = list(
      readr::col_character(),
      readr::col_character(),
      readr::col_character(),
      readr::col_character(),
      readr::col_number(),
      readr::col_number(),
      readr::col_character(),
      readr::col_character(),
      readr::col_character(),
      readr::col_character(),
      readr::col_character(),
      readr::col_character(),
      readr::col_character(),
      readr::col_character(),
      readr::col_integer(),
      readr::col_integer(),
      readr::col_guess(),
      readr::col_character(),
      readr::col_date()
    )
  )

# remove the temp file
unlink(temp)

###_____________________________________________________________________________
# manipulate the US file, filter down to Iowa, and get formatting right
# for colnames, refer to: https://download.geonames.org/export/dump/readme.txt
###_____________________________________________________________________________

US_clean <- US |>
  dplyr::rename(
    geonameid = X1,
    name = X2,
    asciiname = X3,
    alternatenames = X4,
    latitude = X5,
    longitude = X6,
    feature_class = X7,
    feature_code = X8,
    country_code = X9,
    cc2 = X10,
    admin1_code = X11,
    admin2_code = X12,
    admin3_code = X13,
    admin4_code = X14,
    population = X15,
    elevation = X16,
    dem = X17,
    timezone = X18,
    modification_date = X19
  ) |>
  dplyr::filter(admin1_code == "IA", feature_class == "P") |>
  # remove the (historical) suffix to the "abandonded" populated places
  dplyr::mutate(
    name = stringr::str_squish(stringr::str_remove(
      name,
      pattern = "\\s\\(historical\\)"
    ))
  )

###_____________________________________________________________________________
# load in geonames county-level data
###_____________________________________________________________________________

geonames_county <-
  readr::read_delim(
    file = "https://download.geonames.org/export/dump/admin2Codes.txt",
    col_names = F
  ) |>
  dplyr::rename(
    code = X1,
    name = X2,
    asciiname = X3,
    geonameID = X4
  ) |>
  dplyr::mutate(
    country_code = stringr::str_split(code, "\\.", simplify = TRUE)[, 1],
    state_code = stringr::str_split(code, "\\.", simplify = TRUE)[, 2],
    county_code = stringr::str_split(code, "\\.", simplify = TRUE)[, 3],
    .after = code
  )

###_____________________________________________________________________________
# geonames admin2 data filtered data to US and Iowa
###_____________________________________________________________________________

geonames_admin2_iowa <- geonames_county |>
  dplyr::filter(country_code == "US", state_code == "IA")

###_____________________________________________________________________________
# join the county names / codes from geonames to the city data from geonames as their codes are unique
# geonames does not use the same codes as Census Bureau
###_____________________________________________________________________________

# final manipulations of the Iowa data to join to the AED data
Iowa_Data_Final <- US_clean |>
  dplyr::left_join(
    geonames_admin2_iowa |> dplyr::select(county_code, name),
    by = c("admin2_code" = "county_code"),
    suffix = c("_city", "_county")
  ) |>
  dplyr::relocate(name_county, .after = name_city) |>
  dplyr::mutate(
    name_county = stringr::str_remove_all(
      name_county,
      pattern = "\\s[Cc]ounty"
    ),
    name_city = stringr::str_to_upper(name_city)
  ) |>
  dplyr::filter(
    !(name_city == "CENTERVILLE" & name_county == "Boone") &
      !(name_city == "PLEASANT HILL" &
        name_county == "Van Buren") &
      !(name_city == "HOLY CROSS" &
        name_county == "Delaware") &
      !(name_city == "FOREST CITY" &
        name_county == "Howard") &
      !(name_city == "GENEVA" & name_county == "Benton") &
      !(name_city == "WESTFIELD" &
        name_county == "Poweshiek") &
      !(name_city == "TROY" & name_county == "Lucas") &
      !(name_city == "WASHINGTON" &
        name_county == "Woodbury") &
      !(name_city == "WEBSTER" & name_county == "Madison") &
      !(name_city == "RIVERSIDE" &
        name_county == "Woodbury") &
      !(name_city == "WASHINGTON" &
        name_county == "Franklin") &
      !(name_city == "VAN")
  ) |>
  dplyr::left_join(
    county_data |> dplyr::select(County, `Region: Preparedness`, Designation),
    by = c("name_county" = "County")
  ) |>
  dplyr::relocate(`Region: Preparedness`, .after = name_county)

# geonames is missing some of the cities / towns in Iowa, this is a product
# of the analyses below and may need to be revised periodically

missing_location_data <- tibble::tibble(
  geonameid = generate_random_ID(7),
  # Generating random geoname IDs
  name_city = c(
    "TERRILL",
    "LEMARS",
    "FREDRICKSBURG",
    "MOLVILLE",
    "CALLENDAR",
    "SYDNEY",
    "DESOTO"
  ),
  name_county = c(
    "Dickinson",
    "Plymouth",
    "Chickasaw",
    "Woodbury",
    "Webster",
    "Fremont",
    "Dallas"
  ),
  latitude = c(
    43.305473,
    42.7942,
    42.964586,
    42.488210,
    42.362592,
    40.7592,
    41.5316
  ),
  longitude = c(
    -94.971433,
    -96.1656,
    -92.198465,
    -96.069997,
    -94.293268,
    -95.6668,
    -94.0078
  ),
  population = rep(NA, 7),
  elevation = rep(NA, 7),
  dem = rep(NA, 7),
  timezone = rep(NA_character_, 7),
  modification_date = rep(lubridate::NA_Date_, 7), # Change to NA_Date_
  `Region: Preparedness` = c("7", "3", "2", "3", "7", "4", "1A"), # Adding region information
  asciiname = rep(NA_character_, 7),
  alternatenames = rep(NA_character_, 7),
  feature_class = rep(NA_character_, 7),
  feature_code = rep(NA_character_, 7),
  country_code = rep(NA_character_, 7),
  cc2 = rep(NA_character_, 7),
  admin1_code = rep(NA_character_, 7),
  admin2_code = c("059", "149", "037", "193", "187", "071", "049"),
  # Adding admin2_code
  admin3_code = rep(NA_character_, 7),
  admin4_code = rep(NA_character_, 7)
) |>
  dplyr::left_join(
    county_data |> dplyr::select(County, Designation),
    by = c("name_county" = "County")
  ) |>
  dplyr::select(colnames(Iowa_Data_Final))

Iowa_Data_Final <- dplyr::bind_rows(Iowa_Data_Final, missing_location_data)

# remove the con object
rm(list = c("con", "temp"))
gc()

###_____________________________________________________________________________
# we will:
# - examine calendar years 2021-2025 of EMS data
# - ingest various tables from each data section of NEMSIS to leverage that
#   approach in the `nemsqar` package
# - visualize statistical outputs for clean reporting
###_____________________________________________________________________________
