# Prepare H3-indexed ERA5-Land weather data for WISE-APP

# This script prepares H3-indexed ERA5-Land weather data files for wise-app.
# See era5land_h3.R for how to download the raw ERA5-Land data and convert to H3-indexed parquet files.

rm(list = ls())

#------------------------------------------------------------------------------#

# load libraries
library(DBI)
library(duckdb)
library(duckdbfs)
load_h3()
library(dplyr)

# load helper functions
source("code/utils.R")

#------------------------------------------------------------------------------#
# User settings

# Path to data folder (where output files will be saved)
data_path <- "data/"

# path to WISE-APP variable list
varlist_path <- "data/variable_list.csv"

# path to survey list (for list of countries)
surveylist_path <- "data/survey_list.csv"

# OPTIONAL path to pre-processed ERA5-Land H3-indexed parquet files
era5land_path <- "~/Library/CloudStorage/OneDrive-WBG/Household survey locations to H3/02_data/h3/era5land/"

#------------------------------------------------------------------------------#
# load and validate WISE-APP variable list
if (file.exists(varlist_path)) {
  varlist <- read.csv(varlist_path)
  validate_varlist(varlist)
} else {
  stop("Variable list file not found at ", varlist_path)
}
#------------------------------------------------------------------------------#
# get country codes from survey list
if (file.exists(surveylist_path)) {
  surveylist <- read.csv(surveylist_path)
  # get country codes from survey list (if code column exists)
  if (!"code" %in% colnames(surveylist)) stop("Survey list file does not contain 'code' column.") 
  code_list <- surveylist |> pull(code) |> unique()
  if (length(code_list) == 0) stop("No country codes found in survey list 'code' column.")
} else {
  stop("Survey list file not found at ", varlist_path)
}
#------------------------------------------------------------------------------#
# Loop over country codes and prepare weather data for each country
for (n in 1:length(code_list)){ 
  code <- code_list[n] 
  message(paste0("Processing weather data for: ", code)) 

  # skip if data already exists for this country 
  if (file.exists(paste0(data_path, code, "_weather.parquet"))) { 
    message(paste0("Weather data file already exists for: ", code, ". Skipping.")) 
    next 
  }
  
  # If path to pre-processed ERA5-Land H3-indexed parquet files is provided and file exists
  if (exists("era5land_path") && !is.null(era5land_path) && 
    file.exists(paste0(era5land_path, code, "_era5land_h3_6.parquet"))) {
      message(paste0("Using pre-processed ERA5-Land H3-indexed parquet file for: ", code))
    
      # load weather data
      era5land_h3_6 <- open_dataset(paste0(era5land_path, code, "_era5land_h3_6.parquet")) 
      
      # load survey h3 data
      survey_h3 <- list.files(data_path, pattern = paste0(code, ".*_h3\\.parquet$"), full.names = TRUE) |> 
        open_dataset() |> select(h3) |> distinct() 
    
      # keep only h3 cells with survey data
      era5land_h3_6 |> 
        rename(h3 = h3_6) |>
        inner_join(survey_h3, by = "h3") |>
        # tidy variables
        tidy_vars(varlist) |>
        write_dataset(paste0(data_path, code, "_weather.parquet"))

  } else { message(paste0("Pre-processed ERA5-Land H3-indexed parquet file not found"))

    # download ERA5-Land data for this country from Climate Data Store
    message(paste0("Downloading ERA5-Land data for country code: ", code, " from Copernicus Climate Data Store..."))
    
    # function to download data
    warning("Not implemented yet")
    next

    # indexing ERA5-Land data to H3 grid

    # clean and tidy variables

  }
  message(paste0("✓ Weather data for country code ", code, " processed"))
} # end of country loop