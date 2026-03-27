# Prepare H3-indexed CMIP6 weather data for WISE-APP

# This script prepares H3-indexed CMIP6 weather data files for wise-app.

rm(list = ls())
mem.maxVSize(32000)

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

# OPTIONAL path to pre-processed CMIP6 H3-indexed parquet files
cmip6_path <- "~/Library/CloudStorage/OneDrive-WBG/Household survey locations to H3/02_data/h3/cmip6/"

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

# list of SSPs to process
ssp_list <- c("historical", "ssp5_8_5", "ssp3_7_0", "ssp2_4_5") # 

#------------------------------------------------------------------------------#
# Loop over country codes and prepare weather data for each country
for (n in 1:length(code_list)){ 
  code <- code_list[n] 
  message(paste0("Processing weather data for: ", code)) 

  # loop over SSPs
  for (ssp in ssp_list){

    # skip if data already exists for this country 
    if (file.exists(paste0(data_path, code, "_cmip6_",ssp,".parquet"))) { 
      message(paste0("Weather data file already exists for: ", code, ". Skipping.")) 
      next 
    }
    
    # If path to pre-processed CMIP6 H3-indexed parquet files is provided and file exists
    if (file.exists(paste0(cmip6_path, code, "_cmip6_",ssp,"_h3_6.parquet"))) {
        message(paste0("Using pre-processed CMIP6 H3-indexed parquet file for: ", code))
      
        # load weather data
        cmip6_h3_6 <- open_dataset(paste0(cmip6_path, code, "_cmip6_",ssp,"_h3_6.parquet")) 
        
        # load survey h3 data
        survey_h3 <- list.files(data_path, pattern = paste0(code, ".*_h3\\.parquet$"), full.names = TRUE) |> 
          open_dataset() |> select(h3) |> distinct() 
      
        # keep only h3 cells with survey data
        cmip6_h3_6 |> 
          rename(h3 = h3_6) |>
          inner_join(survey_h3, by = "h3") |>
          # tidy variables
          tidy_vars(varlist) |>
          write_dataset(paste0(data_path, code, "_cmip6_",ssp,".parquet"))

    } else { message(paste0("Pre-processed CMIP6 H3-indexed parquet file not found"))

      # download CMIP6 data for this country from Climate Data Store
      message(paste0("Downloading CMIP6 data for country code: ", code, " from Copernicus Climate Data Store..."))
      
      # function to download data
      warning("Not implemented yet", immediate. = TRUE)
      next

      # indexing CMIP6 data to H3 grid

      # clean and tidy variables

    }
    # cleanup DuckDB connections at end of each SSP iteration
    if (exists("cmip6_h3_6")) rm(cmip6_h3_6)
    if (exists("survey_h3"))  rm(survey_h3)
    gc()
    
    message(paste0("✓ CMIP6 ", ssp, "data for ", code, " processed"))
  } # end of SSP loop
} # end of country loop