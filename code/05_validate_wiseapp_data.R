# Validate data inputs prepared for WISE-APP

rm(list = ls())

#------------------------------------------------------------------------------#
# User inputs

# path to data folder (where output files will be saved)
data_path <- "data/"

#------------------------------------------------------------------------------#
# Load required libraries
library(DBI)
library(duckdb)
library(duckdbfs)
library(dplyr)

# load helper functions
source("code/utils.R")

#------------------------------------------------------------------------------#
# variable list
varlist_path <- paste0(data_path, "variable_list.csv")
if (file.exists(varlist_path)) {
  varlist <- read.csv(varlist_path)
  validate_varlist(varlist)
} else {
  stop("Variable list file not found at ", varlist_path)
}

#------------------------------------------------------------------------------#
# survey list
surveylist_path <- paste0(data_path, "survey_list.csv")
if (file.exists(surveylist_path)) {
  surveylist <- read.csv(surveylist_path)
  validate_surveylist(surveylist)
  # if empty, stop with error
  if (nrow(surveylist) == 0) stop("Survey list does not match expected format or is empty.")
} else {
  stop("Survey list file not found at ", varlist_path)
}
message("Survey list contains ", nrow(surveylist), " surveys across ", length(unique(surveylist$code)), " economies.")

#------------------------------------------------------------------------------#
# check survey and H3 data files for each survey in survey list
for (i in 1:nrow(surveylist)) {
  svy_data_path <- paste0(data_path, 
    surveylist$code[i],"_",surveylist$year[i],"_",surveylist$survname[i],"_",surveylist$level[i],".parquet")
  
  h3_data_path <- paste0(data_path, 
    surveylist$code[i],"_",surveylist$year[i],"_",surveylist$survname[i],"_h3.parquet")

  if (!file.exists(svy_data_path)) {
    stop("Data file for ", surveylist$code[i], " not found at ", svy_data_path)
  } 
  if (!file.exists(h3_data_path)) {
    stop("H3 data file for ", surveylist$code[i], " not found at ", h3_data_path)
  }
  # check survey data files have minimum required variables
  required_vars <- c("code", "economy", "year", "survname", "int_year", "int_month", "loc_id")
  survey_data <- open_dataset(svy_data_path) 
  missing_vars <- setdiff(required_vars, colnames(survey_data))
  if (length(missing_vars) > 0) {
    stop("Data file for ", surveylist$code[i], " is missing required variables: ", paste(missing_vars, collapse = ", "))
  }
  
  # check survey data files have at least one variable with outcome = 1 in varlist
  outcome_vars <- varlist$name[varlist$outcome == 1]
  if (!any(outcome_vars %in% colnames(survey_data))) {
    stop("Data file for ", surveylist$code[i], " does not contain any outcome variables specified in variable list.")
  } 

  # check H3 data files have minimum required variables
  required_h3_vars <- c("code", "loc_id", "h3")
  h3_data <- open_dataset(h3_data_path)
  missing_h3_vars <- setdiff(required_h3_vars, colnames(h3_data))
  if (length(missing_h3_vars) > 0) {
    stop("H3 data file for ", surveylist$code[i], " is missing required variables: ", paste(missing_h3_vars, collapse = ", ")) 
  }
}
message("✓  All survey and H3 data files are present and contain required variables.")

#------------------------------------------------------------------------------#
# check weather data files for each country in survey list
codes <- unique(surveylist$code)
for (c in codes) {
  weather_data_path <- paste0(data_path, c, "_weather.parquet")
  if (!file.exists(weather_data_path)) {
    stop("Weather data file for ", c, " not found at ", weather_data_path)
  }
  # check weather data files have minimum required variables
  required_weather_vars <- c("h3", "timestamp")
  weather_data <- open_dataset(weather_data_path)
  missing_weather_vars <- setdiff(required_weather_vars, colnames(weather_data))
  if (length(missing_weather_vars) > 0) {
    stop("Weather data file for ", c, " is missing required variables: ", paste(missing_weather_vars, collapse = ", ")) 
  }
  #check at least one weather variable is present in weather data file
  weather_vars <- varlist$name[varlist$hazard == 1]
  if (!any(weather_vars %in% colnames(weather_data))) {
    stop("Weather data file for ", c, " does not contain any hazard variables specified in variable list.")
  } 
}
message("✓  All weather data files are present and contain required variables.")

#------------------------------------------------------------------------------#
# check cpi ppp data is present
cpi_ppp_data_path <- paste0(data_path, "cpi_ppp.csv")
if (!file.exists(cpi_ppp_data_path)) {
  stop("CPI and PPP data file not found at ", cpi_ppp_data_path)
}
# check cpi ppp data file has required variables
cpi_ppp_data <- read.csv(cpi_ppp_data_path)
required_cpi_ppp_vars <- c("code", "year", "data_level", "cpi", "ppp2021")
missing_cpi_ppp_vars <- setdiff(required_cpi_ppp_vars, colnames(cpi_ppp_data))
if (length(missing_cpi_ppp_vars) > 0) {
  stop("CPI and PPP data file is missing required variables: ", paste(missing_cpi_ppp_vars, collapse = ", ")) 
}
# check there is cpi and ppp for each code and year in survey list
survey_codes_years <- surveylist %>% select(code, year) %>% distinct()
cpi_ppp_codes_years <- cpi_ppp_data %>% select(code, year) %>% distinct()
missing_cpi_ppp_codes_years <- anti_join(survey_codes_years, cpi_ppp_codes_years, by = c("code", "year"))
if (nrow(missing_cpi_ppp_codes_years) > 0) {
  stop("CPI and PPP data file is missing data for the following code and year combinations in survey list: ", 
        paste(apply(missing_cpi_ppp_codes_years, 1, function(x) paste(x, collapse = "-")), collapse = ", "))
}
message("✓  CPI and PPP data file is present and contains required variables, and has data for all code and year combinations in survey list.")