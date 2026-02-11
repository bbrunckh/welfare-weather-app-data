# Download and prepare GMD household survey data for WISE-APP

# This script downloads GMD geocoded household survey microdata, 
# prepares individual and household level datasets for WISE-APP, 
# prepares corresponding H3 data, and compiles a survey list. 
# It also logs any errors encountered during processing.

rm(list = ls())

#------------------------------------------------------------------------------#
# User inputs

# path to data folder (where output files will be saved)
data_path <- "data/"

# path to WISE-APP variable list
varlist_path <- "data/variable_list.csv"

# OPTIONAL path to existing survey list to be updated
surveylist_path <- "data/survey_list.csv"

# OPTIONAL choose specific GMD surveys to process (remove to process all)
surveys <- tibble::tibble(
  code = c("GNB", "GNB"),
  year = c(2018L, 2021L)
) 

# set dlw token for downloading GMD data (use .Renviron for security)
dlw::dlw_set_token(Sys.getenv("DLW_TOKEN"))

  # to add dlw token to user's .Renviron: 
    # 1. get token from https://datalibweb2.worldbank.org (expires after 30 days)
    # 2. usethis::edit_r_environ() to open R environment file
    # 3. add line to environment file: DLW_TOKEN = "your_token"
    # 4. save and restart R session

#------------------------------------------------------------------------------#
# load libraries
library(dlw)
options(dlw.local_dir = "~/dlw/")
options(dlw.verbose = FALSE)
library(DBI)
library(duckdb)
library(duckdbfs)
library(dplyr)
library(pipr) # used to get poverty rates from PIP for validation checks

# load helper functions
source("code/utils.R")

#------------------------------------------------------------------------------#
# load and validate WISE-APP variable list
if (file.exists(varlist_path)) {
  varlist <- read.csv(varlist_path)
  validate_varlist(varlist)
} else {
  stop("Variable list file not found at ", varlist_path)
}

#------------------------------------------------------------------------------#
# load and validate WISE-APP survey list (if provided)
if (file.exists(surveylist_path)) {
  surveylist <- validate_surveylist(read.csv(surveylist_path))
} else {
  surveylist <- validate_surveylist()
  surveylist
}

#------------------------------------------------------------------------------#

# Get GMD catalog with latest version of SPAT module
spat_cat <- dlw_server_catalog("GMD")[
  Collection == "GMD" & Module == "SPAT"][
    , .SD[toupper(Vermast) == max(toupper(Vermast))],
    by = .(Year, Country)][
      , .SD[toupper(Veralt) == max(toupper(Veralt), na.rm = TRUE)],
      by = .(Year, Country)][
          order(Country)]

# Filter to specific surveys if provided
if (!missing(surveys) && !is.null(surveys)) {
  spat_cat <- spat_cat |>
    inner_join(surveys, by = c("Country" = "code", "Year" = "year"))
  if (nrow(spat_cat) == 0) {
    stop("No matching surveys found in GMD catalog for provided survey list.")
  }
}
#------------------------------------------------------------------------------#
# get country names from PIP
pip_countries <- get_aux("countries")

#------------------------------------------------------------------------------#

# initialize error log
errors <- c()

#------------------------------------------------------------------------------#
# Loop to process geocoded GMD surveys

for (n in 1:nrow(spat_cat)){

  # Survey info
  code <- spat_cat$Country_code[n]
  year <- as.integer(spat_cat$Survey_year[n])
  survname <- spat_cat$Survey_acronym[n]
  economy <- pip_countries |> filter(country_code == !!code) |> pull(country_name)
  vermast <- spat_cat$Vermast[n]
  veralt <- spat_cat$Veralt[n]

  message(paste0("Processing ", code, " ", year, " ", survname))

  # Skip if already in survey list
  if (any(surveylist$code == code & surveylist$year == year & 
    surveylist$survname == survname &  surveylist$source == "GMD")){
    errors <- c(errors, paste0(code, " ", year, " already exists in survey list!"))
    message(errors[[length(errors)]])
    next
  }

  # Try to load SPAT module, otherwise log error and skip
  error_occurred <- FALSE
  tryCatch({
    spat <- dlw_get_gmd(code, year, module = "SPAT", vermast = vermast, veralt = veralt)
  }, error = function(e) {
    errors <<- c(errors, paste0("Failed to get SPAT module for ", code, " ", year))
    message(errors[[length(errors)]])
    error_occurred <<- TRUE
  })
  if (error_occurred) {next} else {
    message("SPAT module loaded successfully.")
  }

  # Skip if interview month missing in SPAT for > 50% households
  if (sum(is.na(spat$int_month))/nrow(spat)>0.5) {
    errors <<- c(errors, paste0("interview month missing for >50% households in SPAT for ", code, " ", year))
    message(errors[[length(errors)]])
    next
  } else message("SPAT module contains interview month for majority of households.")

  # Try to load GMD ALL module, otherwise GPWG module, otherwise log error and skip
  error_occurred <- FALSE
  tryCatch({
    gmd <- dlw_get_gmd(code, year, module = "ALL", vermast = vermast, veralt = veralt)
    message("GMD ALL module loaded successfully.")
  }, error = function(e1) {
    # Use GPWG if error
    tryCatch({
      gmd <- dlw_get_gmd(code, year, module = "GPWG", vermast = vermast, veralt = veralt)
      message("GMD GPWG module loaded successfully.")
    }, error = function(e2) {
      errors <<- c(errors, paste0("Failed to get ALL or GPWG module for ", code, " ", year))
      message(errors[[length(errors)]])
      error_occurred <<- TRUE
    })
  })
  if (error_occurred) { next } 

  # to duckdb for faster processing
  survey_db <- as_tibble(gmd) |> as_dataset()

  #----------------------------------------------------------------------------#
  # Harmonize GMD variables

  # construct empty GMD variables needed if they are not in data (to avoid errors in later processing steps)
  gmd_vars <- c(
    "code", "year", "strata", "psu", "hhid", "pid", 
    "hhsize", "weight", "welfare", "male", "age", "urban",
    "educy", "educat7", "educat5", "educat4", "literacy",
    "laborincome", "t_wage_total", "whours", "wmonths", "lstatus", "empstat",
    "industrycat4", "industrycat10", "industry_orig",
    "occup", "occup_orig", "njobs", "healthins", "socialsec",
    "internet", "ownhouse","rooms", "cooksource",
    "imp_wat_rec", "piped", "piped_to_prem", "imp_san_rec", 
    "electricity", "cellphone", "computer", "internet", "radio", "tv", "fridge",
    "washmach", "stove", "fan", "ac", "car", "bcycle", "mcycle", "boat",
    "ownland", "agriland", "area_agriland", "ownagriland"
  )

  gmd_add <- setdiff(gmd_vars, colnames(survey_db))
  if (length(gmd_add)>0){
  survey_db <- survey_db |>
    mutate(!!!setNames(rep(list(NA), length(gmd_add)), gmd_add))
  }
  
  # harmonize different names for the same variable
  survey_db <- survey_db |> 
    mutate(code = if_else(is.na(code), countrycode, code),
          hhid = as.character(hhid),
          hhsize = if_else(is.na(hhsize),hsize,hhsize),
          weight = if_else(is.na(weight),weight_p,weight)) |>
    # drop if missing welfare or weight
    filter(!is.na(welfare), !is.na(weight)) 

  #----------------------------------------------------------------------------#
  # Prepare individual level data for WISE-APP

  survey_db <- survey_db |>
    mutate(

    # ID variables
    code = !!code,
    survname = !!survname,
    economy = !!economy,

    # Outcomes

    # Welfare, convert to daily (LCU per capita)
    welfare = welfare/365, 
    
    # Labor market outcomes, not working age = NA
    across(c("laborincome", "t_wage_total", "lstatus", "empstat", "industrycat4"), 
      ~ if_else(age>=15 & age <=64, ., NA)), 
    laborincome = laborincome/365, # convert to daily labor income (LCU)
    wage  = t_wage_total/365, # convert to daily wage (LCU)
    employed = case_when(lstatus == 1 ~ 1, !is.na(lstatus) ~ 0), # missing = NA
    unemployed = case_when(lstatus == 2 ~ 1, !is.na(lstatus) ~ 0), # missing = NA
    notinlf = case_when(lstatus == 3 ~ 1, !is.na(lstatus) ~ 0), # missing = NA
    selfemployed = case_when(empstat == 4 ~ 1, !is.na(empstat) ~ 0), # missing = NA
    agriculture = case_when(industrycat4 == 1 ~ 1, !is.na(industrycat4) ~ 0), # missing = NA
    industry = case_when(industrycat4 == 2 ~ 1, !is.na(industrycat4) ~ 0), # missing = NA
    services = case_when(industrycat4 == 3 ~ 1, !is.na(industrycat4) ~ 0), # missing = NA
  
    # Demographics (missing = NA)
    agecat = case_when(
      age < 15 ~ "0-14",
      age >= 15 & age < 25 ~ "15-24",
      age >= 25 & age < 35 ~ "25-34",
      age >= 35 & age < 45 ~ "35-44",
      age >= 45 & age < 55 ~ "45-54",
      age >= 55 & age < 65 ~ "55-64",
      age >= 65 ~ "65+"),
          
    # Education/Literacy (missing = NA, < 15 years = NA))
    across(c("educat7", "educat5", "educat4", "educy"), ~ if_else(age<15, NA, .)),
    educ_com1 = case_when(educat7>=3 ~ 1, educat5>=3 ~ 1, educat4>=2 ~ 1, 
      !is.na(educat7) | !is.na(educat5) | !is.na(educat4) ~ 0),
    educ_com2 = case_when(educat7>=3 ~ 1, educat5>=4 ~ 1, educat4>=5 ~ 1, 
      !is.na(educat7) | !is.na(educat5) | !is.na(educat4) ~ 0),
    educ_com3 = case_when(educat7>=4 ~ 1, educat5>=5 ~ 1,  educat4>=6 ~ 1, 
      !is.na(educat7) | !is.na(educat5) | !is.na(educat4) ~ 0),

    # Employment (not working age = NA i.e. <15 or >64 years)
    across(c("whours", "wmonths", "industrycat10", "industry_orig", 
    "occup", "occup_orig", "njobs"), ~ if_else(age>=15 & age <=64, ., NA)),

    # Health insurance and social security (no recoding)
    
    # Assets (no recoding)
  
    # Household characteristics
    solidcookfuel = case_when(cooksource == 1 || cooksource == 3 ~ 1, !is.na(cooksource) ~ 0),
    internet = case_when(internet <= 3 ~ 1, internet ==4 ~ 0),
    ownhouse = case_when(ownhouse == 1 ~ 1, !is.na(ownhouse) ~ 0),
    renthouse = case_when(ownhouse == 2 ~ 1, !is.na(ownhouse) ~ 0)
    )
  
  # Area level variables (from SPAT)

    # prepare SPAT data
    spat_db <- as_tibble(spat) |> as_dataset() |>
      mutate(hhid = as.character(hhid)) |>
      select(-survname, -urban, -ends_with(c("_m1", "_sy", "_ref")))
  
    # merge SPAT data
    survey_db <- survey_db |>
      select(-int_month, -int_year) |>
      left_join(spat_db, by = c("code", "year", "hhid"))
  
  # Tidy individual level data
  wise_ind <- tidy_vars(survey_db, varlist)

  # Check unique IDs, skip if duplicates
  if (!check_unique_ids(wise_ind, c("pid", "hhid"), paste(code, year))) {
    errors <- c(errors, paste0("pid not unique in ", code, " ", year))
    next
  }
    
  # Check poverty rate, log error if mismatch (but don't skip)
  if (!check_poverty_rate(wise_ind, code, year)) {
    errors <- c(errors, paste0("$3.00 poverty rate mismatch for ", code, " ", year))
  }
  # Save individual level data
  write_dataset(wise_ind, paste0(data_path, code, "_", year, "_",survname,"_ind.parquet"))
  
  #----------------------------------------------------------------------------#
  # Prepare household level data for WISE-APP

  hh_vars <- c("code", "year", "survname", "economy", "hhid", "hhsize", "urban",
  "internet", "ownhouse","rooms", "cooksource", "imp_wat_rec", "piped", 
  "piped_to_prem", "imp_san_rec", "electricity")

  # Summarise variables at household level
  survey_db_hh <- survey_db |>
    summarise(
    # mean across household members (mean for welfare since simulated GMD data has >1 per hhid)
    across(c("welfare", "educy"), ~ mean(.x, na.rm = TRUE)), 
    # labor market outcomes (per working age member)
    across(c("laborincome", "wage", "whours", "wmonths"), 
    ~ sum(.x, na.rm = TRUE) / sum(age >= 15 & age <= 64, na.rm = TRUE), .names = "{.col}_hh"),
    # sum across household members          
    across(c("weight"), ~ sum(.x, na.rm = TRUE)),
    # max across household members for education (any)         
    across(c("literacy", "educat7", "educat5", "educat4", "educ_com1", "educ_com2", "educ_com3"),
    ~ max(.x, na.rm = TRUE), .names = "{.col}_hh"),
    # max across household members (any, include assets here just in case)
    across(c("healthins", "socialsec", "cellphone", "computer", "radio", "tv", "fridge",
    "washmach", "stove", "fan", "ac", "car", "bcycle", "mcycle", "boat",
    "ownland", "agriland", "ownagriland"), ~ max(.x, na.rm = TRUE)),
    # dependency ratio
    depend = if_else(sum(age>=15 & age <65, na.rm = TRUE)>0,
                    (sum(age<15, na.rm = TRUE) + sum(age>=65, na.rm = TRUE))/sum(age>=15 & age <65, na.rm = TRUE),
                    NA),
      .by = all_of(hh_vars)) |> 
    rename(educy_hh = educy) 
  
  # Tidy household level data
  wise_hh <- tidy_vars(survey_db_hh, varlist)

  # Check unique IDs, skip if duplicates
  if (!check_unique_ids(wise_hh, "hhid", paste(code, year))) {
    errors <- c(errors, paste0("hhid not unique in ", code, " ", year))
    next
  }
    
  # Check poverty rate, log error if mismatch (but don't skip)
  if (!check_poverty_rate(wise_hh, code, year)) {
    errors <- c(errors, paste0("$3.00 poverty rate mismatch for ", code, " ", year))
  }

  # Save household level data
  write_dataset(wise_hh, paste0(data_path, code, "_", year, "_",survname,"_hh.parquet"))
  
  #----------------------------------------------------------------------------#
  # Try to load H3 module, otherwise log error and skip
  error_occurred <- FALSE
  tryCatch({
    h3 <- dlw_get_gmd(code, year, module = "H3", vermast = vermast, veralt = veralt)
    message("H3 module loaded successfully.")
  }, error = function(e) {
    errors <<- c(errors, paste0("Failed to get H3 module for ", code, " ", year))
    message(errors[[length(errors)]])
    error_occurred <<- TRUE
  })
  if (error_occurred) {next} 

  # Prepare H3 level data for WISE-APP
  wise_h3 <- as_tibble(h3) |> as_dataset() |>
    # use H3Index (uint64_t) representation, level 6 (to merge weather data)
    mutate(h3 = h3_string_to_h3(h3_6)) |> 
    tidy_vars(varlist)

  # Save H3 level data
  write_dataset(wise_h3, paste0(data_path, code, "_", year, "_",survname,"_h3.parquet"))

#------------------------------------------------------------------------------#
# Update survey list
  for (level in c("ind", "hh")){
    surveylist = bind_rows(surveylist,
      tibble(
        economy = economy, 
        code = code, 
        survname = survname,
        year = year, 
        level = level,
        obs = switch(level, "ind" = nrow(wise_ind), "hh" = nrow(wise_hh)),
        source = "GMD"
      )
    )
  }
message(paste0("✓  ",code, " ", year, " ", survname, " processed successfully with ", nrow(wise_ind), " individual records and ", nrow(wise_hh), " household records."))
} # end of survey loop

#------------------------------------------------------------------------------#

# Save survey list
write.csv(surveylist, paste0(data_path, "survey_list.csv"), row.names = FALSE)

# Save variable list to data folder
write.csv(varlist, paste0(data_path, "variable_list.csv"), row.names = FALSE)

# Save error log (if any errors) to data folder
if (length(errors) > 0){
  write.csv(errors, paste0(data_path, "GMD_surveys_errors.csv"), row.names = FALSE)
}