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

# set dlw token for downloading GMD data (from .Renviron for security)
dlw_set_token(Sys.getenv("DLW_TOKEN"))

  # to add dlw token to user's .Renviron: 
    # 1. get token from https://datalibweb2.worldbank.org (expires after 30 days)
    # 2. usethis::edit_r_environ() to open R environment file
    # 3. add line to environment file: DLW_TOKEN = "your_token"
    # 4. save and restart R session

# OPTIONAL path to existing survey list to be updated
surveylist_path <- "data/survey_list_old.csv"

# OPTIONAL choose specific GMD surveys to process (leave empty to process all)
surveys <- tibble::tibble(
  code = c("GNB", "GNB"),
  year = c(2018L, 2021L)
) 
# surveys <- NULL # to process all surveys

#------------------------------------------------------------------------------#
# load libraries
library(dlw)
options(dlw.local_dir = "~/dlw/")
library(duckdbfs)
library(dplyr)

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
        , fname := substr(FileName, 1, nchar(FileName) - 8)][
          order(Country)]

# Filter to specific surveys if provided
if (!missing(surveys) && !is.null(surveys)) {
  spat_cat <- spat_cat |>
    inner_join(surveys, by = c("Country" = "code", "Year" = "year"))
  if (nrow(spat_cat) == 0) {
    stop("No matching surveys found in GMD catalog for provided survey list.")
  }
}
# initialize error log
errors <- c()

#------------------------------------------------------------------------------#
# Loop to process geocoded GMD surveys

for (n in 1:nrow(spat_cat)){

  # Survey info
  code <- spat_cat$Country_code[n]
  year <- spat_cat$Survey_year[n]
  survname <- spat_cat$Survey_acronym[n]

  message(paste0("Processing ", code, " ", year, " ", survname))

  # Skip if already in survey list
  if (any(surveylist$code == code & surveylist$year == year & 
    surveylist$survname == survname &  surveylist$source == "GMD")){
    errors <- c(errors, paste0(code, " ", year, " already exists in survey list!"))
    message(errors[[length(errors)]])
    next
  }

  # Base file name for GMD modules (SPAT, ALL/GPWG, H3)
  fname <- substr(spat_cat$FileName[n], 1, nchar(spat_cat$FileName[n]) - 8)

  # Try to load SPAT module, otherwise log error and skip
  error_occurred <- FALSE
  tryCatch({
      spat <- dlw_get_data(code, paste0(fname, "SPAT.dta"))
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
    gmd <- dlw_get_data(code, paste0(fname, "ALL.dta"))
  }, error = function(e1) {
    # Use GPWG if error
    tryCatch({
      gmd <- dlw_get_data(code, paste0(fname, "GPWG.dta"))
    }, error = function(e2) {
      errors <<- c(errors, paste0("Failed to get ALL or GPWG module for ", code, " ", year))
      message(errors[[length(errors)]])
      error_occurred <<- TRUE
    })
  })
  if (error_occurred) { next } else {
    message("GMD ALL or GPWG module loaded successfully.")
  }

  # to duckdb for processing
  survey_db <- as_dataset(gmd)

  #----------------------------------------------------------------------------#
  # Harmonize GMD variables

  # construct empty GMD variables if not included in dataset (to avoid errors in later processing steps)
  gmd_add <- setdiff(gmd_vars, colnames(survey_db))
  if (length(gmd_add)>0){
  survey_db <- survey_db |>
    mutate(!!!setNames(rep(list(NA), length(gmd_add)), gmd_add))
  }
  
  # harmonize different names for the same variable, 
   survey_db <- survey_db |> 
     mutate(code = if_else(is.na(code), countrycode, code),
            hhid = as.character(hhid),
            hhsize = if_else(is.na(hhsize),hsize,hhsize),
            weight = if_else(is.na(weight),weight_p,weight),
            subnatid1 = if_else(is.na(subnatid1),subnatid,subnatid1)) |>
     # drop if missing welfare or weight or urban
     filter(!is.na(welfare), !is.na(weight), !is.na(urban))

  #----------------------------------------------------------------------------#
  # Prepare individual level data for WISE-APP

    # IDs and dates
  
    # Demographics
    survey_db <- survey_db |>
      mutate(agecat = case_when(
        age < 15 ~ "0-14",
        age >= 15 & age < 25 ~ "15-24",
        age >= 25 & age < 35 ~ "25-34",
        age >= 35 & age < 45 ~ "35-44",
        age >= 45 & age < 55 ~ "45-54",
        age >= 55 & age < 65 ~ "55-64",
        age >= 65 ~ "65+"))

    # Education/Literacy (missing = NA)
    survey_db <- survey_db |>
      mutate(
        educ_com1 = case_when(
          educat7>=3 ~ 1, educat5>=3 ~ 1, educat4>=2 ~ 1, 
          !is.na(educat7) | !is.na(educat5) | !is.na(educat4) ~ 0),
        educ_com2 = case_when(
          educat7>=3 ~ 1, educat5>=4 ~ 1, educat4>=5 ~ 1, 
          !is.na(educat7) | !is.na(educat5) | !is.na(educat4) ~ 0),
        educ_com3 = case_when(
          educat7>=4 ~ 1, educat5>=5 ~ 1,  educat4>=6 ~ 1, 
          !is.na(educat7) | !is.na(educat5) | !is.na(educat4) ~ 0))

    # Employment (missing = NA)
    survey_db <- survey_db |>
      mutate(
        employed = case_when(lstatus == 1 ~ 1, !is.na(lstatus) ~ 0),
        unemployed = case_when(lstatus == 2 ~ 1, !is.na(lstatus) ~ 0),
        notinlf = case_when(lstatus == 3 ~ 1, !is.na(lstatus) ~ 0),
        employed_year = case_when(lstatus_year == 1 ~ 1, !is.na(lstatus_year) ~ 0),
        unemployed_year = case_when(lstatus_year == 2 ~ 1, !is.na(lstatus_year) ~ 0),
        notinlf_year = case_when(lstatus_year == 3 ~ 1, !is.na(lstatus_year) ~ 0),
        selfemployed = case_when(empstat == 4 ~ 1, !is.na(empstat) ~ 0),
        selfemployed_year = case_when(empstat_year == 4 ~ 1, !is.na(empstat_year) ~ 0),
        agriculture = case_when(industrycat4 == 1 ~ 1, !is.na(industrycat4) ~ 0),
        industry = case_when(industrycat4 == 2 ~ 1, !is.na(industrycat4) ~ 0),
        services = case_when(industrycat4 == 3 ~ 1, !is.na(industrycat4) ~ 0),
        agriculture_year = case_when(industrycat4_year == 1 ~ 1, !is.na(industrycat4_year) ~ 0),
        industry_year = case_when(industrycat4_year == 2 ~ 1, !is.na(industrycat4_year) ~ 0),
        services_year = case_when(industrycat4_year == 3 ~ 1, !is.na(industrycat4_year) ~ 0))
  
    # Assets - no recoding needed
  
    # Outcomes for individual level data
    survey_db <- survey_db |>
      mutate(
        welfare = welfare/365, # convert to daily welfare for poverty calculations
        wage = t_wage_total = t_wage_total/365, # convert to daily wages 
        laborincome = laborincome/365, # convert to daily labor income
      ) 
  
  
      # Household characteristics (missing = NA)
      survey_db <- survey_db |>
        mutate(
          solidcookfuel = case_when(cooksource == 1 || cooksource == 3 ~ 1, !is.na(cooksource) ~ 0),
          internet_access = case_when(internet <= 3 ~ 1, internet ==4 ~ 0),
          ownhouse_secure = case_when(ownhouse == 1 ~ 1, !is.na(ownhouse) ~ 0),
          renthouse = case_when(ownhouse == 2 ~ 1, !is.na(ownhouse) ~ 0))

    # Area level variables (from SPAT)
  
      # prepare SPAT data
      spat_db <- as_dataset(spat) |>
        mutate(hhid = as.character(hhid)) |>
        select(-survname, -urban, -ends_with(c("_m1", "_sy", "_ref")))
    
      # merge SPAT data
      survey_db <- survey_db |>
        select(-int_month, -int_year) |>
        left_join(survey_db, spat_db) 
  
  # Tidy individual level data
  wise_ind <- survey_db |>
    # keep only WISE-APP variables
    select(any_of(pull(varlist,name))) |>
    # ensure variable types match variable list
    mutate(across(any_of(num_vars), as.numeric),
           across(any_of(log_vars), as.logical),
           across(any_of(int_vars), as.integer),
           across(any_of(fact_vars), as.factor),
           across(any_of(char_vars), as.character),
           across(any_of(date_vars), as.Date)) |>
    collect()
  
  # Validation checks 
    
    # Check no duplicate IDs at individual level
    if (any(duplicated(pull(wise_ind, pid)))) {
      errors <- c(errors, paste0("pid not unique in ", code, " ", year))
      message(errors[[length(errors)]])
      next
    }
  
    # Check poverty rate vs PIP, log error if >0.1pp difference but don't skip

      # $3.00 poverty rate in PIP
      pip_poor_300ln <- pipr::get_stats(code, year) |> pull(headcount)

      # $3.00 poverty rate from survey
      # need to calculate this properly with cpi and ppp conversion
      svy_poor_300ln <- weighted.mean(wise_ind$poor_300ln, wise_ind$weight)
  
    if (round(pip_poor_300ln, 1) != round(svy_poor_300ln, 1)){
      errors <- c(errors, paste0("$3.00 poverty rate in individual data = ",
       svy_poor_300ln ," vs PIP = ", pip_poor_300ln, "for ", code, " ", year))
      message(errors[[length(errors)]])
    }

  # Save individual level data
  write_dataset(wise_ind, paste0(data_path, code, "_", year, "_ind.parquet"))
  
  #----------------------------------------------------------------------------#
  # Prepare household level data for WISE-APP
    
  # Summarise variables at household level
   survey_db <- survey_db |>
     summarise(
       across(c("welfare"), # use mean welfare (simulated GMD data has >1 per hhid)
              ~ mean(.x, na.rm = TRUE)), 
       across(c("t_wage_total", "laborincome", "weight"),
              ~ sum(.x, na.rm = TRUE)),
       across(c("cellphone"),
              ~ max(.x, na.rm = TRUE)),
       across(c("literacy", "educat7", "educat5", "educat4"),
              ~ max(.x[age>=15], na.rm = TRUE)),
       educy = mean(educy[age>=15], na.rm = TRUE),
       depend = if_else(sum(age>=15 & age <65, na.rm = TRUE)>0,
                        (sum(age<15, na.rm = TRUE) + sum(age>=65, na.rm = TRUE))/sum(age>=15 & age <65, na.rm = TRUE),
                        NA),
       across(c("male", "lstatus", "empstat", "ocusec", "industrycat10",
                "industrycat4", "occup", "lstatus_year", "empstat_year", 
                "ocusec_year", "industrycat10_year", "industrycat4_year", 
                "occup_year", "njobs"),
              ~ first(.x[relationharm==1])),
     .by = all_of(setdiff(gmd_hh_vars, "welfare"))) |> ungroup()
   
   
  # Tidy household level data
  wise_hh <- survey_db |>
    # keep only WISE-APP variables
    select(any_of(pull(varlist,name))) |>
    # ensure variable types match variable list
    mutate(across(any_of(c(integer_cols, cat_cols, logical_cols)), as.integer),
           across(any_of(numeric_cols), as.numeric)) |>
    collect() |> 
    mutate(across(any_of(string_cols), as.character))

   
  # Validation checks
  
    # Check no duplicate IDs at household level
    if (any(duplicated(pull(wise_hh, hhid)))) {
      errors <- c(errors, paste0("hhid not unique in ", code, " ", year))
      message(errors[[length(errors)]])
      next
    }

    # Check $3.00 poverty rate vs PIP still ok, log error and skip if >0.1pp difference
    pip_poor_300ln <- get_stats(code, year) |> pull(headcount)
    svy_poor_300ln <- weighted.mean(wise_hh$poor_300)

    if (round(pip_poor_300ln, 1) != round(svy_poor_300ln, 1)){
      errors <- c(errors, paste0("$3.00 poverty rates in household data does not match PIP for ", code, " ", year))
      message(errors[[length(errors)]])
      next
    }

  # Save household level data
  write_dataset(wise_hh, paste0(data_path, code, "_", year, "_hh.parquet"))

  #----------------------------------------------------------------------------#
  # Try to load H3 module, otherwise log error and skip
  error_occurred <- FALSE
  tryCatch({
      h3 <- dlw_get_data(code, paste0(fname, "H3.dta"))
  }, error = function(e) {
    errors <<- c(errors, paste0("Failed to get H3 module for ", code, " ", year))
    message(errors[[length(errors)]])
    error_occurred <<- TRUE
  })
  if (error_occurred) {next} 

  # Prepare H3 level data for WISE-APP
  wise_h3 <- as_dataset(h3) |>
    # use H3Index (uint64_t) representation, level 6 (to merge weather data)
    mutate(h3 = h3_string_to_h3(h3_6)) |> 
    select(any_of(pull(varlist,name))) |>
    mutate(across(any_of(c(integer_cols, cat_cols, logical_cols)), as.integer),
           across(any_of(numeric_cols), as.numeric)) |>
    collect() |> 
    mutate(across(any_of(string_cols), as.character))

  # Save H3 level data
  write_dataset(wise_h3, paste0(data_path, code, "_", year, "_h3.parquet"))

#------------------------------------------------------------------------------#
# Update survey list
  for (level in c("ind", "hh")){
    survey_list = bind_rows(survey_list,
      tibble(
        countryname = countryname, 
        code = code, 
        survname = survname,
        year = year, 
        level = level,
        obs = switch(level, "ind" = nrow(wise_ind), "hh" = nrow(wise_hh)),
        source = "GMD"
      )
    )
  }

} # end of survey loop

# fix country names in survey list
survey_list <- survey_list |>
  mutate(countryname = if_else(code =="CIV", "Côte d`Ivoire", countryname))

# Save survey list
write.csv(survey_list, paste0(data_path, "survey_list.csv"), row.names = FALSE)

# Save variable list to data folder
write.csv(variable_list, paste0(data_path, "variable_list.csv"), row.names = FALSE)

# Save error log (if any errors)
if (length(errors) > 0){
  write.csv(errors, paste0(data_path, "GMD_surveys_errors.csv"), row.names = FALSE)
}

#------------------------------------------------------------------------------#
# WISE-APP variable list

gmd_vars <- c(
  filter(varlist, !is.na(ALL)) |> pull(`gmd varname`),
  filter(varlist, !is.na(`gmd altname`)) |> pull(`gmd altname`))

gmd_hh_vars <- filter(
  varlist, !is.na(wiseapp) & !is.na(ALL) & is.na(`hh aggregation`)) |> 
  pull(varname)

wise_vars <- filter(varlist, !is.na(wiseapp)) |> 
  select(wiseapp, varname, label, datatype)

integer_cols <- filter(wise_vars, datatype == "Integer") |> pull(varname)
numeric_cols <- filter(wise_vars, datatype == "Numeric") |> pull(varname)
logical_cols <- filter(wise_vars, datatype == "Binary") |> pull(varname)
cat_cols <- filter(wise_vars, datatype == "Categorical") |> pull(varname)
string_cols <- filter(wise_vars, datatype == "String") |> pull(varname)
  
  # # drop empty columns
  # survey_clean <- survey_clean |>
  #   select(where(~ !((all(is.na(.))) || is.character(.) && all(is.na(.) | . == ""))))