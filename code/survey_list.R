# Create survey list metadata file for WISE-APP

#------------------------------------------------------------------------------#
# User inputs

# path to wise-app data/ directory 
data_path <- Sys.getenv("WISEAPP_DATA_PATH") 

#------------------------------------------------------------------------------#
# Load required libraries
library(DBI)
library(duckdb)
library(duckdbfs)
library(dplyr)
library(readr)

# load helper functions
source("code/utils.R")

#------------------------------------------------------------------------------#
# create survey list metadata file based on microdata files in data directory
levels   <- c("hh", "ind", "firm")

survey_list <- purrr::map(levels, function(lvl) {

  files <- fs::dir_ls(
    path   = fs::path(data_path, "microdata", lvl),
    glob   = "*.parquet",
    recurse = TRUE
  )

  if (length(files) == 0) return(NULL)

  purrr::map(files, function(f) {

    # Parse filename: {code}_{year}_{survname}_{source}_{level}.parquet
    parts <- strsplit(fs::path_ext_remove(fs::path_file(f)), "_")[[1]]
    if (length(parts) < 5) {
      warning("Skipping unexpected filename: ", f)
      return(NULL)
    }
    code    <- parts[1]
    year    <- as.integer(parts[2])
    survname <- parts[3]
    source  <- parts[4]
    # parts[5] is level — derived from directory, not filename

    # Read only the columns needed
    pf <- open_dataset(f)
    obs     <- pf |> tally() |> pull(n) |> as.integer()
    economy <- pf |> head(1) |> pull(economy)

    tibble::tibble(
      economy  = economy,
      code     = code,
      year     = year,
      survname = survname,
      level    = lvl,
      obs      = obs,
      source   = source
    )

  }) |> purrr::list_rbind()

}) |> purrr::list_rbind()

#------------------------------------------------------------------------------#
# validate survey list file
validate_surveylist(survey_list)

if (nrow(survey_list) == 0) {
  warning("No valid survey files found in data directory.")
} else {
  survey_list |>
    dplyr::count(level) |>
    dplyr::mutate(msg = paste0("  ", level, ": ", n, " survey(s)")) |>
    dplyr::pull(msg) |>
    paste(collapse = "\n") |>
    (\(x) message("survey_list.csv written for ", length(unique(survey_list$code)), " economies:\n", x))()
}
# arrange
survey_list <- arrange(survey_list, code, year, survname, level, source)
survey_list
#------------------------------------------------------------------------------#
# save survey list metadata file
write_csv(survey_list, file.path(data_path, "metadata", "survey_list.csv"))
