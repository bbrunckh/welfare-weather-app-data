#' Check if dataframe columns match expected specification
#'
#' Validates that a dataframe contains exactly the expected columns with 
#' matching data types. Prints informative messages if there are missing 
#' columns, extra columns, or class mismatches.
#'
#' @param df A data frame to validate
#' @param expected A data frame with expected column names and types 
#'
#' @return Logical. Returns `TRUE` if all columns match expectations, 
#'   `FALSE` otherwise. Prints messages describing any discrepancies.
#'
#' @examples
#' # Define expected structure
#' expected <- tibble::tibble(name = character(), year = integer(), true = logical())
#' 
#' # Check a dataframe
#' df <- data.frame(name = "Alice", year = 2020, true = TRUE)
#' check_columns(df, expected)
#'
check_columns <- function(df, expected) {
  # Extract column names and types from the expected dataframe
  expected_names <- names(expected)
  expected_types <- sapply(expected, class)
  
  # Get actual columns and classes
  actual_cols <- sapply(df, class)
  
  # Check if column names match exactly
  if (!identical(sort(names(df)), sort(expected_names))) {
    missing <- setdiff(expected_names, names(df))
    extra <- setdiff(names(df), expected_names)
    
    if (length(missing) > 0) {
      message("Missing columns: ", paste(missing, collapse = ", "))
    }
    if (length(extra) > 0) {
      message("Extra columns: ", paste(extra, collapse = ", "))
    }
    return(FALSE)
  }
  
  # Check if classes match
  mismatched <- character()
  for (col in expected_names) {
    if (actual_cols[[col]] != expected_types[[col]]) {
      mismatched <- c(mismatched, 
                     paste0(col, " (expected: ", expected_types[[col]], 
                           ", actual: ", actual_cols[[col]], ")"))
    }
  }
  
  if (length(mismatched) > 0) {
    message("Column class mismatches: ", paste(mismatched, collapse = "; "))
    return(FALSE)
  }
  
  TRUE
}

#' Validate survey list structure and data
#'
#' Checks that a dataframe matches the expected survey list schema with correct
#' column names, data types, and no missing values in any columns.
#'
#' @param df A data frame to validate against the survey list schema
#'
#' @return If valid, returns the input dataframe. If invalid, returns an empty
#'   tibble with the correct schema structure. Prints informative messages 
#'   about any issues found.
#'
#' @examples
#' survey_list <- tibble::tibble(
#'   economy = "Kenya",
#'   code = "KEN",
#'   year = 2020L,
#'   survname = "KIHBS",
#'   level = "hh",
#'   obs = 1000L,
#'   source = "GMD"
#' )
#' validate_surveylist(survey_list)
#'
validate_surveylist <- function(df) {

  message("Validating survey list structure and data...")

  # Define expected survey list schema
  survey_schema <- tibble::tibble(
    economy = character(),
    code = character(),
    year = integer(),
    survname = character(),
    level = character(),
    obs = integer(),
    source = character()
  )

  # If no data frame provided, return empty tibble with correct structure
  if (missing(df) || is.null(df)) {
    message("No survey list provided. Returning empty tibble with correct structure.")
    return(survey_schema)
  }
  
  # Check columns match schema
  if (!check_columns(df, survey_schema)) {
    message("Survey list does not match expected schema. Returning empty tibble with correct structure.")
    return(survey_schema)
  }
  
  # Check for empty/missing values
  empty_cols <- character()
  for (col in names(df)) {
    if (any(is.na(df[[col]]) | df[[col]] == "")) {
      n_empty <- sum(is.na(df[[col]]) | df[[col]] == "")
      empty_cols <- c(empty_cols, paste0(col, " (", n_empty, " empty)"))
    }
  }
  
  if (length(empty_cols) > 0) {
    message("Columns with empty values: ", paste(empty_cols, collapse = "; "))
    message("Survey list contains missing values. Returning empty tibble with correct structure.")
    return(survey_schema)
  }
  message("Survey list is valid.")
  df
}

#' Validate variable list structure and data
#'
#' Checks that a dataframe matches the expected variable list schema with correct
#' column names, data types, and no missing values in any columns. Also validates
#' business rules for variable relationships and requirements.
#'
#' @param df A data frame to validate against the variable list schema
#'
#' @return If valid, invisibly returns TRUE. If invalid, returns error. Prints informative messages 
#'   about any issues found.
#'
#' @examples
#' varlist <- tibble::tibble(
#'  name = "code",
#'  label ="country code",
#'  type = "character",
#'  units = "ISO3",
#'  id = 1L,
#'  outcome = 0L,
#'  hazard = 0L,
#'  ind = 0L,
#'  hh = 0L,
#'  firm = 0L,
#'  area = 0L,
#'  interact = 0L,
#'  fe = 1L
#')
#' validate_varlist(varlist)
#'
validate_varlist <- function(df) {

  message("Validating variable list structure and data...")

  # Define expected survey list schema
  varlist_schema <- tibble::tibble(
    name = character(),
    label = character(),
    type = character(),
    units = character(),
    id = integer(),
    outcome = integer(),
    hazard = integer(),
    ind = integer(),
    hh = integer(),
    firm = integer(),
    area = integer(),
    interact = integer(),
    fe = integer()
  )

  # If no data frame provided, stop
  if (missing(df) || is.null(df)) {
    stop("No variable list provided!")
  }
  
  # Check columns match schema
  if (!check_columns(df, varlist_schema)) {
    stop("Variable list does not match expected schema!")
  }
  
  # Check for empty/missing values (allow units to be empty)
  empty_cols <- character()
  for (col in setdiff(names(df), "units")) {
    if (any(is.na(df[[col]]) | df[[col]] == "")) {
      n_empty <- sum(is.na(df[[col]]) | df[[col]] == "")
      empty_cols <- c(empty_cols, paste0(col, " (", n_empty, " empty)"))
    }
  }
  
  if (length(empty_cols) > 0) {
    message("Columns with empty values: ", paste(empty_cols, collapse = "; "))
    stop("Variable list contains missing values!")
  }

  # Define required variables with their expected specifications
  required_specs <- tibble::tribble(
    ~name,        ~label,                      ~type,        ~units, ~id, ~outcome, ~hazard, ~ind, ~hh, ~firm, ~area, ~interact, ~fe,
    "code",       "Country code",              "character",  "",     1L,  0L,       0L,       0L,   0L,  0L,    0L,    0L,        1L,
    "economy",    "Economy",                   "character",  "",     1L,  0L,       0L,       0L,   0L,  0L,    0L,    0L,        0L,
    "year",       "Starting year of survey",   "integer",    "",     1L,  0L,       0L,       0L,   0L,  0L,    0L,    0L,        1L,
    "survname",   "Survey acronym",            "character",  "",     1L,  0L,       0L,       0L,   0L,  0L,    0L,    0L,        0L,
    "loc_id",     "Spatial unit ID",           "character",  "",     1L,  0L,       0L,       0L,   0L,  0L,    0L,    0L,        1L,
    "h3",         "H3 cell index",             "character",  "",     1L,  0L,       0L,       0L,   0L,  0L,    0L,    0L,        0L,
    "int_year",   "Interview year",            "integer",    "",     1L,  0L,       0L,       0L,   0L,  0L,    0L,    0L,        1L,
    "int_month",  "Interview month",           "integer",    "",     1L,  0L,       0L,       0L,   0L,  0L,    0L,    0L,        1L,
    "timestamp",  "Month",           "Date",       "",     1L,  0L,       0L,       0L,   0L,  0L,    0L,    0L,        0L
  )
  
  # Check all required variables are included
  missing_required <- setdiff(required_specs$name, df$name)
  if (length(missing_required) > 0) {
    stop("Required variables missing from variable list: ", 
         paste(missing_required, collapse = ", "))
  }
  
  # Check required variables have correct specifications
  for (i in seq_len(nrow(required_specs))) {
    req_var <- required_specs$name[i]
    varlist_row <- df[df$name == req_var, ]
    expected_row <- required_specs[i, ]
    
    # Compare each column (excluding name)
    for (col in setdiff(names(expected_row), "name")) {
      if (varlist_row[[col]] != expected_row[[col]]) {
        stop("Required variable '", req_var, "' has incorrect specification for '", col, 
             "': expected ", expected_row[[col]], ", found ", varlist_row[[col]])
      }
    }
  }
  
  # At least one outcome variable required
  if (sum(df$outcome == 1) < 1) {
    stop("Variable list must contain at least one outcome variable!")
  }
  
  # At least one hazard variable required
  if (sum(df$hazard == 1) < 1) {
    stop("Variable list must contain at least one hazard variable!")
  }
  
  # Validate type values are standard R classes
  valid_types <- c("numeric", "integer", "logical", "character", "factor", "Date")
  invalid_types <- df$type[!df$type %in% valid_types]
  if (length(invalid_types) > 0) {
    stop("Invalid type values found: ", paste(unique(invalid_types), collapse = ", "),
         ". Valid types are: ", paste(valid_types, collapse = ", "))
  }
  
  # Check units: numeric/integer can have units, others should be empty
  numeric_vars <- df$name[df$type %in% c("numeric", "integer")]
  other_vars <- df$name[!df$type %in% c("numeric", "integer")]
  
  invalid_units <- df$name[df$name %in% other_vars & df$units != ""]
  if (length(invalid_units) > 0) {
    stop("Non-numeric variables should have empty units: ", 
         paste(invalid_units, collapse = ", "))
  }
  
  # Validate indicator column relationships
  for (i in seq_len(nrow(df))) {
    var_name <- df$name[i]
    
    # Only id variables can have fe = 1
    if (df$fe[i] == 1 & df$id[i] != 1) {
      stop("Variable '", var_name, "' has fe = 1 but id != 1. Only id variables can have fe = 1.")
    }
    
    # Hazard variables cannot have any other indicator = 1
    if (df$hazard[i] == 1) {
      other_indicators <- c("id", "outcome", "ind", "hh", "firm", "area", "interact", "fe")
      if (any(df[i, other_indicators] == 1)) {
        stop("Variable '", var_name, "' has hazard = 1 but also has other indicators = 1. ",
             "Hazard variables cannot have other indicators.")
      }
    }
    
    # Outcome variables can only be combined with hh, ind, firm, area, or interact
    if (df$outcome[i] == 1) {
      invalid_combos <- c("id", "hazard", "fe")
      if (any(df[i, invalid_combos] == 1)) {
        stop("Variable '", var_name, "' has outcome = 1 but also has invalid indicator(s). ",
             "Outcome variables can only combine with hh, ind, firm, area, or interact.")
      }
    }
    
    # hh variables cannot be combined with firm or area
    if (df$hh[i] == 1) {
      if (df$firm[i] == 1 | df$area[i] == 1) {
        stop("Variable '", var_name, "' has hh = 1 combined with firm or area. ",
             "hh variables cannot be combined with firm or area.")
      }
    }
    
    # ind variables cannot be combined with hh or firm
    if (df$ind[i] == 1) {
      if (df$hh[i] == 1 | df$firm[i] == 1) {
        stop("Variable '", var_name, "' has ind = 1 combined with hh or firm. ",
             "ind variables cannot be combined with hh or firm.")
      }
    }
    
    # area variables cannot be combined with ind or hh
    if (df$area[i] == 1) {
      if (df$ind[i] == 1 | df$hh[i] == 1) {
        stop("Variable '", var_name, "' has area = 1 combined with ind or hh. ",
             "area variables cannot be combined with ind or hh.")
      }
    }
  }

  message("Variable list is valid.")
  invisible(TRUE)
}

#' Tidy Data Frame Based on Variable List
#'
#' Coerces columns in a data frame to the types specified in a variable list.
#' Only columns present in both the data frame and variable list are modified.
#'
#' @param df A data frame or tibble to be tidied. Can also be a duckdb connection.
#' @param varlist A data frame with at least two columns: `name` (character) 
#'   containing variable names, and `type` (character) containing R type names.
#'   Valid types are: "numeric", "integer", "logical", "character", "factor", "Date".
#'
#' @return A data frame with columns coerced to the specified types.
#'
#' @examples
#' \dontrun{
#' # Create a variable list
#' varlist <- data.frame(
#'   name = c("age", "income", "employed", "name"),
#'   type = c("integer", "numeric", "logical", "character")
#' )
#' 
#' # Tidy a data frame
#' df_clean <- tidy_vars(df, varlist)
#' 
#'
#' @seealso \code{\link{mutate}}, \code{\link{across}}
#' @export
#' 
tidy_vars <- function(df, varlist, gmd = NULL) {
  # Extract variable names by type
  num_vars  <- varlist$name[varlist$type == "numeric"]
  int_vars  <- varlist$name[varlist$type == "integer"]
  log_vars  <- varlist$name[varlist$type == "logical"]
  char_vars <- varlist$name[varlist$type == "character"]
  fact_vars <- varlist$name[varlist$type == "factor"]
  date_vars <- varlist$name[varlist$type == "Date"]

  # Apply conversions — treat factor as character in DuckDB (no ENUM issues)
  df_tidy <- df |>
    select(any_of(pull(varlist, name))) |>
    mutate(
      across(any_of(num_vars),               as.numeric),
      across(any_of(c(int_vars, log_vars)),  as.integer),
      across(any_of(c(char_vars, fact_vars)), as.character),
      across(any_of(date_vars),              as.Date)
    )

  # identify columns with data (not NA and not empty string)
  char_cols <- intersect(c(char_vars, fact_vars), colnames(df_tidy))
  num_cols  <- intersect(c(num_vars, int_vars, log_vars), colnames(df_tidy))

  cols_to_keep <- df_tidy |>
    summarise(
      across(any_of(char_cols), ~ max(case_when(is.na(.x) | .x == "" ~ 0, TRUE ~ 1), na.rm = TRUE)),
      across(any_of(num_cols),  ~ max(case_when(is.na(.x) ~ 0, TRUE ~ 1), na.rm = TRUE))
    ) |>
    collect() |>
    select(where(~ . == 1)) |>
    colnames()

  out <- df_tidy |> select(all_of(cols_to_keep), any_of(date_vars)) |> collect()

    # Re-attach haven value labels as factors after collect()
    # For _hh suffix variables, look up labels from the base variable name in gmd
    if (!is.null(gmd)) {
      for (v in intersect(c(fact_vars, paste0(fact_vars, "_hh")), colnames(out))) {
        # strip _hh suffix to find source variable in gmd
        base_v <- sub("_hh$", "", v)
        if (base_v %in% colnames(gmd) && inherits(gmd[[base_v]], "haven_labelled")) {
          # extract integer codes and their corresponding labels
          lbls      <- haven::as_factor(gmd[[base_v]])
          lbl_codes <- as.integer(attr(gmd[[base_v]], "labels"))  # named integer vector
          lbl_names <- names(attr(gmd[[base_v]], "labels"))       # character label names
          # map integer codes in out to factor levels
          out[[v]] <- factor(
            as.integer(as.numeric(out[[v]])),
            levels = lbl_codes,
            labels = lbl_names
          )
        }
      }
    }

  out
}

##' Check for Duplicate IDs in Dataset
#'
#' Validates that a specified ID variable (or combination of variables) is unique 
#' within a dataset. Useful for ensuring data quality in household or individual-level surveys.
#'
#' @param df A data frame or tibble to check for duplicate IDs.
#' @param id_var Character vector specifying the name(s) of the ID variable(s) to check.
#'   Can be a single variable (e.g., "hhid") or combination (e.g., c("hhid", "pid")).
#' @param survey_info Optional character string with survey information (e.g., 
#'   "GNB 2018") to include in the error message. If NULL, a generic message is used.
#'
#' @return Logical. Returns TRUE if no duplicates found, FALSE if duplicates exist.
#'   Also prints a message indicating the result.
#'
#' @examples
#' \dontrun{
#' # Check single ID variable
#' check_unique_ids(wise_hh, "hhid", paste(code, year))
#' 
#' # Check combination of ID variables
#' check_unique_ids(wise_ind, c("hhid", "pid"), paste(code, year, survname))
#' 
#' # Use in validation workflow
#' if (!check_unique_ids(wise_hh, "hhid", paste(code, year))) {
#'   next  # Skip to next iteration if duplicates found
#' }
#' }
#'
#' @seealso \code{\link{duplicated}}
#' @export
check_unique_ids <- function(df, id_var, survey_info = NULL) {
  # Collect data if it's a duckdb connection 
  df <- collect(df)
  # Check if all ID variables exist in dataframe
  missing_vars <- setdiff(id_var, names(df))
  if (length(missing_vars) > 0) {
    stop("ID variable(s) not found in dataframe: ", paste(missing_vars, collapse = ", "))
  }
  
  # Check for duplicates
  if (length(id_var) == 1) {
    # Single variable case
    has_duplicates <- any(duplicated(pull(df, !!id_var)))
    id_label <- id_var
  } else {
    # Multiple variables case - check combination
    has_duplicates <- any(duplicated(df[, id_var]))
    id_label <- paste(id_var, collapse = " + ")
  }
  
  # Construct message
  if (has_duplicates) {
    if (!is.null(survey_info)) {
      message("✗ ", id_label, " not unique in ", survey_info)
    } else {
      message("✗ ", id_label, " not unique in dataset")
    }
    return(FALSE)
  } else {
    if (!is.null(survey_info)) {
      message("✓ ", id_label, " is unique in ", survey_info)
    } else {
      message("✓ ", id_label, " is unique in dataset")
    }
    return(TRUE)
  }
}

#' Check Poverty Rate Against PIP Benchmark
#'
#' Validates that the poverty rate calculated from survey data matches the 
#' World Bank's Poverty and Inequality Platform (PIP) estimate within an 
#' acceptable tolerance. This check ensures data quality and consistency 
#' with official poverty statistics.
#'
#' @param df A data frame containing household-level survey data with welfare,
#'   weight, and other required variables.
#' @param code Character string specifying the three-letter country code 
#'   (e.g., "GNB").
#' @param year Integer specifying the survey year (e.g., 2018).
#' @param tolerance Numeric tolerance for acceptable difference between survey
#'   and PIP poverty rates. Default is 0.001 (0.1 percentage points).
#' @param welfare_var Character string specifying the name of the welfare 
#'   variable in df. Default is "welfare".
#' @param weight_var Character string specifying the name of the weight 
#'   variable in df. Default is "weight".
#'
#' @return Logical. Returns TRUE if poverty rates match within tolerance, 
#'   FALSE otherwise. Also prints a message with the comparison results.
#'
#' @examples
#' \dontrun{
#' # Check $3.00 poverty rate
#' check_poverty_rate(wise_hh, code, year)
#' 
#' # Check $3.00 poverty rate (international poverty line)
#' check_poverty_rate(wise_hh, code, year)
#' 
#' # Use in validation workflow
#' if (!check_poverty_rate(wise_hh, code, year)) {
#'   errors <- c(errors, paste0("Poverty rate mismatch for ", code, " ", year))
#'   next
#' }
#' }
#'
#' @seealso \code{\link[pipr]{get_stats}}
#' @export
check_poverty_rate <- function(df, code, year, 
                               tolerance = 0.001,
                               welfare_var = "welfare",
                               weight_var = "weight") {
  
  # Collect data if it's a duckdb connection
  df <- collect(df)
  
  # Get PIP poverty statistics
  # Get PIP poverty statistics - retry up to 3 times on httr2 errors
  pip_stats <- NULL
  for (attempt in 1:3) {
    tryCatch({
      pip_stats <- pipr::get_stats(code, year)
      break  # success - exit retry loop
    }, error = function(e) {
      if (attempt < 3) {
        message("PIP API error (attempt ", attempt, "/3), retrying in 5s: ", conditionMessage(e))
        Sys.sleep(5)
      } else {
        message("PIP API failed after 3 attempts for ", code, " ", year, ": ", conditionMessage(e))
      }
    })
  }

  # if empty or null, exit and print message
  if (is.null(pip_stats) || nrow(pip_stats) == 0) {
    message("No PIP statistics found for ", code, " ", year, ". Cannot check poverty rate.")
    return(FALSE)
  }

  pip_poor <- pull(pip_stats, headcount)
  
  # Calculate poverty rate from survey data
  svy_poor <- weighted.mean(
    df[[welfare_var]] / pip_stats$cpi / pip_stats$ppp < 3,
    df[[weight_var]]
  )
  
  # Check if rates match within tolerance
  match <- abs(round(pip_poor, 3) - round(svy_poor, 3)) <= tolerance
  
  # Print comparison message
  if (match) {
    message("✓ Poverty rate matches PIP: survey = ", round(svy_poor, 3), 
            ", PIP = ", round(pip_poor, 3), " for ", code, " ", year)
  } else {
    message("✗ Poverty rate mismatch: survey = ", round(svy_poor, 3), 
            ", PIP = ", round(pip_poor, 3), " for ", code, " ", year,
            " (difference = ", round(abs(svy_poor - pip_poor), 3), ")")
  }
  
  return(match)
}