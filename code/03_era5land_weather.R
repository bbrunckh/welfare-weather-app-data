# Prepare H3-indexed ERA5-Land weather data for WISE-APP
#
# Index strategy:
#   For every H3 cell, compute the centroid lon/lat and snap to the nearest
#   ERA5-Land 0.1° grid point by rounding. Multiple H3 cells will map to the
#   same grid point — this is expected (they share the same coarse ERA5 pixel).
#   This guarantees no H3 cell is missed.

rm(list = ls())

# ---------------------------------------------------------------------------
# Libraries
# ---------------------------------------------------------------------------

library(duckdbfs)
library(ncdf4)
library(data.table)
library(dplyr)
library(dbplyr)
library(glue)
library(purrr)
library(bit64)

source("code/utils.R")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

data_path <- Sys.getenv("WISEAPP_DATA_PATH")

varlist_path    <- file.path(data_path, "metadata", "variable_list.csv")
surveylist_path <- file.path(data_path, "metadata", "survey_list.csv")

era5land_path <- "/Users/bbrunckhorst/Library/CloudStorage/OneDrive-WBG/Household survey locations to H3/02_data/raw/era5land"

h3_path  <- file.path(data_path, "microdata", "h3")
out_path <- file.path(data_path, "hazard", "weather", "historical")

h3_level <- 6L

weather_vars <- c("t", "tn", "tx", "tx35", "tr", "r", "rx5day", "r20", "mrsos", "spei6")

  # # test with one variable
  # weather_vars <- c("t")

round0 <- c("tx35", "tr", "r", "r20", "rx5day")
round1 <- c("t", "tn", "tx")
round2 <- c("mrsos", "spei6")
clamp3 <- c("spei6")

# ---------------------------------------------------------------------------
# Load and validate WISE-APP variable list
# ---------------------------------------------------------------------------

if (file.exists(varlist_path)) {
  varlist <- read.csv(varlist_path)
  validate_varlist(varlist)
} else {
  stop("Variable list file not found at ", varlist_path)
}

# ---------------------------------------------------------------------------
# Country list
# ---------------------------------------------------------------------------

code_list <- read.csv(surveylist_path) |> pull(code) |> unique()

  # # test with one country
  # code_list <- "BFA"

# ---------------------------------------------------------------------------
# Helper: H3 cells → snapped ERA5 grid points
# ---------------------------------------------------------------------------

h3_snapped <- function(code) {
  dir   <- file.path(h3_path, code)
  files <- list.files(dir, pattern = "\\.parquet$", full.names = TRUE)

  open_dataset(files) |>
    mutate(h3 = h3_cell_to_parent(h3_string_to_h3(h3), h3_level)) |>
    distinct(h3) |>
    mutate(
      grid_lat = round(h3_cell_to_lat(h3), 1),
      grid_lon = round(h3_cell_to_lng(h3), 1)   # fix: lon not lng
    ) |>
    select(h3, grid_lon, grid_lat)
}

# ---------------------------------------------------------------------------
# Helper: register one .nc variable as a DuckDB view
# Returns a lazy tbl with columns: lon, lat, timestamp, <varname>
# Chunks by year to avoid memory limits for large countries (e.g. BRA)
# ---------------------------------------------------------------------------

register_nc_variable <- function(varname, country_lonlat, year_chunk = NULL) {
  nc_path <- file.path(era5land_path, glue("{varname}.nc"))

  nc <- nc_open(nc_path)

  lon_all    <- ncvar_get(nc, "lon")
  lat_all    <- ncvar_get(nc, "lat")
  time_raw   <- ncvar_get(nc, "time")
  time_units <- ncatt_get(nc, "time", "units")$value
  origin     <- as.Date(
    regmatches(time_units, regexpr("\\d{4}-\\d{2}-\\d{2}", time_units))
  )
  timestamps <- origin + time_raw
  fill_val   <- ncatt_get(nc, varname, "missing_value")$value

  lon_idx <- which(round(lon_all, 1) %in% unique(country_lonlat$grid_lon))
  lat_idx <- which(round(lat_all, 1) %in% unique(country_lonlat$grid_lat))

  lon_sub <- lon_all[lon_idx]
  lat_sub <- lat_all[lat_idx]

  # filter timestamps to requested year chunk (if provided)
  if (!is.null(year_chunk)) {
    t_idx <- which(as.integer(format(timestamps, "%Y")) == year_chunk)
  } else {
    t_idx <- seq_along(timestamps)
  }
  timestamps_sub <- timestamps[t_idx]

  vals <- ncvar_get(
    nc, varname,
    start = c(min(lon_idx), min(lat_idx), min(t_idx)),
    count = c(length(lon_idx), length(lat_idx), length(t_idx))
  )
  vals[vals == fill_val] <- NA
  nc_close(nc)

  n_lon <- length(lon_sub)
  n_lat <- length(lat_sub)
  n_t   <- length(timestamps_sub)

  grid <- expand.grid(lon = lon_sub, lat = lat_sub)

  dt <- data.table(
    lon       = round(rep(grid$lon, times = n_t), 1),
    lat       = round(rep(grid$lat, times = n_t), 1),
    timestamp = rep(timestamps_sub, each = n_lon * n_lat),
    value     = as.vector(vals)
  )

  needed <- data.table(lon = country_lonlat$grid_lon, lat = country_lonlat$grid_lat)
  dt     <- dt[needed, on = .(lon, lat), nomatch = NA]

  con <- duckdbfs:::cached_connection()
  view_name <- glue("nc_{varname}_{year_chunk %||% 'all'}")
  duckdb::duckdb_register(con, view_name, dt)
  dplyr::tbl(con, view_name) |> rename(!!varname := value)
}

# ---------------------------------------------------------------------------
# Helper: get all years available in any nc file
# ---------------------------------------------------------------------------
get_nc_years <- function(varname) {
  nc_path <- file.path(era5land_path, glue("{varname}.nc"))
  nc         <- nc_open(nc_path)
  time_raw   <- ncvar_get(nc, "time")
  time_units <- ncatt_get(nc, "time", "units")$value
  origin     <- as.Date(
    regmatches(time_units, regexpr("\\d{4}-\\d{2}-\\d{2}", time_units))
  )
  nc_close(nc)
  sort(unique(as.integer(format(origin + time_raw, "%Y"))))
}

# ---------------------------------------------------------------------------
# Main loop: one country at a time, chunked by year for large countries
# ---------------------------------------------------------------------------

# threshold: if grid points exceed this, process year-by-year
large_country_threshold <- 10000L

for (code in code_list) {
  message("\n========== ", code, " ==========")

  # Skip if output already exists
  dir.create(file.path(out_path, code), showWarnings = FALSE)
  out_file <- file.path(out_path, code, glue("{code}_era5land.parquet"))
  if (file.exists(out_file)) {
    message("Output already exists, skipping: ", out_file)
    next
  }

  country_grid <- h3_snapped(code)

  country_lonlat <- country_grid |>
    select(grid_lon, grid_lat) |>
    distinct() |>
    collect()

  n_grid <- nrow(country_lonlat)
  message("  Country grid points: ", n_grid)

  # chunk by year for large countries to avoid vector memory limit
  years <- get_nc_years(weather_vars[1])
  chunk_by_year <- n_grid > large_country_threshold

  if (chunk_by_year) {
    message("  Large country — processing ", length(years), " years individually")
    chunk_files <- character(0)
  }

  process_year <- function(yr) {
    yr_label <- if (chunk_by_year) yr else NULL

    var_tbls <- map(weather_vars, function(varname) {
      nc_tbl <- register_nc_variable(varname, country_lonlat, year_chunk = yr_label)

      country_grid |>
        inner_join(nc_tbl, by = c("grid_lon" = "lon", "grid_lat" = "lat")) |>
        group_by(h3, timestamp) |>
        summarise(!!varname := mean(!!sym(varname), na.rm = TRUE), .groups = "drop")
    })

    combined <- reduce(
      var_tbls[-1],
      ~ left_join(.x, .y, by = c("h3", "timestamp")),
      .init = var_tbls[[1]]
    )

    combined |>
      mutate(
        across(any_of(clamp3), ~ if_else(is.na(.x), NA, pmax(-3, pmin(.x, 3)))),
        across(any_of(round0), ~ round(.x, 0)),
        across(any_of(round1), ~ round(.x, 1)),
        across(any_of(round2), ~ round(.x, 2))
      ) |>
      select(h3, timestamp, all_of(weather_vars)) |>
      arrange(h3, timestamp)
  }

  if (chunk_by_year) {
    # process one year at a time, write chunks to temp parquet files
    for (yr in years) {
      message("  Year: ", yr)
      chunk_file <- file.path(out_path, code, glue("{code}_era5land_{yr}.parquet"))
      result     <- process_year(yr)
      write_dataset(result, chunk_file, options = c("COMPRESSION ZSTD"))
      chunk_files <- c(chunk_files, chunk_file)
      # free DuckDB registered views and R memory between years
      con <- duckdbfs:::cached_connection()
      for (v in weather_vars) {
        try(duckdb::duckdb_unregister(con, glue("nc_{v}_{yr}")), silent = TRUE)
      }
      gc()
    }
    # combine all yearly chunks into final parquet
    message("  Combining ", length(chunk_files), " yearly chunks...")
    open_dataset(chunk_files) |>
      arrange(h3, timestamp) |>
      write_dataset(out_file, options = c("COMPRESSION ZSTD"))
    # remove temp chunk files
    file.remove(chunk_files)

  } else {
    # small country — process all years at once
    message("  Reading ", paste(weather_vars, collapse = ", "))
    result <- process_year(NULL)
    write_dataset(result, out_file, options = c("COMPRESSION ZSTD"))
  }

  n_rows <- open_dataset(out_file) |> count() |> pull(n)
  message("  Written: ", out_file, "  [", format(n_rows, big.mark = ","), " rows]")

  duckdbfs::close_connection()
  gc()
}
message("\nDone.")

# ---------------------------------------------------------------------------
# Validation (last country processed)
# ---------------------------------------------------------------------------

message("\n--- Validation (last country) ---")

sample_tbl <- open_dataset(
  file.path(out_path, tail(code_list, 1), glue("{tail(code_list, 1)}_era5land.parquet"))
)

sample_tbl |> head(5) |> collect() |> print()

sample_tbl |>
  summarise(
    n_rows       = n(),
    n_cells      = n_distinct(h3),
    n_cells_no_data = sum(as.integer(is.na(t)), na.rm = TRUE),
    n_timestamps = n_distinct(timestamp),
    t_min        = min(timestamp, na.rm = TRUE),
    t_max        = max(timestamp, na.rm = TRUE)
  ) |>
  collect() |> print()

message("\n--- Validation (check no h3 missing) ---")

h3_survey <- h3_snapped(tail(code_list, 1)) |> pull(h3)

h3_era5land <- open_dataset(
  file.path(out_path, tail(code_list, 1), glue("{tail(code_list, 1)}_era5land.parquet"))
) |> distinct(h3) |> pull(h3)

missing_h3 <- setdiff(h3_survey, h3_era5land)
if (length(missing_h3) == 0) {
  message("No missing H3 cells")
} else {
  message("Missing H3 cells: ", length(missing_h3))
  print(head(missing_h3))
}
