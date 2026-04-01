# Prepare H3-indexed CMIP6 climate data for WISE-APP
#
# Index strategy:
#   For every H3 cell, compute the centroid lon/lat and snap to the nearest
#   CMIP6 1° grid point by rounding. Multiple H3 cells will map to the
#   same grid point — this is expected (they share the same coarse CMIP6 pixel).
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
cmip6_path <- "C:/Users/wb587256/OneDrive - WBG/Household survey locations to H3/02_data/raw/cmip6"
out_path <- file.path(data_path, "hazard", "weather", "projections")

h3_level <- 4L

ssp_list <- c("historical", "ssp245", "ssp370", "ssp585")
  # # test with one scenario
  # ssp_list <- "historical"

min_year <- 1950L
max_year <- 2100L

weather_vars <- c("t", "tn", "tx", "tx35", "tr", "r", "rx5day", "r20", "mrsos", "spei6")

  # # test with one variable
  # weather_vars <- c("t")

round0 <- c("tx35", "tr", "r", "r20", "rx5day")
round1 <- c("t", "tn", "tx")
round2 <- c("mrsos", "spei6")
clamp3 <- c("spei6")

# url for WBG admin-0 level geopackage
url <- "https://datacatalogfiles.worldbank.org/ddh-published/0038272/5/DR0095370/World Bank Official Boundaries (GeoPackage)/World Bank Official Boundaries - Admin 0.gpkg"


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
  code_list <- "AGO"

# ---------------------------------------------------------------------------
# Helper: H3 cells covering country → snapped ERA5 grid points
# ---------------------------------------------------------------------------
h3_snapped <- function(code, url, h3_level, h3_lookup = 7L, round_digits = 1) {
  
  con <- cached_connection()
  load_spatial()
  load_h3()
  DBI::dbExecute(con, "SET allow_asterisks_in_http_paths = true;")
  
  url <- gsub(" ", "%20", url)
  
  query <- paste0("
    SELECT DISTINCT
      h3_cell_to_parent(
        unnest(h3_polygon_wkb_to_cells(ST_AsWKB(ST_MakeValid(geom_part)), ", h3_lookup, ")),
        ", h3_level, "
      ) AS h3
    FROM (
      SELECT unnest(ST_Dump(geom))['geom'] AS geom_part
      FROM st_read('", url, "')
      WHERE ISO_A3 = '", code, "'
    )
    WHERE ST_GeometryType(geom_part) IN ('POLYGON', 'MULTIPOLYGON')
  ")
  
  dplyr::tbl(con, dplyr::sql(query)) |>
    dplyr::mutate(
      grid_lat = round(h3_cell_to_lat(h3), round_digits),
      grid_lon = round(h3_cell_to_lng(h3), round_digits)
    ) |>
    dplyr::select(h3, grid_lon, grid_lat)
}

# test
# h3_snapped("BFA", url, h3_level = 5L) |> head() |> print()


# ---------------------------------------------------------------------------
# Helper: register one .nc variable as a DuckDB view
# Returns a lazy tbl with columns: lon, lat, timestamp, model, <varname>
# Chunks by year to avoid memory limits for large countries
# Array dimension order: [lon, lat, time, member]
# ---------------------------------------------------------------------------

register_nc_variable <- function(varname, ssp, country_lonlat, year_chunk = NULL) {
  nc_path <- file.path(cmip6_path, glue("{varname}_{ssp}.nc"))
  nc <- nc_open(nc_path)

  lon_all    <- ncvar_get(nc, "lon")
  lat_all    <- ncvar_get(nc, "lat")
  time_raw   <- ncvar_get(nc, "time")
  time_units <- ncatt_get(nc, "time", "units")$value
  origin     <- as.Date(regmatches(time_units, regexpr("\\d{4}-\\d{2}-\\d{2}", time_units)))
  timestamps <- origin + time_raw
  member_ids <- ncvar_get(nc, "member_id")
  n_member   <- length(member_ids)

  lon_offset <- lon_all[1] - floor(lon_all[1])
  lat_offset <- lat_all[1] - floor(lat_all[1])

  lon_idx <- which(round(lon_all + lon_offset) %in% unique(country_lonlat$grid_lon))
  lat_idx <- which(round(lat_all + lat_offset) %in% unique(country_lonlat$grid_lat))

  lon_sub <- lon_all[lon_idx]
  lat_sub <- lat_all[lat_idx]

  # filter timestamps to requested year chunk (if provided)
  if (!is.null(year_chunk)) {
    t_idx <- which(as.integer(format(timestamps, "%Y")) == year_chunk)
  } else {
    t_idx <- seq_along(timestamps)
  }

  # limit to min/max year config (safety check)
  t_idx <- t_idx[as.integer(format(timestamps[t_idx], "%Y")) >= min_year &
                  as.integer(format(timestamps[t_idx], "%Y")) <= max_year]
  timestamps_sub <- timestamps[t_idx]

  n_lon <- length(lon_sub)
  n_lat <- length(lat_sub)
  n_t   <- length(timestamps_sub)

  # read all members in one call; array order is [lon, lat, time, member]
  vals <- ncvar_get(
    nc, varname,
    start = c(min(lon_idx), min(lat_idx), min(t_idx), 1),
    count = c(n_lon, n_lat, n_t, n_member)
  )
  # replace sentinel fill values — exact equality is unreliable at 1e38 scale
  vals[vals < -1e20 | vals > 1e20] <- NA
  nc_close(nc)

  # expand.grid order must match array dimension order: lon, lat, time, member
  grid <- expand.grid(
    lon       = lon_sub,
    lat       = lat_sub,
    timestamp = timestamps_sub,
    model     = member_ids
  )

 dt <- data.table(
    lon       = round(grid$lon + lon_offset),
    lat       = round(grid$lat + lat_offset),
    timestamp = grid$timestamp,
    model     = grid$model,
    value     = as.vector(vals)
  )

  needed <- data.table(lon = country_lonlat$grid_lon, lat = country_lonlat$grid_lat)
  dt <- dt[needed, on = .(lon, lat), nomatch = NA]

  con <- duckdbfs:::cached_connection()
  view_name <- glue("nc_{varname}_{ssp}_{year_chunk %||% 'all'}")
  duckdb::duckdb_register(con, view_name, dt)
  dplyr::tbl(con, view_name) |> rename(!!varname := value)
}

# ---------------------------------------------------------------------------
# Helper: get all years available in a given .nc file, filtered to config range
# ---------------------------------------------------------------------------

get_nc_years <- function(varname, ssp) {
  nc_path    <- file.path(cmip6_path, glue("{varname}_{ssp}.nc"))
  nc         <- nc_open(nc_path)
  time_raw   <- ncvar_get(nc, "time")
  time_units <- ncatt_get(nc, "time", "units")$value
  origin     <- as.Date(regmatches(time_units, regexpr("\\d{4}-\\d{2}-\\d{2}", time_units)))
  nc_close(nc)
  years <- sort(unique(as.integer(format(origin + time_raw, "%Y"))))
  years[years >= min_year & years <= max_year]
}

# ---------------------------------------------------------------------------
# Main loop: one country at a time, chunked by year for large countries
# ---------------------------------------------------------------------------

# threshold: if grid points exceed this, process year-by-year.
# at 1 degree resolution even large countries have only a few hundred grid
# points, but the member dimension (x30) multiplies memory, so keep low.
large_country_threshold <- 500L

for (code in code_list) {
  message("\n========== ", code, " ==========")

  # H3 grid and lon/lat lookup are SSP-independent — compute once per country
  country_grid <- h3_snapped(code, url, h3_level, round_digits = 0)
  country_lonlat <- country_grid |>
    select(grid_lon, grid_lat) |>
    distinct() |>
    collect()

  n_grid <- nrow(country_lonlat)
  message("  Country grid points: ", n_grid)

  for (ssp in ssp_list) {
    message("  SSP: ", ssp)

    # skip if output already exists
    dir.create(file.path(out_path, code), showWarnings = FALSE)
    out_file <- file.path(out_path, code, glue("{code}_cmip6_{ssp}.parquet"))
    if (file.exists(out_file)) {
      message("  Output already exists, skipping: ", out_file)
      next
    }

    # chunk by year for large countries to avoid memory limits
    years         <- get_nc_years(weather_vars[1], ssp)
    chunk_by_year <- n_grid > large_country_threshold

    if (chunk_by_year) {
      message("  Large country - processing ", length(years), " years individually")
      chunk_files <- character(0)
    }

    process_year <- function(yr) {
      yr_label <- if (chunk_by_year) yr else NULL

      var_tbls <- map(weather_vars, function(varname) {
        nc_tbl <- register_nc_variable(varname, ssp, country_lonlat, year_chunk = yr_label)

        country_grid |>
          inner_join(nc_tbl, by = c("grid_lon" = "lon", "grid_lat" = "lat")) |>
          group_by(h3, model, timestamp) |>
          summarise(!!varname := mean(!!sym(varname), na.rm = TRUE), .groups = "drop")
      })

      combined <- reduce(
        var_tbls[-1],
        ~ left_join(.x, .y, by = c("h3", "model", "timestamp")),
        .init = var_tbls[[1]]
      )

      combined |>
        mutate(
          across(any_of(clamp3), ~ if_else(is.na(.x), NA, pmax(-3, pmin(.x, 3)))),
          across(any_of(round0), ~ round(.x, 0)),
          across(any_of(round1), ~ round(.x, 1)),
          across(any_of(round2), ~ round(.x, 2)),
          h3 = as.integer64(h3)
        ) |>
        select(h3, model, timestamp, all_of(weather_vars)) |>
        arrange(h3, model, timestamp)
    }

    if (chunk_by_year) {
      # process one year at a time, write chunks to temp parquet files
      for (yr in years) {
        message("  Year: ", yr)
        chunk_file <- file.path(out_path, code, glue("{code}_cmip6_{ssp}_{yr}.parquet"))
        result     <- process_year(yr)
        write_dataset(result, chunk_file, options = c("COMPRESSION ZSTD"))
        chunk_files <- c(chunk_files, chunk_file)
        # free DuckDB registered views and R memory between years
        con <- duckdbfs:::cached_connection()
        for (v in weather_vars) {
          try(duckdb::duckdb_unregister(con, glue("nc_{v}_{ssp}_{yr}")), silent = TRUE)
        }
        gc()
      }
      # combine all yearly chunks into final parquet
      message("  Combining ", length(chunk_files), " yearly chunks...")
      open_dataset(chunk_files) |>
        arrange(h3, model, timestamp) |>
        write_dataset(out_file, options = c("COMPRESSION ZSTD, ROW_GROUP_SIZE 1000000"))
      # remove temp chunk files
      file.remove(chunk_files)

    } else {
      # small country - process all years at once
      message("  Reading ", paste(weather_vars, collapse = ", "))
      result <- process_year(NULL)
      write_dataset(result, out_file, options = c("COMPRESSION ZSTD, ROW_GROUP_SIZE 1000000"))
    }

    n_rows <- open_dataset(out_file) |> count() |> pull(n)
    message("  Written: ", out_file, "  [", format(n_rows, big.mark = ","), " rows]")

  } # end ssp loop

  duckdbfs::close_connection()
  gc()

} # end country loop
message("\nDone.")

# ---------------------------------------------------------------------------
# Validation (last country processed)
# ---------------------------------------------------------------------------

message("\n--- Validation (last country) ---")
code_list <- "AGO"
sample_tbl <- open_dataset(
  file.path(out_path, tail(code_list, 1), glue("{tail(code_list, 1)}_cmip6_historical.parquet"))
)

sample_tbl |> head(5) |> collect() |> print()

sample_tbl |>
  summarise(
    n_rows       = n(),
    n_cells      = n_distinct(h3),
    n_models     = n_distinct(model),
    n_timestamps = n_distinct(timestamp),
    t_min        = min(timestamp, na.rm = TRUE),
    t_max        = max(timestamp, na.rm = TRUE)
  ) |>
  collect() |> print()

message("\n--- Validation (check no h3 missing) ---")

h3_survey <- h3_snapped(tail(code_list, 1), url, h3_level, round_digits = 0) |> pull(h3)

h3_cmip6 <- open_dataset(
  file.path(out_path, tail(code_list, 1), glue("{tail(code_list, 1)}_cmip6_historical.parquet"))
) |> distinct(h3) |> pull(h3)

missing_h3 <- setdiff(h3_survey, h3_cmip6)
if (length(missing_h3) == 0) {
  message("No missing H3 cells")
} else {
  message("Missing H3 cells: ", length(missing_h3))
  print(head(missing_h3))
}
