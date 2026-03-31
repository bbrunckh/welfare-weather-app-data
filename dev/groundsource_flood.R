# Groundsource flood data schema
# Source: https://zenodo.org/records/18647054/files/groundsource_2026.parquet
# 
# Columns:
# - uuid: Unique identifier for each record
# - area_km2: Area of the reported location polygon (numeric)
# - start_date: Initial day of documented flood (YYYY-MM-DD format)
# - end_date: Final consecutive day of documented flood (YYYY-MM-DD format)
#   If single-day flood, end_date == start_date
# - geometry: Spatial boundary in WGS 84 (EPSG:4326)
#   Can be polygon or multipolygon depending on geocoding

library(duckdbfs)
load_h3()
load_spatial()
library(dplyr)

# Load groundsource flood data from zenodo
gs_url <- "https://zenodo.org/records/18647054/files/groundsource_2026.parquet"

con <- duckdbfs:::cached_connection()
DBI::dbExecute(con, "SET enable_geoparquet_conversion = false")
DBI::dbExecute(con, glue::glue(
  "CREATE OR REPLACE TEMPORARY VIEW gs AS
   SELECT * EXCLUDE geometry, ST_GeomFromWKB(geometry) AS geometry
   FROM read_parquet('{gs_url}')"
))

gs <- tbl(con, "gs")

# Index polygons to H3 at level 7
# Step 1: Explode multipolygons to single polygons, make valid
# Uses st_dump to unnest multipolygons, array_extract to get geometry,
# and st_makevalid to fix any invalid geometries
DBI::dbExecute(con, "
  CREATE OR REPLACE TEMPORARY VIEW gs_poly AS
  SELECT
    uuid,
    start_date,
    end_date,
    st_makevalid(
      array_extract(unnest(st_dump(ST_GeomFromWKB(geometry))), 'geom')
    ) AS geom
  FROM read_parquet('{gs_url}')
" |> glue::glue())

# Step 2: Extract H3 cells at level 7 for each polygon using overlap mode
# h3_polygon_wkb_to_cells_experimental returns all H3 cells overlapping the polygon
DBI::dbExecute(con, "
  CREATE OR REPLACE TEMPORARY VIEW gs_h3 AS
  SELECT
    uuid,
    start_date,
    end_date,
    unnest(
      h3_polygon_wkb_to_cells_experimental(ST_AsWKB(geom), 7, 'overlap')
    ) AS h3
  FROM gs_poly
  WHERE ST_GeometryType(geom) = 'POLYGON'
")

# Step 3: Generate monthly time series
# For each h3 cell + event, generate one row per month the flood was active
# timestamp = first day of each month the flood overlapped
DBI::dbExecute(con, "
  CREATE OR REPLACE TEMPORARY VIEW gs_monthly AS
  SELECT DISTINCT
    h3,
    date_trunc('month', month_date)::DATE AS timestamp
  FROM gs_h3,
  LATERAL (
    SELECT unnest(
      generate_series(
        date_trunc('month', start_date::DATE)::DATE,
        date_trunc('month', end_date::DATE)::DATE,
        INTERVAL '1 month'
      )
    ) AS month_date
  ) months
")

# Step 4: Write to parquet - one row per unique (h3, timestamp) combination
out_path <- "data/groundsource_h3.parquet"

DBI::dbExecute(con, glue::glue("
  COPY (
    SELECT h3, timestamp
    FROM gs_monthly
    ORDER BY timestamp, h3
  ) TO '{out_path}'
  (FORMAT PARQUET)
"))

message("✓ Written to ", out_path)

# Quick check
open_dataset("data/groundsource_h3.parquet") |> glimpse()