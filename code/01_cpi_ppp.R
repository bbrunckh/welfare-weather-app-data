# Download latest CPI and PPP conversion factors for WISE-APP

# This script downloads CPI and PPP conversion factors from World Bank PIP 
# and prepares them for use in WISE-APP. 

rm(list = ls())

#------------------------------------------------------------------------------#
# User inputs

# path to wise-app data/ directory 
data_path <- Sys.getenv("WISEAPP_DATA_PATH") 

#------------------------------------------------------------------------------#
# load libraries
library(pipr)
library(dplyr)
library(readr)

#------------------------------------------------------------------------------#
# dowload latest CPI and 2021 PPP conversion factors from World Bank PIP
cpi <- pipr::get_aux("cpi")
ppp <- pipr::get_aux("ppp")

# merge and clean
cpi_ppp <- cpi |>
  rename(cpi = value) |>
  left_join(select(ppp, -year), by = c("country_code", "data_level")) |>
  rename(ppp2021 = value) |>
  filter(!is.na(cpi), !is.na(ppp2021)) |>
  select(code = country_code, year, data_level, cpi, ppp2021)

# save metadata file with CPI and PPP conversion factors
write_csv(cpi_ppp, file.path(data_path, "metadata", "cpi_ppp.csv"))