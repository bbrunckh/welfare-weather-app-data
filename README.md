# Prepare data for WISE-APP

This repository contains code used to pre-process microdata and weather data for analysis in WISE-APP.

Data sources:
-   Global Monitoring Database (GMD) household surveys
-   ERA5-Land global climate reanalysis (monthly aggregates)
-   ...

### 1. Define variables

`data/variable_list.csv` defines all variables derived from microdata and weather data. Each row documents a variable, its meaning, and how it can be used in the app:

-   `name`: Variable name as used in the data and code.
-   `label`: Human-readable description of the variable.
-   `type`: Data type (e.g., "numeric", "integer", "character", "factor", "logical", "Date").
-   `units`: Units of measurement (e.g., "LCU", "Celsius", "mm").
-   `id`: Indicates if the variable is an ID, date or other special variable (e.g. weight).
-   `outcome`: Indicates if the variable is an outcome of interest (1 if yes, 0 otherwise).
-   `weather`: Indicates if the variable measures weather (1 if yes, 0 otherwise).
-   `hh`: Indicates if the variable is at the household level (1 if yes, 0 otherwise).
-   `ind`: Indicates if the variable is at the individual level (1 if yes, 0 otherwise).
-   `firm`: Indicates if the variable is at the firm level (1 if yes, 0 otherwise).
-   `area`: Indicates if the variable is at the spatial unit level (1 if yes, 0 otherwise).
-   `interact`: Indicates if the variable can be an interaction term (1 if yes, 0 otherwise).
-   `fe`: Indicates if the variable can be used as a fixed effect (1 if yes, 0 otherwise).

Some variables must be included in the variable list and data for WISE-APP to function:
- `code`, `year`, `survname`, `int_year`, `int_month`, `h3`, `loc_id`, `timestamp`
- at least one outcome variable and at least one weather variable

### 2. Process data

The scripts in the `code/` folder prepare data inputs for WISE-APP. They clean, harmonize, and derive variables as defined in `variable_list.csv`. They also generate `survey_list.csv`. Each row in `survey_list.csv` describes the survey data prepared for analysis in WISE-APP:

-   `economy`: Economy name 
-   `code`: 3-letter country code 
-   `year`: Survey year
-   `survname`: Survey acronym
-   `level`: Data level (e.g., "ind", "hh", "firm")
-   `obs`: Number of observations
-   `source`: Data source (e.g., "GMD")

`code/cpi_ppp.R` pulls the latest Consumer Price Index (CPI) values and Purchasing Power Parity (PPP) values used by the World Bank to compute poverty and inequality statistics to `data/cpi_ppp.csv`. WISE-APP uses these to convert monetary variables in surveys (such as welfare) to 2021 PPP for comparability across survey years and countries.

All datasets WISE-APP requires are saved in the `data/` folder with the following naming convention:

- Variable list: `variable_list.csv` 
- Survey list: `survey_list.csv` 
- CPI & PPP conversion factors: `cpi_ppp.csv` 
- Survey data: `{code}_{year}_{survname}_{level}.parquet`
- Survey H3 data (for weather merge): `{code}_{year}_{survname}_h3.parquet`
- Weather data (monthly timeseries): `{code}_weather.parquet`