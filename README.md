# Prepare data for WISE-APP

This repository contains code used to pre-process microdata and weather data for analysis in WISE-APP.

Data sources:
-   Global Monitoring Database (GMD) household surveys
-   ERA5-Land global climate reanalysis (monthly aggregates)
-   ...

### 1. Define variables

The variables derived from microdata and weather data should be defined in `variable_list.csv`. Each row documents a variable, its meaning, and how it can be used in the app across these columns:

-   `name`: Variable name as used in the data and code.
-   `label`: Human-readable description of the variable.
-   `type`: Data type (e.g., "numeric", "integer", "character", "factor", "logical", "Date").
-   `units`: Units of measurement (e.g., "LCU", "Celsius", "mm"), 
-   `id`: Indicates the variable is an ID, date or other special variable (e.g. weight).
-   `outcome`: Indicates the variable is an outcome of interest (1 if yes, 0 otherwise).
-   `weather`: Indicates the variable measures weather (1 if yes, 0 otherwise).
-   `hh`: Indicates the variable is measured at the household level (1 if yes, 0 otherwise).
-   `ind`: Indicates the variable is measured at the individual level (1 if yes, 0 otherwise).
-   `firm`: Indicates the variable is measured at the firm level (1 if yes, 0 otherwise).
-   `area`: Indicates the variable is measured at the area level (1 if yes, 0 otherwise).
-   `interact`: Indicates the variable can be an interaction term (1 if yes, 0 otherwise).
-   `fe`: Indicates the variable can be used as a fixed effect (1 if yes, 0 otherwise).

Some variables are required in the variable list for WISE-APP to function:
- `code`, `economy`, `year`, `survname`, `int_year`, `int_month`, `h3`, `loc_id`, `timestamp`
- at least one outcome variable and at least one weather variable
- the function `validate_varlist()` in `code/utils.R` checks the format of the variable list is valid and provides clear error messages. 

### 2. Prepare data for WISE-APP

The scripts in the `code/` folder prepare data inputs for WISE-APP. They clean, harmonize, and derive variables as defined in `variable_list.csv`. 

1. `code/gmd_surveys.R` produces individual and household level data files from GMD surveys. It also prepares the H3 data file used to merge weather data and `survey_list.csv`. Each row in `survey_list.csv` describes the surveys prepared for WISE-APP using these columns:
    -   `economy`: Economy name 
    -   `code`: 3-letter country code 
    -   `year`: Survey year
    -   `survname`: Survey acronym
    -   `level`: Data level (e.g., "ind", "hh", "firm")
    -   `obs`: Number of observations
    -   `source`: Data source (e.g., "GMD")

2. `era5land_weather.R` produces H3 level 6 data files with weather variables from ERA5-Land. 

3. `code/cpi_ppp.R` saves the latest Consumer Price Index (CPI) values and Purchasing Power Parity (PPP) values used by the World Bank to compute poverty and inequality statistics to `cpi_ppp.csv`. WISE-APP uses these to convert monetary variables in surveys (such as welfare) to 2021 PPP for comparability across survey years and countries.

4. `code/validate_wiseapp_data.R` checks for issues in data files prepared for WISE-APP (below) and provides clear error messages. 

#### WISE-APP data files
The datasets prepared for WISE-APP follow a standard naming convention:

- Variable list: `variable_list.csv` 
- Survey list: `survey_list.csv` 
- CPI & PPP conversion factors: `cpi_ppp.csv` 
- Survey data files: `{code}_{year}_{survname}_{level}.parquet`
- Survey H3 data files (for weather merge): `{code}_{year}_{survname}_h3.parquet`
- Weather data files (monthly timeseries): `{code}_weather.parquet`

#### H3 spatial index
WISE-APP uses the H3 spatial indexing system and timestamps to flexibly merge microdata and weather data:

- Survey data files (`{code}_{year}_{survname}_{level}.parquet`) must include a `loc_id` variable defining unique spatial units for that survey, as well as `int_year` and `int_month`.
- Survey H3 data files (`{code}_{year}_{survname}_h3.parquet`) must include `loc_id` and `h3`, which defines the H3 hexagon cell IDs corresponding to each spatial unit in a survey.
- Weather data files (`{code}_weather.parquet`) must include `h3` and `timestamp`, which are used to match weather to the location (using `loc_id`:`h3` in H3 data) and date of surveys.

![H3 spatial mapping](docs/locations.png)

### 3. Use the data in WISE-APP
The data files above can be saved to a local directory or any remote file system that WISE-APP can connect to.
...

