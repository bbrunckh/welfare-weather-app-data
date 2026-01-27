# Prepare data for WISE-APP

This repository contains code used to pre-process microdata and weather data for analysis in WISE-APP.

Data sources:

-   Household surveys from Global Monitoring Database (GMD)

-   ERA5-Land global climate reanalysis

-   ...

### 1. Define variables

`data/variable_list.csv` defines all variables derived from microdata and weather data. Each row documents a variable, its meaning, and how it can be used in the app:

-   `name`: Variable name as used in the data and code.
-   `label`: Human-readable description of the variable.
-   `type`: Data type (e.g., numeric, integer, character, factor, logical, Date).
-   `outcome`: Indicates if the variable is an outcome of interest (1 if yes, 0 otherwise).
-   `weather`: Indicates if the variable is weather-related (1 if yes, 0 otherwise).
-   `hh`: Indicates if the variable is at the household level (1 if yes, 0 otherwise).
-   `ind`: Indicates if the variable is at the individual level (1 if yes, 0 otherwise).
-   `firm`: Indicates if the variable is at the firm level (1 if yes, 0 otherwise).
-   `area`: Indicates if the variable is at the geographic or spatial unit level (1 if yes, 0 otherwise).
-   `interact`: Indicates if the variable can be an interaction term (1 if yes, 0 otherwise).
-   `fe`: Indicates if the variable can be used as a fixed effect (1 if yes, 0 otherwise).
-   `weight`: Indicates if the variable is a weight for analysis (1 if yes, 0 otherwise).

### 2. Process data

The scripts in the `code/` folder prepare data inputs for WISE-APP. They clean, harmonize, and derive variables as defined in `variable_list.csv`.

They also generate `survey_list.csv`. Each row in `survey_list.csv` describes the survey data prepared for analysis in WISE-APP.

-   `countryname`: Country name (string)

-   `code`: 3-letter country code (string)

-   `year`: Survey year (integer)

-   `survname`: Survey name (string)

-   `level`: Data level (e.g., "individual", "household", "firm")

-   `obs`: Number of observations (integer)

-   `source`: Data source (string)