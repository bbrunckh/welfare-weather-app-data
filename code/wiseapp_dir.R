# Set-up data directory for WISE-APP

# data/
# ├── metadata/
# │   └── variable_list.csv
# │
# ├── microdata/
# │   ├── hh/
# │   ├── ind/
# │   ├── firm/
# │   └── h3/
# │
# └── hazard/
#     ├── weather/
#     └── events/

#------------------------------------------------------------------------------#
# User inputs

# path to wise-app data/ directory
data_path <- "data/"

# path to pre-existing WISE-APP variable list (see Readme for template)
varlist_path <- "data/variable_list.csv"

#------------------------------------------------------------------------------#
# create data directory and subfolders if it doesn't exist

dir.create(data_path, showWarnings = FALSE)
dir.create(file.path(data_path, "metadata"), showWarnings = FALSE)
dir.create(file.path(data_path, "microdata"), showWarnings = FALSE)
dir.create(file.path(data_path, "microdata", "hh"), showWarnings = FALSE)
dir.create(file.path(data_path, "microdata", "ind"), showWarnings = FALSE)
dir.create(file.path(data_path, "microdata", "firm"), showWarnings = FALSE)
dir.create(file.path(data_path, "microdata", "h3"), showWarnings = FALSE)
dir.create(file.path(data_path, "hazard"), showWarnings = FALSE)
dir.create(file.path(data_path, "hazard", "weather"), showWarnings = FALSE)
dir.create(file.path(data_path, "hazard", "events"), showWarnings = FALSE)

# copy variable to metadata folder if it exists
if (file.exists(varlist_path)) {
  file.copy(varlist_path, file.path(data_path, "metadata", "variable_list.csv"), overwrite = TRUE)
  message("✓ Copied variable list to ", file.path(data_path, "metadata", "variable_list.csv"))
} else {
  message("⚠️ Variable list not found at ", varlist_path, "\nPlease create a variable list and save it to this location.")
}
