# Set-up data directory for WISE-APP

# data/
# ├── metadata/
# │
# ├── microdata/
# │   ├── hh/
# │   ├── ind/
# │   ├── firm/
# │   └── h3/
# │
# └── hazard/
#     ├── weather/
#     │   ├── historical/
#     │   └── projections/
#     └── events/
#         ├── historical/
#         └── probabilistic/

#------------------------------------------------------------------------------#
# User inputs

# path to wise-app data/ directory 
data_path <- Sys.getenv("WISEAPP_DATA_PATH") 

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
dir.create(file.path(data_path, "hazard", "weather", "historical"), showWarnings = FALSE)
dir.create(file.path(data_path, "hazard", "weather", "projections"), showWarnings = FALSE)
dir.create(file.path(data_path, "hazard", "events"), showWarnings = FALSE)
dir.create(file.path(data_path, "hazard", "events", "historical"), showWarnings = FALSE)
dir.create(file.path(data_path, "hazard", "events", "probabilistic"), showWarnings = FALSE)

message("✓ Created data directory and subfolders at ", data_path)