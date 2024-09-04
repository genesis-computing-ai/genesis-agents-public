#!/bin/bash

#!/bin/bash

# Usage: bash ./upgrade_ext.sh [directory_path]
# directory_path is the path to the genesis directory e.g. /Users/mrainey/Documents/GitHub

# Before running this script, make sure to:
# 1. Install SnowCLI (https://docs.snowflake.com/en/user-guide/snowsql-install-config)
# 2. Add the following connections using SnowCLI:
#    snow connection add GENESIS-EXT-PROVIDER
#        Account is : MMB84124
# These connections are required for the commands below to work properly.
# Setup key pair authentication for this account (doc TBD)

# Assign parameter to variable or default to ~/ if not provided
DIRECTORY_PATH=${1:-~/}

# Ensure the directory path does not end with a slash
DIRECTORY_PATH=${DIRECTORY_PATH%/}

# Login to image repo
snow spcs image-registry token --connection GENESIS-EXT-PROVIDER --format=JSON
snow spcs image-registry token --connection GENESIS-EXT-PROVIDER --format=JSON | docker login dshrnxx-genesis.registry.snowflakecomputing.com --username 0sessiontoken --password-stdin

# Build Docker image
docker build --rm -t genesis_app:latest --platform linux/amd64 .
# Tag Docker image
docker tag genesis_app:latest dshrnxx-genesis.registry.snowflakecomputing.com/genesisapp_master/code_schema/service_repo/genesis_app:latest
# Push Docker image
docker push dshrnxx-genesis.registry.snowflakecomputing.com/genesisapp_master/code_schema/service_repo/genesis_app:latest
echo "Docker push successful"

# Clear stage
snow sql -c GENESIS-EXT-PROVIDER -q "RM @GENESISAPP_APP_PKG_EXT.CODE_SCHEMA.APP_CODE_STAGE"

# Upload streamlit files
snow sql -c GENESIS-EXT-PROVIDER -q "PUT file://$DIRECTORY_PATH/genesis/streamlit_gui/Genesis.py @GENESISAPP_APP_PKG_EXT.CODE_SCHEMA.APP_CODE_STAGE/code_artifacts/streamlit AUTO_COMPRESS=FALSE OVERWRITE=TRUE"
snow sql -c GENESIS-EXT-PROVIDER -q "PUT file://$DIRECTORY_PATH/genesis/streamlit_gui/utils.py @GENESISAPP_APP_PKG_EXT.CODE_SCHEMA.APP_CODE_STAGE/code_artifacts/streamlit AUTO_COMPRESS=FALSE OVERWRITE=TRUE"
snow sql -c GENESIS-EXT-PROVIDER -q "PUT file://$DIRECTORY_PATH/genesis/streamlit_gui/*.png @GENESISAPP_APP_PKG_EXT.CODE_SCHEMA.APP_CODE_STAGE/code_artifacts/streamlit AUTO_COMPRESS=FALSE OVERWRITE=TRUE"
snow sql -c GENESIS-EXT-PROVIDER -q "PUT file://$DIRECTORY_PATH/genesis/streamlit_gui/*.yml @GENESISAPP_APP_PKG_EXT.CODE_SCHEMA.APP_CODE_STAGE/code_artifacts/streamlit AUTO_COMPRESS=FALSE OVERWRITE=TRUE"

# Upload streamlit files
snow sql -c GENESIS-EXT-PROVIDER -q "PUT file://$DIRECTORY_PATH/genesis/streamlit_gui/.streamlit/config.toml @GENESISAPP_APP_PKG_EXT.CODE_SCHEMA.APP_CODE_STAGE/code_artifacts/streamlit/.streamlit AUTO_COMPRESS=FALSE OVERWRITE=TRUE"

# Upload streamlit page files
snow sql -c GENESIS-EXT-PROVIDER -q "PUT file://$DIRECTORY_PATH/genesis/streamlit_gui/page_files/*.py @GENESISAPP_APP_PKG_EXT.CODE_SCHEMA.APP_CODE_STAGE/code_artifacts/streamlit/page_files AUTO_COMPRESS=FALSE OVERWRITE=TRUE"

# Upload SQL files
snow sql -c GENESIS-EXT-PROVIDER -q "PUT file://$DIRECTORY_PATH/genesis/snowflake_app/setup_script.sql @GENESISAPP_APP_PKG_EXT.CODE_SCHEMA.APP_CODE_STAGE AUTO_COMPRESS=FALSE OVERWRITE=TRUE"

# Upload MD files
snow sql -c GENESIS-EXT-PROVIDER -q "PUT file://$DIRECTORY_PATH/genesis/snowflake_app/readme.md @GENESISAPP_APP_PKG_EXT.CODE_SCHEMA.APP_CODE_STAGE AUTO_COMPRESS=FALSE OVERWRITE=TRUE"

# Upload YML files
snow sql -c GENESIS-EXT-PROVIDER -q "PUT file://$DIRECTORY_PATH/genesis/snowflake_app/*.yml @GENESISAPP_APP_PKG_EXT.CODE_SCHEMA.APP_CODE_STAGE AUTO_COMPRESS=FALSE OVERWRITE=TRUE"

output=$(snow sql -c GENESIS-EXT-PROVIDER -q "ALTER APPLICATION PACKAGE GENESISAPP_APP_PKG_EXT ADD PATCH FOR VERSION V0_1 USING @GENESISAPP_APP_PKG_EXT.CODE_SCHEMA.APP_CODE_STAGE")

# Output the result of the first command
echo "First command output:"
echo "$output"

# Extract the patch number using awk
patch_number=$(echo "$output" | awk -F'|' '/Patch/ {gsub(/^[ \t]+|[ \t]+$/, "", $4); print $4}')

# Output the extracted patch number
echo "Extracted patch number: $patch_number"

# Check if patch_number is empty
if [ -z "$patch_number" ]; then
    echo "Failed to extract patch number. Exiting."
    exit 1
fi

# Run the second command with the extracted patch number
# snow sql -c GENESIS-EXT-PROVIDER -q "ALTER APPLICATION PACKAGE GENESISAPP_APP_PKG_EXT SET DEFAULT RELEASE DIRECTIVE VERSION = V0_4 PATCH = $patch_number;"

# echo "Patch $patch_number has been set as the default release directive."

# Check if patch number is 130
if [ "$patch_number" -eq 130 ]; then
    echo "WARNING: You will need to upgrade your version number before your next patch"
fi

# snow sql -c GENESIS-EXT-CONSUMER -q "alter application genesis_bots upgrade"

# snow sql -c GENESIS-EXT-CONSUMER -q "show services"

echo "App package update complete. Create new patch, set distribution=EXTERNAL, release directive, and upgrade consumers, if needed."

# todo: add primary version fixing when needed:
#ALTER APPLICATION PACKAGE GENESISAPP_APP_PKG_EXT DROP VERSION V0_2 ;
#ALTER APPLICATION PACKAGE GENESISAPP_APP_PKG_EXT ADD VERSION V0_4 USING @GENESISAPP_APP_PKG_EXT.CODE_SCHEMA.APP_CODE_STAGE;
#show versions in APPLICATION PACKAGE GENESISAPP_APP_PKG_EXT;
