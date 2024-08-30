#!/bin/bash

#!/bin/bash

# Before running this script, make sure to:
# 1. Install SnowCLI (https://docs.snowflake.com/en/user-guide/snowsql-install-config)
# 2. Add the following connections using SnowCLI:
#    snow connection add GENESIS-ALPHA-PROVIDER
#        Account is : MMB84124
#    snow connection add GENESIS-ALPHA-CONSUMER
#        Account is : eqb52188
# These connections are required for the commands below to work properly.
# You may need to make a new ACCOUNTADMIN user without SSO/MFA for these

# Build Docker image
docker build --rm -t genesis_app:latest --platform linux/amd64 .
# Tag Docker image
docker tag genesis_app:latest dshrnxx-genesis.registry.snowflakecomputing.com/genesisapp_master/code_schema/service_repo/genesis_app:latest
# Push Docker image
docker push dshrnxx-genesis.registry.snowflakecomputing.com/genesisapp_master/code_schema/service_repo/genesis_app:latest

# Run make_alpha_sis_launch.py
python3 ./streamlit_gui/make_alpha_sis_launch.py
# Upload streamlit files
snow sql -c GENESIS-ALPHA-PROVIDER -q "PUT file:///Users/justin/Documents/Code/genesis/streamlit_gui/*.* @GENESISAPP_APP_PKG.CODE_SCHEMA.APP_CODE_STAGE/code_artifacts/streamlit AUTO_COMPRESS=FALSE OVERWRITE=TRUE"

# Upload streamlit page files
snow sql -c GENESIS-ALPHA-PROVIDER -q "PUT file:///Users/justin/Documents/Code/genesis/streamlit_gui/page_files/*.py @GENESISAPP_APP_PKG.CODE_SCHEMA.APP_CODE_STAGE/code_artifacts/streamlit/page_files AUTO_COMPRESS=FALSE OVERWRITE=TRUE"

# Upload SQL files
snow sql -c GENESIS-ALPHA-PROVIDER -q "PUT file:///Users/justin/Documents/Code/genesis/snowflake_app/*.sql @GENESISAPP_APP_PKG.CODE_SCHEMA.APP_CODE_STAGE AUTO_COMPRESS=FALSE OVERWRITE=TRUE"

# Upload MD files
snow sql -c GENESIS-ALPHA-PROVIDER -q "PUT file:///Users/justin/Documents/Code/genesis/snowflake_app/*.md @GENESISAPP_APP_PKG.CODE_SCHEMA.APP_CODE_STAGE AUTO_COMPRESS=FALSE OVERWRITE=TRUE"

# Upload YML files
snow sql -c GENESIS-ALPHA-PROVIDER -q "PUT file:///Users/justin/Documents/Code/genesis/snowflake_app/*.yml @GENESISAPP_APP_PKG.CODE_SCHEMA.APP_CODE_STAGE AUTO_COMPRESS=FALSE OVERWRITE=TRUE"

output=$(snow sql -c GENESIS-ALPHA-PROVIDER -q "ALTER APPLICATION PACKAGE GENESISAPP_APP_PKG ADD PATCH FOR VERSION V0_4 USING @GENESISAPP_APP_PKG.CODE_SCHEMA.APP_CODE_STAGE")

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
snow sql -c GENESIS-ALPHA-PROVIDER -q "ALTER APPLICATION PACKAGE GENESISAPP_APP_PKG SET DEFAULT RELEASE DIRECTIVE VERSION = V0_4 PATCH = $patch_number;"

echo "Patch $patch_number has been set as the default release directive."

# Check if patch number is 130
if [ "$patch_number" -eq 130 ]; then
    echo "WARNING: You will need to upgrade your version number before your next patch"
fi

snow sql -c GENESIS-ALPHA-CONSUMER -q "alter application genesis_bots_alpha upgrade"

snow sql -c GENESIS-ALPHA-CONSUMER -q "show services"

echo "Upgrade complete"

# todo: add primary version fixing when needed:
#ALTER APPLICATION PACKAGE GENESISAPP_APP_PKG DROP VERSION V0_2 ;
#ALTER APPLICATION PACKAGE GENESISAPP_APP_PKG ADD VERSION V0_4 USING @GENESISAPP_APP_PKG.CODE_SCHEMA.APP_CODE_STAGE;
#show versions in APPLICATION PACKAGE GENESISAPP_APP_PKG;
