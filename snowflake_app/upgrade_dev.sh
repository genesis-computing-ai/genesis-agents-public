#!/bin/bash

#!/bin/bash

# Before running this script, make sure to:
# 1. Install SnowCLI (https://docs.snowflake.com/en/user-guide/snowsql-install-config)
# 2. Add the following connections using SnowCLI:
#    snow connection add GENESIS-DEV-PROVIDER
#        Account is : nmb71612
#    snow connection add GENESIS-DEV-CONSUMER-2
#        Account is : rdb46973
# These connections are required for the commands below to work properly.
# You may need to make a new <authorized role> user without SSO/MFA for these

# Assign parameter to variable or default to ~/ if not provided
DIRECTORY_PATH=${1:-~/}

# Ensure the directory path does not end with a slash
DIRECTORY_PATH=${DIRECTORY_PATH%/}
# DIRECTORY_PATH=/Users/justin/Documents/Code

# Login to image repo
snow spcs image-registry token --connection GENESIS-DEV-PROVIDER --format=JSON
snow spcs image-registry token --connection GENESIS-DEV-PROVIDER --format=JSON | docker login dshrnxx-genesis-dev.registry.snowflakecomputing.com --username 0sessiontoken --password-stdin

# Copy main.py to sis_launch.py
cp ./streamlit_gui/main.py ./streamlit_gui/Genesis.py

# Build Docker image
docker build --rm -t genesis_app:latest --platform linux/amd64 .

# Tag Docker image
docker tag genesis_app:latest dshrnxx-genesis-dev.registry.snowflakecomputing.com/genesisapp_master/code_schema/service_repo/genesis_app:latest

# Push Docker image
docker push dshrnxx-genesis-dev.registry.snowflakecomputing.com/genesisapp_master/code_schema/service_repo/genesis_app:latest

# Clear stage
snow sql -c GENESIS-DEV-PROVIDER -q "RM @GENESISAPP_APP_PKG.CODE_SCHEMA.APP_CODE_STAGE"

# Upload streamlit files
snow sql -c GENESIS-DEV-PROVIDER -q "PUT file://$DIRECTORY_PATH/genesis/streamlit_gui/Genesis.py @GENESISAPP_APP_PKG.CODE_SCHEMA.APP_CODE_STAGE/code_artifacts/streamlit AUTO_COMPRESS=FALSE OVERWRITE=TRUE"
snow sql -c GENESIS-DEV-PROVIDER -q "PUT file://$DIRECTORY_PATH/genesis/streamlit_gui/utils.py @GENESISAPP_APP_PKG.CODE_SCHEMA.APP_CODE_STAGE/code_artifacts/streamlit AUTO_COMPRESS=FALSE OVERWRITE=TRUE"
snow sql -c GENESIS-DEV-PROVIDER -q "PUT file://$DIRECTORY_PATH/genesis/streamlit_gui/*.png @GENESISAPP_APP_PKG.CODE_SCHEMA.APP_CODE_STAGE/code_artifacts/streamlit AUTO_COMPRESS=FALSE OVERWRITE=TRUE"
snow sql -c GENESIS-DEV-PROVIDER -q "PUT file://$DIRECTORY_PATH/genesis/streamlit_gui/*.yml @GENESISAPP_APP_PKG.CODE_SCHEMA.APP_CODE_STAGE/code_artifacts/streamlit AUTO_COMPRESS=FALSE OVERWRITE=TRUE"

# Upload streamlit files
snow sql -c GENESIS-DEV-PROVIDER -q "PUT file://$DIRECTORY_PATH/genesis/streamlit_gui/.streamlit/config.toml @GENESISAPP_APP_PKG.CODE_SCHEMA.APP_CODE_STAGE/code_artifacts/streamlit/.streamlit AUTO_COMPRESS=FALSE OVERWRITE=TRUE"

# Upload streamlit page files
snow sql -c GENESIS-DEV-PROVIDER -q "PUT file://$DIRECTORY_PATH/genesis/streamlit_gui/page_files/*.py @GENESISAPP_APP_PKG.CODE_SCHEMA.APP_CODE_STAGE/code_artifacts/streamlit/page_files AUTO_COMPRESS=FALSE OVERWRITE=TRUE"

# Upload SQL files
snow sql -c GENESIS-DEV-PROVIDER -q "PUT file://$DIRECTORY_PATH/genesis/snowflake_app/setup_script.sql @GENESISAPP_APP_PKG.CODE_SCHEMA.APP_CODE_STAGE AUTO_COMPRESS=FALSE OVERWRITE=TRUE"

# Upload MD files
snow sql -c GENESIS-DEV-PROVIDER -q "PUT file://$DIRECTORY_PATH/genesis/snowflake_app/readme.md @GENESISAPP_APP_PKG.CODE_SCHEMA.APP_CODE_STAGE AUTO_COMPRESS=FALSE OVERWRITE=TRUE"

# Upload YML files
snow sql -c GENESIS-DEV-PROVIDER -q "PUT file://$DIRECTORY_PATH/genesis/snowflake_app/*.yml @GENESISAPP_APP_PKG.CODE_SCHEMA.APP_CODE_STAGE AUTO_COMPRESS=FALSE OVERWRITE=TRUE"

output=$(snow sql -c GENESIS-DEV-PROVIDER -q "ALTER APPLICATION PACKAGE GENESISAPP_APP_PKG ADD PATCH FOR VERSION V0_9 USING @GENESISAPP_APP_PKG.CODE_SCHEMA.APP_CODE_STAGE")

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
snow sql -c GENESIS-DEV-PROVIDER -q "ALTER APPLICATION PACKAGE GENESISAPP_APP_PKG SET DEFAULT RELEASE DIRECTIVE VERSION = V0_9 PATCH = $patch_number;"

echo "Patch $patch_number has been set as the default release directive."

# Check if patch number is 130
if [ "$patch_number" -eq 130 ]; then
    echo "WARNING: You will need to upgrade your version number before your next patch"
fi

if [ "$2" == "False" ]; then
    snow sql -c GENESIS-DEV-PROVIDER -q "ALTER APPLICATION GENESIS_BOTS UPGRADE USING VERSION V0_9;"
else
    snow sql -c GENESIS-DEV-CONSUMER-2 -q "alter application genesis_bots upgrade"

    # snow sql -c GENESIS-DEV-CONSUMER-2 -q "show services"

    snow sql -c GENESIS-DEV-CONSUMER-2 -q "call genesis_bots.core.run_arbitrary('grant all on warehouse app_xsmall to application role app_public;');"
    snow sql -c GENESIS-DEV-CONSUMER-2 -q "call genesis_bots.core.run_arbitrary('grant all on service GENESIS_BOTS.APP1.GENESISAPP_HARVESTER_SERVICE to application role app_public;');"
    snow sql -c GENESIS-DEV-CONSUMER-2 -q "call genesis_bots.core.run_arbitrary('grant all on service GENESIS_BOTS.APP1.GENESISAPP_KNOWLEDGE_SERVICE to application role app_public;');"
    snow sql -c GENESIS-DEV-CONSUMER-2 -q "call genesis_bots.core.run_arbitrary('grant all on service GENESIS_BOTS.APP1.GENESISAPP_TASK_SERVICE to application role app_public;');"
    snow sql -c GENESIS-DEV-CONSUMER-2 -q "call genesis_bots.core.run_arbitrary('grant all on service GENESIS_BOTS.APP1.GENESISAPP_SERVICE_SERVICE to application role app_public;');"
    snow sql -c GENESIS-DEV-CONSUMER-2 -q "call genesis_bots.core.run_arbitrary('grant all on all tables in schema GENESIS_BOTS.APP1 to application role app_public;');"
    snow sql -c GENESIS-DEV-CONSUMER-2 -q "call genesis_bots.core.run_arbitrary('grant select on GENESIS_BOTS.APP1.LLM_RESULTS to application role app_public;');"
    snow sql -c GENESIS-DEV-CONSUMER-2 -q "call genesis_bots.core.run_arbitrary('grant all on schema GENESIS_BOTS.APP1 to application role app_public;');"
    snow sql -c GENESIS-DEV-CONSUMER-2 -q "show applications;"
fi

echo "Upgrade complete"

# todo: add primary version fixing when needed:
#ALTER APPLICATION PACKAGE GENESISAPP_APP_PKG DROP VERSION V0_3 ;
#ALTER APPLICATION PACKAGE GENESISAPP_APP_PKG ADD VERSION V0_8 USING @GENESISAPP_APP_PKG.CODE_SCHEMA.APP_CODE_STAGE;#show versions in APPLICATION PACKAGE GENESISAPP_APP_PKG;
#ALTER APPLICATION PACKAGE GENESISAPP_APP_PKG SET DEFAULT RELEASE DIRECTIVE VERSION = V0_8 PATCH = 0;
#show versions in APPLICATION PACKAGE GENESISAPP_APP_PKG;
