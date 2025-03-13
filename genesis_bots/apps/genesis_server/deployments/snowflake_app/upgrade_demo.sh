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
# You may need to make a new <authorized role> user without SSO/MFA for these

# Assign parameter to variable or default to ~/ if not provided
DIRECTORY_PATH=${1:-~/}

# Ensure the directory path does not end with a slash
DIRECTORY_PATH=${DIRECTORY_PATH%/}

echo "starting"
# Run make_alpha_sis_launch.py
python3 genesis_bots/apps/streamlit_gui/make_demo_compute_pool.py
sleep 5

# Login to image repo
# snow spcs image-registry token --connection GENESIS-ALPHA-PROVIDER --format=JSON
snow spcs image-registry token --connection GENESIS-ALPHA-PROVIDER --format=JSON | docker login dshrnxx-genesis.registry.snowflakecomputing.com --username 0sessiontoken --password-stdin


# Build Docker image
docker build --rm -t genesis_app:latest --platform linux/amd64 .
# Tag Docker image
docker tag genesis_app:latest dshrnxx-genesis.registry.snowflakecomputing.com/genesisapp_master/code_schema/service_repo/genesis_app:latest
# Push Docker image
docker push dshrnxx-genesis.registry.snowflakecomputing.com/genesisapp_master/code_schema/service_repo/genesis_app:latest
# Clear stage
snow sql -c GENESIS-ALPHA-PROVIDER -q "RM @GENESISAPP_APP_PKG_COMM.CODE_SCHEMA.APP_CODE_STAGE"

# Upload streamlit files
snow sql -c GENESIS-ALPHA-PROVIDER -q "PUT file://$DIRECTORY_PATH/genesis_bots/apps/streamlit_gui/Genesis.py @GENESISAPP_APP_PKG_COMM.CODE_SCHEMA.APP_CODE_STAGE/code_artifacts/streamlit AUTO_COMPRESS=FALSE OVERWRITE=TRUE"
snow sql -c GENESIS-ALPHA-PROVIDER -q "PUT file://$DIRECTORY_PATH/genesis_bots/apps/streamlit_gui/utils.py @GENESISAPP_APP_PKG_COMM.CODE_SCHEMA.APP_CODE_STAGE/code_artifacts/streamlit AUTO_COMPRESS=FALSE OVERWRITE=TRUE"
snow sql -c GENESIS-ALPHA-PROVIDER -q "PUT file://$DIRECTORY_PATH/genesis_bots/apps/streamlit_gui/*.png @GENESISAPP_APP_PKG_COMM.CODE_SCHEMA.APP_CODE_STAGE/code_artifacts/streamlit AUTO_COMPRESS=FALSE OVERWRITE=TRUE"
snow sql -c GENESIS-ALPHA-PROVIDER -q "PUT file://$DIRECTORY_PATH/genesis_bots/apps/streamlit_gui/*.yml @GENESISAPP_APP_PKG_COMM.CODE_SCHEMA.APP_CODE_STAGE/code_artifacts/streamlit AUTO_COMPRESS=FALSE OVERWRITE=TRUE"

# Upload streamlit files
snow sql -c GENESIS-ALPHA-PROVIDER -q "PUT file://$DIRECTORY_PATH/genesis_bots/apps/streamlit_gui/.streamlit/config.toml @GENESISAPP_APP_PKG_COMM.CODE_SCHEMA.APP_CODE_STAGE/code_artifacts/streamlit/.streamlit AUTO_COMPRESS=FALSE OVERWRITE=TRUE"

# Upload streamlit page files
snow sql -c GENESIS-ALPHA-PROVIDER -q "PUT file://$DIRECTORY_PATH/genesis_bots/apps/streamlit_gui/page_files/*.py @GENESISAPP_APP_PKG_COMM.CODE_SCHEMA.APP_CODE_STAGE/code_artifacts/streamlit/page_files AUTO_COMPRESS=FALSE OVERWRITE=TRUE"

# Upload SQL files
snow sql -c GENESIS-ALPHA-PROVIDER -q "PUT file://$DIRECTORY_PATH/genesis_bots/apps/genesis_server/deployments/snowflake_app/setup_script.sql @GENESISAPP_APP_PKG_COMM.CODE_SCHEMA.APP_CODE_STAGE AUTO_COMPRESS=FALSE OVERWRITE=TRUE"

# Upload MD files
snow sql -c GENESIS-ALPHA-PROVIDER -q "PUT file://$DIRECTORY_PATH/genesis_bots/apps/genesis_server/deployments/snowflake_app/readme.md @GENESISAPP_APP_PKG_COMM.CODE_SCHEMA.APP_CODE_STAGE AUTO_COMPRESS=FALSE OVERWRITE=TRUE"

# Upload YML files
snow sql -c GENESIS-ALPHA-PROVIDER -q "PUT file://$DIRECTORY_PATH/genesis_bots/apps/genesis_server/deployments/snowflake_app/*.yml @GENESISAPP_APP_PKG_COMM.CODE_SCHEMA.APP_CODE_STAGE AUTO_COMPRESS=FALSE OVERWRITE=TRUE"

output=$(snow sql -c GENESIS-ALPHA-PROVIDER -q "ALTER APPLICATION PACKAGE GENESISAPP_APP_PKG_COMM ADD PATCH FOR VERSION V0_1 USING @GENESISAPP_APP_PKG_COMM.CODE_SCHEMA.APP_CODE_STAGE")

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
snow sql -c GENESIS-ALPHA-PROVIDER -q "ALTER APPLICATION PACKAGE GENESISAPP_APP_PKG_COMM SET DEFAULT RELEASE DIRECTIVE VERSION = V0_1 PATCH = $patch_number;"

echo "Patch $patch_number has been set as the default release directive."

# Check if patch number is 130
if [ "$patch_number" -eq 130 ]; then
    echo "WARNING: You will need to upgrade your version number before your next patch"
fi

snow sql -c GENESIS-ALPHA-CONSUMER -q "alter application genesis_bots upgrade"

snow sql -c GENESIS-ALPHA-CONSUMER -q "show services"

snow sql -c GENESIS-ALPHA-CONSUMER -q "call genesis_bots.core.run_arbitrary('grant all on warehouse app_xsmall to application role app_public;');"
snow sql -c GENESIS-ALPHA-CONSUMER -q "call genesis_bots.core.run_arbitrary('grant all on service genesis_bots.APP1.GENESISAPP_HARVESTER_SERVICE to application role app_public;');"
snow sql -c GENESIS-ALPHA-CONSUMER -q "call genesis_bots.core.run_arbitrary('grant all on service genesis_bots.APP1.GENESISAPP_KNOWLEDGE_SERVICE to application role app_public;');"
snow sql -c GENESIS-ALPHA-CONSUMER -q "call genesis_bots.core.run_arbitrary('grant all on service genesis_bots.APP1.GENESISAPP_TASK_SERVICE to application role app_public;');"
snow sql -c GENESIS-ALPHA-CONSUMER -q "call genesis_bots.core.run_arbitrary('grant all on service genesis_bots.APP1.GENESISAPP_SERVICE_SERVICE to application role app_public;');"
snow sql -c GENESIS-ALPHA-CONSUMER -q "call genesis_bots.core.run_arbitrary('grant all on all tables in schema genesis_bots.APP1 to application role app_public;');"
snow sql -c GENESIS-ALPHA-CONSUMER -q "call genesis_bots.core.run_arbitrary('grant select on genesis_bots.APP1.LLM_RESULTS to application role app_public;');"
snow sql -c GENESIS-ALPHA-CONSUMER -q "call genesis_bots.core.run_arbitrary('grant all on schema genesis_bots.APP1 to application role app_public;');"
snow sql -c GENESIS-ALPHA-CONSUMER -q "show applications;"

# python3 -c "import sys; sys.path.append('${PROJECT_ROOT}/apps/streamlit_gui'); from make_alpha_sis_launch import revert_genesis_bots; revert_genesis_bots()"


echo "Upgrade complete"

# todo: add primary version fixing when needed:
#ALTER APPLICATION PACKAGE $GENESISAPP_APP_PKG_COMM DROP VERSION V0_2 ;
#ALTER APPLICATION PACKAGE $GENESISAPP_APP_PKG_COMM ADD VERSION V0_1 USING @GENESISAPP_APP_PKG_COMM.CODE_SCHEMA.APP_CODE_STAGE;
#show versions in APPLICATION PACKAGE $GENESISAPP_APP_PKG_COMM;
