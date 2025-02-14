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

# Login to image repo
snow --config-file ~/.snowcli/config.toml spcs image-registry token --connection GENESIS-DEV-PROVIDER --format=JSON
snow --config-file ~/.snowcli/config.toml spcs image-registry token --connection GENESIS-DEV-PROVIDER --format=JSON | docker login dshrnxx-genesis-dev.registry.snowflakecomputing.com --username 0sessiontoken --password-stdin

# Build Docker image
docker build --rm -t genesis_app:latest --platform linux/amd64 .

# Tag Docker image
docker tag genesis_app:latest dshrnxx-genesis-dev.registry.snowflakecomputing.com/genesisapp_master/code_schema/service_repo/genesis_app:latest

# Push Docker image
docker push dshrnxx-genesis-dev.registry.snowflakecomputing.com/genesisapp_master/code_schema/service_repo/genesis_app:latest

# Clear stage
snow --config-file ~/.snowcli/config.toml sql -c GENESIS-DEV-PROVIDER -q "RM @GENESISAPP_APP_PKG.CODE_SCHEMA.APP_CODE_STAGE"

# Upload streamlit files
snow --config-file ~/.snowcli/config.toml sql -c GENESIS-DEV-PROVIDER -q "PUT file://$DIRECTORY_PATH/genesis_bots/genesis_bots/apps/streamlit_gui/Genesis.py @GENESISAPP_APP_PKG.CODE_SCHEMA.APP_CODE_STAGE/code_artifacts/streamlit AUTO_COMPRESS=FALSE OVERWRITE=TRUE"
snow --config-file ~/.snowcli/config.toml sql -c GENESIS-DEV-PROVIDER -q "PUT file://$DIRECTORY_PATH/genesis_bots/apps/streamlit_gui/utils.py @GENESISAPP_APP_PKG.CODE_SCHEMA.APP_CODE_STAGE/code_artifacts/streamlit AUTO_COMPRESS=FALSE OVERWRITE=TRUE"
snow --config-file ~/.snowcli/config.toml sql -c GENESIS-DEV-PROVIDER -q "PUT file://$DIRECTORY_PATH/genesis_bots/apps/streamlit_gui/*.png @GENESISAPP_APP_PKG.CODE_SCHEMA.APP_CODE_STAGE/code_artifacts/streamlit AUTO_COMPRESS=FALSE OVERWRITE=TRUE"
snow --config-file ~/.snowcli/config.toml sql -c GENESIS-DEV-PROVIDER -q "PUT file://$DIRECTORY_PATH/genesis_bots/apps/streamlit_gui/*.yml @GENESISAPP_APP_PKG.CODE_SCHEMA.APP_CODE_STAGE/code_artifacts/streamlit AUTO_COMPRESS=FALSE OVERWRITE=TRUE"

# Upload streamlit files
snow --config-file ~/.snowcli/config.toml sql -c GENESIS-DEV-PROVIDER -q "PUT file://$DIRECTORY_PATH/genesis_bots/apps/streamlit_gui/.streamlit/config.toml @GENESISAPP_APP_PKG.CODE_SCHEMA.APP_CODE_STAGE/code_artifacts/streamlit/.streamlit AUTO_COMPRESS=FALSE OVERWRITE=TRUE"

# Upload streamlit page files
snow --config-file ~/.snowcli/config.toml sql -c GENESIS-DEV-PROVIDER -q "PUT file://$DIRECTORY_PATH/genesis_bots/apps/streamlit_gui/page_files/*.py @GENESISAPP_APP_PKG.CODE_SCHEMA.APP_CODE_STAGE/code_artifacts/streamlit/page_files AUTO_COMPRESS=FALSE OVERWRITE=TRUE"

# Upload SQL files
snow --config-file ~/.snowcli/config.toml sql -c GENESIS-DEV-PROVIDER -q "PUT file://$DIRECTORY_PATH/genesis_bots/apps/genesis_server/deployments/snowflake_app/setup_script.sql @GENESISAPP_APP_PKG.CODE_SCHEMA.APP_CODE_STAGE AUTO_COMPRESS=FALSE OVERWRITE=TRUE"

# Upload MD files
snow --config-file ~/.snowcli/config.toml sql -c GENESIS-DEV-PROVIDER -q "PUT file://$DIRECTORY_PATH/gensis_bots/apps/genesis_server/deployments/snowflake_app/readme.md @GENESISAPP_APP_PKG.CODE_SCHEMA.APP_CODE_STAGE AUTO_COMPRESS=FALSE OVERWRITE=TRUE"

# Upload YML files
snow --config-file ~/.snowcli/config.toml sql -c GENESIS-DEV-PROVIDER -q "PUT file://$DIRECTORY_PATH/gensis_bots/apps/genesis_server/deployments/snowflake_app/*.yml @GENESISAPP_APP_PKG.CODE_SCHEMA.APP_CODE_STAGE AUTO_COMPRESS=FALSE OVERWRITE=TRUE"


json_data=$(snow --config-file ~/.snowcli/config.toml sql -c GENESIS-DEV-PROVIDER --format json -q "show versions in application package GENESISAPP_APP_PKG;")

# Extract specific fields
echo "$json_data" | jq '.[-1].version'
echo "$json_data" | jq '.[-1].patch'
version=$(echo "$json_data" | jq '.[-1].version')
max_patch_number=$(echo "$json_data" | jq '.[-1].patch')
version=${version//\"/}
if [ "$max_patch_number" -eq 130 ]; then
    number_part=$(echo "$version" | sed -E 's/^V[0-9]+_([0-9]+)/\1/')
    old_version="V0_$((number_part - 1))"
    new_version="V0_$((number_part + 1))"

    echo "Dropping version $old_version"
    if ! snow --config-file ~/.snowcli/config.toml sql -c GENESIS-DEV-PROVIDER -q "ALTER APPLICATION PACKAGE GENESISAPP_APP_PKG DROP VERSION $old_version ;"; then
        echo "Error occurred: Unable to drop version $old_version. Continuing with the script..."
    else
        echo "Successfully dropped version $old_version."
    fi
    echo "Creating new version $new_version"
    if ! snow --config-file ~/.snowcli/config.toml sql -c GENESIS-DEV-PROVIDER -q "ALTER APPLICATION PACKAGE GENESISAPP_APP_PKG ADD VERSION $new_version USING @GENESISAPP_APP_PKG.CODE_SCHEMA.APP_CODE_STAGE"; then
        echo "Error occurred: Unable to create version $new_version. Exiting script..."
        exit 1
    else
        echo "Successfully created version $new_version."
        patch_number=0
        version=$new_version
    fi
else

    output=$(snow --config-file ~/.snowcli/config.toml sql -c GENESIS-DEV-PROVIDER -q "ALTER APPLICATION PACKAGE GENESISAPP_APP_PKG ADD PATCH FOR VERSION $version USING @GENESISAPP_APP_PKG.CODE_SCHEMA.APP_CODE_STAGE")

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
fi

# Run the second command with the extracted patch number
snow --config-file ~/.snowcli/config.toml sql -c GENESIS-DEV-PROVIDER -q "ALTER APPLICATION PACKAGE GENESISAPP_APP_PKG SET DEFAULT RELEASE DIRECTIVE VERSION = $version PATCH = $patch_number;"

echo "Patch $patch_number has been set as the default release directive."

snow --config-file ~/.snowcli/config.toml sql -c GENESIS-DEV-CONSUMER-2 -q "alter application genesis_bots upgrade"

snow --config-file ~/.snowcli/config.toml sql -c GENESIS-DEV-CONSUMER-2 -q "call genesis_bots.core.run_arbitrary('grant all on warehouse app_xsmall to application role app_public;');"
snow --config-file ~/.snowcli/config.toml sql -c GENESIS-DEV-CONSUMER-2 -q "call genesis_bots.core.run_arbitrary('grant all on service GENESIS_BOTS.APP1.GENESISAPP_HARVESTER_SERVICE to application role app_public;');"
snow --config-file ~/.snowcli/config.toml sql -c GENESIS-DEV-CONSUMER-2 -q "call genesis_bots.core.run_arbitrary('grant all on service GENESIS_BOTS.APP1.GENESISAPP_KNOWLEDGE_SERVICE to application role app_public;');"
snow --config-file ~/.snowcli/config.toml sql -c GENESIS-DEV-CONSUMER-2 -q "call genesis_bots.core.run_arbitrary('grant all on service GENESIS_BOTS.APP1.GENESISAPP_TASK_SERVICE to application role app_public;');"
snow --config-file ~/.snowcli/config.toml sql -c GENESIS-DEV-CONSUMER-2 -q "call genesis_bots.core.run_arbitrary('grant all on service GENESIS_BOTS.APP1.GENESISAPP_SERVICE_SERVICE to application role app_public;');"
snow --config-file ~/.snowcli/config.toml sql -c GENESIS-DEV-CONSUMER-2 -q "call genesis_bots.core.run_arbitrary('grant all on all tables in schema GENESIS_BOTS.APP1 to application role app_public;');"
snow --config-file ~/.snowcli/config.toml sql -c GENESIS-DEV-CONSUMER-2 -q "call genesis_bots.core.run_arbitrary('grant select on GENESIS_BOTS.APP1.LLM_RESULTS to application role app_public;');"
snow --config-file ~/.snowcli/config.toml sql -c GENESIS-DEV-CONSUMER-2 -q "call genesis_bots.core.run_arbitrary('grant all on schema GENESIS_BOTS.APP1 to application role app_public;');"
snow --config-file ~/.snowcli/config.toml sql -c GENESIS-DEV-CONSUMER-2 -q "show applications;"

echo "Upgrade complete"
