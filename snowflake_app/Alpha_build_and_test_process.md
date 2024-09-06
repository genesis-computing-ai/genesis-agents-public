
# Alpha Demo Build and Test Process

## Prerequisites

Before running this script, make sure to:

1. Install SnowCLI (https://docs.snowflake.com/en/user-guide/snowsql-install-config)
2. Add the following connections using SnowCLI:
   ```
    snow connection add GENESIS-ALPHA-PROVIDER
        Account is : MMB84124
    snow connection add GENESIS-ALPHA-CONSUMER
        Account is : eqb52188
   ```
   These connections are required for the commands below to work properly.
   You may need to make a new authorized user without SSO/MFA for these.

## Alpha Environment Upgrade

To upgrade the alpha demo environment, run:

bash ./snowflake_app/upgrade_alpha.sh

## To get service status to see if its back up after the upgrade

snow sql -c GENESIS-ALPHA-CONSUMER -q "describe service genesis_bots.app1.genesisapp_service_service"

## To see various logs from alpha demo environment

snow sql -c GENESIS-ALPHA-CONSUMER -q "SELECT SYSTEM\$GET_SERVICE_LOGS('GENESIS_BOTS_ALPHA.APP1.GENESISAPP_SERVICE_SERVICE',0,'genesis',1000);"
snow sql -c GENESIS-ALPHA-CONSUMER -q "SELECT SYSTEM\$GET_SERVICE_LOGS('GENESIS_BOTS_ALPHA.APP1.GENESISAPP_TASK_SERVICE',0,'genesis-task-server',1000);
"
snow sql -c GENESIS-ALPHA-CONSUMER -q "SELECT SYSTEM\$GET_SERVICE_LOGS('GENESIS_BOTS_ALPHA.APP1.GENESISAPP_KNOWLEDGE_SERVICE',0,'genesis-knowledge',
1000);"
snow sql -c GENESIS-ALPHA-CONSUMER -q "SELECT SYSTEM\$GET_SERVICE_LOGS('GENESIS_BOTS_ALPHA.APP1.GENESISAPP_HARVESTER_SERVICE',0,'genesis-harvester',
1000);"