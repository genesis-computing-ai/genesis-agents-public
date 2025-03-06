
# Development Build and Test Process

## Prerequisites

Before running this script, make sure to:

1. Install SnowCLI (https://docs.snowflake.com/en/user-guide/snowsql-install-config)
2. Add the following connections using SnowCLI:
   ```
   snow connection add GENESIS-DEV-PROVIDER
       Account is : nmb71612
   snow connection add GENESIS-DEV-CONSUMER-2
       Account is : rdb46973
   ```
   These connections are required for the commands below to work properly.
   You may need to make a new <authorized role> user without SSO/MFA for these.

## Development Environment Upgrade

To upgrade the development environment, run:

bash ./snowflake_app/upgrade_dev.sh

## To get service status to see if its back up after the upgrade

snow sql -c GENESIS-DEV-CONSUMER-2 -q "describe service genesis_bots.app1.genesisapp_service_service"

## To see various logs from Dev consumer environment

snow sql -c GENESIS-DEV-CONSUMER-2 -q "SELECT SYSTEM\$GET_SERVICE_LOGS('GENESIS_BOTS.APP1.GENESISAPP_SERVICE_SERVICE',0,'genesis',1000);"
snow sql -c GENESIS-DEV-CONSUMER-2 -q "SELECT SYSTEM\$GET_SERVICE_LOGS('GENESIS_BOTS.APP1.GENESISAPP_TASK_SERVICE',0,'genesis-task-server',1000);
"
snow sql -c GENESIS-DEV-CONSUMER-2 -q "SELECT SYSTEM\$GET_SERVICE_LOGS('GENESIS_BOTS.APP1.GENESISAPP_KNOWLEDGE_SERVICE',0,'genesis-knowledge',
1000);"
snow sql -c GENESIS-ALPHA-CONSUMER -q "SELECT SYSTEM\$GET_SERVICE_LOGS('GENESIS_BOTS.APP1.GENESISAPP_HARVESTER_SERVICE',0,'genesis-harvester',
1000);"