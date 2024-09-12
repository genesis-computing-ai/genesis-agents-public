

#!/bin/bash

#!/bin/bash

# Before running this script, make sure to:
# 1. Install SnowCLI (https://docs.snowflake.com/en/user-guide/snowsql-install-config)
# 2. Add the following connections using SnowCLI:
#    snow connection add GENESIS-ALPHA-CONSUMER
#        Account is : eqb52188
# These connections are required for the commands below to work properly.
# You may need to make a new <authorized role> user without SSO/MFA for these

# Run make_alpha_sis_launch.py
python3 ./streamlit_gui/make_alpha_sis_launch.py


# Upload streamlit files
snow sql -c GENESIS-ALPHA-CONSUMER -q "PUT file:///Users/justin/Documents/Code/genesis/streamlit_gui/Genesis.py @genesis_test.genesis_jl.stream_test/code_artifacts/streamlit AUTO_COMPRESS=FALSE OVERWRITE=TRUE"
snow sql -c GENESIS-ALPHA-CONSUMER -q "PUT file:///Users/justin/Documents/Code/genesis/streamlit_gui/utils.py @genesis_test.genesis_jl.stream_test/code_artifacts/streamlit AUTO_COMPRESS=FALSE OVERWRITE=TRUE"
snow sql -c GENESIS-ALPHA-CONSUMER -q "PUT file:///Users/justin/Documents/Code/genesis/streamlit_gui/*.png @genesis_test.genesis_jl.stream_test/code_artifacts/streamlit AUTO_COMPRESS=FALSE OVERWRITE=TRUE"
snow sql -c GENESIS-ALPHA-CONSUMER -q "PUT file:///Users/justin/Documents/Code/genesis/streamlit_gui/*.yml @genesis_test.genesis_jl.stream_test/code_artifacts/streamlit AUTO_COMPRESS=FALSE OVERWRITE=TRUE"

# Upload streamlit files
snow sql -c GENESIS-ALPHA-CONSUMER -q "PUT file:///Users/justin/Documents/Code/genesis/streamlit_gui/.streamlit/config.toml @genesis_test.genesis_jl.stream_test/code_artifacts/streamlit/.streamlit AUTO_COMPRESS=FALSE OVERWRITE=TRUE"

# Upload streamlit page files
snow sql -c GENESIS-ALPHA-CONSUMER -q "PUT file:///Users/justin/Documents/Code/genesis/streamlit_gui/page_files/*.py @genesis_test.genesis_jl.stream_test/code_artifacts/streamlit/page_files AUTO_COMPRESS=FALSE OVERWRITE=TRUE"

# Then in a worksheet run this:
# call genesis_bots_alpha.core.run_arbitrary('grant all on service GENESIS_BOTS_ALPHA.APP1.GENESISAPP_HARVESTER_SERVICE to application role app_public;');
# call genesis_bots_alpha.core.run_arbitrary('grant all on service GENESIS_BOTS_ALPHA.APP1.GENESISAPP_KNOWLEDGE_SERVICE to application role app_public;');
# call genesis_bots_alpha.core.run_arbitrary('grant all on service GENESIS_BOTS_ALPHA.APP1.GENESISAPP_TASK_SERVICE to application role app_public;');
# call genesis_bots_alpha.core.run_arbitrary('grant all on service GENESIS_BOTS_ALPHA.APP1.GENESISAPP_SERVICE_SERVICE to application role app_public;');
# call genesis_bots_alpha.core.run_arbitrary('grant all on all tables in schema GENESIS_BOTS_ALPHA.CORE to application role app_public;');
# call genesis_bots_alpha.core.run_arbitrary('grant all on all functions in schema GENESIS_BOTS_ALPHA.APP1 to application role app_public;');

# CREATE OR REPLACE STREAMLIT CORE.GENESIS
#     FROM '/code_artifacts/streamlit'
#     MAIN_FILE = '/Genesis.py';