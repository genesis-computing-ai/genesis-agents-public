#!/bin/bash

if [ "$GENESIS_MODE" = "HARVESTER" ]; then
    echo "Running Genesis Harvester"

    export PYTHONPATH=$PYTHONPATH:~/bot_os
    export PYTHONPATH=$PYTHONPATH:/src/app/
    export PYTHONPATH=$PYTHONPATH:/

    python3 /src/app/schema_explorer/standalone_harvester.py

else
    echo "Running Genesis Server"

    streamlit run streamlit_gui/streamlit_sis_v1.py --server.port=8501 --server.address=0.0.0.0 &

    export PYTHONPATH=$PYTHONPATH:~/bot_os
    export PYTHONPATH=$PYTHONPATH:/src/app/
    export PYTHONPATH=$PYTHONPATH:/

    python3 /src/app/demo/bot_os_multibot_1.py 
fi