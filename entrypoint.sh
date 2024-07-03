#!/bin/bash

if [ "$GENESIS_MODE" = "KNOWLEDGE" ]; then
    echo "Running Genesis Knowledge Server"

    export PYTHONPATH=$PYTHONPATH:~/bot_os
    export PYTHONPATH=$PYTHONPATH:/src/app/
    export PYTHONPATH=$PYTHONPATH:/

    python3 /src/app/knowledge/bot_os_knowledge.py

elif [ "$GENESIS_MODE" = "HARVESTER" ]; then
    echo "Running Genesis Harvester"

    export PYTHONPATH=$PYTHONPATH:~/bot_os
    export PYTHONPATH=$PYTHONPATH:/src/app/
    export PYTHONPATH=$PYTHONPATH:/

    python3 /src/app/schema_explorer/standalone_harvester.py

elif [ "$GENESIS_MODE" = "TASK_SERVER" ]; then
    echo "Running Genesis Task Server"

    export PYTHONPATH=$PYTHONPATH:~/bot_os
    export PYTHONPATH=$PYTHONPATH:/src/app/
    export PYTHONPATH=$PYTHONPATH:/

    python3 /src/app/demo/bot_os_task_server.py 

else
    echo "Running Genesis Bot Server"

    streamlit run streamlit_gui/streamlit_sis_v1.py --server.port=8501 --server.address=0.0.0.0 &

    export PYTHONPATH=$PYTHONPATH:~/bot_os
    export PYTHONPATH=$PYTHONPATH:/src/app/
    export PYTHONPATH=$PYTHONPATH:/

    python3 /src/app/demo/bot_os_multibot_1.py 
fi