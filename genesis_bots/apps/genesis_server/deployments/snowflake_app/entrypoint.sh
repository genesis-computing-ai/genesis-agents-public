#!/bin/bash

set -e

export TTYD_PORT=1234
export WORKDIR=/tmp

#echo "Running shellinabox for debugging"

#ttyd -p ${TTYD_PORT} -W bash &> ${WORKDIR}/ttyd.log &

/usr/bin/shellinaboxd \
    --port=1234 \
    --disable-ssl \
    --no-beep \
    --service "/:LOGIN" \
    --css /etc/shellinabox/options-enabled/00_White\ On\ Black.css &


#ttyd -p 1234 \
#    --cwd /src/app \
#    -H "Content-Security-Policy: default-src 'self' 'unsafe-inline' 'unsafe-eval'; img-src 'self' data:; connect-src 'self' ws: wss:;" \
#   bash -il &

if [ "$GENESIS_MODE" = "KNOWLEDGE" ]; then
    echo "Running Genesis Knowledge Server"

    export PYTHONPATH=$PYTHONPATH:~/bot_os
    export PYTHONPATH=$PYTHONPATH:/src/app/
    export PYTHONPATH=$PYTHONPATH:/

    python3 /src/app/genesis_bots/services/bot_os_knowledge.py

elif [ "$GENESIS_MODE" = "HARVESTER" ]; then
    echo "Running Genesis Harvester"

    export PYTHONPATH=$PYTHONPATH:~/bot_os
    export PYTHONPATH=$PYTHONPATH:/src/app/
    export PYTHONPATH=$PYTHONPATH:/

    python3 /src/app/genesis_bots/services/standalone_harvester.py

elif [ "$GENESIS_MODE" = "TASK_SERVER" ]; then
    echo "Running Genesis Task Server"

    export PYTHONPATH=$PYTHONPATH:~/bot_os
    export PYTHONPATH=$PYTHONPATH:/src/app/
    export PYTHONPATH=$PYTHONPATH:/

    python3 /src/app/genesis_bots/services/bot_os_task_server.py

else

    echo "Running Streamlit"

    streamlit run /src/app/genesis_bots/apps/streamlit_gui/Genesis.py --server.port=8501 --server.address=0.0.0.0 &

    export PYTHONPATH=$PYTHONPATH:~/bot_os
    export PYTHONPATH=$PYTHONPATH:/src/app/
    export PYTHONPATH=$PYTHONPATH:/

    # echo "Running Genesis Voice Demo Server"

    # cd genesis-voice

    # # npm cache clean --force

    # # npm i
    # # npm install --verbose | tee /dev/stdout

    # echo "Running Relay server on 8081"


    # PORT=8081 npm run relay &

    # echo "Running Voice server on port 3000"


    # DANGEROUSLY_DISABLE_HOST_CHECK=true PORT=3000 npm start &

    # cd ..

#    echo "Running Datacube Endpoint"


#    streamlit run /src/app/streamlit_gui/streamlit_show_datacube.py --server.port=8502 --server.address=0.0.0.0 &

    echo "Running Genesis Bot Server"


    python3 /src/app/genesis_bots/apps/genesis_server/bot_os_multibot_1.py
fi

