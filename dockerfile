FROM python:3.11-slim
WORKDIR /src/app
COPY requirements.txt /src/app
RUN apt-get update && apt-get install gcc -y && apt-get install g++ -y && apt-get install python3-dev -y && apt-get clean
RUN pip install --upgrade pip && \
    pip install -r requirements.txt
# RUN python3 -m spacy download en_core_web_md
EXPOSE 8501 8080 8000 5678 1234 3000 8081 8502 7681
# COPY genesis-voice ./genesis-voice


# Install Node.js 18.x and npm
# WORKDIR /src/app/genesis-voice
# RUN apt-get update && apt-get install -y curl && \
#     curl -fsSL https://deb.nodesource.com/setup_18.x | bash - && \
#     apt-get install -y nodejs && \
#     npm install -g npm@latest

# Install git
RUN apt-get update && \
    apt-get install -y git && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*
# Verify git is accessible from Python
RUN python3 -c "import subprocess; subprocess.run(['git', '--version'], check=True)"
# RUN npm install express cors node-fetch dotenv http https ws http-proxy
# RUN npm i github:openai/openai-realtime-api-beta --save
# RUN npm install react-scripts

# Install gcloud
# RUN curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | sudo gpg --dearmor -o /usr/share/keyrings/cloud.google.gpg
# RUN sudo apt-get install apt-transport-https ca-certificates gnupg curl
# RUN echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list && curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | sudo gpg --dearmor -o /usr/share/keyrings/cloud.google.gpg && apt-get update -y && apt-get install google-cloud-cli -y
# RUN gcloud services enable drive.googleapis.com

WORKDIR /src/app
RUN mkdir -p /src/app/apps/genesis_server
RUN mkdir -p /src/app/apps/streamlit_gui
COPY genesis_bots ./genesis_bots
COPY apps/genesis_server/bot_os_multibot_1.py ./apps/genesis_server/bot_os_multibot_1.py
COPY apps/streamlit_gui ./apps/streamlit_gui
COPY apps/genesis_server/deployments/snowflake_app/entrypoint.sh /entrypoint.sh
COPY apps/demos ./apps/demos

RUN apt-get update && apt-get install -y procps
RUN apt-get -y update && \
    apt-get -y install  \
        curl

# Install ttyd
RUN VER=$( curl --silent "https://api.github.com/repos/tsl0922/ttyd/releases/latest"| grep '"tag_name"'|sed -E 's/.*"([^"]+)".*/\1/') \
    && curl -LO https://github.com/tsl0922/ttyd/releases/download/$VER/ttyd.x86_64 \
    && mv ttyd.* /usr/local/bin/ttyd \
    && chmod +x /usr/local/bin/ttyd

RUN apt-get update && \
    apt-get install -y shellinabox && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/* && \
    echo "root:root" | chpasswd

#RUN gcloud services enable drive.googleapis.com

RUN chmod +x /entrypoint.sh
HEALTHCHECK CMD curl --fail http://localhost:8501/_stcore/health

ENTRYPOINT ["/entrypoint.sh"]