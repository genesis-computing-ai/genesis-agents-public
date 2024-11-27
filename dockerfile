FROM python:3.10-slim
WORKDIR /src/app
COPY requirements.txt /src/app
RUN apt-get update && apt-get install gcc -y && apt-get install g++ -y && apt-get install python3-dev -y && apt-get clean
RUN pip install --upgrade pip && \
    pip install -r requirements.txt
RUN python3 -m spacy download en_core_web_md
EXPOSE 8501 8080 8000 5678 1234 3000 8081 8502 7681
COPY genesis-voice ./genesis-voice

# Install Node.js 18.x and npm
WORKDIR /src/app/genesis-voice
RUN apt-get update && apt-get install -y curl && \
    curl -fsSL https://deb.nodesource.com/setup_18.x | bash - && \
    apt-get install -y nodejs && \
    npm install -g npm@latest
# Install git
RUN apt-get update && \
    apt-get install -y git && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*
# Verify git is accessible from Python
RUN python3 -c "import subprocess; subprocess.run(['git', '--version'], check=True)"
RUN npm install express cors node-fetch dotenv http https ws http-proxy
RUN npm i github:openai/openai-realtime-api-beta --save
RUN npm install react-scripts

WORKDIR /src/app
COPY llm_openai ./llm_openai
COPY llm_reka ./llm_reka
COPY llm_mistral ./llm_mistral
COPY llm_cortex ./llm_cortex
COPY llm_gemini ./llm_gemini
COPY llm_anthropic ./llm_anthropic
COPY schema_explorer ./schema_explorer
COPY knowledge ./knowledge
#COPY default_data ./default_data
COPY default_files ./default_files
COPY golden_defaults ./golden_defaults
COPY default_functions ./default_functions
COPY slack ./slack
COPY streamlit_gui ./streamlit_gui
COPY bot_genesis ./bot_genesis
COPY connectors ./connectors
COPY auto_ngrok ./auto_ngrok
COPY core ./core
COPY demo ./demo
COPY development ./development
COPY embed ./embed
COPY generated_modules ./generated_modules
COPY entrypoint.sh /entrypoint.sh
COPY teams ./teams
COPY nodetest ./nodetest
COPY data_dev_tools ./data_dev_tools

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

RUN chmod +x /entrypoint.sh
HEALTHCHECK CMD curl --fail http://localhost:8501/_stcore/health

ENTRYPOINT ["/entrypoint.sh"]