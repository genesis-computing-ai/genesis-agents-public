FROM python:3.10-slim
WORKDIR /src/app
COPY requirements.txt /src/app
RUN apt-get update && apt-get install gcc -y && apt-get install g++ -y && apt-get install python3-dev -y && apt-get clean
RUN pip install --upgrade pip && \
    pip install -r requirements.txt
RUN python3 -m spacy download en_core_web_md
EXPOSE 8501 8080 8000 5678 1234
COPY llm_openai ./llm_openai
COPY llm_reka ./llm_reka
COPY llm_mistral ./llm_mistral
COPY llm_cortex ./llm_cortex
COPY llm_gemini ./llm_gemini
COPY schema_explorer ./schema_explorer
COPY knowledge ./knowledge
#COPY default_data ./default_data
COPY default_files ./default_files
COPY default_processes ./default_processes
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
COPY default_files ./default_files
COPY entrypoint.sh /entrypoint.sh
COPY teams ./teams
RUN apt-get update && apt-get install -y procps
RUN apt-get -y update && \
    apt-get -y install  \
        curl

# Install ttyd
RUN VER=$( curl --silent "https://api.github.com/repos/tsl0922/ttyd/releases/latest"| grep '"tag_name"'|sed -E 's/.*"([^"]+)".*/\1/') \
    && curl -LO https://github.com/tsl0922/ttyd/releases/download/$VER/ttyd.x86_64 \
    && mv ttyd.* /usr/local/bin/ttyd \
    && chmod +x /usr/local/bin/ttyd
    
RUN chmod +x /entrypoint.sh
HEALTHCHECK CMD curl --fail http://localhost:8501/_stcore/health
ENTRYPOINT ["/entrypoint.sh"]