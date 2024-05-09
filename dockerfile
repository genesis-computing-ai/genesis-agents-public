FROM python:3.10-slim
WORKDIR /src/app
COPY requirements.txt /src/app
RUN apt-get update && apt-get install gcc -y && apt-get install g++ -y && apt-get install python3-dev -y && apt-get clean
RUN pip install --upgrade pip && \
    pip install -r requirements.txt
RUN python3 -m spacy download en_core_web_md
EXPOSE 8501 8080 8000
COPY llm_openai ./llm_openai
COPY llm_reka ./llm_reka
COPY llm_mistral ./llm_mistral
COPY llm_cortex ./llm_cortex
COPY schema_explorer ./schema_explorer
COPY slack ./slack
COPY streamlit_gui ./streamlit_gui
COPY bot_genesis ./bot_genesis
COPY connectors ./connectors
COPY core ./core
COPY demo ./demo
COPY generated_modules ./generated_modules
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh
HEALTHCHECK CMD curl --fail http://localhost:8501/_stcore/health
ENTRYPOINT ["/entrypoint.sh"]