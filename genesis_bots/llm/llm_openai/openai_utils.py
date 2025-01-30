import os
from openai import AzureOpenAI, OpenAI
from genesis_bots.llm.llm_anthropic.anthropic_extended_openai import ExtendedOpenAI

# use_external will be used whenever we connect to an external API (xai, gemini, anthropic) that supports the OpenAI API
# ExtendedOpenAI is a wrapper that makes Anthropic and Bedrock compatible with the OpenAI API
def get_openai_client(use_external=False):# -> OpenAI | AzureOpenAI:
    if use_external and os.getenv("OPENAI_EXTERNAL_API_KEY"):
        client = ExtendedOpenAI(api_key=os.getenv("OPENAI_EXTERNAL_API_KEY"),
                         **({"base_url": os.getenv("OPENAI_EXTERNAL_BASE_URL")} if os.getenv("OPENAI_EXTERNAL_BASE_URL") else {}),
                         )
    elif os.getenv("AZURE_OPENAI_API_ENDPOINT"):
        client = AzureOpenAI(api_key=os.getenv("OPENAI_API_KEY"),
                             api_version="2024-08-01-preview",
                             azure_endpoint=os.getenv("AZURE_OPENAI_API_ENDPOINT"))
    else:
        client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
        
    return client
