import os
from openai import AzureOpenAI, OpenAI


def get_openai_client(use_external=False):# -> OpenAI | AzureOpenAI:
    if use_external and os.getenv("OPENAI_EXTERNAL_API_KEY"):
        client = OpenAI(api_key=os.getenv("OPENAI_EXTERNAL_API_KEY"),
                         base_url=os.getenv("OPENAI_EXTERNAL_BASE_URL"),
                         )
    elif os.getenv("AZURE_OPENAI_API_ENDPOINT"):
        client = AzureOpenAI(api_key=os.getenv("OPENAI_API_KEY"),
                             api_version="2024-08-01-preview",
                             azure_endpoint=os.getenv("AZURE_OPENAI_API_ENDPOINT"))
    else:
        client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
    return client
