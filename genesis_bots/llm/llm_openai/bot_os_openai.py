from os import getenv
from genesis_bots.llm.llm_openai.bot_os_openai_chat import BotOsAssistantOpenAIChat
from genesis_bots.llm.llm_openai.bot_os_openai_asst import BotOsAssistantOpenAIAsst

def BotOsAssistantOpenAI(*args, **kwargs):
    if getenv("USE_OPENAI_CHAT_API", "False").lower() == "true":
        return BotOsAssistantOpenAIChat(*args, **kwargs)
    return BotOsAssistantOpenAIAsst(*args, **kwargs)
