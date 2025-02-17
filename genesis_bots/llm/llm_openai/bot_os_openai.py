from os import getenv
from genesis_bots.llm.llm_openai.bot_os_openai_chat import BotOsAssistantOpenAIChat
from genesis_bots.llm.llm_openai.bot_os_openai_asst import BotOsAssistantOpenAIAsst, StreamingEventHandler

def BotOsAssistantOpenAI(*args, **kwargs):
    if getenv("OPENAI_USE_ASSISTANTS", "False").lower() == "true":
        return BotOsAssistantOpenAIAsst(*args, **kwargs)
    elif getenv("BOT_OS_USE_LEGACY_OPENAI_IMPL", "False").lower() == "true":
        return BotOsAssistantOpenAIAsst(*args, **kwargs)
    return BotOsAssistantOpenAIChat(*args, **kwargs)
