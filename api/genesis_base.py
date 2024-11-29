from abc import ABC
from pydantic import BaseModel

class BotGuardrails(BaseModel):
    def preprocess(self, message):
        pass
    def postprocess_response(self, response):
        pass

class BotKnowledgeGuardrails(BotGuardrails):
    def preprocess(self, message):
        return message
    def postprocess_response(self, response):
        return response

class GenesisBot(BaseModel):
    BOT_ID: str
    BOT_NAME: str
    #BOT_DESCRIPTION: str
    #TOOL_LIST: List[str]
    BOT_IMPLEMENTATION: str
    FILES: str # List[str]
    
    API_APP_ID: str
    AUTH_STATE: str
    AUTH_URL: str
    AVAILABLE_TOOLS: str
    BOT_AVATAR_IMAGE: str
    BOT_INSTRUCTIONS: str
    BOT_INTRO_PROMPT: str
    BOT_SLACK_USER_ID: str
    CLIENT_ID: str
    CLIENT_SECRET: str
    DATABASE_CREDENTIALS: str
    RUNNER_ID: str
    SLACK_ACTIVE: str
    SLACK_APP_LEVEL_KEY: str
    SLACK_APP_TOKEN: str
    SLACK_CHANNEL_ID: str
    SLACK_SIGNING_SECRET: str
    SLACK_USER_ALLOW: str
    TEAMS_ACTIVE: str
    TEAMS_APP_ID: str
    TEAMS_APP_PASSWORD: str
    TEAMS_APP_TENANT_ID: str
    TEAMS_APP_TYPE: str
    UDF_ACTIVE: str

    guardrails: BotKnowledgeGuardrails = BotKnowledgeGuardrails()

    def __str__(self):
        return f"GenesisBot(BOT_ID={self.BOT_ID}, BOT_NAME={self.BOT_NAME}, BOT_IMPLEMENTATION={self.BOT_IMPLEMENTATION})"

class GenesisServer(ABC):
    def __init__(self, scope):
        self.scope = scope
        self.bots = []
        self.adapters = []
    def add_bot(self, bot: GenesisBot):
        self.bots.append(bot)
    def add_adapter(self, adapter):
        self.adapters.append(adapter)
    def get_all_adapters(self):
        return self.adapters
    def run_tool(self, tool_name, tool_parameters):
        pass
    def add_message(self, bot_id, message, thread_id):
        pass
    def get_message(self, bot_id, thread_id):
        pass
