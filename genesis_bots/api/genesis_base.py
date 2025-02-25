import json
from   typing                   import Optional
import yaml

class RequestHandle:
    '''
    A simple class to hold message submission information. This is useful for fetching all responses associated with the sbmitted message.
    Respects both dot notation and dict-style access.
    '''
    def __init__(self,
                 request_id: str,
                 bot_id: str,
                 thread_id: str = None
                 ):
        self.request_id = str(request_id)
        self.bot_id = str(bot_id)
        self.thread_id = str(thread_id) if thread_id is not None else None


    def __getitem__(self, key):
        if hasattr(self, key):
            return getattr(self, key)
        raise KeyError(key)


    def __repr__(self):
        return f"{self.__class__.__name__}(request_id={self.request_id}, bot_id={self.bot_id}, thread_id={self.thread_id})"


class GenesisBotConfig:
    def __init__(self,
                 bot_id: str,
                 bot_implementation: str,
                 bot_instructions: str,
                 bot_intro_prompt: str = None,
                 bot_name: str = None,
                 available_tools: list[str] = None,
                 files: Optional[list[str]] = None,
                 runner_id: str = "snowflake-1",
                 slack_active: bool = False,
                 slack_deployed: bool = False,
                 udf_active: bool = True
                 ):
        """
        Initialize a GenesisBotConfig instance.

        :param bot_id: A unique human-readable identifier for the bot.
        :param bot_name: The name of the bot, defaults to bot_id if omitted.
        :param bot_implementation: The LLM provider, can be one of "openai", "cortex", "anthropic".
        :param bot_instructions: The prompt defining this bot: its task, personality, special instructions, etc.
        :param bot_intro_prompt: The prompt used by the bot to introduce itself in a new chat thread.
        :param available_tools: List of strings representing the names of tools (function groups) available to this bot.
        :param files: Optional list of file paths to provide for the bot.
        :param runner_id: A genesis-internal identifier.
        :param slack_active: Whether the bot has an active Slack interface.
        :param udf_active: Whether the bot has an active direct chat interface.
        """
        self.bot_id = bot_id
        self.bot_name = bot_name if bot_name is not None else bot_id
        self.bot_implementation = bot_implementation
        self.bot_instructions = bot_instructions
        self.bot_intro_prompt = bot_intro_prompt
        self.available_tools = list(available_tools) if available_tools is not None else []
        self.files = list(files) if files is not None else []
        self.runner_id = runner_id or ""
        self.slack_active = self._bool_to_YN(slack_active)
        self.slack_deployed = slack_deployed
        self.udf_active = self._bool_to_YN(udf_active)


    @classmethod
    def _bool_to_YN(cls, value: str|bool) -> str:
        if isinstance(value, str):
            if value.lower() in ["y", "yes", "true", "1"]:
                return "Y"
            elif value.lower() in ["n", "no", "false", "0"]:
                return "N"
            else:
                raise ValueError(f"Invalid value for boolean value: {value}")
        elif isinstance(value, bool):
            return "Y" if value else "N"
        else:
            raise ValueError(f"failed to convert {repr(value)} to boolean representation")


    def to_json(self) -> str:
        """
        Convert the GenesisBotConfig instance to a JSON string with upper-cased keys.

        :return: A JSON string representation of the instance.
        """
        data = {
            "BOT_ID": self.bot_id,
            "BOT_NAME": self.bot_name,
            "BOT_IMPLEMENTATION": self.bot_implementation,
            "BOT_INSTRUCTIONS": self.bot_instructions,
            "BOT_INTRO_PROMPT": self.bot_intro_prompt,
            "AVAILABLE_TOOLS": self.available_tools,
            "FILES": self.files,
            "RUNNER_ID": self.runner_id,
            "SLACK_ACTIVE": self.slack_active,
            "SLACK_DEPLOYED": self.slack_deployed,
            "UDF_ACTIVE": self.udf_active
        }
        return json.dumps(data)


    @classmethod
    def from_json(cls, json_data: str):
        """
        Create a GenesisBotConfig instance from a JSON string.

        :param json_data: A JSON string representation of the instance.
        :return: A GenesisBotConfig instance.
        """
        data = json.loads(json_data)
        # Convert all keys to lower-case
        data = {k.lower(): v for k, v in data.items()}
        return cls(**data)


    @classmethod
    def from_yaml(cls, yaml_data: str):
        """
        :param yaml_data: A YAML string representation of the instance.
        :return: A GenesisBotConfig instance.
        """
        data = yaml.safe_load(yaml_data)
        # Convert all keys to lower-case
        data = {k.lower(): v for k, v in data.items()}
        return cls(**data)



# Clent-side bot_tool support
#-----------------------------------------
import collections
from   genesis_bots.core        import bot_os_tools2 as core_tools

_ALL_BOTS_ = core_tools._ALL_BOTS_TOKEN_

def bot_client_tool(**param_descriptions):
    return core_tools.gc_tool(_group_tags_=[core_tools.REMOTE_TOOL_FUNCS_GROUP],
                             **param_descriptions)


def is_bot_client_tool(func):
    return (core_tools.is_tool_func(func)
            and core_tools.REMOTE_TOOL_FUNCS_GROUP in core_tools.get_tool_func_descriptor(func).groups)


def get_tool_func_descriptor(func):
    return core_tools.get_tool_func_descriptor(func)

# Utility functions
#-----------------------------------------

def canonicalize_json_result_dict(res: dict) -> dict:
    """
    Canonicalize the result of a JSON response from the server by converting all keys to lowercase. recursively.
    """
    def _lowercase_keys(d):
        if isinstance(d, collections.abc.Mapping):
            return {k.lower(): _lowercase_keys(v) for k, v in d.items()}
        return d

    return _lowercase_keys(res)
