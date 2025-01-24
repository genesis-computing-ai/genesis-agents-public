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


# TODO: cleanup below

# class GenesisBot(BaseModel):
#     BOT_ID: str
#     BOT_NAME: str
#     BOT_IMPLEMENTATION: str
#     FILES: str # List[str]

#     API_APP_ID: str
#     AUTH_STATE: str
#     AUTH_URL: str
#     AVAILABLE_TOOLS: str
#     BOT_AVATAR_IMAGE: str
#     BOT_INSTRUCTIONS: str
#     BOT_INTRO_PROMPT: str
#     BOT_SLACK_USER_ID: str
#     CLIENT_ID: str
#     CLIENT_SECRET: str
#     DATABASE_CREDENTIALS: str
#     RUNNER_ID: str
#     SLACK_ACTIVE: str
#     SLACK_APP_LEVEL_KEY: str
#     SLACK_APP_TOKEN: str
#     SLACK_CHANNEL_ID: str
#     SLACK_SIGNING_SECRET: str
#     SLACK_USER_ALLOW: list[str]|str
#     TEAMS_ACTIVE: str
#     TEAMS_APP_ID: str
#     TEAMS_APP_PASSWORD: str
#     TEAMS_APP_TENANT_ID: str
#     TEAMS_APP_TYPE: str
#     UDF_ACTIVE: str

#     def __str__(self):
#         return f"GenesisBot(BOT_ID={self.BOT_ID}, BOT_NAME={self.BOT_NAME}, BOT_IMPLEMENTATION={self.BOT_IMPLEMENTATION})"

# class GenesisToolDefinition(BaseModel):
#     TOOL_NAME: str
#     TOOL_DESCRIPTION: str
#     PARAMETERS: Dict[str, Dict[str, str]]  # {"param_name": { "type": "string", "description": "description" },

# class GenesisMetadataStore():
#     scope: str
#     sub_scope: str
#     def __init__(self, scope, sub_scope):
#         #super().__init__(scope=scope)
#         self.scope = scope
#         self.sub_scope = sub_scope
#     def insert_or_update_metadata(self, metadata_type: str, name: str, metadata: BaseModel):
#         raise NotImplementedError("insert_or_update_metadata not implemented")
#     def get_metadata(self, metadata_type: str, name: str, name2: str = None) -> BaseModel:
#         raise NotImplementedError("get_metadata not implemented")
#     def get_all_metadata(self, metadata_type: str, name: str = None):
#         raise NotImplementedError("get_all_metadata not implemented")
#     def upload_extended_tool(self, tool: GenesisToolDefinition, python_code: str, return_type: str, packages: list[str] = None):
#         raise NotImplementedError("upload_extended_tool not implemented")


# class LocalMetadataStore(GenesisMetadataStore):
#     metadata_dir: str = "./metadata"  # Set a default value for metadata_dir
#     def __init__(self, scope, sub_scope):
#         super().__init__(scope, sub_scope)

#     def insert_or_update_metadata(self, metadata_type: str, name: str, metadata: BaseModel):
#         metadata_dict = metadata.model_dump()
#         metadata_dict['type'] = metadata.__class__.__name__  # Store the class name for later instantiation
#         if not os.path.exists(self.metadata_dir):
#             os.makedirs(self.metadata_dir)
#         metadata_type_dir = f"{self.metadata_dir}/{self.scope}/{metadata_type}"
#         if not os.path.exists(metadata_type_dir):
#             os.makedirs(metadata_type_dir)
#         with open(f"{metadata_type_dir}/{name}.json", "w") as f:
#             import json
#             json.dump(metadata_dict, f)
#     def get_metadata(self, metadata_type: str, name: str, name2: str = None) -> BaseModel:
#         try:
#             if name2:
#                 file_path = f"{self.metadata_dir}/{self.scope}/{metadata_type}/{name}/{name2}.json"
#             else:
#                 file_path = f"{self.metadata_dir}/{self.scope}/{metadata_type}/{name}.json"
#             with open(file_path, "r") as f:
#                 metadata_dict = json.load(f)
#                 # Dynamically instantiate the original object type
#                 metadata_class = getattr(sys.modules[__name__], metadata_dict['type'])
#                 return metadata_class(**metadata_dict)
#         except FileNotFoundError:
#             return None
#     def get_all_metadata(self, metadata_type: str, fields_to_return=None, first_filter=None, second_filter=None, last_n:int=None):
#     #def get_all_metadata(self, metadata_type: str, name: str = None):
#         metadata_type_dir = f"{self.metadata_dir}/{self.scope}/{metadata_type}"
#         if not os.path.exists(metadata_type_dir):
#             return []
#         metadata_list = [json.load(open(f"{metadata_type_dir}/{f.name}", "r")) for f in os.scandir(metadata_type_dir) if f.is_file()]
#         import pandas as pd
#         metadata_list = pd.DataFrame(metadata_list)
#         if fields_to_return:
#             metadata_list = metadata_list[fields_to_return]
#         if first_filter:
#             raise NotImplementedError("First filter not implemented")
#         if second_filter:
#             raise NotImplementedError("Second filter not implemented")
#         if last_n:
#             metadata_list = metadata_list.tail(last_n)

#         return metadata_list

# class DatabaseMetadataStore(GenesisMetadataStore):
#     metadata_type_mapping: Dict[str, Tuple[str, str, Optional[str]]] = {
#         "GenesisBot": ("BOT_SERVICING", "BOT_ID", None),
#         "GenesisProject": ("PROJECTS", "PROJECT_ID", None),
#         "GenesisProjectAsset": ("PROJECT_ASSETS", "PROJECT_ID", "ASSET_ID"),
#         "GenesisProcess": ("PROCESSES", "PROCESS_ID", None),
#         "GenesisNote": ("NOTEBOOK", "NOTE_ID", None),
#         "GenesisKnowledge": ("KNOWLEDGE", "KNOWLEDGE_THREAD_ID", None),
#         "GenesisHarvestResults": ("HARVEST_RESULTS", "SOURCE_NAME", None),
#         "GenesisMessage": ("MESSAGE_LOG", "BOT_ID", "THREAD_ID"),
#     }
#     conn: SnowflakeConnection = None

#     def __init__(self, scope, sub_scope="app1", conn=None):
#         super().__init__(scope, sub_scope)
#         self.sub_scope = sub_scope
#         if not conn:
#             raise NotImplementedError("DatabaseMetadataStore requires a connection")
#         self.conn = conn
#      #   if conn is None:
#      #       from connectors import get_global_db_connector
#      #       self.db_adapter = get_global_db_connector(os.getenv("GENESIS_SOURCE", "Snowflake"))
#      #   else:
#     #        self.db_adapter = conn

#     def _format_value(self,value):
#         if isinstance(value, dict):
#             # Convert dict to valid JSON format for Snowflake
#             return f"parse_json('{json.dumps(value)}')"
#         elif isinstance(value, str):
#             # Escape single quotes in strings by doubling them
#             escaped_value = value.replace("'", "''")
#             return f"'{escaped_value}'"
#         else:
#             # Default handling for other data types
#             return f"'{value}'"

#     def insert_or_update_metadata(self, metadata_type: str, name: str, metadata: BaseModel):
#         cursor = self.conn.cursor()
#         table_name, filter_column, _ = self.metadata_type_mapping.get(metadata_type, (None, None))
#         if not table_name:
#             raise ValueError(f"Unknown metadata type: {metadata_type}")

#         # Handle both dictionary and Pydantic model inputs
#         if isinstance(metadata, dict):
#             metadata_dict = metadata
#         else:
#             # Try model_dump() first (Pydantic V2), fall back to dict() (Pydantic V1)
#             try:
#                 metadata_dict = metadata.model_dump()
#             except AttributeError:
#                 metadata_dict = metadata.dict()

#         metadata_dict['type'] = metadata.__class__.__name__  # Store the class name for later instantiation

#         # Exclude 'type' from columns
#         columns = [field for field in metadata_dict.keys() if field != "type"]

#         values = [self._format_value(metadata_dict[column]) for column in columns]
#         select_expressions = [f"{value} AS {col}" for col, value in zip(columns, values)]

#         # Generate the MERGE statement
#         query = f"""
#         MERGE INTO {self.sub_scope}.{table_name} AS target
#         USING (
#             SELECT {', '.join(select_expressions)}
#         ) AS source
#         ON target.{filter_column} = source.{filter_column}
#         WHEN MATCHED THEN
#             UPDATE SET {', '.join([f"{column} = source.{column}" for column in columns])}
#         WHEN NOT MATCHED THEN
#             INSERT ({', '.join(columns)})
#             VALUES ({', '.join([f"source.{column}" for column in columns])})
#         """.strip().replace("\n", " ")

#         cursor.execute(query)
#         self.conn.commit()

#     def get_metadata(self, metadata_type: str, name: str) -> BaseModel:
#         cursor = self.conn.cursor()
#         table_name, filter_column, _ = self.metadata_type_mapping.get(metadata_type, (None, None))
#         if not table_name:
#             raise ValueError(f"Unknown metadata type: {metadata_type}")
#         if self.sub_scope == "app1": # only necessary if connecting remotely
#             query = f"SELECT * FROM {self.sub_scope}.{table_name} WHERE {filter_column} = \\\'{name}\\\'"
#             cursor.execute("call core.run_arbitrary('%s')" % query) # TODO: don't need run_arbitrary if running locally
#         else:
#             query = f"SELECT * FROM {self.sub_scope}.{table_name} WHERE {filter_column} = '{name}'"
#             cursor.execute(query)
#         metadata_dict = cursor.fetchone()
#         if metadata_dict:
#             metadata_class = getattr(sys.modules[__name__], metadata_type)
#             if self.sub_scope == "app1": # only necessary if connecting remotely
#                 metadata_dict = json.loads(metadata_dict[0])[0]
#             else:
#                 metadata_dict = cursor.fetch_pandas_all().to_dict()
#                 metadata_dict = {k: v[0] if len(v) == 1 else v for k, v in metadata_dict.items()}
#             metadata_dict = {k: v if v is not None else '' for k, v in metadata_dict.items()}
#             return metadata_class(**metadata_dict)
#         else:
#             return None

#     def get_all_metadata(self, metadata_type: str, first_filter=None, second_filter=None, last_n:int=None, fields_to_return=None):

#         table_name, filter_column, second_filter_field = self.metadata_type_mapping.get(metadata_type, (None, None, None))
#         if not table_name:
#             raise ValueError(f"Unknown metadata type: {metadata_type}")
#         if not fields_to_return:
#             fields_to_return = [filter_column] + ([second_filter_field] if second_filter_field is not None else [])
#         query = f"SELECT {', '.join(fields_to_return)} FROM {self.scope}.{self.sub_scope}.{table_name}"
#         params = []
#         if first_filter:
#             query += f" WHERE {filter_column} = %s"
#             params.append(first_filter)
#         if second_filter:
#             query += f" AND {second_filter_field} = %s"
#             params.append(second_filter)
#         if last_n:
#             query += f" ORDER BY timestamp DESC LIMIT %s"
#             params.append(last_n)
#         try:
#             if self.sub_scope == "app1": # only necessary if connecting remotely
#                 query = "call core.run_arbitrary('%s')" % query
#                 cursor = self.conn.cursor()
#                 cursor.execute(query, params)
#                 metadata_list = cursor.fetchall()
#                 metadata_list = json.loads(metadata_list[0][0])
#                 metadata_list = pd.DataFrame(metadata_list, columns=fields_to_return)
#                 metadata_list = metadata_list.to_dict(orient="records")
#             else:
#                # db_adapter = get_global_db_connector(os.getenv("GENESIS_SOURCE", "Snowflake"))
#                 cursor = self.conn.cursor()
#                 cursor.execute(query, params)
#                 metadata_list = cursor.fetchall()
#                 metadata_list = pd.DataFrame(metadata_list, columns=fields_to_return)
#                 metadata_list = metadata_list.to_dict(orient="records")
#              #   metadata_list = cursor.fetch_pandas_all().to_dict(orient="records")
#             metadata_list = [item.get(filter_column) for item in metadata_list]
#         except Exception as e:
#             print(f"Error getting metadata: {e}")
#             return []
#         finally:
#             cursor.close()
#         return metadata_list


# class SnowflakeMetadataStore(DatabaseMetadataStore):
#     def __init__(self, scope, sub_scope="app1"):
#         conn = SnowflakeConnection(
#             account=os.getenv("SNOWFLAKE_ACCOUNT_OVERRIDE"),
#             user=os.getenv("SNOWFLAKE_USER_OVERRIDE"),
#             password=os.getenv("SNOWFLAKE_PASSWORD_OVERRIDE"),
#             database=scope,
#             warehouse=os.getenv("SNOWFLAKE_WAREHOUSE_OVERRIDE"),
#             role=os.getenv("SNOWFLAKE_ROLE_OVERRIDE")
#         )
#         super().__init__(scope, sub_scope, conn)

# class SqliteMetadataStore(DatabaseMetadataStore):
#     def __init__(self, scope=None, sub_scope=None, conn=None):
#  #       conn = sqlite3.connect(f"{scope}.db")
#         from genesis_bots.connectors import get_global_db_connector
#         conn = get_global_db_connector("Snowflake").connection  # this is correct, Snowflake, not SQLite
#         super().__init__(scope, sub_scope, conn)


# class GenesisProject(BaseModel):
#     PROJECT_ID: str
#     PROJECT_NAME: str
#     DESCRIPTION: str
#     CREATED_AT: str
#     CURRENT_STATUS: str
#     PROJECT_MANAGER_BOT_ID: str
#     REQUESTED_BY_USER: str
#     TARGET_COMPLETION_DATE: str
#     UPDATED_AT: str

#     # def toggle_item_completion(self, item_index: int):
#     #     if item_index in self.project_items:
#     #         name, completed = self.project_items[item_index]
#     #         self.project_items[item_index] = (name, not completed)
#     #     else:
#     #         raise ValueError("Item index not found")

# class GenesisProjectAsset(BaseModel):
#     PROJECT_ID: str
#     ASSET_ID: str
#     DESCRIPTION: str
#     CREATED_AT: datetime
#     UPDATED_AT: datetime
#     GIT_PATH: str

# class GenesisProcess(BaseModel):
#     CREATED_AT: datetime
#     UPDATED_AT: datetime
#     PROCESS_ID: str
#     BOT_ID: str
#     PROCESS_NAME: str
#     PROCESS_INSTRUCTIONS: str
#     PROCESS_DESCRIPTION: str
#     NOTE_ID: str
#     PROCESS_CONFIG: str
#     HIDDEN: bool|str

# class GenesisNote(BaseModel):
#     NOTE_ID: str
#     NOTE_NAME: str
#     BOT_ID: str
#     NOTE_TYPE: str
#     NOTE_CONTENT: str
#     NOTE_PARAMS: str

# class GenesisKnowledge(BaseModel):
#     TIMESTAMP: datetime
#     THREAD_ID: str
#     KNOWLEDGE_THREAD_ID: str
#     PRIMARY_USER: str
#     BOT_ID: str
#     LAST_TIMESTAMP: datetime
#     THREAD_SUMMARY: str
#     USER_LEARNING: str
#     TOOL_LEARNING: str
#     DATA_LEARNING: str

# class GenesisMessage(BaseModel):
#     TIMESTAMP: datetime
#     TIMESTAMP_NTZ: datetime
#     BOT_ID: str
#     BOT_NAME: str
#     THREAD_ID: str
#     MESSAGE_TYPE: str
#     MESSAGE_PAYLOAD: str
#     MESSAGE_METADATA: str
#     TOKENS_IN: int
#     TOKENS_OUT: int
#     FILES: str
#     CHANNEL_TYPE: str
#     CHANNEL_NAME: str
#     PRIMARY_USER: str
#     TASK_ID: str


# Clent-side bot_tool support
#-----------------------------------------
from   genesis_bots.core        import bot_os_tools2 as core_tools

_ALL_BOTS_ = core_tools._ALL_BOTS_TOKEN_

def bot_client_tool(**param_descriptions):
    return core_tools.gc_tool(_group_tags_=core_tools.REMOTE_TOOL_FUNCS_GROUP,
                             **param_descriptions)


def is_bot_client_tool(func):
    return (core_tools.is_tool_func(func)
            and core_tools.REMOTE_TOOL_FUNCS_GROUP in core_tools.get_tool_func_descriptor(func).groups)


def get_tool_func_descriptor(func):
    return core_tools.get_tool_func_descriptor(func)
