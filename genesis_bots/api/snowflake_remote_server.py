import uuid
from snowflake.connector import SnowflakeConnection
from genesis_bots.api.genesis_base import GenesisMetadataStore, GenesisServer, SnowflakeMetadataStore
import os
import urllib.parse

class GenesisSnowflakeServer(GenesisServer):
    def __init__(self, scope, sub_scope, bot_list=None, fast_start=False):
        super().__init__(scope, sub_scope)
        self.conn = SnowflakeConnection(
            account=os.getenv("SNOWFLAKE_ACCOUNT_OVERRIDE"),
            user=os.getenv("SNOWFLAKE_USER_OVERRIDE"),
            password=os.getenv("SNOWFLAKE_PASSWORD_OVERRIDE"),
            database=scope,
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE_OVERRIDE"),
            role=os.getenv("SNOWFLAKE_ROLE_OVERRIDE")
        )
        self.cursor = self.conn.cursor()
    
    def get_metadata_store(self) -> GenesisMetadataStore:
        return SnowflakeMetadataStore(self.scope, self.sub_scope)
    
    def add_message(self, bot_id, message, thread_id) -> str|dict:
        if not thread_id:
            thread_id = str(uuid.uuid4())
        message = urllib.parse.quote(message)
        self.cursor.execute(f"select {self.scope}.app1.submit_udf('{message}', '{thread_id}', '{{\"bot_id\": \"{bot_id}\"}}')")
        request_id = self.cursor.fetchone()[0]
        #return f"Request submitted on thread {thread_id} . To get response use: get_response --bot_id {bot_id} --request_id {request_id}"
        return {"request_id": request_id,
                "bot_id": bot_id,
                "thread_id": thread_id}
    
    def get_message(self, bot_id, request_id):
        self.cursor.execute(f"select {self.scope}.app1.lookup_udf('{request_id}', '{bot_id}')")
        response = self.cursor.fetchone()[0]
        if response == 'not found':
            return None
        return response
