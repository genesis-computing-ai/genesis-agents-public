import re
import time
from   uuid                     import UUID

from   genesis_bots.api.genesis_base \
                                import (GenesisBotConfig, RequestHandle,
                                        _ALL_BOTS_)
from   genesis_bots.api.server_proxy \
                                import GenesisServerProxyBase

class GenesisAPI:

    def __init__(self,
                 server_proxy: GenesisServerProxyBase,
                 ):
        # Set default environment variables if not already set
        import os
        os.environ["GENESIS_SOURCE"] = "Snowflake"  # should always be Snowflake as all metadata goes through Snowflake Connector even when
        #self.scope = scope
        #self.sub_scope = sub_scope
        assert issubclass(type(server_proxy), GenesisServerProxyBase) and type(server_proxy) is not GenesisServerProxyBase, (
            f"server_proxy must be a strict subclass of GenesisServerProxyBasee. Got: {type(server_proxy)}")
        self.server_proxy = server_proxy
        self.server_proxy.connect()


    def register_bot(self, bot: GenesisBotConfig):
        self.server_proxy.register_bot(bot)


    def list_available_bots(self) -> list[GenesisBotConfig]:
        return self.server_proxy.list_available_bots()


    def register_client_tool(self, bot_id, tool_func, timeout_seconds=60):
        self.server_proxy.register_client_tool(bot_id, tool_func, timeout_seconds)


    def unregister_client_tool(self, func_or_name, bot_id=_ALL_BOTS_):
        self.server_proxy.unregister_client_tool(func_or_name, bot_id)


    def upload_file(self, file_path, file_name, contents):
        return self.server_proxy.upload_file(file_path, file_name, contents)


    def submit_message(self, bot_id, message:str, thread_id:str|UUID=None) -> RequestHandle:
        if thread_id is not None:
            thread_id = str(thread_id)
        return self.server_proxy.submit_message(bot_id, message=message, thread_id=thread_id)


    def get_response(self, bot_id, request_id=None, timeout_seconds=None, print_stream=False) -> str:
        time_start = time.time()
        done = False
        last_response = "" # contains the full (cumulated) response, cleaned up from the trailing "chat" suffix ('💬')
        while timeout_seconds is None or time.time() - time_start < timeout_seconds:
            response = self.server_proxy.get_message(bot_id, request_id)
            if response is not None:
                if response.endswith('💬'): # remove trailing chat bubble
                    response = response[:-1]
                else:
                    done = True
                # Store the new content before any formatting
                new_content = response[len(last_response):]
                last_response = response  # Update last_response before formatting
                
                # Format the new content for display only
                if print_stream:
                    display_content = re.sub(r'(?<!\n)(🤖|🧰)', r'\n\1', new_content)
                    print(f"\033[96m{display_content}\033[0m", end='', flush=True)  # Cyan text

                if done:
                    return response

                time.sleep(0.2)
        return None


    def shutdown(self):
        self.server_proxy.shutdown()


    def __enter__(self):
        # Allow ClientAPI to be used as a resource manager that shuts itself down
        return self


    def __exit__(self, exc_type, exc_value, traceback):
        # Allow ClientAPI to be used as a resource manager that shuts itself down
        self.shutdown()



