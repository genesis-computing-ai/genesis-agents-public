import re
import time
from   uuid                     import UUID


from   genesis_bots.api.genesis_base \
                                import (GenesisBotConfig, RequestHandle,
                                        _ALL_BOTS_,
                                        canonicalize_json_result_dict)
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
        self._server_proxy = server_proxy
        self._server_proxy.connect()

        self.gitfiles = self._GitFiles(server_proxy)


    def register_bot(self, bot: GenesisBotConfig):
        self._server_proxy.register_bot(bot)


    def list_available_bots(self) -> list[GenesisBotConfig]:
        return self._server_proxy.list_available_bots()


    def register_client_tool(self, bot_id, tool_func, timeout_seconds=60):
        self._server_proxy.register_client_tool(bot_id, tool_func, timeout_seconds)


    def run_genesis_tool(self, tool_name: str, params: dict, bot_id: str) -> dict:
        return self._server_proxy.run_genesis_tool(tool_name, params, bot_id)


    def unregister_client_tool(self, func_or_name, bot_id=_ALL_BOTS_):
        self._server_proxy.unregister_client_tool(func_or_name, bot_id)


    def upload_file(self, file_path, file_name, contents):
        return self._server_proxy.upload_file(file_path, file_name, contents)


    def submit_message(self, bot_id, message:str, thread_id:str|UUID=None) -> RequestHandle:
        if thread_id is not None:
            thread_id = str(thread_id)
        return self._server_proxy.submit_message(bot_id, message=message, thread_id=thread_id)


    def get_response(self, bot_id, request_id=None, timeout_seconds=None, print_stream=False) -> str:
        time_start = time.time()
        done = False
        last_response = "" # contains the full (cumulated) response, cleaned up from the trailing "chat" suffix ('💬')
        while timeout_seconds is None or time.time() - time_start < timeout_seconds:
            response = self._server_proxy.get_message(bot_id, request_id)
            if response is not None:
                if len(response) > 2 and response.endswith(' 💬'): # remove trailing chat bubble
                    response = response[:-2]
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


    class _GitFiles:
        def __init__(self, server_proxy):
            self.server_proxy = server_proxy


        def read(self, file_path, bot_id=None):
            bot_id = bot_id or "Eve" # remove once it becomes redundant
            res = self.server_proxy.run_genesis_tool(tool_name="git_action",
                                                     params={"action": "read_file",
                                                             "file_path": file_path},
                                                     bot_id=bot_id)
            res = canonicalize_json_result_dict(res)
            if res["success"]:
                return res["content"]
            else:
                error_msg = res.get("error", "Unknown error")
                raise ValueError(res.get("error", "Unknown error"))


        def list_files(self, bot_id=None):
            bot_id = bot_id or "Eve" # remove once it becomes redundant
            res = self.server_proxy.run_genesis_tool(tool_name="git_action",
                                                     params={"action": "list_files"},
                                                     bot_id=bot_id)
            res = canonicalize_json_result_dict(res)
            if res["success"]:
                return res["files"]
            else:
                raise ValueError(res["message"])


        def write(self, file_path, content, commit_message=None, bot_id=None, adtl_info_out=None):
            bot_id = bot_id or "Eve" # remove once it becomes redundant
            res = self.server_proxy.run_genesis_tool(tool_name="git_action",
                                                     params={"action": "write_file",
                                                             "file_path": file_path,
                                                             "content": content,
                                                             "commit_message": commit_message},
                                                     bot_id=bot_id)
            res = canonicalize_json_result_dict(res)
            is_success = res.pop("success", False)
            if adtl_info_out is not None:
                assert isinstance(adtl_info_out, dict), (
                    f"adtl_info_out must be a dict or None. Got: {type(adtl_info_out)}")
                adtl_info_out.update(res)
            if is_success:
                return True
            else:
                raise ValueError(res["error"])


        def commit(self, commit_message, bot_id=None):
            bot_id = bot_id or "Eve" # remove once it becomes redundant
            res = self.server_proxy.run_genesis_tool(tool_name="git_action",
                                                     params={"action": "commit",
                                                             "commit_message": commit_message},
                                                     bot_id=bot_id)
            res = canonicalize_json_result_dict(res)
            if res["success"]:
                return True
            else:
                raise ValueError(res["error"])


    def shutdown(self):
        self._server_proxy.shutdown()


    def __enter__(self):
        # Allow ClientAPI to be used as a resource manager that shuts itself down
        return self


    def __exit__(self, exc_type, exc_value, traceback):
        # Allow ClientAPI to be used as a resource manager that shuts itself down
        self.shutdown()



