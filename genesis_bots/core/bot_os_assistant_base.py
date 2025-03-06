from   abc                      import abstractmethod
from   collections              import deque
from   genesis_bots.core.bot_os_input        import BotOsInputMessage, BotOsOutputMessage
from   genesis_bots.core.bot_os_tools2       import (PARAM_IMPLICIT_FROM_CONTEXT,
                                        get_tool_func_descriptor, is_tool_func)
import dill
import json
from   multiprocessing          import Process
import sys

from   genesis_bots.core.logging_config      import logger

def get_tgt_pcnt():
    '''get target percentage to trim messages'''

    tgt_pcnt_env_name = 'CTX_TRIM_TARGET_PCNT'
    tgt_pcnt_env_val = os.getenv(tgt_pcnt_env_name, 50)

    try:
        tgt_pcnt = int(tgt_pcnt_env_val)
        return tgt_pcnt
    except ValueError:
        logger.error(f'invalid value: env var {tgt_pcnt_env_name}=\'{tgt_pcnt_env_val}\' must be number between 1 and 100')
        return None

class BotOsAssistantInterface:
    @abstractmethod
    def __init__(
        self,
        name: str,
        instructions: str,
        tools: list[dict] = [],
        available_functions={},
        files=[],
        update_existing=False,
        log_db_connector=None,
        bot_id="default_bot_id",
        bot_name="default_bot_name",
        all_tools: list[dict] = [],
        all_functions={},
        all_function_to_tool_map={},
        skip_vectors=False,
    ) -> None:
        self.bot_id = bot_id
        self.bot_name = bot_name
        self.user_allow_cache = {}

    #@staticmethod
    #@abstractmethod
    #def load_by_name(name: str):
    #    pass

    @abstractmethod
    def create_thread(self) -> str:
        pass

    @abstractmethod
    def add_message(self, input_message: BotOsInputMessage):
        pass

    @abstractmethod
    def check_runs(self, event_callback):
        pass

    @abstractmethod
    def is_active(self) -> deque:
        pass

    @abstractmethod
    def is_processing_runs(self) -> deque:
        pass

    @abstractmethod
    def get_done_map(self) -> dict:
        pass


def execute_function_blocking(
    func_name: str, arguments: dict, available_functions: dict
):
    """
    run a specified function in the foreground
    """

    function = available_functions.get(func_name)
    results = None
    if function:
        try:
            results = function(**arguments)
            #          thread = Thread(target=wrapper, args=(s_arguments,)) # comma matters to prevent args getting converted into tuple
            #   return(str(results))
            return results
        except Exception as e:
            logger.info(f"Error: {str(e)}")
            return f"caught exception {str(e)} trying to run {func_name}"
    else:
        return f"Error function {func_name} does not exist"

import os
import tempfile


def fork_function_call(function_serialized, func_name, temp_file_path, args):
    try:
        function = dill.loads(function_serialized)
        results = function(**args)
        with open(temp_file_path, "wb") as temp_file:
            dill.dump(results, temp_file)
    except Exception as e:
        with open(temp_file_path, "wb") as temp_file:
            dill.dump(
                str(f"caught exception {str(e)} trying to run {func_name}"), temp_file
            )
    finally:
        sys.exit(0)


def create_func_wrapper(function, func_name):
    temp_file_descriptor, temp_file_path = tempfile.mkstemp()

    def run_task_with_exception_handling(args):
        try:
            logger.info(f"\nCREATE_FUNCTION_WRAPPER: function type = {type(function)}\n")
            function_serialized = dill.dumps(function)
            p = Process(
                target=fork_function_call,
                args=(function_serialized, func_name, temp_file_path, args),
                name=func_name,
            )
            p.start()
            p.join(timeout=180)
            if p.is_alive():
                p.terminate()
                raise Exception("Process timeout")
            with open(temp_file_path, "rb") as temp_file:
                results = dill.load(temp_file)
            logger.debug(f"_execute_function - {func_name} produced results: {results}")
            return results
        except Exception as e:
            logger.error(f"Task failed with exception: {e}")
            return f"caught exception {str(e)} trying to run {func_name}"
        finally:
            os.close(temp_file_descriptor)
            os.remove(temp_file_path)

    return run_task_with_exception_handling


def execute_function(
    func_name: str,
    arguments,
    available_functions,
    completion_callback,
    thread_id: str,
    bot_id: str,
    status_update_callback=None,
    session_id=None,
    input_metadata=None,
    run_id = None,
):
    logger.info(f"fn execute_function - {func_name}")
    function = available_functions.get(func_name, None)
    if function is None:
        logger.info(f"fn execute_function - _{func_name} (trying with added underscore)")
        function = available_functions.get('_'+func_name, None)
        if function is not None:
            func_name = '_' + func_name
    if function is not None:
        s_arguments = json.loads(arguments)

        # Handle parameters that are implicitly required from context
        # in functions that were defined with gc_tool decorator.
        # we 'inject' those into the call using the local variables
        if is_tool_func(function):
            try:
                tool_func_descriptor = get_tool_func_descriptor(function)
                for param in tool_func_descriptor.parameters_desc:
                    if param.required is PARAM_IMPLICIT_FROM_CONTEXT:
                        recognized_context_params = ["bot_id", "run_id", "session_id", "thread_id"]
                        if param.name in recognized_context_params:
                            assert param.name in locals()
                            s_arguments[param.name] = locals()[param.name]
                            s_arguments[param.name] = locals()[param.name]
                        else:
                            raise ValueError(f"Function {func_name}: parameter  {param.name} flagged as 'PARAM_IMPLICIT_FROM_CONTEXT' but is "
                                            f"not one of the recognized context parameters: {recognized_context_params}")
            except Exception as e:
                err_msg = f"Error while processing function {func_name}: {str(e)}"
                logger.error(err_msg)
                completion_callback(err_msg)
                return


        # special case for dispatch_bot_id
        try:
            if "dispatch_bot_id" in function.__code__.co_varnames:  # FixMe: expose this as a tool arg that can be set by the AI
                s_arguments["dispatch_bot_id"] = bot_id
        except:
            pass
        if True or func_name.startswith("_"):  # run internal BotOS functions in-process
            if func_name.startswith("_"):
                s_arguments["thread_id"] = thread_id # redundant for 'new style' tool functions, which specify this explicitly
            if func_name == '_image_analysis':
                try:
                    if "thread_ts" in input_metadata:
                        s_arguments["input_thread_id"] = input_metadata["thread_ts"]
                    elif "thread_id" in input_metadata:
                        s_arguments["input_thread_id"] = input_metadata["thread_id"] 
                except:
                    pass

            if func_name == '_run_process':
                s_arguments["bot_id"] = bot_id

            if func_name == '_delegate_work':
                s_arguments["status_update_callback"] = status_update_callback
                s_arguments["session_id"] = session_id
                s_arguments["input_metadata"] = input_metadata
                s_arguments["run_id"] = run_id
            if func_name in {'_run_query', '_query_database', '_search_metadata', '_search_metadata_detailed', '_get_full_table_details', '_run_snowpark_python', '_send_email', '_manage_artifact', '_manage_tests', '_set_harvest_control_data', '_get_harvest_control_data', '_list_database_connections'}:
                s_arguments["bot_id"] = bot_id
                if 'query' in s_arguments:
                    s_arguments['query'] = 'USERQUERY::' + s_arguments['query']
            # execute the function
            completion_callback(
                execute_function_blocking(func_name, s_arguments, available_functions)
            )
        else:
            # out-of-process execution (deprecated)
            try:
                if func_name.upper() == "RUN_QUERY":
                    s_arguments["bot_id"] = bot_id
            except:
                pass
            try:
                wrapper = create_func_wrapper(function, func_name)
                completion_callback(wrapper(s_arguments))
            except Exception as e:
                completion_callback(f"caught exception {str(e)} trying to run {func_name}")
    else:
        # function not found
        completion_callback(
            f"!FN_MISSING - Error function {func_name} does not exist for bot {bot_id}.\n Calling it again will not help, check the function name and make sure its correct including any _'s in the name. Available functions, len:\n{len(available_functions)}"
        )


class BotOsAssistantTester(BotOsAssistantInterface):
    def __init__(
        self,
        name: str,
        instructions: str,
        tools: list[dict],
        available_functions: dict,
        files,
        update_existing: bool,
    ) -> None:
        logger.debug(f"BotOsAssistantTester:__iniit__ - name={name}")

    thread_counter = 0

    def create_thread(self) -> str:
        logger.debug("BotOsAssistantTester:create_thread")
        BotOsAssistantTester.thread_counter = BotOsAssistantTester.thread_counter + 1
        return f"thread_{BotOsAssistantTester.thread_counter}"

    def add_message(self, input_message: BotOsInputMessage):
        logger.debug(f"BotOsAssistantTester:add_message - message={input_message}")

    def check_runs(self, event_callback):
        logger.debug("BotOsAssistantTester:check_runs")
        event_callback(
            "session_1",
            BotOsOutputMessage(
                thread_id=f"thread_{BotOsAssistantTester.thread_counter}",
                status="completed",
                output="Hello!",
                messages="Message 1",
            ),
        )
