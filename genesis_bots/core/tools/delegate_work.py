from typing import Any, Dict, Optional
import json
import os
import time
import uuid
import jsonschema
from genesis_bots.core.logging_config import logger

from genesis_bots.llm.llm_openai.openai_utils import get_openai_client
from genesis_bots.core.bot_os_llm import BotLlmEngineEnum
from genesis_bots.core.bot_os_input import BotOsOutputMessage
from genesis_bots.demo.app import genesis_app

from genesis_bots.core.bot_os_tools2 import (
    BOT_ID_IMPLICIT_FROM_CONTEXT,
    THREAD_ID_IMPLICIT_FROM_CONTEXT,
    ToolFuncGroup,
    ToolFuncParamDescriptor,
    gc_tool,
)

from genesis_bots.core.tools.tool_helpers import chat_completion

from genesis_bots.connectors import get_global_db_connector
db_adapter = get_global_db_connector()

delegate_work = ToolFuncGroup(
    name="delegate_work",
    description="Functions to delegate work to other bots.",
    lifetime="PERSISTENT",
)

UPDATE_INTERVAL_SECONDS = 10  # Check for updates every 15 seconds

@gc_tool(
    program_id="ID of the program to run (e.g. 'mapping_research_and_proposal' or 'create_mappings_project')", 
    todo_id="ID of the todo to process (required for mapping_research_and_proposal program)",
    project_id="Project ID argument for the program (not needed for create_mappings_project program)",
    root_folder="Root folder argument for the program (not needed for create_mappings_project program)",
    g_sheet_id="Google Sheet ID for the project config file (required for create_mappings_project program)",
    bot_id=BOT_ID_IMPLICIT_FROM_CONTEXT,
    _group_tags_=[delegate_work],
)
def run_program(
    program_id: str,
    bot_id: str,
    todo_id: str = None,
    thread_id: str = None,
    run_id: str = None,
    session_id: str = None,
    project_id: str = None,
    root_folder: str = None,
    g_sheet_id: str = None,
    status_update_callback: str = None,
    input_metadata: str = None,
):
    """
    Run an external program with specified parameters and monitor its progress.
    
    This function executes external Python programs and provides real-time status updates
    about their progress. It supports various programs including mapping research,
    proposal generation, and project creation workflows.
    
    The function saves complete program output to a temporary file while also providing
    periodic summarized updates about the program's progress.
    """
    import subprocess, sys
    from genesis_bots.llm.llm_openai.bot_os_openai import StreamingEventHandler

    def _update_streaming_status(msg, run_id, session_id, thread_id, status_update_callback, input_metadata):
        message_obj = {
            "type": "tool_call",
            "text": msg
        }
        if run_id is not None:
            if run_id not in StreamingEventHandler.run_id_to_messages:
                StreamingEventHandler.run_id_to_messages[run_id] = []
            StreamingEventHandler.run_id_to_messages[run_id].append(message_obj)

            if StreamingEventHandler.run_id_to_output_stream.get(run_id) is not None:
                StreamingEventHandler.run_id_to_output_stream[run_id] = msg
            else:
                StreamingEventHandler.run_id_to_output_stream[run_id] = msg

            status_update_callback(session_id, BotOsOutputMessage(thread_id=thread_id, status="in_progress", output=msg+" 💬", messages=None, input_metadata=input_metadata))

    def process_program_output(cmd, tmp_output_file, program_id):
        last_response = ""
        previous_summary = ""
        _last_summary_time = time.time()
        last_processed_output = ""  # Track what we've already processed
        
        # Send initial kickoff message
        if status_update_callback:
            status_msg = f"🔄 Running program: {program_id}\nOutput file: {tmp_output_file}"
            _update_streaming_status(status_msg, run_id, session_id, thread_id, status_update_callback, input_metadata)
        
        with open(tmp_output_file, "w") as outfile:
            process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                universal_newlines=True,
                bufsize=1,
                env=dict(os.environ, PYTHONUNBUFFERED="1")
            )
            
            while True:
                output_line = process.stdout.readline()
                if output_line == '' and process.poll() is not None:
                    break
                    
                if output_line:
                    outfile.write(output_line)
                    outfile.flush()
                    last_response += output_line
                    
                    current_time = time.time()
                    if (current_time - _last_summary_time) >= UPDATE_INTERVAL_SECONDS:
                        try:
                            new_output = last_response[len(last_processed_output):]
                            if not new_output.strip():
                                continue
                            
                            summary_response = ""
                            if not previous_summary:
                                summary_response = chat_completion(
                                    f"An external program is running. Please summarize in a few words what is happening based on this output. Be VERY Brief, use just a few words, not even a complete sentence.\nProgram output:\n\n{new_output}"
                                , db_adapter=db_adapter, fast=True)
                            else:
                                summary_response = chat_completion(
                                    f"Based on these previous status updates:\n{previous_summary}\n\nNEW output to analyze:\n{new_output}\n\nVery briefly summarize ONLY what's new and different from the previous updates. If the new output doesn't add any significant new information, return only NO_CHANGE. Be extremely concise. Dont talk about the 'the output' just say whats happening thats new, and dont repeat things youve already said."
                                , db_adapter=db_adapter, fast=True)
                            
                            # More robust NO_CHANGE check
                            cleaned_response = summary_response.strip()
                            if (cleaned_response and 
                                cleaned_response != 'NO_CHANGE' and 
                                not cleaned_response.upper().startswith('NO_CHANGE')):
                                previous_summary = previous_summary + summary_response + '\n'
                                status_msg = f"🔄 ... {summary_response}"
                                _update_streaming_status(status_msg, run_id, session_id, thread_id, status_update_callback, input_metadata)
                                last_processed_output = last_response
                            
                            _last_summary_time = current_time
                        except Exception as e:
                            logger.error(f"Error getting program output summary: {str(e)}")
                            _last_summary_time = current_time
        
        return process.poll()

    if program_id == "create_mappings_project":
        # Check if g_sheet_id is provided
        if not g_sheet_id:
            return {
                "error": "g_sheet_id must be provided and needs to point to the project config file, and be available to the bot's gsuite user"
            }

        # Get genesis_db from environment variable
        genesis_db = os.getenv("GENESIS_INTERNAL_DB_SCHEMA", "genesis_test").split(".")[0]
        
        # Set up command for subprocess
        cmd = [
            "python", 
            "-m",
            "api_examples.data_engineering.mapping_research_and_proposal",
            "--genesis_db", genesis_db,
            "--new_from_sheet", g_sheet_id
        ]

        # Define the output file in /tmp/
        tmp_output_file = f"tmp/{program_id}_{str(uuid.uuid4())}.txt"
        
        try:
            return_code = process_program_output(cmd, tmp_output_file, program_id)
            
            # Read final program output
            with open(tmp_output_file, 'r') as f:
                program_output = f.read()

            # Sanitize program output to ensure valid JSON
            try:
                # Try to encode/decode to catch any invalid UTF-8 characters
                program_output = program_output.encode('utf-8', errors='replace').decode('utf-8')
                # Replace any control characters except newlines and tabs
                program_output = ''.join(char for char in program_output 
                                       if char in ('\n', '\t') or (ord(char) >= 32 and ord(char) != 127))
            except Exception as e:
                program_output = f"Error sanitizing output: {str(e)}"

            # Truncate program output if over 10k chars
            if len(program_output) > 5000:
                truncated_output = program_output[:5000] + "\n\n[Output truncated at 5k chars - full output available in file: " + tmp_output_file + "]"
                result = {"success": True, "return_code": return_code, "output_file": tmp_output_file, "program_output": truncated_output, "output_truncated": True}
            else:
                result = {"success": True, "return_code": return_code, "output_file": tmp_output_file, "program_output": program_output}
            if status_update_callback:
                status_msg = f"✅ Program completed: {program_id}"
                _update_streaming_status(status_msg, run_id, session_id, thread_id, status_update_callback, input_metadata)
        
        except Exception as e:
            result = {"error": str(e)}
            if status_update_callback:
                status_msg = f"❌ Error running program {program_id}: {str(e)}"
                _update_streaming_status(status_msg, run_id, session_id, thread_id, status_update_callback, input_metadata)
            return result

    elif program_id == "mapping_research_and_proposal":
        # Get genesis_db from environment variable
        genesis_db = os.getenv("GENESIS_INTERNAL_DB_SCHEMA", "genesis_test").split(".")[0]
        
        # Set up command for subprocess
        cmd = [
            "python",
            "-m",
            "api_examples.data_engineering.mapping_research_and_proposal",
            "--genesis_db", genesis_db,
            "--todo-id", todo_id
        ]
        
        if root_folder:
            cmd.extend(["--base-file-path", root_folder])
            
        if project_id:
            cmd.extend(["--project-id", project_id])
        
        # Define the output file in /tmp/
        tmp_output_file = f"tmp/{program_id}_{str(uuid.uuid4())}.txt"
        
        try:
            return_code = process_program_output(cmd, tmp_output_file, program_id)
            
            # Read final program output
            with open(tmp_output_file, 'r') as f:
                program_output = f.read()

            # Sanitize program output to ensure valid JSON
            try:
                # Try to encode/decode to catch any invalid UTF-8 characters
                program_output = program_output.encode('utf-8', errors='replace').decode('utf-8')
                # Replace any control characters except newlines and tabs
                program_output = ''.join(char for char in program_output 
                                       if char in ('\n', '\t') or (ord(char) >= 32 and ord(char) != 127))
            except Exception as e:
                program_output = f"Error sanitizing output: {str(e)}"

            # Truncate program output if over 10k chars
            if len(program_output) > 5000:
                truncated_output = program_output[:5000] + "\n\n[Output truncated at 5k chars - full output available in file: " + tmp_output_file + "]"
                result = {"success": True, "return_code": return_code, "output_file": tmp_output_file, "program_output": truncated_output, "output_truncated": True}
            else:
                result = {"success": True, "return_code": return_code, "output_file": tmp_output_file, "program_output": program_output}
            if status_update_callback:
                status_msg = f"✅ Program completed: {program_id}"
                _update_streaming_status(status_msg, run_id, session_id, thread_id, status_update_callback, input_metadata)
        
        except Exception as e:
            result = {"error": str(e)}
            if status_update_callback:
                status_msg = f"❌ Error running program {program_id}: {str(e)}"
                _update_streaming_status(status_msg, run_id, session_id, thread_id, status_update_callback, input_metadata)
            return result
    
    elif program_id == "missing_fields_research":
        # Get genesis_db from environment variable
        genesis_db = os.getenv("GENESIS_INTERNAL_DB_SCHEMA", "genesis_test").split(".")[0]
        
        # Set up command for subprocess
        cmd = [
            "python",
            "-m",
            "customer_demos.medt.medt_demo_processor",
            "--genesis_db", genesis_db,
            "--todo-id", todo_id
        ]
        
        if root_folder:
            cmd.extend(["--base-file-path", root_folder])
            
        if project_id:
            cmd.extend(["--project-id", project_id])
        
        # Define the output file in /tmp/
        tmp_output_file = f"tmp/{program_id}_{str(uuid.uuid4())}.txt"
        
        try:
            return_code = process_program_output(cmd, tmp_output_file, program_id)
            
            # Read final program output
            with open(tmp_output_file, 'r') as f:
                program_output = f.read()

            # Sanitize program output to ensure valid JSON
            try:
                # Try to encode/decode to catch any invalid UTF-8 characters
                program_output = program_output.encode('utf-8', errors='replace').decode('utf-8')
                # Replace any control characters except newlines and tabs
                program_output = ''.join(char for char in program_output 
                                       if char in ('\n', '\t') or (ord(char) >= 32 and ord(char) != 127))
            except Exception as e:
                program_output = f"Error sanitizing output: {str(e)}"

            # Truncate program output if over 10k chars
            if len(program_output) > 5000:
                truncated_output = program_output[:5000] + "\n\n[Output truncated at 5k chars - full output available in file: " + tmp_output_file + "]"
                result = {"success": True, "return_code": return_code, "output_file": tmp_output_file, "program_output": truncated_output, "output_truncated": True}
            else:
                result = {"success": True, "return_code": return_code, "output_file": tmp_output_file, "program_output": program_output}
            if status_update_callback:
                status_msg = f"✅ Program completed: {program_id}"
                _update_streaming_status(status_msg, run_id, session_id, thread_id, status_update_callback, input_metadata)
        
        except Exception as e:
            result = {"error": str(e)}
            if status_update_callback:
                status_msg = f"❌ Error running program {program_id}: {str(e)}"
                _update_streaming_status(status_msg, run_id, session_id, thread_id, status_update_callback, input_metadata)
            return result
    else:
        result = {"error": f"Program {program_id} not found or not supported"}
        return result
    return {
        "result": result
    }



@gc_tool(
    prompt="The prompt to delegate to the target bot",
    target_bot="The bot ID or name to delegate the work to",
    max_retries="The maximum number of retries to wait for a valid JSON response",
    timeout_seconds="The maximum number of seconds to wait for a valid JSON response",
 #   status_update_callback="The callback function to update the status of the delegation",
 #   session_id="The session ID for the delegation",
  #  input_metadata="The input metadata for the delegation",
  #  run_id="The run ID for the delegation",
# callback_id="The callback ID for the delegation",
    bot_id=BOT_ID_IMPLICIT_FROM_CONTEXT,
    thread_id=THREAD_ID_IMPLICIT_FROM_CONTEXT,
    _group_tags_=[delegate_work],
)
def _delegate_work(
    # if fast model errors, use regular one
    # x add a followup option to followup on a thread vs starting a new one
    # todo, add system prompt override, add tool limits, have delegated jobs skip the thread knowledge injection, etc.
    # x dont save to llm results table for a delegation
    # x see if they have a better time finding other bots now
    # x cancel the delegated run if timeout expires
    # make STOP on the main thread also cancel any inflight delegations
    # x fix bot todo updating, make sure full bot id is in assigned bot field so it can update todos, or allow name too
    # x allow work and tool calls from downstream bots to optionally filter back up to show up in slack while they are working (maybe with a summary like o1 does of whats happening)
    prompt: str,
    target_bot: str = None,
    max_retries: int = 3,
    timeout_seconds: int = 300,
    status_update_callback: str = None,
    session_id: str = None,
    input_metadata: str = None,
    run_id: str = None,
    callback_id: str = None,
    bot_id: str = None,
    thread_id: str = None,
) -> Dict[str, Any]:
    """
    Delegates a task to another bot.
    """
    og_thread_id = thread_id

    def _update_streaming_status(target_bot, current_summary, run_id, session_id, thread_id, status_update_callback, input_metadata):
        from genesis_bots.llm.llm_openai.bot_os_openai import StreamingEventHandler

        msg = f"      🤖 {target_bot}: _{current_summary}_"

        message_obj = {
            "type": "tool_call",
            "text": msg
        }
        if run_id is not None:
            if run_id not in StreamingEventHandler.run_id_to_messages:
                StreamingEventHandler.run_id_to_messages[run_id] = []
            StreamingEventHandler.run_id_to_messages[run_id].append(message_obj)

            # Initialize the array for this run_id if it doesn't exist
            if StreamingEventHandler.run_id_to_output_stream.get(run_id,None) is not None:
                if StreamingEventHandler.run_id_to_output_stream.get(run_id,"").endswith('\n'):
                    StreamingEventHandler.run_id_to_output_stream[run_id] += "\n"
                else:
                    StreamingEventHandler.run_id_to_output_stream[run_id] += "\n\n"
                StreamingEventHandler.run_id_to_output_stream[run_id] += msg
                msg = StreamingEventHandler.run_id_to_output_stream[run_id]
            else:
                StreamingEventHandler.run_id_to_output_stream[run_id] = msg

            status_update_callback(session_id, BotOsOutputMessage(thread_id=thread_id, status="in_progress", output=msg+" 💬", messages=None, input_metadata=input_metadata))

    # current_summary = "Starting delegation"
    # _update_streaming_status(target_bot, current_summary, run_id, session_id, thread_id, status_update_callback, input_metadata)

    server = genesis_app.server

    if server is None:
        return {
            "success": False,
            "error": "ToolBelt server reference not set. Cannot delegate work."
        }

    try:
        # Get target session
        target_session = None
        for session in server.sessions:
            if (target_bot is not None and session.bot_id.upper() == target_bot.upper()) or (target_bot is not None and session.bot_name.upper() == target_bot.upper()):
                target_session = session
                break

        if not target_session:

            bots_udf_adapter = genesis_app.bot_id_to_udf_adapter_map.get(target_bot, None)
            if bots_udf_adapter is None:
                try:
                    from genesis_bots.demo.routes.slack import bot_install_followup

                    bot_install_followup(target_bot, no_slack=True)
                    bots_udf_adapter = genesis_app.bot_id_to_udf_adapter_map.get(target_bot, None)

                    for session in server.sessions:
                        if (target_bot is not None and session.bot_id.upper() == target_bot.upper()) or (target_bot is not None and session.bot_name.upper() == target_bot.upper()):
                            target_session = session
                            break
                except Exception as e:
                    pass
            if bots_udf_adapter is None:
                from genesis_bots.connectors import get_global_db_connector
                bb_db_connector = get_global_db_connector()
                genbot_internal_project_and_schema = os.getenv('GENESIS_INTERNAL_DB_SCHEMA','None')
                if genbot_internal_project_and_schema is None:
                    genbot_internal_project_and_schema = os.getenv('ELSA_INTERNAL_DB_SCHEMA','None')
                if genbot_internal_project_and_schema == 'None':
                    raise ValueError("ENV Variable GENESIS_INTERNAL_DB_SCHEMA is not set.")
                if genbot_internal_project_and_schema is not None:
                    genbot_internal_project_and_schema = genbot_internal_project_and_schema.upper()
                db_schema = genbot_internal_project_and_schema.split('.')
                project_id = db_schema[0]
                dataset_name = db_schema[1]
                bot_servicing_table = os.getenv('BOT_SERVICING_TABLE', 'BOT_SERVICING')
                bots = bb_db_connector.db_list_all_bots(project_id=project_id,
                                                       dataset_name=dataset_name,
                                                       bot_servicing_table=bot_servicing_table,
                                                       runner_id=None,
                                                       full=False,
                                                       slack_details=False,
                                                       with_instructions=False)
                # Extract bot IDs and names from available bots
                bot_info = [{"bot_id": bot.get('bot_id', ''), "bot_name": bot.get('bot_name', '')} for bot in bots]
                return {
                    "success": False,
                    "message": f"Bot '{target_bot}' not found. Available bots:",
                    "available_bots": bot_info
                }

        # Create new thread
        # Find the UDFBotOsInputAdapter
        udf_adapter = None
        for adapter in target_session.input_adapters:
            if adapter.__class__.__name__ == "UDFBotOsInputAdapter":
                udf_adapter = adapter
                break

        if udf_adapter is None:
            raise ValueError("No UDFBotOsInputAdapter found in target session")

        #   thread_id = target_session.create_thread(udf_adapter)
        # Add initial message
        # Define a generic JSON schema that all delegated tasks should conform to
        expected_json_schema = {
            "type": "object",
            "properties": {
                "status": {
                    "type": "string",
                    "enum": ["success", "error", "partial"],
                    "description": "The status of the task execution"
                },
                "message": {
                    "type": "string",
                    "description": "A human readable message describing the result"
                },
                "data": {
                    "type": "object",
                    "description": "The actual result data from executing the task, if applicable"
                },
                "errors": {
                    "type": "array",
                    "items": {
                        "type": "string"
                    },
                    "description": "Any errors that occurred during execution"
                }
            },
            "required": ["status", "message"]
        }
        validation_prompt = f"""
        You are being delegated tasks from another bot.
        Please complete the following task(s) to the best of your ability, then return the results.
        If appropriate, use this JSON format for your response:

        {json.dumps(expected_json_schema, indent=2)}

        Task(s):
        {prompt}
        """

        # Create thread ID for this task
        if callback_id is not None:
            thread_id = callback_id
        else:
            thread_id = 'delegate_' + str(uuid.uuid4())
        # Generate and store UUID for thread tracking
        uu = udf_adapter.submit(
            input=validation_prompt,
            thread_id=thread_id,
            bot_id={},
            file={}
        )

        # Wait for response with timeout
        start_time = time.time()
        attempts = 0
        last_response = ""
        previous_summary = ""
        _last_summary_time = time.time()
        while attempts < max_retries and (time.time() - start_time) < timeout_seconds:
            # Check if response available
            response = udf_adapter.lookup_udf(uu)
            # Check if response ends with chat emoji
            if response:
                if response != last_response:
                    # Track last summary time
                    current_time = time.time()
                    if (current_time - _last_summary_time) >= UPDATE_INTERVAL_SECONDS:  # Changed from 5 to 15 seconds
                        # Send current streaming response for summarization via chat completion
                        last_response = response
                        try:
                            # Only summarize if response has changed since last check
                            summary_response = ""
                            if previous_summary == "":
                                summary_response = chat_completion(
                                    f"An AI bot is doing work, you are monitoring it. Please summarize in a few words what is happening in this ongoing response from another bot so far.  Be VERY Brief, use just a few words, not even a complete sentence.  Don't put a period on the end if its just one sentence or less.\nHere is the bots output so far:\n\n{response.strip()[:-2]}"
                                , db_adapter=db_adapter, fast=True)
                            else:
                                summary_response = chat_completion(
                                    f"An AI bot is doing work, you are monitoring it.  Based on its previous status updates, you have provided these summaries so far: \n<PREVIOUS_SUMMARIES_START>\n{previous_summary}\n</PREVIOUS_SUMMARIES_END>\n\nThe current output of the bot so far:\n<BOTS_OUTPUT_START>{response.strip()[:-2]}\n</BOTS_OUTPUT_END>\n\nNOW, Very briefly, in just a few words, summarize anything new the bot has done since the last update, that you have not mentioned yet in a previous summary.  Be VERY Brief, use just a few words, not even a complete sentence.  Don't put a period on the end if its just one sentence or less.  Don't repeat things you already said in previous summaries. If there has been no substantial change in the status, return only NO_CHANGE."
                                , db_adapter=db_adapter, fast=True)
                            if summary_response and summary_response != 'NO_CHANGE':
                                previous_summary = previous_summary + summary_response + '\n'
                                current_summary = summary_response
                                _update_streaming_status(target_bot, current_summary, run_id, session_id, og_thread_id, status_update_callback, input_metadata)
                            _last_summary_time = current_time
                        except Exception as e:
                            logger.error(f"Error getting response summary: {str(e)}")
                            _last_summary_time = current_time
            if response and response.strip().endswith("💬"):
                time.sleep(2)
                continue
            if response:
                try:
                    # Extract the last JSON object from the response string
                    # Try to find JSON in code blocks first
                    # json_matches = re.findall(r'```json\n(.*?)\n```', response, re.DOTALL)
                    # if not json_matches:
                    #    # If no code blocks, try to find any JSON object in the response
                    #    json_matches = re.findall(r'\{[^{}]*(?:\{[^{}]*\}[^{}]*)*\}', response)
                    # if json_matches:
                    #    result = json.loads(json_matches[-1])  # Get last JSON match
                    # else:
                    #    # Try parsing the whole response as JSON if no code blocks found
                    #    result = json.loads(response)

                    # Validate against schema
                    # jsonschema.validate(result, expected_json_schema)
                    return {
                        "success": True,
                        "result": response,
                        "callback_id": thread_id
                    }
                except (json.JSONDecodeError, jsonschema.ValidationError):
                    # Invalid JSON or schema mismatch - retry
                    attempts += 1
                    if attempts < max_retries:
                        # Send retry prompt
                        last_response = ""
                        previous_summary = ""
                        retry_prompt = f"""
                        Your previous response was not in the correct JSON format.
                        Please try again and respond ONLY with a JSON object matching this schema:
                        {json.dumps(expected_json_schema, indent=2)}
                        """
                        uu = udf_adapter.submit(
                            input=retry_prompt,
                            thread_id=thread_id,
                            bot_id={},
                            file={}
                        )
                        _update_streaming_status(target_bot, 'Bot provided incorrect JSON response format, retrying...', run_id, session_id, thread_id, status_update_callback, input_metadata)

            time.sleep(1)

        # If we've timed out, send stop command
        if (time.time() - start_time) >= timeout_seconds:
            # Send stop command to same thread
            udf_adapter.submit(
                input="!stop",
                thread_id=thread_id,
                bot_id={},
                file={}
            )

        if (time.time() - start_time) >= timeout_seconds:
            return {
                "success": False,
                "error": f"Timed out after {timeout_seconds} seconds waiting for valid JSON response"
            }
        else:
            return {
                "success": False,
                "error": f"Failed to get valid JSON response after {attempts} failed attempts"
            }

    except Exception as e:
        return {
            "success": False,
            "error": f"Error delegating work: {str(e)}"
        }


delegate_work_functions = [_delegate_work, run_program]

# Called from bot_os_tools.py to update the global list of functions
def get_delegate_work_functions():
    return delegate_work_functions
