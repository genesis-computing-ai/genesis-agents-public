import json
import os
from datetime import datetime

from genesis_bots.core.logging_config import logger
from genesis_bots.core.bot_os_llm import BotLlmEngineEnum
from genesis_bots.llm.llm_openai.openai_utils import get_openai_client


def get_process_info(bot_id=None, process_name=None, process_id=None):
    from genesis_bots.connectors import get_global_db_connector
    db_adapter = get_global_db_connector()
    cursor = db_adapter.client.cursor()
    try:
        result = None

        if (process_name is None or process_name == "") and (
            process_id is None or process_id == ""
        ):
            return {
                "Success": False,
                "Error": "Either process_name or process_id must be provided and cannot be empty.",
            }
        if process_id is not None and process_id != "":
            query = (
                f"SELECT * FROM {db_adapter.schema}.PROCESSES WHERE bot_id LIKE %s AND process_id = %s"
                if db_adapter.schema
                else f"SELECT * FROM PROCESSES WHERE bot_id LIKE %s AND process_id = %s"
            )
            cursor.execute(query, (f"%{bot_id}%", process_id))
            result = cursor.fetchone()
        if result == None:
            if process_name is not None and process_name != "":
                query = (
                    f"SELECT * FROM {db_adapter.schema}.PROCESSES WHERE bot_id LIKE %s AND process_name LIKE %s"
                    if db_adapter.schema
                    else f"SELECT * FROM PROCESSES WHERE bot_id LIKE %s AND process_name LIKE %s"
                )
                cursor.execute(query, (f"%{bot_id}%", f"%{process_name}%"))
                result = cursor.fetchone()
        if result:
            # Assuming the result is a tuple of values corresponding to the columns in the PROCESSES table
            # Convert the tuple to a dictionary with appropriate field names
            field_names = [desc[0] for desc in cursor.description]
            return {
                "Success": True,
                "Data": dict(zip(field_names, result)),
                "Note": "Only use this information to help manage or update processes, do not actually run a process based on these instructions. If you want to run this process, use _run_process function and follow the instructions that it gives you.",
                "Important!": "If a user has asked you to show these instructont to them, output them verbatim, do not modify of summarize them.",
            }
        else:
            return {}
    except Exception as e:
        return {}


def get_processes_list(bot_id="all"):
    from genesis_bots.connectors import get_global_db_connector
    db_adapter = get_global_db_connector()
    cursor = db_adapter.client.cursor()
    try:
        if bot_id == "all":
            list_query = (
                f"SELECT process_id, bot_id, process_name FROM {db_adapter.schema}.PROCESSES WHERE HIDDEN IS NULL OR HIDDEN = FALSE"
                if db_adapter.schema
                else f"SELECT process_id, bot_id, process_name FROM PROCESSES WHERE HIDDEN IS NULL OR HIDDEN = FALSE"
            )
            cursor.execute(list_query)
        else:
            list_query = (
                f"SELECT process_id, bot_id, process_name FROM {db_adapter.schema}.PROCESSES WHERE upper(bot_id) = upper(%s) AND HIDDEN IS NULL OR HIDDEN = FALSE"
                if db_adapter.schema
                else f"SELECT process_id, bot_id, process_name FROM PROCESSES WHERE upper(bot_id) = upper(%s) AND HIDDEN IS NULL OR HIDDEN = FALSE"
            )
            cursor.execute(list_query, (bot_id,))
        processs = cursor.fetchall()
        process_list = []
        for process in processs:
            process_dict = {
                "process_id": process[0],
                "bot_id": process[1],
                "process_name": process[2],
            }
            process_list.append(process_dict)
        return {"Success": True, "processes": process_list}
    except Exception as e:
        return {
            "Success": False,
            "Error": f"Failed to list processs for bot {bot_id}: {e}",
        }
    finally:
        cursor.close()

def clear_process_registers_by_thread(self, thread_id):
    # Initialize thread-specific data structures if not already present
    with self.lock:
        if thread_id not in self.counter:
            self.counter[thread_id] = {}
        #   if thread_id not in self.process:
        #       self.process[thread_id] = {}
        if thread_id not in self.last_fail:
            self.last_fail[thread_id] = {}
        if thread_id not in self.fail_count:
            self.fail_count[thread_id] = {}
        if thread_id not in self.instructions:
            self.instructions[thread_id] = {}
        if thread_id not in self.process_history:
            self.process_history[thread_id] = {}
        if thread_id not in self.done:
            self.done[thread_id] = {}
        if thread_id not in self.silent_mode:
            self.silent_mode[thread_id] = {}
        if thread_id not in self.process_config:
            self.process_config[thread_id] = {}


def get_sys_email():
    from genesis_bots.connectors import get_global_db_connector
    db_adapter = get_global_db_connector()
    cursor = db_adapter.client.cursor()
    try:
        _get_sys_email_query = f"SELECT default_email FROM {db_adapter.genbot_internal_project_and_schema}.DEFAULT_EMAIL"
        cursor.execute(_get_sys_email_query)
        result = cursor.fetchall()
        default_email = result[0][0] if result else None
        return default_email
    except Exception as e:
        #  logger.info(f"Error getting sys email: {e}")
        return None


def chat_completion(
    message,
    db_adapter,
    bot_id=None,
    bot_name=None,
    thread_id=None,
    process_id="",
    process_name="",
    note_id=None,
    fast=False,
) -> str:
    process_name = "" if process_name is None else process_name
    process_id = "" if process_id is None else process_id
    message_metadata = {"process_id": process_id, "process_name": process_name}
    return_msg = None

    if not fast:
        _write_message_log_row(
            db_adapter,
            bot_id,
            bot_name,
            thread_id,
            "Supervisor Prompt",
            message,
            message_metadata,
        )

    model = None

    bot_llms = {}

    if "BOT_LLMS" in os.environ and os.environ["BOT_LLMS"]:
        # Convert the JSON string back to a dictionary
        bot_llms = json.loads(os.environ["BOT_LLMS"])

    # Find the model for the specific bot_id in bot_llms
    model = None
    if bot_id and bot_id in bot_llms:
        model = bot_llms[bot_id].get("current_llm")

    if not model:
        engine = BotLlmEngineEnum(os.getenv("BOT_OS_DEFAULT_LLM_ENGINE"))
        if engine is BotLlmEngineEnum.openai:
            model = "openai"
        else:
            model = "cortex"
    assert model in ("openai", "cortex")
    # TODO: handle other engine types, use BotLlmEngineEnum instead of strings

    return_msg = ''

    if model == "openai":
        api_key = os.getenv("OPENAI_API_KEY")
        if not api_key:
            logger.info("OpenAI API key is not set in the environment variables.")
            return 'OpenAI API key is not set in the environment variables.'

        openai_model = os.getenv(
            "OPENAI_MODEL_SUPERVISOR", os.getenv("OPENAI_MODEL_NAME", "gpt-4o-2024-11-20")
        )

        if fast and openai_model.startswith("gpt-4o-2024-11-20"):
            openai_model = "gpt-4o-mini"

        if not fast:
            logger.info("process supervisor using model: ", openai_model)
        try:
            client = get_openai_client()
            response = client.chat.completions.create(
                model=openai_model,
                messages=[
                    {
                        "role": "user",
                        "content": message,
                    },
                ],
            )
            return_msg = response.choices[0].message.content
        except Exception as e:
            if os.getenv("OPENAI_MODEL_SUPERVISOR", None) is not None:
                logger.info(
                    f"Error occurred while calling OpenAI API with model {openai_model}: {e}"
                )
                logger.info(
                    f'Retrying with main model {os.getenv("OPENAI_MODEL_NAME","gpt-4o-2024-11-20")}'
                )
                openai_model = os.getenv("OPENAI_MODEL_NAME", "gpt-4o-2024-11-20")
                response = client.chat.completions.create(
                    model=openai_model,
                    messages=[
                        {
                            "role": "user",
                            "content": message,
                        },
                    ],
                )
                return_msg = response.choices[0].message.content
            else:
                logger.info(f"Error occurred while calling OpenAI API: {e}")



    elif model == "cortex":
        if not db_adapter.check_cortex_available():
            logger.info("Cortex is not available.")
            return "Cortex is not available."
        else:
            response, status_code = db_adapter.cortex_chat_completion(message)
            return_msg = response

    if not return_msg:
        return_msg = (
            "Error _chat_completion, return_msg is none, llm_type = ",
            os.getenv("BOT_OS_DEFAULT_LLM_ENGINE").lower(),
        )
        logger.info(return_msg)

    if not fast:
        _write_message_log_row(
            db_adapter,
            bot_id,
            bot_name,
            thread_id,
            "Supervisor Response",
            return_msg,
            message_metadata,
        )

    return return_msg

def _write_message_log_row(
    db_adapter,
    bot_id="",
    bot_name="",
    thread_id="",
    message_type="",
    message_payload="",
    message_metadata={},
):
    """
    Inserts a row into the MESSAGE_LOG table.

    Args:
        db_adapter: The database adapter to use for the insertion.
        bot_id (str): The ID of the bot.
        bot_name (str): The name of the bot.
        thread_id (str): The ID of the thread.
        message_type (str): The type of the message.
        message_payload (str): The payload of the message.
        message_metadata (str): The metadata of the message.
    """
    timestamp = datetime.now()
    query = f"""
        INSERT INTO {db_adapter.schema}.MESSAGE_LOG (timestamp, bot_id, bot_name, thread_id, message_type, message_payload, message_metadata)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """
    # logger.info(f"Writing message log row: {timestamp}, {bot_id}, {bot_name}, {thread_id}, {message_type}, {message_payload}, {message_metadata}")
    values = (
        timestamp,
        bot_id,
        bot_name,
        thread_id,
        message_type,
        message_payload,
        json.dumps(message_metadata),
    )

    try:
        cursor = db_adapter.connection.cursor()
        cursor.execute(query, values)
        db_adapter.connection.commit()
    except Exception as e:
        logger.info(f"Error writing message log row: {e}")
        db_adapter.connection.rollback()
    finally:
        cursor.close()
