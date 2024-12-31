import json
import os
from core.logging_config import logger
from core.bot_os_tools2 import BotLlmEngineEnum
from core.bot_os_tools2 import get_openai_client


def chat_completion(
    self,
    message,
    db_adapter,
    bot_id=None,
    bot_name=None,
    thread_id=None,
    process_id="",
    process_name="",
    note_id=None,
    fast=False,
):
    process_name = "" if process_name is None else process_name
    process_id = "" if process_id is None else process_id
    message_metadata = {"process_id": process_id, "process_name": process_name}
    return_msg = None

    if not fast:
        self._write_message_log_row(
            db_adapter,
            bot_id,
            bot_name,
            thread_id,
            "Supervisor Prompt",
            message,
            message_metadata,
        )

    model = None

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

    if model == "openai":
        api_key = os.getenv("OPENAI_API_KEY")
        if not api_key:
            logger.info("OpenAI API key is not set in the environment variables.")
            return None

        openai_model = os.getenv(
            "OPENAI_MODEL_SUPERVISOR", os.getenv("OPENAI_MODEL_NAME", "gpt-4o")
        )

        if fast and openai_model.startswith("gpt-4o"):
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
        except Exception as e:
            if os.getenv("OPENAI_MODEL_SUPERVISOR", None) is not None:
                logger.info(
                    f"Error occurred while calling OpenAI API with model {openai_model}: {e}"
                )
                logger.info(
                    f'Retrying with main model {os.getenv("OPENAI_MODEL_NAME","gpt-4o")}'
                )
                openai_model = os.getenv("OPENAI_MODEL_NAME", "gpt-4o")
                response = client.chat.completions.create(
                    model=openai_model,
                    messages=[
                        {
                            "role": "user",
                            "content": message,
                        },
                    ],
                )
            else:
                logger.info(f"Error occurred while calling OpenAI API: {e}")

        return_msg = response.choices[0].message.content

    elif model == "cortex":
        if not db_adapter.check_cortex_available():
            logger.info("Cortex is not available.")
            return None
        else:
            response, status_code = db_adapter.cortex__chat_completion(message)
            return_msg = response

    if return_msg is None:
        return_msg = (
            "Error _chat_completion, return_msg is none, llm_type = ",
            os.getenv("BOT_OS_DEFAULT_LLM_ENGINE").lower(),
        )
        logger.info(return_msg)

    if not fast:
        self._write_message_log_row(
            db_adapter,
            bot_id,
            bot_name,
            thread_id,
            "Supervisor Response",
            return_msg,
            message_metadata,
        )

    return return_msg