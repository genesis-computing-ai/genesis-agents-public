import asyncio
from enum import Enum
from threading import Thread
import time
import uuid
from flask import Response, make_response, request
from pydantic import BaseModel, Field, validator
from typing import Any, Dict, List, Union

def openbb_query(bot_id_to_udf_adapter_map: Dict[str, Any], default_bot_id: str):
    class RoleEnum(str, Enum):
        ai = "ai"
        human = "human"

    class LlmMessage(BaseModel):
        role: RoleEnum = Field(
            ...,  # Using Ellipsis for required fields
            description="The role of the entity that is creating the message"
        )
        content: str = Field(..., description="The content of the message")  # Marking field as required

    class ContextualWidget(BaseModel):
        uuid: str = Field(..., description="The UUID of the widget.")
        name: str = Field(..., description="The name of the widget.")
        description: str = Field(
            ...,  # Marking as required
            description="A description of the data contained in the widget"
        )
        metadata: Union[Dict[str, Any], None] = Field(
            default=None,
            description="Additional widget metadata (e.g., the selected ticker, etc)",
        )
        content: str = Field(..., description="The data content of the widget")
        thread_id: str = Field(description="Context thread for the conversation")
                               #default_factory=lambda: f"thread_{self.name}_{self.uuid}")
        bot_id: str = Field(..., description="Genesis Bot to route to")

    class AgentQueryRequest(BaseModel):
        messages: List[LlmMessage] = Field(
            ...,
            description="A list of messages to submit to the copilot."
        )
        context: Union[str, List[ContextualWidget], None] = Field(
            default=None, description="Additional context. Can either be a string, or a list of user-selected widgets."
        )
        use_docs: bool = Field(
            default=False, description="Set True to use uploaded docs when answering query."
        )

        @validator("messages")
        def check_messages_not_empty(cls, value):
            if not value:
                raise ValueError("messages list cannot be empty.")
            return value

    data = request.get_json()  # Assuming the request body is JSON
    request_obj = AgentQueryRequest(**data)
    if request_obj.context is None:
        thread_id = f"thread_{uuid.uuid4()}"
        bot_id = default_bot_id
    elif isinstance(request_obj.context, str):
        thread_id = request_obj.context
        bot_id = default_bot_id
    else:
        thread_id = request_obj.context[0].thread_id
        bot_id = request_obj.context[0].bot_id

    bots_udf_adapter = bot_id_to_udf_adapter_map.get(bot_id)
    if bots_udf_adapter:
        input_text_id = bots_udf_adapter.submit(str(request_obj.messages), thread_id)
    else:
        return make_response({"error": "No UDF adapter found for the given bot ID."}, 404)

    def generate2(input_text_id: str, output_queue: asyncio.Queue):
        async def async_generate():
            while True:
                resp = bots_udf_adapter.lookup_udf(input_text_id)
                if resp is None:
                    await output_queue.put("thinking...\n\n")
                    await asyncio.sleep(1)
                else:
                    await output_queue.put(f"data: {resp}\n\n")
                    break

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(async_generate())

    def stream_response(input_text_id):
        output_queue = asyncio.Queue()

        Thread(target=generate2, args=(input_text_id, output_queue)).start()

        def generate():
            while True:
                output = output_queue.get_nowait()
                if output:
                    yield output
                else:
                    break

        return Response(generate(), mimetype='text/event-stream')

    return stream_response(input_text_id)
