from openai import OpenAI
from openai.types.chat import ChatCompletion, ChatCompletionMessage
from openai.types.chat.chat_completion import Choice, CompletionUsage
from typing import Dict, List, Optional, Union
import anthropic
from anthropic.types import MessageParam
import os
import time

class ExtendedOpenAI(OpenAI):
    """
    Extended OpenAI client that handles OpenAI and Anthropic models.
    """
    
    def __init__(self, **kwargs):
        """Initialize using standard OpenAI arguments"""
        super().__init__(**kwargs)
        self.anthropic_client = None
        self._wrap_chat_completions()

    def _ensure_anthropic_client(self):
        """Lazily initialize Anthropic client"""
        if self.anthropic_client is None:
            self.anthropic_client = anthropic.Client(api_key=self.api_key)

    def _convert_to_anthropic_messages(self, messages: List[Dict]) -> List[MessageParam]:
        """Convert OpenAI message format to Anthropic format."""
        anthropic_messages = []
        
        for message in messages:
            role = message["role"]
            content = message["content"]
            
            if role == "system":
                anthropic_messages.append({
                    "role": "user",
                    "content": f"System: {content}"
                })
            elif role in ["assistant", "user"]:
                anthropic_messages.append({
                    "role": role,
                    "content": content
                })
                
        return anthropic_messages

    def _create_chat_completion(self, response_data: Dict) -> ChatCompletion:
        """Create a proper OpenAI ChatCompletion object from response data"""
        message = ChatCompletionMessage(
            role="assistant",
            content=response_data["choices"][0]["message"]["content"]
        )
        
        choice = Choice(
            finish_reason=response_data["choices"][0]["finish_reason"],
            index=0,
            message=message,
            logprobs=None
        )

        usage = CompletionUsage(
            completion_tokens=0,
            prompt_tokens=0,
            total_tokens=0
        )
        
        return ChatCompletion(
            id=response_data["id"],
            choices=[choice],
            created=response_data["created"],
            model=response_data["model"],
            object="chat.completion",
            system_fingerprint=None,
            usage=usage
        )

    def _wrap_chat_completions(self):
        """Wrap the original chat.completions.create method to handle routing"""
        original_create = self.chat.completions.create
        
        def wrapped_create(
            model: str,
            messages: List[Dict],
            temperature: float = 1.0,
            max_tokens: Optional[int] = None,
            top_p: Optional[float] = None,
            stream: bool = False,
            **kwargs
        ):
            # Route to Anthropic if model starts with "claude"
            if model.startswith("claude"):
                self._ensure_anthropic_client()
                
                try:
                    anthropic_messages = self._convert_to_anthropic_messages(messages)
                    response = self.anthropic_client.messages.create(
                        model=model,
                        messages=anthropic_messages,
                        temperature=temperature,
                        max_tokens=max_tokens if max_tokens else 4096,
                        top_p=top_p if top_p else 1,
                        stream=stream,
                        **kwargs
                    )
                    
                    if stream:
                        return self._anthropic_stream_response_generator(response, model)
                    
                    response_data = {
                        "id": str(response.id),
                        "object": "chat.completion",
                        "created": int(time.time()),
                        "model": model,
                        "choices": [{
                            "index": 0,
                            "message": {
                                "role": "assistant",
                                "content": response.content[0].text
                            },
                            "finish_reason": "stop"
                        }],
                        "usage": {
                            "prompt_tokens": 0,
                            "completion_tokens": 0,
                            "total_tokens": 0
                        }
                    }
                    
                    return self._create_chat_completion(response_data)
                    
                except Exception as e:
                    raise Exception(f"Anthropic API Error: {str(e)}")
            else:
                # Use original OpenAI implementation
                return original_create(
                    model=model,
                    messages=messages,
                    temperature=temperature,
                    max_tokens=max_tokens,
                    top_p=top_p,
                    stream=stream,
                    **kwargs
                )
        
        # Replace the create method
        self.chat.completions.create = wrapped_create

    def _anthropic_stream_response_generator(self, response_stream, model: str):
        """Convert Anthropic stream to OpenAI stream format"""
        for chunk in response_stream:
            if not chunk.content:  # Skip empty chunks
                continue
                
            message = ChatCompletionMessage(
                role="assistant",
                content=chunk.content[0].text
            )
            
            choice = Choice(
                finish_reason=None,
                index=0,
                message=message,
                logprobs=None
            )
            
            usage = CompletionUsage(
                completion_tokens=0,
                prompt_tokens=0,
                total_tokens=0
            )
            
            yield ChatCompletion(
                id=str(chunk.id),
                choices=[choice],
                created=int(time.time()),
                model=model,
                object="chat.completion.chunk",
                system_fingerprint=None,
                usage=usage
            )