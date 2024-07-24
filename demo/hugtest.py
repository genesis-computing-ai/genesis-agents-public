from huggingface_hub import InferenceClient
import minijinja

client = InferenceClient(
    "meta-llama/Meta-Llama-3.1-70B-Instruct",
    token="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",
)

message = client.chat_completion(
	messages=[{"role": "ipython", "content": "What is the capital of France?"}],
	max_tokens=500,
	stream=False,
)
print(message.choices[0], end="")
