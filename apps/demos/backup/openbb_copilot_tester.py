import httpx

BASE_URL = "http://localhost:8080/udf_proxy/openbb"

# Query, without context.
print("\n\nTesting query:")
print("==============")
with httpx.stream(
    "POST",
    f"{BASE_URL}/v1/query",
    json={"messages": [{"role": "human", "content": "What is your name?"}],
          "context":  "thread-123" #[{"thread_id": "thread-123"}]
          },
) as response:
    for chunk in response.iter_text():
        print(chunk, end="", flush=True)


# Query, with context.
print("\n\nTesting query with context:")
print("==============")
with httpx.stream(
    "POST",
    f"{BASE_URL}/v1/query",
    json={
        "messages": [
            {"role": "human", "content": "What is the current stock price of TSLA?"}
        ],
        "context": [
            {
                "uuid": "12345-abcde",
                "name": "Stock price widget",
                "description": "Contains the stock price of a ticker.",
                "metadata": {"ticker": "TSLA"},
                "content": "The stock price is $99.95",
                "thread_id": "thread-123",
                "bot_id": "mg-local-eve-test-1"
            }
        ],
    },
) as response:
    for chunk in response.iter_text():
        print(chunk, end="", flush=True)


# Query, with history.
print("\n\nTesting query with chat history:")
print("==============")
with httpx.stream(
    "POST",
    f"{BASE_URL}/v1/query",
    json={
        "messages": [
            {
                "role": "human",
                "content": "Only provide Yes or No as your answer and NOTHING ELSE. Is AAPL a famous company?",
            },
            {"role": "ai", "content": "Yes."},
            {"role": "human", "content": "What about TSLA?"},
        ],
        "context": "thread-123"
    },
) as response:
    for chunk in response.iter_text():
        print(chunk, end="", flush=True)