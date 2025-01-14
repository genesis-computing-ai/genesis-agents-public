from openai import OpenAI
client = OpenAI()
model = 'o1'
print("model ", model)
messages = []

while True:
    user_input = input("You: ")
    if user_input.lower() == 'quit':
        break

    messages.append({
        "role": "user",
        "content": user_input
    })



    response = client.chat.completions.create(
        model=model,
        messages=messages
    )

 #   print(response.choices[0].message.content)

    assistant_response = response.choices[0].message.content
    print(f"Assistant: {assistant_response}")

    messages.append({
        "role": "assistant",
        "content": assistant_response
    })
