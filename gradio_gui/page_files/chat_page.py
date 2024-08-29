import gradio as gr
import time
import uuid
from utils import get_bot_details, submit_to_udf_proxy, get_response_from_udf_proxy

def create_chat_interface():
    bot_details = get_bot_details()
    if not bot_details:
        print("Warning: No bot details available. Using default bot.")
        bot_details = [{"bot_id": "default", "bot_name": "Default Bot"}]
    
    bot_details.sort(key=lambda x: (not "Eve" in x["bot_name"], x["bot_name"]))
    bot_names = [bot["bot_name"] for bot in bot_details]
    bot_ids = [bot["bot_id"] for bot in bot_details]

    def chat(message, history, selected_bot):
        bot_id = bot_ids[bot_names.index(selected_bot)]
        conversation_id = str(uuid.uuid4())
        submit_result = submit_to_udf_proxy(message, conversation_id, bot_id)
        if submit_result.startswith("Error:"):
            history.append((message, submit_result))
            return "", history

        time.sleep(1)  # Wait for the response to be ready
        bot_response = get_response_from_udf_proxy(conversation_id, bot_id)
        if bot_response.startswith("Error:"):
            history.append((message, bot_response))
        else:
            history.append((message, bot_response))
        return "", history

    with gr.Blocks() as chat_interface:
        chatbot = gr.Chatbot()
        msg = gr.Textbox()
        clear = gr.Button("Clear")
        bot_dropdown = gr.Dropdown(choices=bot_names, label="Select a bot", value=bot_names[0])

        msg.submit(chat, [msg, chatbot, bot_dropdown], [msg, chatbot])
        clear.click(lambda: [], None, chatbot, queue=False)

    return chat_interface

# Remove the if __name__ == "__main__" block from here