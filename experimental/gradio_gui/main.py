import gradio as gr
from page_files import chat_page

def create_app():
    with gr.Blocks() as app:
        gr.Markdown("# Genesis Bots Chat")
        chat_interface = chat_page.create_chat_interface()
    return app

if __name__ == "__main__":
    app = create_app()
    app.launch()