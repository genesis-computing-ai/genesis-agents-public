import streamlit as st
import os
import re

# Set Streamlit to wide mode.
st.set_page_config(layout="wide")

def strip_ansi(text):
    """
    Remove ANSI escape sequences from a string.
    """
    ansi_escape = re.compile(r'(?:\x1B[@-_][0-?]*[ -/]*[@-~])')
    return ansi_escape.sub('', text)

# Path to the file you want to tail.
log_file = "tmp/program_output.txt"

st.title("Log File Output")

def render_textarea(content):
    """
    Returns HTML for a read-only textarea displaying the log content.
    The textarea is styled to be 90% of the viewport width and 80% of the viewport height,
    and is centered on the page.
    """
    return f"""
<textarea id="log_box" readonly style="
    width: 90vw;
    height: 80vh;
    overflow-y: auto;
    white-space: pre-wrap;
    border: 1px solid #ddd;
    margin: auto;
    padding: 10px;
    background-color: #f8f8f8;
    font-family: monospace;
    color: #000;
    display: block;
">
{content}
</textarea>
<script>
    var logBox = document.getElementById("log_box");
    if (logBox) {{
        logBox.scrollTop = logBox.scrollHeight;
    }}
</script>
"""

# Read and display the file content
try:
    if os.path.exists(log_file):
        with open(log_file, "r") as f:
            content = f.read()
        content = strip_ansi(content)
        st.markdown(render_textarea(content), unsafe_allow_html=True)
    else:
        st.info("Log file not found yet. Waiting for file to be created...")
except Exception as e:
    st.error(f"Error reading file: {e}")