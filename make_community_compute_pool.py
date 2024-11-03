import re
import os
from core.logging_config import setup_logger
logger = setup_logger(__name__)

def replace_genesis_bots(input_file, output_file):
    with open(input_file, 'r') as file:
        content = file.read()
    
    # Replace the specific line
    modified_content = re.sub(r'GENESIS_POOL', 'GENESIS_COMMUNITY_POOL', content)
    
    with open(output_file, 'w') as file:
        file.write(modified_content)

def replace_genesis_bots_eai(input_file, output_file):
    with open(input_file, 'r') as file:
        content = file.read()
    
    # Replace the specific line
    modified_content = re.sub(r'GENESIS_EAI', 'GENESIS_COMMUNITY_EAI', content)
    
    with open(output_file, 'w') as file:
        file.write(modified_content)       

# Get the directory of this script
script_dir = os.path.dirname(os.path.abspath(__file__))

# Define input and output file paths relative to the script directory
files_to_change = ['snowflake_app/setup_script.sql','streamlit_gui/main.py','streamlit_gui/sis_launch.py','streamlit_gui/page_files/config_pool.py','streamlit_gui/page_files/grant_wh.py','streamlit_gui/page_files/start_service.py','streamlit_gui/page_files/start_stop.py']
for file in files_to_change:
    input_file = os.path.join(script_dir, file)
    output_file = os.path.join(script_dir, file)

    # Usage
    replace_genesis_bots(input_file, output_file)
    logger.info(f"Replacement complete. New file '{output_file}' created with GENESIS_COMMUNITY_POOL.")

# Define input and output file paths relative to the script directory
files_to_change = ['connectors/snowflake_connector.py','snowflake_app/setup_script.sql','streamlit_gui/main.py','streamlit_gui/sis_launch.py','streamlit_gui/page_files/config_eai.py','streamlit_gui/page_files/start_service.py','streamlit_gui/page_files/start_stop.py']
for file in files_to_change:
    input_file = os.path.join(script_dir, file)
    output_file = os.path.join(script_dir, file)

    # Usage
    replace_genesis_bots_eai(input_file, output_file)
    logger.info(f"Replacement complete. New file '{output_file}' created with GENESIS_COMMUNITY_EAI.")