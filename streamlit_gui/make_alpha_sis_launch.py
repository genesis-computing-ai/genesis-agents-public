import re
import os

def replace_genesis_bots():
    file_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'Genesis.py')
    with open(file_path, 'r') as file:
        content = file.read()
    modified_content = re.sub(r'app_name = "GENESIS_BOTS"', 'app_name = "GENESIS_BOTS_ALPHA"', content)
    with open(file_path, 'w') as file:
        file.write(modified_content)

def revert_genesis_bots():
    file_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'Genesis.py')
    with open(file_path, 'r') as file:
        content = file.read()
    modified_content = re.sub(r'app_name = "GENESIS_BOTS_ALPHA"', 'app_name = "GENESIS_BOTS"', content)
    with open(file_path, 'w') as file:
        file.write(modified_content)

# These functions can now be imported and called from other scripts:
# from your_script_name import replace_genesis_bots, revert_genesis_bots