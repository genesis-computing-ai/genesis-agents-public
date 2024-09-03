import re
import os

def replace_genesis_bots(input_file, output_file):
    with open(input_file, 'r') as file:
        content = file.read()
    
    # Replace the specific line
    modified_content = re.sub(r'app_name = "GENESIS_BOTS"', 'app_name = "GENESIS_BOTS_ALPHA"', content)
    
    with open(output_file, 'w') as file:
        file.write(modified_content)

# Get the directory of this script
script_dir = os.path.dirname(os.path.abspath(__file__))

# Define input and output file paths relative to the script directory
input_file = os.path.join(script_dir, 'main.py')
output_file = os.path.join(script_dir, 'sis_launch.py')

# Usage
replace_genesis_bots(input_file, output_file)
print(f"Replacement complete. New file '{output_file}' created with GENESIS_BOTS_ALPHA.")