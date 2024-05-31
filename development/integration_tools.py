# integration_tools.py
import importlib
import json
import os
import re
import sys
import shutil
import logging
from datetime import datetime

# Set up a logger for the module
logger = logging.getLogger(__name__)

# Create a timestamped backup of existing module
def _backup_existing_module(module_path):
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    backup_path = f"{module_path}--{timestamp}.bak"
    shutil.copy(module_path, backup_path)
    logger.info(f"Backup of '{module_path}' created at '{backup_path}'")
    return backup_path

# Merge new code into existing module code (placeholder for actual logic)
def _merge_module_code(existing_code, new_code):
    # In practice, use a library like difflib to merge
    return new_code

# Write the Python code to a new file
def _write_code_to_file(module_path, code):
    with open(module_path, 'w') as file:
        file.write(code)

# Import module from given path
def _import_module_from_path(module_name, module_path):
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module

def _create_tool_function_description(module_name, function_name, function_description, parameters):
    # Helper function to create a tool function description with parameters for a new function
    adjusted_parameters = {key: {k: v for k, v in value.items() if k != 'required'} for key, value in parameters.items()}
    required_parameters = [key for key, value in parameters.items() if value.get('required', True)]
    description = {
        "type": "function",
        "function": {
            "name": f"{module_name}--{function_name}",
            "description": function_description,
            "parameters": {
                "type": "object",
                "properties": adjusted_parameters,
                "required": required_parameters
            }
        }
    }
    return description

def _write_tool_function_description(module_path, function_name, description):
    # Helper function to append the tool function description to the module file
    with open(module_path, 'a') as file:
        file.write("\n# Start of Generated Description\n")
        description_var = f"TOOL_FUNCTION_DESCRIPTION_{function_name.upper()} = "
        formatted_description = json.dumps(description, indent=4)
        file.write(description_var + formatted_description)
        file.write("\n# End of Generated Description\n")

def _extract_parameters_from_code(function_code, function_name):
    """
    Extract parameters from the function code for a specified function name without extracting comments.
    
    Example of function code:
        def example_function(url, timeout=5):
            
    In this example, 'url' and 'timeout' are parameters extracted from the function definition.
    """
    parameters = {}
    # Split the function code into lines for analysis
    lines = function_code.split('\n')
    # Regex to find the line with the specified function's definition
    param_regex = re.compile(rf'^\s*(async\s+)?def {function_name}\((.*?)\)(\s*->\s*.+)?\s*:')
    #param_regex = re.compile(r'^\s*(async\s+)?def\s+{function_name}\s*\((.*?)\)\s*(->\s*.+)?\s*:')

    function_found = False
    for line in lines:
        # Check if the line contains the definition of the specified function
        if param_regex.search(line):
            function_found = True
            # Extract parameters from the matched group, considering both async and non-async functions
            params_str = param_regex.search(line).group(2)
            params = [param.strip() for param in params_str.split(',') if param.strip()]
            for param in params:
                param_parts = param.split('=')
                param_name = param_parts[0].strip()
                # Determine if the parameter is required
                is_required = len(param_parts) == 1
                parameters[param_name] = {"type": "string", "description": "No description", "required": is_required}
            # Break the loop after processing the specified function's parameters
            break
    if not function_found:
        raise ValueError(f"Function {function_name} not found in the provided code.")
    return parameters
    
# Main integrate_code function to create/update modules
def integrate_code(function_code, function_description, 
                   fully_qualified_main_function_name, fully_qualified_test_function_name, parameter_comments="{}",
                   bot_framework_directory="generated_modules", strategy='overwrite', dry_run=False):
    
    module_name = fully_qualified_main_function_name.split('--')[0]
    if not (os.path.isdir(bot_framework_directory)):
        logger.error("Invalid bot framework directory path.")
        return {"success": False, "error": "Invalid bot framework directory path."}

    module_path = os.path.join(bot_framework_directory, f"{module_name}.py")
    module_exists = os.path.isfile(module_path)

    if dry_run:
        dry_run_action = 'Update' if module_exists else 'Create'
        logger.info(f"Dry run: {dry_run_action} module '{module_name}' at '{module_path}'")
        return {"success": True, "message": f"Dry run: {dry_run_action} module '{module_name}' at '{module_path}'"}

    if module_exists and strategy == 'version':
        _backup_existing_module(module_path)
    
    if strategy == 'overwrite':
        # For overwrite strategy, ensure the file is empty before writing new content
        with open(module_path, 'w') as file:
            file.truncate(0)
    elif module_exists and strategy == 'merge':
        try:
            with open(module_path, 'r') as file:
                existing_code = file.read()
            function_code = _merge_module_code(existing_code, function_code)
        except Exception as e:
            logger.exception("Failed to read/merge existing module code.")
            return {"success": False, "error": "Failed to read/merge existing module code."}
    try:
        _write_code_to_file(module_path, function_code)
        module = _import_module_from_path(module_name, module_path)

        action = 'Updated' if module_exists else 'Created'
        logger.info(f"{action} module '{module_name}' at '{module_path}'")

        main_function_name = fully_qualified_main_function_name.split('--')[-1]
        # Always parse parameters from the code
        parameters_description = _extract_parameters_from_code(function_code, main_function_name)
        # Then, if parameter_comments are provided as a JSON string, update the descriptions
        parameter_comments_dict = json.loads(parameter_comments)
        for param, comment in parameter_comments_dict.items():
            if param in parameters_description:
                parameters_description[param]["description"] = comment

        tool_function_description = _create_tool_function_description(module_name, main_function_name, function_description, parameters_description)
        
        # If not a dry run, append the description to the module
        if not dry_run:
            _write_tool_function_description(module_path, main_function_name, tool_function_description)
            logger.info("Tool function description has been appended to the module.")
        
        # Run the test function before registering the main function
        test_results = None
        test_function_name = fully_qualified_test_function_name.split('--')[-1]
        test_function = getattr(module, test_function_name, None)
        if test_function:
            try:
                test_results = test_function()
            except Exception as e:
                logger.exception(f"Running test function '{test_function_name}' resulted in an error: {e}")
                return {"success": False, "error": f"Test function '{test_function_name}' error: {e}", "test_results": test_results}
        else:
            logger.error(f"Test function '{test_function_name}' not found in the module.")
            return {"success": False, "error": f"Test function '{test_function_name}' not found in the module."}

        function_pointer = getattr(module, main_function_name, None)
        if function_pointer:
            # Add a dictionary mapping from the main function name to the action function to the module file
            with open(module_path, 'a') as module_file:
                module_file.write("\n# Start of Generated Mapping\n")
                module_file.write(f"{main_function_name}_action_function_mapping = {{'{module_name}--{main_function_name}': {function_pointer.__name__}}}\n")
                module_file.write("# End of Generated Mapping\n")
            _register_function(fully_qualified_main_function_name, function_description, function_pointer, fully_qualified_test_function_name)
            logger.info(f"Function '{module_name}' has been registered successfully.")
        else:
            logger.error(f"Failed to find function '{main_function_name}' in the module for registration.")
            return {"success": False, "error": f"Failed to find function '{main_function_name}' in the module for registration."}

        return {"success": True, "message": f"Function '{module_name}' has been registered successfully.", "test_results": test_results, "reminder": "Remind the user that if they want you to be able to use this tool yourself, they need to call add_new_tools_to_bot even for this bot, as otherwise you won't be able to use it yourself."}
    except Exception as e:
        logger.exception(f"Failed to write or import module '{module_name}': {e}")
        return {"success": False, "error": f"Failed to write or import module '{module_name}': {e}"}

def _register_function(fully_qualified_function_name, function_description, function_pointer, fully_qualified_test_function_name=None):
    from bot_genesis.make_baby_bot import add_or_update_available_tool

    function_name = fully_qualified_function_name.split('--')[-1]
    logger.info(f"Registering function '{function_name}' with description: '{function_description}'.")
    if fully_qualified_test_function_name:
        test_function_name = fully_qualified_test_function_name.split('--')[-1]
        logger.info(f"Test function '{test_function_name}' was used for testing '{function_name}' successfully.")
        add_or_update_available_tool(tool_name=function_name, tool_description=function_description)
    # Placeholder for actual registration logic

def access_tool_code(fully_qualified_tool_name):
    """
    Accesses the existing code of a tool within the bot framework for review and potential improvements.

    :param fully_qualified_tool_name: The fully qualified name of the tool to be accessed.
    :return: The code of the specified tool, its module name, and bot framework directory or an error message if the tool cannot be found.
    """
    module_name = fully_qualified_tool_name.split('--')[0]
    bot_framework_directory = lookup_bot_framework_directory(module_name)
    
    if not bot_framework_directory:
        return {"success": False, "error": f"Bot framework directory for tool '{fully_qualified_tool_name}' could not be found."}
    
    try:
        module_path = os.path.join(bot_framework_directory, module_name + ".py")
        with open(module_path, 'r') as module_file:
            code = module_file.read()
            # Strip out function description and function mapping for regeneration
            code = re.sub(r'# Start of Generated Description[\s\S]*?# End of Generated Description', '', code)
            code = re.sub(r'# Start of Generated Mapping[\s\S]*?# End of Generated Mapping', '', code)
            code = code.rstrip()  # Strip off trailing blank lines
            return {
                "success": True, 
                "code": code,
                "module_name": module_name,
                "bot_framework_directory": bot_framework_directory
            }
    except FileNotFoundError:
        logger.error(f"Module '{module_name}' not found in the directory '{bot_framework_directory}'.")
        return {"success": False, "error": f"Module '{module_name}' not found in the directory '{bot_framework_directory}'."}

def lookup_bot_framework_directory(module_name):
    """
    Placeholder function to lookup the bot framework directory for a given module name.
    
    :param module_name: The name of the module.
    :return: The bot framework directory.
    """
    # This function should be implemented to return actual values based on the module_name
    return "generated_modules"


#integration_tool_descriptions = [];

integration_tool_descriptions = [
    {
        "type": "function",
        "function": {
            "name": "integrate_code",
            "description": "Dynamically integrates or updates a Python module without async functions within the bot framework, facilitating the creation or modification of modules based on a specified strategy.",
            "parameters": {
                "type": "object",
                "properties": {
                    "function_code": {
                        "type": "string",
                        "description": "The Python code to be integrated into the module. For updates, the code can be merged with existing module code based on the chosen strategy. Include the test function in the code."
                    },
                    "strategy": {
                        "type": "string",
                        "enum": ["overwrite", "merge", "version"],
                        "description": "Determines the approach for updating an existing module. Acceptable values are 'overwrite', 'merge', or 'version'. Defaults to 'overwrite'."
                    },
                    "function_description": {
                        "type": "string",
                        "description": "A brief description of the function being integrated. This is utilized for generating tool function descriptions."
                    },
                    "fully_qualified_main_function_name": {
                        "type": "string",
                        "description": "The fully qualified name of the main function within the module that will be exposed as a tool."
                    },
                    "fully_qualified_test_function_name": {
                        "type": "string",
                        "description": "The fully qualified name of a zero-parameter test function within the module to be used for testing the main function before registration."
                    },
                    "parameter_comments": {
                        "type": "string",
                        "description": "A JSON string of parameter names and their descriptions to be used for documentation. Optional."
                    }
                },
                "required": ["module_name", "function_code", "function_description", "main_function_name", "test_function_name"]
            }
        }   
    },
    {
        "type": "function",
        "function": {
            "name": "access_tool_code",
            "description": "Provides access to the existing code of a tool within the bot framework, allowing for review and potential improvements.",
            "parameters": {
                "type": "object",
                "properties": {
                    "fully_qualified_tool_name": {
                        "type": "string",
                        "description": "The fully qualified name of the tool to be accessed. This is used to look up the module name and bot framework directory."
                    }
                },
                "required": ["tool_name"]
            }
        }
    }
]


#integration_tools_old = {"integrate_code": integrate_code, "access_tool_code": access_tool_code}

#integration_tools = {"integrate_code": "integration_tool_adapter.integrate_code", "access_tool_code":"integration_tool_adapter.access_tool_code"}
integration_tools = {"integrate_code": "development.integration_tools.integrate_code", "access_tool_code":"development.integration_tools.access_tool_code"}

#integration_tools = {}
#integration_tools_new = {}


def test_code_tools(module_name, function_name):
    code = access_tool_code(f"{module_name}--{function_name}")
    if code["success"] == False:
        code = {"code":"""
def generate_pdf_report(data_source_name, report_name):\n    print(f"{data_source_name} {report_name}")
def test_generate_pdf_report():\n    generate_pdf_report("source1", "reportA")
        """}

    integrate_code(
        function_code=code["code"],
        strategy='overwrite',  # Options: 'overwrite', 'merge', 'version'
        function_description="Generates a PDF report from the specified data source and report name.",
        parameter_comments="""{
            "data_source": "The data source for generating the report.",
            "report_name": "The name of the generated report."
        }""",
        fully_qualified_main_function_name=f"{module_name}--{function_name}",
        fully_qualified_test_function_name=f"{module_name}--test_{function_name}"
    )

#test_code_tools("pdf_report_3", "generate_pdf_report")
#test_code_tools("retrieve_image_base64", "retrieve_image_base64")

