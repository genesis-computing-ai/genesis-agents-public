import os
import json
import sys
import pkgutil
import inspect
from textwrap import dedent
from genesis_bots.llm.llm_openai.openai_utils import get_openai_client
from genesis_bots.core.bot_os_llm import BotLlmEngineEnum
from genesis_bots.core.logging_config import logger


def _create_snowpark_connection(self):
    try:
        from snowflake.snowpark import Session

        connection_parameters = {
            "account": os.getenv("SNOWFLAKE_ACCOUNT_OVERRIDE"),
            "user": os.getenv("SNOWFLAKE_USER_OVERRIDE"),
            "password": os.getenv("SNOWFLAKE_PASSWORD_OVERRIDE"),
            "role": os.getenv("SNOWFLAKE_ROLE_OVERRIDE", "PUBLIC"),  # optional
            "warehouse": os.getenv(
                "SNOWFLAKE_WAREHOUSE_OVERRIDE", "XSMALL"
            ),  # optional
            "database": os.getenv(
                "SNOWFLAKE_DATABASE_OVERRIDE", "GENESIS_TEST"
            ),  # optional
            "schema": os.getenv(
                "GENESIS_INTERNAL_DB_SCHEMA", "GENESIS_TEST.GENESIS_JL"
            ),  # optional
        }

        sp_session = Session.builder.configs(connection_parameters).create()

    except Exception as e:
        logger.info(f"Cortex not available: {e}")
        sp_session = None
    return sp_session

def escallate_for_advice(self, purpose, code, result, packages):
    if True:

        if packages is None or packages == '':
            packages_list = 'No packages specified'
        else:
            packages_list = packages
        message = f"""A less smart AI bot is trying to write code to run in Snowflake Snowpark

### PURPOSE OF CODE: This is the task they are trying to accomplish:

{purpose}

### PACKAGES LIST: The bot said these non-standard python packages would be used and they were indeed successfully installed:

{packages_list}

### CODE: The bot wrote this code:

{code}

### RESULT: The result of trying to run it is:

{result}

### GENERAL SNOWPARK TIPS: Here are some general tips on how to use Snowpark in this environment:

1. If you want to access a file, first save it to stage, and then access it at its stage path, not just /tmp.
2. Be sure to return the result in the global scope at the end of your code.
3. If you want to return a file, save it to /tmp (not root) then base64 encode it and respond like this: image_bytes = base64.b64encode(image_bytes).decode('utf-8')
result = {{ 'type': 'base64file', 'filename': file_name, 'content': image_bytes, 'mime_type': <mime_type>}}.
4. Do not create a new Snowpark session, use the 'session' variable that is already available to you.
5. Use regular loops not list comprehension
6. If packages are missing, make sure they are included in the PACKAGES list. Many such as matplotlib, pandas, etc are supported.


### SNOWPARK EXAMPLE: Here is an example of successfully using Snowpark for a different task that may be helpful to you:

from snowflake.snowpark.types import StructType, StructField, StringType, IntegerType
from snowflake.snowpark.functions import udf, col

# Define the stage path
stage_file_path = '@GENESIS_BOTS.JANICE_7G8H9J_WORKSPACE.MY_STAGE/state.py'

# Create a schema for reading the CSV file
schema = StructType([
StructField("value", StringType(), True)
])

# Read the CSV file from the stage
file_df = session.read.schema(schema).option("COMPRESSION", "NONE").csv(stage_file_path)

# Define a Python function to count characters
def count_characters(text):
return len(text) if text else 0

# Register the UDF to be used in the Snowpark
count_characters_udf = udf(count_characters, return_type=IntegerType(), input_types=[StringType()])

# Apply the UDF to calculate the total number of characters
character_counts = file_df.withColumn("char_count", count_characters_udf(col("value")))

# Sum all character counts
total_chars = character_counts.agg({{"char_count": "sum"}}).collect()[0][0]

# Return the total number of characters
result = total_chars
"""

        message += """

### SNOWPARK EXAMPLE: Here is an example of successfully using Snowpark for a different task (drawing a) that may be helpful to you:

import matplotlib.pyplot as plt

# Load data from the Snowflake table into a Snowpark DataFrame
df = session.table('GENESIS_BOTS.JANICE_7G8H9J_WORKSPACE.RANDOM_TRIPLES')

# Collect the data to local for plotting
rows = df.collect()
x = [row['X'] for row in rows]
y = [row['Y'] for row in rows]
z = [row['Z'] for row in rows]

# Create bubble chart
plt.figure(figsize=(10, 6))
plt.scatter(x, y, s=[size * 10 for size in z], alpha=0.5, c=z, cmap='viridis')
plt.colorbar(label='Z Value')
plt.title('Bubble Chart of Random Triples')
plt.xlabel('X Value')
plt.ylabel('Y Value')
plt.grid(True)

# Save the chart as an image file
plt.savefig('/tmp/bubble_chart.png')

# Encode and return image
import base64
with open('/tmp/bubble_chart.png', 'rb') as image_file:
image_bytes = base64.b64encode(image_file.read()).decode('utf-8')

result = {'type': 'base64file', 'filename': 'bubble_chart.png', 'content': image_bytes}
"""

        message  +=  """

### SNOWPARK EXAMPLE: Here is an example of successfully using Snowpark for a different task (generating data and saving to a table) that may be helpful to you:

import numpy as np

# Generate random triples without the unnecessary parameter
random_triples = [{'x': int(np.random.randint(1, 1001)),
               'y': int(np.random.randint(1, 1001)),
               'z': int(np.random.randint(1, 21))} for _ in range(500)]

# Create a Snowpark DataFrame from the random triples
df = session.create_dataframe(random_triples, schema=['x', 'y', 'z'])

# Define the table name
table_name = 'GENESIS_BOTS.JANICE_7G8H9J_WORKSPACE.RANDOM_TRIPLES'

# Write the DataFrame to a Snowflake table
df.write.mode('overwrite').save_as_table(table_name)

# Return the result indicating success
result = {'message': 'Table created successfully', 'full_table_name': table_name}
"""

        if 'is not defined' in result["Error"]:
            message += """

### NOTE ON IMPORTS: If you def functions in your code, include any imports needed by the function inside the function, as the imports outside function won't convey. For example:

import math

def calc_area_of_circle(radius):
import math  # import again here as otherwise it wont work

area = math.pi * radius ** 2
return round(area, 2)

result = f'The area of a circle of radius 1 is {calc_area_of_circle(1)} using pi as {math.pi}'
"""

        if 'csv' in code:
            message += """

### SNOWPARK CSV EXAMPLE: I see you may be trying to handle CSV files. If useful here's an example way to handle CSVs in Snowpark:

from snowflake.snowpark.functions import col

stage_name = "<fully qualified location>"
file_path = "<csv file name>"

# Read the CSV file from the stage into a DataFrame
df = session.read.option("field_delimiter", ",").csv(f"@{stage_name}/{file_path}")

# Define the table name where you want to save the data
table_name = "<fully qualified output table name with your workspace database and schema specified>"

# Save the DataFrame to the specified table
df.write.mode("overwrite").save_as_table(table_name)

# Verify that the data was saved
result_df = session.table(table_name)
row_count = result_df.count()

result = f'Table {table_name} created, row_count {row_count}.  If the CSV had a header, they are in the first row of the table and can be handled with post-processing SQL to apply them as column names and then remove that row.'"""

        if 'Faker' in code or 'faker' in code:
            message += """

### SNOWPARK FAKER EXAMPLE: Here is an example of how to import and use Faker thay may be helpful to you to fix this error:
from faker import Faker

# Create fake data
fake = Faker()
data = []

# use a regular for loop, NOT list comprehension
for i in range(20):
data.append({'name': fake.name(), 'email': fake.email(), 'address': fake.address()})

# Drop existing table if it exists
session.sql('DROP TABLE IF EXISTS GENESIS_BOTS.<workspace schema here>.FAKE_CUST').collect()

# Create a new dataframe from the fake data
dataframe = session.createDataFrame(data, schema=['name', 'email', 'address'])

# Write the dataframe to the table
dataframe.write.saveAsTable('<your workspace db.schema>.FAKE_CUST', mode='overwrite')

# Set the result message
result = 'Table FAKE_CUST created successfully.'
"""

        message += """\n\n### YOUR ACTION: So, now, please provide suggestions to the bot on how to fix this code so that it runs successfully in Snowflake Snowpark.\n"""

        potential_result = self.chat_completion_for_escallation(message=message)
        # logger.info(potential_result)
        return potential_result

    else:
        return None

def add_hints(self, purpose, result, code, packages):

    if isinstance(result, str) and result.startswith('Error:'):
        result = {"Error": result}

    if isinstance(result, dict) and 'Error' in result:
        potential_result = self.escallate_for_advice(purpose, code, result, packages)
        if potential_result is not None:
            # result = potential_result
            result['Suggestion'] = potential_result
        # return potential_result

    return result

def run_python_code(self,
                    purpose: str = None,
                    code: str = None,
                    packages: str = None,
                    thread_id=None,
                    bot_id=None,
                    note_id=None,
                    note_name = None,
                    note_type = None,
                    return_base64 = False,
                    save_artifacts=False
                    ) -> str|dict:
    """
    Executes a given Python code snippet within a Snowflake Snowpark environment, handling various
    scenarios such as code retrieval from notes, package management, and result processing.

    Parameters:
    - purpose (str, optional): The intended purpose of the code execution.
    - code (str, optional): The Python code to be executed.
    - packages (str, optional): A comma-separated list of additional Python packages required.
    - thread_id: Identifier for the current thread.
    - bot_id: Identifier for the bot executing the code.
    - note_id: Identifier for the note from which to retrieve code.
    - note_name: Name of the note from which to retrieve code.
    - return_base64 (bool, optional): Whether to return results as base64 encoded content.
    - save_artifacts (bool, optional): Whether to save output as Artifacts (an arrifact_id will be included in the response)

    Returns:
    - str: The result JSON of the code execution, which may include error messages, file references,
           and/or base64 encoded content.
    """
    # IMPORTANT: keep the description/parameters of this method in sync with the tool description given to the bots (see snowflake_tools.py)

    # Some solid examples to make bots invoke this:
    # use snowpark to create 5 rows of synthetic customer data using faker, return it in json
    # ... save 100 rows of synthetic data like this to a table called CUSTFAKE1 in your workspace
    #
    # use snowpark python to generate a txt file containing the words "hello world". DO NOT save as an artifact.
    # use snowpark python to geneate a plot of the sin() function for all degrees from 0 to 180. DO NOT save as an artifact. Do not return a path to /tmp - instead, return a base64 encoded content as instrcuted in the function description
    # use snowpark python to geneate a plot of the sin() function for all degrees  from 0 to 180. Use save_artifact=True. Do not return a path to /tmp - instead, return a base64 encoded content as instrcuted in the function description
    # use snowpark python code to generate an chart that plots the result of the following query as a timeseries: query SNOWFLAKE_SAMPLE_DATA.TPCH_SF10.ORDERS table and count the number of orders per date in the last 30 available dates. use save_artifact=false

    import ast
    import os

    def cleanup(proc_name):         # Drop the temporary stored procedure if it was created
        if proc_name is not None and proc_name != 'EXECUTE_SNOWPARK_CODE':
            drop_proc_query = f"DROP PROCEDURE IF EXISTS {self.schema}.{proc_name}(STRING)"
            try:
                self.run_query(drop_proc_query)
                logger.info(f"Temporary stored procedure {proc_name} dropped successfully.")
            except Exception as e:
                logger.info(f"Error dropping temporary stored procedure {proc_name}: {e}")

    try:
        if note_id is not None or note_name is not None:
            note_name = '' if note_name is None else note_name
            note_id = '' if note_id is None else note_id
            get_note_query = f"SELECT note_content, note_params, note_type FROM {self.schema}.NOTEBOOK WHERE NOTE_ID = '{note_id}' OR NOTE_NAME = '{note_name}'"
            cursor = self.connection.cursor()
            cursor.execute(get_note_query)
            code_cursor = cursor.fetchone()

            if code_cursor is None:
                raise IndexError("Code not found for this note.")

            code = code_cursor[0]
            note_type = code_cursor[2]

            if note_type != 'snowpark_python':
                raise ValueError("Note type must be 'snowpark_python' for running python code.")
    except IndexError:
        logger.info("Error: The list 'code' is empty or does not have an element at index 0.")
        return {
                "success": False,
                "error": "Note was not found.",
                }

    except ValueError:
        logger.info("Note type must be 'snowpark_python' for code retrieval.")
        return {
                "success": False,
                "error": "Wrong tool called. Note type must be 'snowpark_python' to use this tool.",
                }

    if (bot_id not in ['eva-x1y2z3', 'Armen2-ps73td', os.getenv("O1_OVERRIDE_BOT","")]) and (bot_id is not None and not bot_id.endswith('-o1or')):
        if '\\n' in code:
            if '\n' not in code.replace('\\n', ''):
                code = code.replace('\\n','\n')
                code = code.replace('\\n','\n')
        code = code.replace("'\\\'","\'")
    # Check if code contains Session.builder
    if "Session.builder" in code:
        return {
            "success": False,
            "error": "You don't need to make a new snowpark session. Use the session already provided in the session variable without recreating it.",
            "reminder": "Also be sure to return the result in the global scope at the end of your code. "
                        "And if you want to return a file, save it to /tmp (not root) then base64 encode it and respond like this: "
                             "image_bytes = base64.b64encode(image_bytes).decode('utf-8')\nresult = { 'type': 'base64file', 'filename': file_name, 'content': image_bytes}."
        }
    if "plt.show" in code:
        return {
            "success": False,
            "error": "You can't use plt.show, instead save and return a base64 encoded file.",
            "reminder": "Also be sure to return the result in the global scope at the end of your code. "
                        "And if you want to return a file, save it to /tmp (not root) then base64 encode it and respond like this: "
                             "image_bytes = base64.b64encode(image_bytes).decode('utf-8')\nresult = { 'type': 'base64file', 'filename': file_name, 'content': image_bytes}."
        }
    if "@MY_STAGE" in code:
        from ...core import global_flags
        workspace_schema_name = f"{global_flags.project_id}.{bot_id.replace(r'[^a-zA-Z0-9]', '_').replace('-', '_')}_WORKSPACE".upper()
        code = code.replace('@MY_STAGE',f'@{workspace_schema_name}.MY_STAGE')
    if "sandbox:/mnt/data" in code:
        from ...core import global_flags
        workspace_schema_name = f"{global_flags.project_id}.{bot_id.replace(r'[^a-zA-Z0-9]', '_').replace('-', '_')}_WORKSPACE".upper()
        return {
            "success": False,
            "error": "You can't reference files in sandbox:/mnt/data, instead add them to your stage and reference them in the stage.",
            "your_stage": workspace_schema_name+".MY_STAGE",
            "reminder": "Also be sure to return the result in the global scope at the end of your code. "
                        "And if you want to return a file, save it to /tmp (not root) then base64 encode it and respond like this: "
                             "image_bytes = base64.b64encode(image_bytes).decode('utf-8')\nresult = { 'type': 'base64file', 'filename': file_name, 'content': image_bytes}."
        }
    # Check if libraries are provided
    proc_name = 'EXECUTE_SNOWPARK_CODE'
    if packages == '':
        packages = None
    if packages is not None:
        # Split the libraries string into a list
        if ' ' in packages and ',' not in packages:
            packages = packages.replace(' ', ',')
        library_list = [lib.strip() for lib in packages.split(',') if lib.strip() not in ['snowflake-snowpark-python', 'snowflake.snowpark','snowflake','base64','pandas']]
        # Remove any Python standard packages from the library_list
        standard_libs = {name for _, name, _ in pkgutil.iter_modules() if name in sys.stdlib_module_names}
        library_list = [lib for lib in library_list if lib not in standard_libs]

        # Create a new stored procedure with the specified libraries
        libraries_str = ', '.join(f"'{lib}'" for lib in library_list)
        import uuid
        # 'matplotlib', 'scikit-learn'
        if (libraries_str is None or libraries_str != ''):
            proc_name = f"sp_{uuid.uuid4().hex}"
            old_new_stored_proc_ddl = dedent(f"""
                CREATE OR REPLACE PROCEDURE {self.schema}.{proc_name}(
                    code STRING
                )
                RETURNS STRING
                LANGUAGE PYTHON
                RUNTIME_VERSION = '3.'
                PACKAGES = ('snowflake-snowpark-python', 'pandas', {libraries_str})
                HANDLER = 'run'
                AS
                $$
                import snowflake.snowpark as snowpark
                import pandas as pd

                def run(session: snowpark.Session, code: str) -> str:
                    local_vars = {{}}
                    local_vars["session"] = session

                    exec(code, globals(), local_vars)

                    if 'result' in local_vars:
                        return str(local_vars['result'])
                    else:
                        return "Error: 'result' is not defined in the executed code"
                $$;""")

            new_stored_proc_ddl = dedent(f"""
                CREATE OR REPLACE PROCEDURE {self.schema}.{proc_name}( code STRING )
                RETURNS STRING
                LANGUAGE PYTHON
                RUNTIME_VERSION = '3.11'
                PACKAGES = ('snowflake-snowpark-python', 'pandas', {libraries_str})
                HANDLER = 'run'
                AS
                $$
                import snowflake.snowpark as snowpark
                import re, importlib

                def run(session: snowpark.Session, code: str) -> str:
                    # Normalize line endings
                    code = code.replace('\\\\r\\\\n', '\\\\n').replace('\\\\r', '\\\\n')

                    # Find all import statements, including 'from ... import ...'
                    import_statements = re.findall(r'^\\s*(import\\s+.*|from\\s+.*\\s+import\\s+.*)$', code, re.MULTILINE)
                    # Additional regex to find 'from ... import ... as ...' statements
                    import_statements += re.findall(r'^from\\s+(\\S+)\\s+import\\s+(\\S+)\\s+as\\s+(\\S+)', code, re.MULTILINE)

                    global_vars = globals().copy()

                    # Handle imports
                    for import_statement in import_statements:
                        try:
                            exec(import_statement, global_vars)
                        except ImportError as e:
                            return f"Error: Unable to import - {{str(e)}}"

                    local_vars = {{}}
                    local_vars["session"] = local_vars["session"] = session

                    try:
                        # Remove import statements from the code before execution
                        code_without_imports = re.sub(r'^\\s*(import\\s+.*|from\\s+.*\\s+import\\s+.*)$', '', code, flags=re.MULTILINE)
                        exec(code_without_imports, global_vars, local_vars)

                        if 'result' in local_vars:
                            return local_vars['result']
                        else:
                            return "Error: 'result' is not defined in the executed code"
                    except Exception as e:
                        return f"Error: {{str(e)}}"
                $$
                """)
            # Execute the new stored procedure creation
            result = self.run_query(new_stored_proc_ddl)

            # Check if the result is a list and if Success is False
            if isinstance(result, dict) and 'Success' in result and result['Success'] == False:
                result['reminder'] = 'You do not need to specify standard python packages in the packages parameter'
                return result

            # Update the stored procedure call to use the new procedure
            stored_proc_call = f"CALL {self.schema}.{proc_name}($${code}$$)"
        else:
            stored_proc_call = f"CALL {self.schema}.execute_snowpark_code($${code}$$)"

    else:
        # Use the default stored procedure if no libraries are specified
        stored_proc_call = f"CALL {self.schema}.execute_snowpark_code($${code}$$)"

    result = self.run_query(stored_proc_call)

    if isinstance(result, list):
        result_json = result
        # Check if result is a list and has at least one element
        if isinstance(result, list) and len(result) > 0:
            # Check if 'EXECUTE_SNOWPARK_CODE' key exists in the first element
            proc_name = proc_name.upper()
            if proc_name in result[0]:
                # If it exists, use its value as the result
                result = result[0][proc_name]
                # Try to parse the result as JSON
                try:
                    result_json = ast.literal_eval(result)
                except Exception as e:
                    # If it's not valid JSON, keep the original string
                    cleanup(proc_name)
                    result_json = result
            else:
                # If 'EXECUTE_SNOWPARK_CODE' doesn't exist, use the entire result as is
                cleanup(proc_name)
                result_json = result
        else:
            # If result is not a list or is empty, use it as is
            cleanup(proc_name)
            result_json = result

        # Check if 'type' and 'filename' are in the JSON
        if isinstance(result_json, dict) and 'type' in result_json and 'filename' in result_json:
            mime_type = result_json.get("mime_type") # may be missing
            if result_json['type'] == 'base64file':
                import base64
                import os

                # Create the directory if it doesn't exist
                os.makedirs(f'./runtime/downloaded_files/{thread_id}', exist_ok=True)

                # Decode the base64 content
                file_content = base64.b64decode(result_json['content'])

                if save_artifacts:
                    # Use the artifacts infra to create an artifact from this content
                    from core.bot_os_artifacts import get_artifacts_store
                    af = get_artifacts_store(self)
                    # Build the metadata for this artifact
                    mime_type = mime_type or 'image/png' # right now we assume png is the defualt for type=base64file
                    metadata = dict(mime_type=mime_type,
                                    thread_id=thread_id,
                                    bot_id=bot_id,
                                    title_filename=result_json["filename"],
                                    func_name=inspect.currentframe().f_code.co_name,
                                    )
                    locl = locals()
                    for inp_field, m_field in (('purpose', 'short_description'),
                                               ('code', 'python_code'),
                                               ('note_id', 'note_id'),
                                               ('note_name', 'note_name'),
                                               ('note_type', 'note_type')):
                        v = locl.get(inp_field)
                        if v:
                            metadata[m_field] = v

                    # Create artifact
                    aid = af.create_artifact_from_content(file_content, metadata, content_filename=result_json["filename"])
                    logger.info(f"Artifact {aid} created for output from python code named {result_json['filename']}")
                    ref_notes = af.get_llm_artifact_ref_instructions(aid)
                    result = {
                        "success": True,
                        "result": f"Output from snowpark is an artifact, which can be later refernced using artifact_id={aid}. "
                                  f"The descriptive name of the file is `{result_json['filename']}`. "
                                  f"The mime type of the file is {mime_type}. "
                                  f"Note: {ref_notes}"
                    }
                else:
                    # Save the file to 'sandbox'
                    file_path = f'./runtime/downloaded_files/{thread_id}/{result_json["filename"]}'
                    with open(file_path, 'wb') as file:
                        file.write(file_content)
                    logger.info(f"File saved to {file_path}")
                    if return_base64:
                        result = {
                            "success": True,
                            "base64_object": {
                                "filename": result_json["filename"],
                                "content": result_json["content"]
                            },
                            "result": "An image or graph has been successfully displayed to the user."
                        }
                    else:
                        result = {
                            "success": True,
                            #"result": f'Snowpark output a file. Output a link like this so the user can see it [description of file](sandbox:/mnt/data/{result_json["filename"]})'
                            "result": f"Output from snowpark is a file. "
                                      f"The descriptive name of the file is `{result_json['filename']}`. "
                                      f"Output a link to this file so the user can see it, using the following formatting rules:"
                                      f" (i) If responding to the user in plain text mode, use markdown like this: '[descriptive name of the file](sandbox:/mnt/data/{result_json['filename']})'. "
                                      f" (ii) If responding to the user in HTML mode, use the most relevant HTML tag to refrence this resource using the url 'sandbox:/mnt/data/{result_json['filename']}' "
                            }
                cleanup(proc_name)
                if (bot_id not in ['eva-x1y2z3','Armen2-ps73td',  os.getenv("O1_OVERRIDE_BOT","")]) and (bot_id is not None and not bot_id.endswith('-o1or')):
                    result = self.add_hints(purpose, result, code, packages)
                return result

            # If conditions are not met, return the original result
            if (bot_id not in ['eva-x1y2z3', 'Armen2-ps73td', os.getenv("O1_OVERRIDE_BOT","")]) and (bot_id is not None and not bot_id.endswith('-o1or')):
                result_json = self.add_hints(purpose, result_json, code, packages)
            cleanup(proc_name)
            return result_json

        cleanup(proc_name)
        if (bot_id not in ['eva-x1y2z3','Armen2-ps73td', os.getenv("O1_OVERRIDE_BOT","")]) and (bot_id is not None and not bot_id.endswith('-o1or')):

            result_json = self.add_hints(purpose, result_json, code, packages)
        return result_json

    # Check if result is a dictionary and contains 'Error'

    cleanup(proc_name)
    if (bot_id not in ['eva-x1y2z3', 'Armen2-ps73td', os.getenv("O1_OVERRIDE_BOT","")]) and (bot_id is not None and not bot_id.endswith('-o1or')):
        result = self.add_hints(purpose, result, code, packages)
    return result

def chat_completion_for_escallation(self, message):
    # self.write_message_log_row(db_adapter, bot_id, bot_name, thread_id, 'Supervisor Prompt', message, message_metadata)
    return_msg = None
    default_env_override = os.getenv("BOT_OS_DEFAULT_LLM_ENGINE")
    bot_os_llm_engine = BotLlmEngineEnum(default_env_override) if default_env_override else None
    if bot_os_llm_engine is BotLlmEngineEnum.openai:
        api_key = os.getenv("OPENAI_API_KEY")
        if not api_key:
            logger.info("OpenAI API key is not set in the environment variables.")
            return None

        openai_model = os.getenv("OPENAI_MODEL_SUPERVISOR",os.getenv("OPENAI_MODEL_NAME","gpt-4o-2024-11-20"))

        logger.info('snowpark escallation using model: ', openai_model)
        try:
            client = get_openai_client()
            response = client.chat.completions.create(
                model=openai_model,
                messages=[
                    {
                        "role": "user",
                        "content": message,
                    },
                ],
            )
        except Exception as e:
            if os.getenv("OPENAI_MODEL_SUPERVISOR", None) is not None:
                openai_model = os.getenv("OPENAI_MODEL_NAME","gpt-4o-2024-11-20")
                logger.info('retry snowpark escallation using model: ', openai_model)
                try:
                    client = get_openai_client()
                    response = client.chat.completions.create(
                        model=openai_model,
                        messages=[
                            {
                                "role": "user",
                                "content": message,
                            },
                        ],
                    )
                except Exception as e:
                    logger.info(f"Error occurred while calling OpenAI API with snowpark escallation model {openai_model}: {e}")
                    return None
            else:
                logger.info(f"Error occurred while calling OpenAI API: {e}")
                return None

        return_msg = response.choices[0].message.content
    else:
        if bot_os_llm_engine is BotLlmEngineEnum.cortex:
            response, status_code = self.cortex_chat_completion(message)
            if status_code != 200:
                logger.info(f"Error occurred while calling Cortex API: {response}")
                return None
            return_msg = response

    return return_msg

def check_eai_assigned(self):
    """
    Retrieves the eai list if set.

    Returns:
        list: An eai list, if set.
    """
    try:
        show_query = f"SHOW SERVICES IN SCHEMA {self.schema}"
        cursor = self.client.cursor()
        cursor.execute(show_query)

        query = f"""SELECT DISTINCT UPPER("external_access_integrations") EAI_LIST FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())) LIMIT 1"""
        cursor = self.client.cursor()
        cursor.execute(query)
        eai_info = cursor.fetchone()

        # Ensure eai_info is not None
        if eai_info:
            columns = [col[0].lower() for col in cursor.description]
            eai_list = [dict(zip(columns, eai_info))]  # Wrap eai_info in a list since fetchone returns a single row
            json_data = json.dumps(eai_list)
        else:
            json_data = json.dumps([])  # Return an empty list if no results

        return {"Success": True, "Data": json_data}

    except Exception as e:
        err = f"An error occurred while getting email address: {e}"
        return {"Success": False, "Error": err}

def get_endpoints(self, type):
    """
    Retrieves a list of all custom endpoints.

    Returns:
        list: A list of custom endpionts.
    """
    try:
        if type == 'ALL':
            query = f"""
                SELECT LISTAGG(ENDPOINT, ', ') WITHIN GROUP (ORDER BY ENDPOINT) AS ENDPOINTS, GROUP_NAME
                FROM {self.genbot_internal_project_and_schema}.CUSTOM_ENDPOINTS
                GROUP BY GROUP_NAME
                ORDER BY GROUP_NAME
            """
        else:
            query = f"""
                SELECT LISTAGG(ENDPOINT, ', ') WITHIN GROUP (ORDER BY ENDPOINT) AS ENDPOINTS, GROUP_NAME
                FROM {self.genbot_internal_project_and_schema}.CUSTOM_ENDPOINTS
                WHERE TYPE = '{type}'
                GROUP BY GROUP_NAME
                ORDER BY GROUP_NAME
            """

        cursor = self.client.cursor()
        cursor.execute(query)
        endpoints = cursor.fetchall()
        columns = [col[0].lower() for col in cursor.description]
        endpoint_list = [dict(zip(columns, endpoint)) for endpoint in endpoints]
        json_data = json.dumps(
            endpoint_list, default=str
        )

        return {"Success": True, "Data": json_data}

    except Exception as e:
        err = f"An error occurred while getting llm info: {e}"
        return {"Success": False, "Error": err}

def delete_endpoint_group(self, group_name):
    try:
        delete_query = f"""DELETE FROM {self.genbot_internal_project_and_schema}.CUSTOM_ENDPOINTS WHERE GROUP_NAME = %s;"""
        cursor = self.client.cursor()
        cursor.execute(delete_query, (group_name,))

        # Commit the changes
        self.client.commit()

        json_data = json.dumps([{'Success': True}])
        return {"Success": True, "Data": json_data}
    except Exception as e:
        err = f"An error occurred while deleting custom endpoint: {e}"
        return {"Success": False, "Data": err}

def set_endpoint(self, group_name, endpoint_name, type):
    try:
        insert_query = f"""INSERT INTO {self.genbot_internal_project_and_schema}.CUSTOM_ENDPOINTS (GROUP_NAME, ENDPOINT, TYPE)
        SELECT %s AS group_name, %s AS endpoint, %s AS type
        WHERE NOT EXISTS (
            SELECT 1
            FROM {self.genbot_internal_project_and_schema}.CUSTOM_ENDPOINTS
            WHERE GROUP_NAME = %s
            AND ENDPOINT = %s
            AND TYPE = %s
        );"""
        cursor = self.client.cursor()
        cursor.execute(insert_query, (group_name, endpoint_name, type, group_name, endpoint_name, type,))

        # Commit the changes
        self.client.commit()

        json_data = json.dumps([{'Success': True}])
        return {"Success": True, "Data": json_data}
    except Exception as e:
        err = f"An error occurred while inserting custom endpoint: {e}"
        return {"Success": False, "Data": err}

def eai_test(self, site):
    try:
        azure_endpoint = "https://example.com"
        eai_list_query = f"""CALL CORE.GET_EAI_LIST('{self.schema}')"""
        cursor = self.client.cursor()
        cursor.execute(eai_list_query)
        eai_list = cursor.fetchone()
        if not eai_list:
            return {"Success": False, "Error": "Cannot check EAI status. No EAI set up."}
        else:

            if site == "azureopenai":
                azure_query = f"""
                    SELECT LLM_ENDPOINT
                    FROM {self.genbot_internal_project_and_schema}.LLM_TOKENS
                    WHERE UPPER(LLM_TYPE) = 'OPENAI'"""
                cursor = self.client.cursor()
                cursor.execute(azure_query)
                azure_endpoint = cursor.fetchone()
                if azure_endpoint is None or azure_endpoint == '':
                    azure_endpoint = "https://example.com"

            create_function_query = f"""
CREATE OR REPLACE FUNCTION {self.project_id}.CORE.CHECK_URL_STATUS(site string)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = 3.11
HANDLER = 'get_status'
EXTERNAL_ACCESS_INTEGRATIONS = ({eai_list[0]})
PACKAGES = ('requests')
AS
$$
import requests

def get_status(site):
check_command = "options"

if site == 'slack':
    url = "https://slack.com"  # Replace with the allowed URL
elif site == 'openai':
    url = "https://api.openai.com/v1/models"  # Replace with the allowed URL
elif site == 'google':
    url = "https://accounts.google.com"  # Replace with the allowed URL
    check_command = "put"
elif site == 'jira':
    url = "https://www.atlassian.net/jira/your-work"  # Replace with the allowed URL
elif site == 'serper':
    url = "https://google.serper.dev"  # Replace with the allowed URL
elif site == 'azureopenai':
    url = "{azure_endpoint}"  # Replace with the allowed URL
else:
    # TODO allow custom endpoints to be tested
    return f"Invalid site: {{site}}"

try:
    # Make an HTTP GET request to the allowed URL
    # response = requests.get(url, timeout=10)
    if check_command == "options":
        response = requests.options(url)
    else:
        response = requests.put(url)
    if response.ok or response.status_code == 302:   # alternatively you can use response.status_code == 200
        result = "Success"
    else:
        result = f"Failure"
except (requests.exceptions.HTTPError, requests.exceptions.ConnectionError) as e:
    result = f"Failure - Unable to establish connection: {{e}}."
except Exception as e:
    result = f"Failure - Unknown error occurred: {{e}}."

return result
$$;
            """
            try:
                function_success = False
                cursor = self.client.cursor()
                cursor.execute(create_function_query)

                if site:
                    function_test_success = False
                    select_query = f"select {self.project_id}.CORE.CHECK_URL_STATUS('{site}')"
                    cursor.execute(select_query)
                    eai_test_result = cursor.fetchone()

                    if 'Success' in eai_test_result:
                        function_test_success = True
            except Exception as e:
                logger.info(f"An error occurred while creating/testing EAI test function: {e}")
                function_test_success = True

            # check for existing EAI assigned to services
            show_query = f"show services in application {self.project_id}"
            cursor.execute(show_query)
            check_eai_query = """
                                SELECT f.VALUE::string FROM table(result_scan(-1)) a,
                                LATERAL FLATTEN(input => parse_json(a."external_access_integrations")) AS f
                                WHERE "name" = 'GENESISAPP_SERVICE_SERVICE';
                            """

            cursor.execute(check_eai_query)
            check_eai_result = cursor.fetchone()

            if check_eai_result:
                function_success = True

    except Exception as e:
        err = f"An error occurred while creating/testing EAI test function: {e}"
        return {"Success": False, "Error": err}

    if function_success == True and function_test_success == True:
        json_data = json.dumps([{'Success': True}])
        return {"Success": True, "Data": json_data}
    else:
        return {"Success": False, "Error": "EAI test failed or EAI not assigned to Genesis"}

def db_get_endpoint_ingress_url(self, endpoint_name: str) -> str:
    """
    Retrieves the ingress URL for a specified endpoint registered within the Genesis (native) App service.
    Call this method only when running in Native app mode.

    Args:
        endpoint_name (str, optional): The name of the endpoint to retrieve the ingress URL for. Defaults to None.

    Returns:
        str or None: The ingress URL of the specified endpoint if found, otherwise None.
    """
    alt_service_name = os.getenv("ALT_SERVICE_NAME", None)
    if alt_service_name:
        query1 = f"SHOW ENDPOINTS IN SERVICE {alt_service_name};"
    else:
        query1 = f"SHOW ENDPOINTS IN SERVICE {self.genbot_internal_project_and_schema}.GENESISAPP_SERVICE_SERVICE;"
    try:
        results = self.run_query(query1)
        udf_endpoint_url = None
        for endpoint in results:
            if endpoint["NAME"] == endpoint_name:
                udf_endpoint_url = endpoint["INGRESS_URL"]
                break
        return udf_endpoint_url
    except Exception as e:
        logger.warning(f"Failed to get {endpoint_name} endpoint URL with error: {e}")
        return None

