import uuid
import json
import yaml
import argparse
from   genesis_bots.api         import GenesisAPI, build_server_proxy
from   genesis_bots.api.utils   import add_default_argparse_options
import os
import time
import sys
from contextlib import contextmanager
import base64

eve_bot_id = 'Eve'
genesis_api_client = None
connection_id = "Snowflake"

if os.environ.get('GENESIS_API_USE_O1','FALSE').upper() == 'TRUE':
    message_prefix = '!o1!'  # Force use of o1 model
    print("-> Using o1 model")
else:
    message_prefix = '!o3-mini!'
    print("-> Using o3-mini model")

print("-> Message prefix: ", message_prefix)

class Tee:
    def __init__(self, *files):
        self.files = files

    def write(self, obj):
        for f in self.files:
            f.write(obj)
            f.flush()  # If you want the output to be visible immediately

    def flush(self):
        for f in self.files:
            f.flush()

@contextmanager
def stdout_redirector(*files):
    original_stdout = sys.stdout
    sys.stdout = Tee(*files)
    try:
        yield
    finally:
        sys.stdout = original_stdout

def print_file_contents(title, file_path, contents):
    """
    Print file contents with formatted headers and separators.
    Args:
        title: Title to display above contents
        file_path: Path to file being displayed
        contents: String contents to print
    """
    print("\033[35m" + "-"*80)
    print(f"{title} from {file_path}:")
    print("-"*80)
    print(contents)
    print("-"*80 + "\033[0m")

def call_genesis_bot(client, bot_id, request, thread = None):
    """Wait for a complete response from the bot, streaming partial results."""
    try:
        print(f"\n\033[94m{'='*80}\033[0m")  # Blue separators
        print(f"\033[92mBot:\033[0m {bot_id}")  # Green label
        print(f"\033[92mPrompt:\033[0m {request}")  # Green label
        print(f"\033[94m{'-'*80}\033[0m")  # Blue separator
        print("\033[92mResponse:\033[0m")  # Green label for response section

        request = client.submit_message(bot_id, request, thread_id=thread)
        response = client.get_response(bot_id, request["request_id"], print_stream=True)

        print(f"\n\033[94m{'-'*80}\033[0m")  # Blue separator
        print("\033[93mResponse complete\033[0m")  # Yellow status
        print(f"\033[94m{'='*80}\033[0m\n")  # Blue separator
        return response
    except Exception as e:
        raise e

def setup_paths(physical_column_name, run_number = 1, genesis_db = 'GENESIS_BOTS', project_id=None):
    """Setup file paths and names for a given requirement."""
    stage_base = f"@{genesis_db}.APP1.bot_git/"
    base_git_path = f'{project_id}/requirements/run{run_number}/'

    return {
        'stage_base': stage_base,
        'base_git_path': base_git_path,
        'source_research_file': f"{physical_column_name}__source_research.txt",
        'mapping_proposal_file': f"{physical_column_name}__mapping_proposal.txt",
        'confidence_report_file': f"{physical_column_name}__confidence_report.txt"
    }

def check_git_file(client, paths, file_name, bot_id):
    """
    Check if file exists in local git or stage, copy to stage if needed.

    Args:
        paths (dict): Dictionary containing path information
        file_name (str): Name of file to check
    Returns:
        str: Contents of the file if found, False if not found
    """

    res = client.gitfiles.read(f"{paths['base_git_path']}{file_name}", bot_id=bot_id)

    return res or False
 
def put_content_into_git_file(client, content, git_file_path, file_name, bot_id):
    """
    Write content directly to a git file.
    
    Args:
        client: Genesis API client instance
        content (str): Content to write to git file
        git_file_path (str): Git directory path to write to
        file_name (str): Name of file to create in git
        bot_id (str): Bot ID to use for git operations
        
    Returns:
        bool: True if successful, False if failed
    """
    try:
        res = client.gitfiles.write(
            f"{git_file_path}{file_name}",
            content,
            bot_id=bot_id
        )
        return res
        
    except Exception as e:
        print(f"Error putting content to git: {str(e)}")
        return False

 
def put_git_file(client, local_file, git_file_path, file_name, bot_id):
    """
    Read a local file and write its contents to git.
    
    Args:
        client: Genesis API client instance
        local_file (str): Path to local file to read
        git_file_path (str): Git directory path to write to
        file_name (str): Name of file to create in git
        bot_id (str): Bot ID to use for git operations
        
    Returns:
        bool: True if successful, False if failed
    """
    try:
        # Try reading as text first
        try:
            is_binary = False
            with open(local_file, 'r') as f:
                content = f.read()
        except UnicodeDecodeError:
            # If text read fails, read as binary and encode as base64
            with open(local_file, 'rb') as f:
                binary_content = f.read()
                content = base64.b64encode(binary_content).decode('utf-8')
                print(f"Reading {local_file} as binary file and encoding as base64")
                is_binary = True
            
        res = client.gitfiles.write(
            f"{git_file_path}{file_name}", 
            content,
            bot_id=bot_id,
            adtl_info={"is_base64": is_binary}
        )
        return res
        
    except Exception as e:
        print(f"Error putting file to git: {str(e)}")
        return False



def perform_pm_summary(client, requirement, paths, bot_id, skip_confidence = False):
    """Have PM bot analyze results and provide structured summary."""
    print("\033[34mGetting PM summary...\033[0m")

    pm_prompt = f'''{message_prefix} Here are requirements for a target field I want you to work on: {requirement}\n
    The mapping proposer bot has saved its results in git at: {paths["base_git_path"]}{paths["mapping_proposal_file"]}\n'''

    if not skip_confidence:
        pm_prompt += f'''The confidence analyst bot has saved its results in git at: {paths["base_git_path"]}{paths["confidence_report_file"]}\n'''

    pm_prompt += f'''
    Use _git_action to retrieve and review the files mentioned above in git, for example:
        git_action(action='read_file',file_path='{paths["base_git_path"]}{paths["mapping_proposal_file"]}')

    Then:

    Based on your review above documents, provide a JSON response with the following fields:
    - UPSTREAM_DB_CONNECTION: The database connection ID where the source data resides (e.g. 'Snowflake', 'my_databricks', etc.)
    - UPSTREAM_TABLE: List of source table(s) needed for the data, with schema prefixes
    - UPSTREAM_COLUMN: List of source column(s) for the mapping
    - TRANSFORMATION_LOGIC: SQL snippet or natural language description of any needed transformations
    - CONFIDENCE_SCORE: Mapping proposers confidence in the mapping, HIGH or LOW.

    Format as valid JSON with these exact field names between !!JSON_START!! and !!JSON_END!! tags. Use git_action to retrieve the files if needed.
    This is being run by an automated process so do not repeat these instructions back to me, and do not stop to ask for futher permission to proceed.
    '''

    thread = uuid.uuid4()
    response = call_genesis_bot(client, bot_id, pm_prompt, thread=thread)

    # Extract JSON between tags
    if "!!JSON_START!!" not in response or "!!JSON_END!!" not in response:
        raise Exception("PM bot response missing JSON tags")

    json_str = response.split("!!JSON_START!!")[1].split("!!JSON_END!!")[0].strip()
    response = json_str
    # Basic validation that we got JSON back
    try:
        summary = json.loads(response)
        required_fields = ["UPSTREAM_DB_CONNECTION", "UPSTREAM_TABLE", "UPSTREAM_COLUMN", "TRANSFORMATION_LOGIC",
                         "CONFIDENCE_SCORE"]
        for field in required_fields:
            if field not in summary:
                raise Exception(f"Missing required field {field} in PM summary")
    except json.JSONDecodeError:
        raise Exception("PM bot did not return valid JSON")

    print("\033[93mPM Summary:\033[0m")
    print("\033[96m" + "-"*80 + "\033[0m")  # Cyan separator
    print(json.dumps(summary, indent=4, sort_keys=True, ensure_ascii=False))
    print("\033[96m" + "-"*80 + "\033[0m")  # Cyan separator
    return {
        'success': True,
        'summary': summary,
        'thread': thread
    }


def save_pm_summary_to_requirements(physical_column_name, summary_fields, table_name):
    """
    Updates the requirements table with PM summary fields and sets status to READY_FOR_REVIEW

    Args:
        physical_column_name (str): The column name to identify the requirement
        summary_fields (dict): Dictionary containing the fields to update
        table_name (str): Fully qualified table name (schema.table)
    """

    # Flatten any array fields into comma-separated strings
    flattened_fields = {}
    for key, value in summary_fields.items():
        if isinstance(value, (list, tuple)):
            # Remove any quotes and join with commas
            cleaned_values = [str(v).replace('"', '').replace("'", '') for v in value]
            flattened_fields[key.lower()] = ", ".join(cleaned_values)
        else:
            # Remove any quotes from single values
            flattened_fields[key.lower()] = str(value).replace('"', '').replace("'", '')
    try:
        update_query = f"""
            UPDATE {table_name}
            SET
                upstream_db_connection = %(upstream_db_connection)s,
                upstream_table = %(upstream_table)s,
                upstream_column = %(upstream_column)s,
                source_research = %(source_research)s,
                mapping_proposal = %(mapping_proposal)s,
                confidence_output = %(confidence_output)s,
                confidence_score = %(confidence_score)s,
                transformation_logic = %(transformation_logic)s,
                status = %(status)s,
                mapping_correct_flag = %(mapping_correct_flag)s,
                mapping_issues = %(mapping_issues)s,
                questions = %(questions)s,
                proposal_made = %(proposal_made)s
            WHERE physical_column_name = %(physical_column_name)s
        """

        params = {
            **flattened_fields,  # Use the flattened fields instead of raw summary_fields
            'physical_column_name': physical_column_name
        }

        # Flatten params into query string
        query_with_params = update_query
        for key, value in params.items():
            if value is None:
                query_with_params = query_with_params.replace(f'%({key})s', 'NULL')
            else:
                # Escape single quotes in values
               # escaped_value = str(value).replace("'", "''")
                query_with_params = query_with_params.replace(f'%({key})s', f"$${str(value)}$$")

        run_snowflake_query(genesis_api_client, query_with_params, eve_bot_id)
    
    except Exception as e:
        raise Exception(f"Failed to update requirement for column {physical_column_name}: {str(e)}")


def update_single_gsheet_cell(client, g_file_id, cell_location, value, pm_bot_id):
    """
    Update a single cell in a Google Sheet.
    
    Args:
        client: Genesis API client instance
        g_file_id: ID of the Google Sheet
        cell_location: Cell location (e.g. 'A1', 'B2')
        value: Value to write to the cell
        pm_bot_id: Bot ID to use for the operation
        
    Returns:
        Result of the update operation
    """
    try:
        # Convert value to string if it's not a basic type that can go in a sheet cell
        if not isinstance(value, (str, int, float, bool)):
            value = str(value)
        
        # Prepend a single quote if the value starts with '='
        if isinstance(value, str) and value.startswith('='):
            value = "'" + value

        result = client.run_genesis_tool(
            tool_name="google_drive",
            params={
                "action": "EDIT_SHEET",
                "g_file_id": g_file_id,
                "g_sheet_cell": cell_location,
                "g_sheet_values": value
            },
            bot_id=pm_bot_id
        )

        if result.get("Success"):
            display_value = str(value)[:200] + "..." if len(str(value)) > 200 else value
            print(f"\033[92mSuccessfully updated cell {cell_location} with value: {display_value}\033[0m")
        else:
            print(f"\033[91mFailed to update cell: {result.get('error')}\033[0m")
            
        return result
        
    except Exception as e:
        print(f"\033[91mError updating cell: {e}\033[0m")
        raise e


def update_gsheet_with_mapping(client, filtered_requirement, summary, gsheet_location, pm_bot_id, source_research_content, mapping_proposal_content, confidence_output, mapping_correct_flag, mapping_issues, questions, proposal_made):
    """
    Update a Google Sheet with mapping results.
    
    Args:
        client: Genesis API client instance
        filtered_requirement: Dictionary containing requirement details
        summary: Dictionary containing mapping summary fields (UPSTREAM_DB_CONNECTION, UPSTREAM_TABLE, UPSTREAM_COLUMN, etc)
        gsheet_location: Google Sheet URL or ID
        pm_bot_id: Bot ID to use for the operation
        source_research_content: Source research content
        mapping_proposal_content: Mapping proposal content
        confidence_output: Confidence analysis output
        
    Returns:
        None
    """
    try:

        # Parse Google Sheet ID from URL if a URL is provided
        if 'google.com' in gsheet_location:
            # Extract the ID from URLs like:
            # https://docs.google.com/spreadsheets/d/FILE_ID/edit#gid=0
            gsheet_location = gsheet_location.split('/d/')[1].split('/')[0]
 
        # Get column A to find row number
        result = client.run_genesis_tool(
            tool_name="google_drive",
            params={
                "action": "GET_SHEET",
                "g_file_id": gsheet_location,
                "g_sheet_cell": "A1:ZZ1"
            },
            bot_id=pm_bot_id
        )

        # Map column names to their indices based on first row headers
        column_headers = result['value']['cell_values'][0]
       
        # Create a map of column names to their column letters
        column_map = {}
        for idx, header in enumerate(column_headers):
            if idx < 26:
                col_letter = chr(65 + idx)  # A-Z
            else:
                # For columns beyond Z (AA, AB, etc)
                first_letter = chr(65 + (idx // 26) - 1)
                second_letter = chr(65 + (idx % 26))
                col_letter = first_letter + second_letter
            column_map[header] = col_letter
            
        col_letter = column_map['PHYSICAL_COLUMN_NAME']

        result = client.run_genesis_tool(
            tool_name="google_drive", 
            params={
                "action": "GET_SHEET",
                "g_file_id": gsheet_location,
                "g_sheet_cell": f"{col_letter}2:{col_letter}1000" # Search column for value
            },
            bot_id=pm_bot_id
        )

        # Find the row number for the physical column name that is being updated
        row_number = None
        i = 2
        for row in result['value']['cell_values']:
            if row[0] == filtered_requirement['PHYSICAL_COLUMN_NAME']:
                row_number = i
                break
            i += 1

        print(f"Row number: {row_number}")

        fields_to_update = [
            {'column': 'UPSTREAM_DB_CONNECTION', 'value': summary['UPSTREAM_DB_CONNECTION']},
            {'column': 'UPSTREAM_TABLE', 'value': summary['UPSTREAM_TABLE']}, 
            {'column': 'UPSTREAM_COLUMN', 'value': summary['UPSTREAM_COLUMN']},
            {'column': 'SOURCE_RESEARCH', 'value': source_research_content},
            {'column': 'MAPPING_PROPOSAL', 'value': mapping_proposal_content},
            {'column': 'CONFIDENCE_OUTPUT', 'value': confidence_output},
            {'column': 'CONFIDENCE_SCORE', 'value': summary['CONFIDENCE_SCORE']},
            {'column': 'TRANSFORMATION_LOGIC', 'value': summary['TRANSFORMATION_LOGIC']},
            {'column': 'STATUS', 'value': 'READY_FOR_REVIEW' if proposal_made else 'QUESTIONS_POSED'},
            {'column': 'MAPPING_CORRECT_FLAG', 'value': mapping_correct_flag},
            {'column': 'MAPPING_ISSUES', 'value': mapping_issues},
            {'column': 'QUESTIONS', 'value': questions},
            {'column': 'PROPOSAL_MADE', 'value': proposal_made}
        ]

        # Loop through fields and update each one
        for field in fields_to_update:
            col_letter = column_map[field['column']]
            cell = f"{col_letter}{row_number}"
            
            update_single_gsheet_cell(
                client=client,
                g_file_id=gsheet_location, 
                cell_location=cell,
                value=field['value'],
                pm_bot_id=pm_bot_id
            )

        return
    
    except Exception as e:
        print(f"\033[31mError updating Google Sheet: {e}\033[0m")
        raise e

def evaluate_results(client, filtered_requirement=None, pm_bot_id=None, mapping_proposal_content=None, correct_answer_for_eval=None, deng_project_config=None):
    """
    Evaluate the mapping results against known correct answers.
    """
    try:
        correct_answer = correct_answer_for_eval
        hint = deng_project_config.get('evaluation_hint', '') if deng_project_config else ''

        message = f'''{message_prefix}
Here is the requirement I want you to evaluate:
{filtered_requirement}

Here is the mapping proposal to evaluate:
{mapping_proposal_content}

Here is the correct answer to compare against:
{correct_answer}

Please evaluate the mapping proposal results against the correct answer for this field.

Compare the following aspects:
1. Is the mapping correct?
2. Are the identified source tables/columns correct?
3. Is the transformation logic correct?

Note that the correct answer may contain extra commentary, and references to specific CTEs.  It is not required that the mapping proposal
incorporate these elements exactly, but that it gets the correct source table(s) and transformation logic.

Provide a detailed analysis of any discrepancies found and identify likely sources of errors in the process.

{hint}

Format your response as a JSON with these fields as follows:

```json
{{
    "MAPPING_CORRECT_FLAG": "TRUE or FALSE or ERROR",
    "CORRECT_ANSWER": "what is the correct answer text",
    "MAPPING_ISSUES": "NONE if mapping is correct, or detailed text explaining the issues if not correct",
}}
```
This is being run by an automated process so do not repeat these instructions back to me, and do not stop to ask for further permission to proceed.'''

        # Send to PM bot for evaluation
        message_str = message
        thread = str(uuid.uuid4())
        evaluation = call_genesis_bot(client, pm_bot_id, message_str, thread=thread)

        json_str = evaluation.split("```json\n")[-1].split("\n```")[0].strip()
        response = json_str
        
        # Basic validation that we got JSON back
        try:
            summary = json.loads(response)
            required_fields = ["MAPPING_CORRECT_FLAG", "CORRECT_ANSWER", "MAPPING_ISSUES"]
            for field in required_fields:
                if field not in summary:
                    raise Exception(f"Missing required field {field} in PM summary")
        except json.JSONDecodeError:
            raise Exception("PM bot did not return valid JSON")

        print("\033[93mEvaluation results:\033[0m")
        print("\033[96m" + "-"*80 + "\033[0m")  # Cyan separator
        print(json.dumps(summary, indent=4, sort_keys=True, ensure_ascii=False))
        print("\033[96m" + "-"*80 + "\033[0m")  # Cyan separator
        if summary["MAPPING_CORRECT_FLAG"].lower() == "true":
            print("\033[92m" + "🎉🎉🎉🎉🎉🎉🎉🎉🎉🎉" + "\033[0m")
        else:
            print("\033[91m" + "🛑🛑🛑🛑🛑🛑🛑🛑🛑🛑" + "\033[0m")

        return evaluation, summary, thread

    except Exception as e:
        print(f"\033[31mError in evaluation: {str(e)}\033[0m")
        raise e


def load_bots_from_yaml(client, bot_team_path, onlybot=None):
    """
    Load and register bots from YAML files in the specified directory.

    Args:
        bot_team_path: Path to directory containing bot YAML files
        onlybot: Optional bot name to only load a specific bot

    """
   # client = GenesisAPI("local-snowflake", scope="GENESIS_TEST", sub_scope="GENESIS_JL")

    # Get all YAML files in directory
    yaml_files = [f for f in os.listdir(bot_team_path) if f.endswith('.yaml')]

    for yaml_file in yaml_files:
        # Skip if file doesn't contain 'source' in bot_id

        file_path = os.path.join(bot_team_path, yaml_file)

        # Load bot config from YAML
        with open(file_path, 'r') as file:
            bot_config = yaml.safe_load(file)

        # Skip if onlybot specified and doesn't match BOT_ID
        if onlybot and not bot_config.get('BOT_ID', '') == (onlybot):
            continue

        print(f"Registering bot from {file_path}")

        try:
            # Register bot with API
            client.register_bot(bot_config)
            print(f"Successfully registered bot {bot_config['BOT_NAME']}")
        except Exception as e:
            print(f"Failed to register bot from {file_path}: {str(e)}")



def associate_git_file_with_project(client, project_id, file_path, description, bot_id):
    """
    Associate a git file with a project as a project asset.
    
    Args:
        client: Genesis API client instance
        project_id: ID of the project to associate the file with
        file_path: Path to the file in git
        description: Description of what this file contains
        bot_id: Bot ID to use for the operation
        
    Returns:
        Result of the asset creation operation
    """
    try:
        asset_details = {
            "description": description,
            "git_path": file_path
        }
        
        result = client.run_genesis_tool(
            tool_name="manage_project_assets",
            params={
                "action": "CREATE",
                "project_id": project_id,
                "asset_details": asset_details,
                "bot_id": bot_id
            },
            bot_id=bot_id
        )
        
        if result.get("success"):
            print(f"\033[92mSuccessfully associated git file {file_path} with project\033[0m")
        else:
            print(f"\033[91mFailed to associate git file: {result.get('error')}\033[0m")
            
        return result
        
    except Exception as e:
        print(f"\033[91mError associating git file with project: {e}\033[0m")
        raise e

def perform_source_research_new(client, requirement, paths, bot_id, pm_bot_id=None, project_id=None, deng_project_config=None, index_id=None):
    """Execute source research step and validate results."""
    try:
        print("\033[34mExecuting source research...\033[0m")

        hint = deng_project_config.get('source_research_hint', '') if deng_project_config else ''

        research_prompt = f'''{message_prefix} Here are requirements for a target field I want you to work on: {requirement}\n
        Save the results in git at: {paths["base_git_path"]}{paths["source_research_file"]}\n

        First explore the available data for the field that may be useful using data_explorer function.
        You may want to try a couple different search terms to make sure your search is comprehensive.
        Be sure to explore various database connections, as the source data may be in a different database connection.  If you are unsure, omit the db_connection parameter when using data_explorer.
        IF there are multiple tables that look relevant, provide full detailed information for all of them.

        {hint}

'''

        research_prompt += f'''
        Then, you will search the document index using _document_index(action='SEARCH', query='<search query>') and _document_index(action='ASK', query='<question>') for related information from past projects and other sources indexed in the document index.
        Use _document_index() NOT cortex_search() !!
        
        To do that, think up at least 3 of each of the following:
        a) Search queries that would be usefull to find results for in the past projects
        b) Some specific questions that you'd like the answer to from the documents or past projects

        Then use the _document_index() tool's SEARCH and ASK actions respectively to search and ask questions.  Use top_n of at least 10.
        Your goal is to find supportable details from the documents that you can reference in your report, that may later be important to determine a correct mapping for this field in the project at hand.
        
        Try to run at least 3 searches, and ask at least 3 questions.

        If one or more of the file you learn about seems particularly relevent, you can read the entire file with:
        git_action(action='read_file',file_path='{paths["base_git_path"]}<past project file name>')

        It is important to search and fully analyze BOTH the data explorer results to find Schema information, and results of your document search and question/answers, and to discuss BOTH in your report.
        When discussing document-found details in your report, describe their sources and transforms independently, don't say things like 'in the same way as described above', referring to the other project, even if you have to repeat things.

        When you're done, be sure to save your detailed results in git using the git_action function at {paths["base_git_path"]}{paths["source_research_file"]}
        Be sure to put a full copy of the contents of your research into that git location, not just a reference to "what you did above."
        Make sure your report contains the results of both your data exploration, and your analysis you performed with the document_index tool (mention the searches you did, what you learned, the questions you asked, any insightful answers, and the sources mentioned to back up those results).

        *** MAKE YOUR REPORT EXTREMELY DETAILED, WITH FULL DDL OF MULTIPLE POTENTIAL SOURCE TABLES (NOTING WHAT CONNECTION_ID EACH RESIDES IN), AND DETAILS FROM DOCUMENTS OR PAST PROJECTS SIMILAR FIELDS (SOURCING AND TRANSFORMS) ***
        *** THE READER OF THIS REPORT WILL NOT HAVE ACCESS TO ANY OTHER RESOURCES AND IT WILL NEED TO PROPOSE MAPPINGS BASED SOLELY ON YOUR REPORT ***

        This is being run by an automated process, so do not repeat these instructions back to me, simply proceed to execute them without asking for further approval.
        '''
      

        thread = str(uuid.uuid4())
        response = call_genesis_bot(client, bot_id, research_prompt, thread = thread)

        file_name = paths["source_research_file"]
        print(f"Checking Git file:\nPaths: {paths}")
        print(f"File Name: {file_name}")
        print(f"Eve Bot ID: {eve_bot_id}")

        try:
            contents = check_git_file(client, paths=paths, file_name=file_name, bot_id=eve_bot_id)
        except Exception as e:
            print(f"Error checking git file: {e}, prompting the bot to try again...")
            contents = None

        if not contents or len(contents) < 100 or isinstance(contents, dict) and 'error' in contents and 'File not found' in contents['error']:
            retry_prompt = f'''{message_prefix} I don't see the full results of your research saved at {paths["base_git_path"]}{paths["source_research_file"]} in git.  Please complete your analysis, and then save your work again using the git_action function.'''
            response = call_genesis_bot(client, bot_id, retry_prompt, thread = thread)
            file_name = paths["source_research_file"]
            try:
                contents = check_git_file(client, paths=paths, file_name=file_name, bot_id=eve_bot_id)
            except Exception as e:
                print(f"Error checking git file after the retry: {e}")
                contents = None
            if not contents or len(contents) < 100 or isinstance(contents, dict) and 'error' in contents and 'File not found' in contents['error']:
                raise Exception('Source research file not found or contains only placeholder')

        # Associate the git file with the project
        git_file_path = f"{paths['base_git_path']}{paths['source_research_file']}"
        description = f"Source research results for {requirement['PHYSICAL_COLUMN_NAME']}"
        associate_git_file_with_project(
            client=client,
            project_id=project_id,
            file_path=git_file_path,
            description=description,
            bot_id=pm_bot_id
        )
        
        print_file_contents("SOURCE RESEARCH",
                           f"{paths['base_git_path']}{paths['source_research_file']}",
                           contents)

        # update the row in the g-sheet with the source researc
        return {
            'success': True,
            'contents': contents,
            'git_file_path': git_file_path,
            'thread': thread
        }

    except Exception as e:
        raise e

#  HINT: When past projects conflict on how to map this field, consider favoring the loan_lending_project.txt project as it is a closer analogue for this project at hand as the Primary option.

def perform_source_research_v2(client, requirement, paths, bot_id, pm_bot_id=None, project_id=None, deng_project_config=None, index_id=None):
    """Execute source research step and validate results."""
    try:
        print("\033[34mExecuting source research...\033[0m")

        thread = str(uuid.uuid4())
        hint = deng_project_config.get('source_research_hint', '') if deng_project_config else ''

        # Step 1: State requirements and have bot repeat them
        initial_prompt = f'''{message_prefix} Here are requirements for a target field I want you to work on: {requirement}
        \nThe table and column described will be in a future table you are helping to figure out how to create. 
        Please repeat back to me your understanding of what field we are researching and what its requirements are.
        This is being run by an automated process, so do not repeat these instructions back to me.'''
        
        response = call_genesis_bot(client, bot_id, initial_prompt, thread=thread)

        # Step 2: Get search term proposals
        search_prompt = f'''{message_prefix} Based on these requirements, propose 3 or more relevant search terms that could be useful to use to search the existing system table metadata to find relevant source data for the mapping that needs to be created.
        Format your response as a numbered list.'''
        
        response = call_genesis_bot(client, bot_id, search_prompt, thread=thread)

        # Steps 3-5: Run data explorer searches one at a time
        for i in range(3):
            if i == 0:
                explorer_prompt = f'''{message_prefix} As you perform the rest of your work, you may find these hints to be of use: {hint}\n\nNow, please use data_explorer to search using search term #{i+1} from your list above. 
                Provide a brief explanation of what you found and whether it seems relevant.'''
            else:
                explorer_prompt = f'''{message_prefix} Now, please use data_explorer to search using search term #{i+1} from your list above. 
                Provide a brief explanation of what you found and whether it seems relevant.'''
            
            response = call_genesis_bot(client, bot_id, explorer_prompt, thread=thread)

        # Step 6: Get document search terms
        doc_search_prompt = f'''{message_prefix} Please propose three search terms to use with document_index() to find relevant sections of documents about sourcing or mapping this column.
        Format your response as a numbered list, but do not execute the searches yet.'''
        
        response = call_genesis_bot(client, bot_id, doc_search_prompt, thread=thread)

        # Steps 7-9: Run document searches one at a time
        for i in range(3):
            doc_search_exec_prompt = f'''{message_prefix} Please execute document_index(action='SEARCH') using search term #{i+1} from your list above.
            Use top_n of at least 10 and explain what relevant information you found.'''
            
            response = call_genesis_bot(client, bot_id, doc_search_exec_prompt, thread=thread)

        # Step 10: Get questions for document index
        questions_prompt = f'''{message_prefix} Please write up 3 specific questions to ask using document_index(action='ASK') that would help us understand this field, where it comes from, and any other relevant details.
        Format your response as a numbered list, but do not execute the questions yet.'''
        
        response = call_genesis_bot(client, bot_id, questions_prompt, thread=thread)

        # Steps 11-13: Run document questions one at a time
        for i in range(3):
            question_exec_prompt = f'''{message_prefix} Please execute document_index(action='ASK') using question #{i+1} from your list above.
            Explain what relevant information you learned from the response.'''
            
            response = call_genesis_bot(client, bot_id, question_exec_prompt, thread=thread)

        # Step 14: Write final report
        report_prompt = f'''{message_prefix} Based on all the searches and research above, please write up a full and detailed report that the next bot can use to propose a mapping and transform.

        Include:
        - Full DDL of all potential source tables you discovered (noting which connection_id each is in)
        - Relevant examples from the documents that would be helpful for this field
        - A detailed analysis of what you learned from both the data exploration and document research
        
        Save your complete report using git_action at: {paths["base_git_path"]}{paths["source_research_file"]}'''
        
        response = call_genesis_bot(client, bot_id, report_prompt, thread=thread)

        # Step 15: Verify git save and retry if needed
        try:
            contents = check_git_file(client, paths=paths, file_name=paths["source_research_file"], bot_id=eve_bot_id)
        except Exception as e:
            print(f"Error checking git file: {e}, prompting the bot to try again...")
            contents = None

        if not contents or len(contents) < 100 or isinstance(contents, dict) and 'error' in contents and 'File not found' in contents['error']:
            retry_prompt = f'''{message_prefix} I don't see your complete report saved at {paths["base_git_path"]}{paths["source_research_file"]} in git.
            Please save your full report again using the git_action function.'''
            
            response = call_genesis_bot(client, bot_id, retry_prompt, thread=thread)
            try:
                contents = check_git_file(client, paths=paths, file_name=paths["source_research_file"], bot_id=eve_bot_id)
            except Exception as e:
                print(f"Error checking git file after retry: {e}")
                contents = None
                
            if not contents or len(contents) < 100 or isinstance(contents, dict) and 'error' in contents and 'File not found' in contents['error']:
                raise Exception('Source research file not found or contains only placeholder')
        
        print_file_contents("SOURCE RESEARCH",
                           f"{paths['base_git_path']}{paths['source_research_file']}",
                           contents)

        # Step 16: Evaluate mapping confidence
        confidence_prompt = f'''{message_prefix} Based on the source research report above, please evaluate if we have enough information to define an exact source and transform for this field with 100% confidence.

        And remember the full requirement is: 
        {requirement}

        Remember, the column we are mapping's requirement states: {requirement['PHYSICAL_COLUMN_NAME']} is defined as: {requirement['COLUMN_DESCRIPTION']}
        
        And remember the hint we gave at the start of this work, which is still applicable now and for the following steps: 
        {hint}

        Now, I want you to return TRUE only if:
        1. You have clear requirements and evidence from schema research and/or past projects on how to proceed
        2. You are 100% confident in how to map this field
        3. The mapping is relatively simple and would not benefit from human review or a few questions to a human before you make it
        3. You have either:
           - An extremely clearly stated requirement that you know exactly how to fullfil based available data identified in source research
           - or, an exactly matching example from a directly related past project, or
           - or, A specific document mention of how to perform this mapping for this data
        4. Note: If your requirement is clear and simple, and you have a clear path forward based on the data identified in source research, then you can return TRUE even if you don't have an exact match from a past project or document, or if past projects conflict or express greater complexity on how to map this field.

        Return FALSE if one or more of the following are true:
        - You are not 100% confident in how to map this field to fulfill the requirement
        - There is not enough detail in the requirement
        - There is no supporting evidence of the mapping approach from a related project or document
        - The mapping and/or joins is complex (derived fields, lots of math, lots of case statements, or similar) and would benefit from human review or a few questions to a human before you make it
        - There may be multiple ways to map this field based on the evidence and the stated requirement isn't specific which way is correct

        Respond with only TRUE or FALSE.'''

        response = call_genesis_bot(client, bot_id, confidence_prompt, thread=thread)

        # Extract just TRUE/FALSE from response
        confidence = 'TRUE' in response.upper() and not 'FALSE' in response.upper()

        questions = None
        if not confidence:
            # Get explanation for lack of confidence
            explain_prompt = f'''{message_prefix} Please explain why you are not confident enough to propose a mapping for this field. 
            Reference specific gaps or uncertainties in the available information.'''
            
            explanation = call_genesis_bot(client, bot_id, explain_prompt, thread=thread)

            # Get clarifying questions
            questions_prompt = f'''{message_prefix} What are a few questions you would want to ask a business stakeholder to help clarify the areas of uncertainty? Restate the name of the field in your questions.'''
            
            questions = call_genesis_bot(client, bot_id, questions_prompt, thread=thread)

            # Save confidence report
            confidence_report = f'''CONFIDENCE: FALSE

EXPLANATION:
{explanation}

CLARIFYING QUESTIONS:
{questions}'''

            put_content_into_git_file(client, confidence_report, paths["base_git_path"], paths["confidence_report_file"], bot_id)
            mapping_proposal = "Not confident enough to propose a mapping."
            put_content_into_git_file(client, mapping_proposal, paths["base_git_path"], paths["mapping_proposal_file"], bot_id)

        else:
            # Get detailed mapping proposal
            proposal_prompt = f'''{message_prefix} Please write a detailed mapping proposal document that includes:

1. The proposed source(s) including:
   - Database connection ID
   - Fully qualified table names
   - Specific source fields

2. Any required transformations or business logic

3. Detailed justification for your mapping proposal with specific references to:
   - Source schema research
   - Document research
   - Past project examples

Please be thorough and specific in your proposal.'''

            mapping_proposal = call_genesis_bot(client, bot_id, proposal_prompt, thread=thread)

            # Save confidence report with proposal
            confidence_report = f'''CONFIDENCE: TRUE

MAPPING PROPOSAL:
{mapping_proposal}'''

            put_content_into_git_file(client, mapping_proposal, paths["base_git_path"], paths["mapping_proposal_file"], bot_id)
            confidence_report = "Confident mapping, see mapping proposal for details."

        return {
            'success': True,
            'contents': {
                'source_research': check_git_file(client, paths=paths, file_name=paths["source_research_file"], bot_id=eve_bot_id),
                'proposal_made': mapping_proposal != "Not confident enough to propose a mapping.",
                'questions': questions,
                'mapping_proposal': mapping_proposal,
                'confidence_report': confidence_report
            },
            'git_file_paths': {
                'source_research': f"{paths['base_git_path']}{paths['source_research_file']}",
                'mapping_proposal': f"{paths['base_git_path']}{paths['mapping_proposal_file']}",
                'confidence_report': f"{paths['base_git_path']}{paths['confidence_report_file']}"
            },
            'thread': thread
        }

    except Exception as e:
        raise e



def perform_mapping_proposal_new(client, requirement, paths, bot_id, pm_bot_id=None, project_id=None, deng_project_config=None):


    f"""{message_prefix}Execute mapping proposal step and validate results."""
    print("\033[34mExecuting mapping proposal...\033[0m")


    hint = deng_project_config.get('mapping_proposer_hint', '') if deng_project_config else ''

    mapping_prompt = f'''{message_prefix} Here are requirements for a target field I want you to work on a mapping proposal for: {requirement}

    The source research bot has already run and saved its results at this git. First, read its report by calling:
    git_action(action='read_file',file_path='{paths["base_git_path"]}{paths["source_research_file"]}')

    {hint}

    Now, first decide if you have enough information from the source research report to make a solid case for a specific mapping proposal.

    For a proposal to be solid you want to see:
    1. Clear source data referenced in the report that supports the mapping and is directly and clearly relevent
    2. A clear mapping logic that either completely obvious based on the source data, or that is supported by very similar mappings found in past projcets

    If the source research (schema and past project information) report does not support a solid mapping proposal, then your response and report mention that you are not able to 
    propose a mapping based on the source research with high confidence, and explain why not.

    Clearly state in your respose if you are or are not making a confident proposal, and if you have HIGH or LOW (anything other than high=LOW) confidence in the mapping, and

    If the source research does support a solid mapping proposal, then do make a proposal for a field mapping.
    Only reference tables and objects and logic that are supported by the source research, be careful not to halicinate any other data sources, tables, or fields..
    Be sure to note both the connection_id and fully qualified table name of any source tables you propose to use.
    Explain your reasoning for each proposed mapping, and note any assumptions you are making, and quote the source research (both about schemas and past projects) to support your case.

    Then save your full results at this git location using git_action: {paths["base_git_path"]}{paths["mapping_proposal_file"]}
    Don't forget use use git_action to save your full and complete mapping results.  Don't just put "see above" or similar as this file will be read by another bot who will not see your full completion output.

    This is being run by an automated process, so do not repeat these instructions back to me, simply proceed to execute them without asking for further approval.'''

    thread = str(uuid.uuid4())
    try:
        response = call_genesis_bot(client, bot_id, mapping_prompt, thread=thread)
        contents = check_git_file(client, paths=paths, file_name=paths["mapping_proposal_file"], bot_id=eve_bot_id)
    except Exception as e:
        print(f"Error during initial mapping proposal call or file check: {e}")
        contents = None

    if not contents or len(contents) < 100 or isinstance(contents, dict) and 'error' in contents and 'File not found' in contents['error']:
        retry_prompt = f'''{message_prefix} I don't see the full results of your mapping proposal saved at {paths["base_git_path"]}{paths["mapping_proposal_file"]} in git, please complete your work, and then try the save using the git_action function.'''
        try:
            response = call_genesis_bot(client, bot_id, retry_prompt, thread=thread)
            contents = check_git_file(client, paths=paths, file_name=paths["mapping_proposal_file"], bot_id=eve_bot_id)
        except Exception as e:
            print(f"Error during retry mapping proposal call or file check: {e}")
            contents = None

        if not contents or len(contents) < 100 or isinstance(contents, dict) and 'error' in contents and 'File not found' in contents['error']:
            raise Exception('Mapping proposal file not found or contains only placeholder')

    print_file_contents("MAPPING PROPOSAL",
                       f"{paths['base_git_path']}{paths['mapping_proposal_file']}",
                       contents)
    
    # Associate the git file with the project
    git_file_path = f"{paths['base_git_path']}{paths['mapping_proposal_file']}"
    description = f"Mapping proposal for {requirement['PHYSICAL_COLUMN_NAME']}"
    associate_git_file_with_project(
        client=client,
        project_id=project_id,
        file_path=git_file_path,
        description=description,
        bot_id=pm_bot_id
        )
    
    result = {
        'success': True,
        'contents': contents,
        'git_file_path': git_file_path,
        'thread': thread
    }

    return result

def perform_confidence_analysis_new(client, requirement, paths, bot_id, deng_project_config=None):
    """Execute confidence analysis step and validate results."""
    print("\033[34mExecuting confidence analysis...\033[0m")

    hint = deng_project_config.get('confidence_scoring_hint', '') if deng_project_config else ''

    confidence_prompt = f'''{message_prefix} Here are requirements for a target field I want you to analyze confidence for: {requirement}

    First read the source research report by calling:
    git_action(action='read_file',file_path='{paths["base_git_path"]}{paths["source_research_file"]}')

    Then read the mapping proposal by calling:
    git_action(action='read_file',file_path='{paths["base_git_path"]}{paths["mapping_proposal_file"]}')

    {hint}

    Now, analyze the confidence level of this mapping proposal using the process in your base bot instructions.

    Then save your confidence analysis at this git location using git_action: {paths["base_git_path"]}{paths["confidence_report_file"]}

    This is being run by an automated process, so do not repeat these instructions back to me, simply proceed to execute them without asking for further approval.'''

    response = call_genesis_bot(client, bot_id, confidence_prompt)

    contents = check_git_file(client,paths=paths, file_name=paths["confidence_report_file"], bot_id=eve_bot_id)

    if not contents or contents.startswith('Placeholder '):
        raise Exception('Confidence report file not found or contains only placeholder')

    print_file_contents("CONFIDENCE ANALYSIS",
                       f"{paths['base_git_path']}{paths['confidence_report_file']}",
                       contents)
    return contents



def parse_arguments():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="A simple CLI chat interface to Genesis bots")
    add_default_argparse_options(parser)
    parser.add_argument(
        '--todo-id',
        type=str,
        default=None,
        help='Optional ID for a specific todo item'
    )
    parser.add_argument(
        '--project-id',
        type=str,
        default=None,
        help='Optional project ID to use instead of default'
    )
    parser.add_argument(
        '--base-file-path',
        type=str,
        default=None,
        help='Optional base file path to use instead of default'
    )
    parser.add_argument(
        '--data-connector-project-id',
        type=str,
        default=None,
        help='Optional data connector project ID to use instead of default'
    )
    return parser.parse_args()


def run_snowflake_query(client, query, bot_id=None, max_rows=1000):
    """
    Run a Snowflake query using the Genesis tool API.
    
    Args:
        client: Genesis API client instance
        query: SQL query string to execute
        bot_id: Bot ID to use for the query (defaults to eve_bot_id if not specified)
    
    Returns:
        Query results from Snowflake
    """
    if bot_id is None:
        bot_id = eve_bot_id
        
    res = client.run_genesis_tool(
        tool_name="query_database", 
        params={
            "query": query,
            "connection_id": connection_id,
            "bot_id": bot_id,
            "max_rows": max_rows
        }, 
        bot_id=bot_id
    )
    
    return res


def export_table_to_gsheets(client, table_name, sheet_name, bot_id=None):
    """
    Run a Snowflake query using the Genesis tool API.
    
    Args:
        client: Genesis API client instance
        query: SQL query string to execute
        bot_id: Bot ID to use for the query (defaults to eve_bot_id if not specified)
    
    Returns:
        Query results from Snowflake
    """
    if bot_id is None:
        bot_id = eve_bot_id

    query = f"SELECT * FROM {table_name}"
        
    res = client.run_genesis_tool(
        tool_name="query_database", 
        params={
            "query": query,
            "connection_id": connection_id,
            "bot_id": bot_id,
            "export_to_google_sheet": True,
            "export_title": sheet_name,
            "max_rows": 100,
        }, 
        bot_id=bot_id
    )
    print("Sent to G-sheets results:")
    print(res)
    return res

def push_knowledge_files_to_git_and_index(client, bot_id, past_projects_dir, past_projects_git_path, index_id):
    """
    Push knowledge base files to git repository.
    
    Args:
        client: Genesis API client instance
        bot_id: Bot ID to use for git operations
    """

    files_to_push = {}

    for root, dirs, files in os.walk(past_projects_dir):
        for file in files:
            local_path = os.path.join(root, file)
            git_path = os.path.relpath(local_path, past_projects_dir)
            files_to_push[local_path] = git_path

    for local_path, git_path in files_to_push.items():
        put_git_file(
            client=client,
            local_file=local_path,
            git_file_path=past_projects_git_path + "/",
            file_name=os.path.basename(git_path),
            bot_id=bot_id
        )
        # Add document to index
        client.run_genesis_tool(
            tool_name="document_index",
            params={
                "action": "ADD_DOCUMENTS",
                "index_name": index_id, 
                "filepath": f"BOT_GIT:{past_projects_git_path}/{os.path.basename(local_path)}"
            },
            bot_id=bot_id
        )
        print(f'...indexed past project file {local_path} in {index_id}, and saved it to git at {past_projects_git_path}/{os.path.basename(local_path)}')
    

def get_past_project_git_locations(client, past_projects_dir, past_projects_git_path, bot_id):
    """
    Return the array of past project locations in git.
    
    Args:
        client: Genesis API client instance
        past_projects_dir: Directory containing past project files
        bot_id: Bot ID to use for git operations

    Returns:
        list: List of git paths for past projects.
    """
    git_locations = []

    for root, dirs, files in os.walk(past_projects_dir):
        for file in files:
            local_path = os.path.join(root, file)
            git_path = past_projects_git_path  + "/" + file
            git_locations.append(git_path)

    return git_locations


def initialize_project(client, bot_id, project_id="deng_requirements_mapping", project_name=None, project_description="Project for mapping source system fields to target system requirements for Data Engineering"):
    """Initialize project by deleting if exists and creating new."""
    print(f"\nInitializing project '{project_id}'...")

    if project_name is None:
        project_name = f"DEng: {project_id}" 

    # Try to delete existing project first
    try:
        client.run_genesis_tool(
            tool_name="manage_projects",
            params={
                "action": "DELETE",
                "project_id": project_id,
                "bot_id": bot_id
            },
            bot_id=bot_id
        )
        print(f"Deleted existing project '{project_id}'")
    except Exception as e:
        print(f"Project deletion failed (likely didn't exist): {e}")

    # Create new project
    try:
        client.run_genesis_tool(
            tool_name="manage_projects",
            params={
                "action": "CREATE",
                "project_id": project_id,
                "project_details": {
                    "project_name": project_name,
                    "description": project_description
                },
                "bot_id": bot_id,
                "static_project_id": True
            },
            bot_id=bot_id
        )
        print(f"Created new project '{project_id}'")
    except Exception as e:
        print(f"Error creating project: {e}")
        raise e
    
def initialize_document_index(client, bot_id, project_id):
    # create a new document index
    try:
        result = client.run_genesis_tool(
            tool_name="document_index",
            params={
                "action": "CREATE_INDEX",
                "index_name": f"{project_id}_past_projects",
                "bot_id": bot_id
            },
            bot_id=bot_id
        )
        if 'Error' in result and result['Error'] == 'Index with the same name already exists':
            print(f"Document index {project_id}_past_projects already exists, removing")
            client.run_genesis_tool(
                tool_name="document_index",
                params={
                    "action": "DELETE_INDEX",
                    "index_name": f"{project_id}_past_projects",
                    "bot_id": bot_id
                },
                bot_id=bot_id
            )
            print(f"Deleted existing document index {project_id}_past_projects")
            result = client.run_genesis_tool(
            tool_name="document_index",
            params={
                "action": "CREATE_INDEX",
                "index_name": f"{project_id}_past_projects",
                "bot_id": bot_id
            },
            bot_id=bot_id
           )
            
        index_id = f"{project_id}_past_projects"
        print(f"Created document index: {index_id}")
        # Save the index ID for later use
        # Save index ID to temp file for later use
        temp_dir = os.path.join(os.getcwd(), 'tmp')
        os.makedirs(temp_dir, exist_ok=True)
        temp_file = os.path.join(temp_dir, f'{project_id}_index_id.txt')
        with open(temp_file, 'w') as f:
            f.write(index_id)
        print(f"Saved document index ID to {temp_file}")
        return index_id

        
    except Exception as e:
        print(f"Error creating document index: {e}")
        raise e

def add_requirements_todos(client, requirements, bot_id, project_id="deng_requirements_mapping", max_todos=-1, root_folder=None):
    """
    Add todos for each requirement to the project.
    
    Args:
        client: Genesis API client instance
        requirements: List of requirements to create todos for
        bot_id: Bot ID to use for creating todos
        project_id: Project ID to add todos to
    """
    print("\nAdding todos for requirements...")
    
    todos = []
    if max_todos > 0:
        requirements_filtered = requirements[:max_todos]
        print(f"Limiting initial Todos to {max_todos} of {len(requirements)}")
    else:
        requirements_filtered = requirements
    for req in requirements_filtered:
        todo = {
            "todo_name": f"Map field: {req['PHYSICAL_COLUMN_NAME']}",
            "what_to_do": f"""
Use _run_program function to run:
 program_id=mapping_research_and_proposal
 project_id={project_id}
 root_folder={root_folder}
 todo_id=<This Todo's id>

Field Details:
- Physical Name: {req.get('PHYSICAL_COLUMN_NAME', '')}
- Logical Name: {req.get('LOGICAL_COLUMN_NAME', '')}
- Description: {req.get('COLUMN_DESCRIPTION', '')}
- Data Type: {req.get('DATA_TYPE', '')}
- Length: {req.get('LENGTH', '')}
- List of Values: {req.get('LIST_OF_VALUES', '')}
            """.strip()
        }
        todos.append(todo)

    try:
        result = client.run_genesis_tool(
            tool_name="create_todos_bulk",
            params={
                "todos": todos,
                "project_id": project_id,
                "bot_id": bot_id
            },
            bot_id=bot_id
        )
        print(f"Successfully added {len(todos)} todos")
        return result
    except Exception as e:
        print(f"Error adding todos: {e}")
        raise e


def add_data_connector_todos(client, bot_id, project_id="deng_data_connector"):
    """
    Add todos for each requirement to the project.
    
    Args:
        client: Genesis API client instance
        bot_id: Bot ID to use for creating todos
        project_id: Project ID to add todos to
    """
    print("\nAdding todos for data connector...")

    todos = []
    
    todo = {
        "todo_name": "Create Iceberg Tables for Databricks Sources",
        "what_to_do": """
Use _run_program tool to run the mapping_research_and_proposal program referencing this Todo's id.

1. Query GENESIS_TEST_JL.DENG_REQUIREMENTSPM_WORKSPACE.DENG_REQUIREMENTS_MAPPING_REQUIREMENTS and get a unique list of any source tables that the mappings use that come from the Databricks connection, based on the UPSTREAM_DB_CONNECTION, UPSTREAM_TABLE, and MAPPING_PROPOSAL fields. Use query_database for these queries as this table may not be indexed in the metadata.

2. Create iceberg tables in Snowflake for each unique source tables on that list. Put them in the BRONZE_STAGE.HEALTHCARE_CLAIMS schema, and use the same table name as their source table name.

3. Verify that you have created all needed Iceberg tables and that they are in the correct location.

4. Query count(*) on the new Snowflake Iceberg tables, and see if they match the row counts you get when querying the source table directly in Databricks.""".strip()
    }
    todos.append(todo)

    try:
        result = client.run_genesis_tool(
            tool_name="create_todos_bulk",
            params={
                "todos": todos,
                "project_id": project_id,
                "bot_id": bot_id
            },
            bot_id=bot_id
        )
        print(f"Successfully added {len(todos)} todos")
        return result
    except Exception as e:
        print(f"Error adding todos: {e}")
        raise e



def record_work(client, todo_id, description, bot_id, thread_id=None, results=None):
    """
    Record work progress on a todo item.
    
    Args:
        client: Genesis API client instance
        todo_id: ID of the todo item to record work for
        description: Description of work performed
        bot_id: Bot ID to use for recording work
        results: Optional results or output from the work
    """
    try:
        # Convert thread_id to string if it's a UUID
        thread_id_str = str(thread_id) if thread_id else None
        
        # Convert results to string if not JSON serializable
        results_str = str(results) if results else None
        
        result = client.run_genesis_tool(
            tool_name="record_todo_work",
            params={
                "todo_id": todo_id,
                "work_description": description,
                "work_results": results_str,
                "bot_id": bot_id,
                "thread_id": thread_id_str
            },
            bot_id=bot_id
        )
        return result
    except Exception as e:
        print(f"Error recording work: {e}")
        raise e


def initialize_system(
    client,
    bot_id=None,
    pm_bot_id=None, 
    genesis_db='GENESIS_BOTS',
    reset_project=False,
    index_files=False,
    req_max=-1,
    project_id="deng_requirements_mapping",
    past_projects_dir=None,
    past_projects_git_path=None,
    eve_bot_id=None,
    data_connector_bot_id=None,
    data_connector_project_id="deng_data_connector",
    requirements_file_name=None,
    requirements_table_name=None,
    root_folder=None,
    project_config=None
):
    """
    Initialize the requirements table in Snowflake if it doesn't exist.
    Loads data from test_requirements.json if table needs to be created.
    
    Args:
        client: Genesis API client instance
        bot_id: Bot ID to use for database operations (defaults to eve_bot_id)
    """
    # Try to query table to check if exists
    
    # Check if source data table exists when running standard demo project
    if project_id == 'deng_requirements_mapping':
        check_source_query = "SELECT COUNT(*) FROM HCLS.BRONZE.RAW_CLAIM_HEADERS"
        try:
            res = run_snowflake_query(client, check_source_query, bot_id)
        except Exception as e:
            print("\033[31mSource data table HCLS.BRONZE.RAW_CLAIM_HEADERS not found. Please run the data creation script first.\033[0m")
            raise Exception("Source data table not found - run data creation script first") from e
        if 'Success' in res and res['Success'] == False:
            raise Exception("Source data table not found - run data creation script first")    
        # Get harvest control data to check what sources are being harvested  
        try:
            harvest_control = client.run_genesis_tool(
                tool_name="get_harvest_control_data",
                params={},
                bot_id=eve_bot_id
            )
            print("Retrieved current harvest control data")
        except Exception as e:
            print(f"\033[31mError getting harvest control data: {e}\033[0m")
            raise e
        # Parse harvest control data and check for HCLS database
        if not isinstance(harvest_control, dict) or 'Data' not in harvest_control:
            raise Exception("Invalid harvest control data format")
        
        try:
            harvest_data = json.loads(harvest_control['Data'])
            harvested_databases = [entry['DATABASE_NAME'].upper() for entry in harvest_data ]
            
            if 'BRONZE' not in harvested_databases and 'HCLS' not in harvested_databases:
                print("\033[31mHCLS or BRONZE database is not being harvested. Adding it to harvest control.\033[0m")
                
                client.run_genesis_tool(
                    tool_name="_set_harvest_control_data", 
                    params={
                        "connection_id": "Snowflake",
                        "database_name": "HCLS",
                        "initial_crawl_complete": False,
                        "refresh_interval": 5,
                        "schema_exclusions": ["INFORMATION_SCHEMA"],
                        "schema_inclusions": []
                    },
                    bot_id=eve_bot_id
                )
                print("Successfully added HCLS database to harvest control")
        except Exception as e:
            print(f"\033[31mError checking harvest control data: {e}\033[0m")
            raise e

    if index_files:
        # Initialize additional document indices if configured in project_config
        if project_config and 'document_indices' in project_config:
            print("Found document_indices in project config, initializing additional indices...")
            indices = initialize_document_indices(client, pm_bot_id, project_id, project_config)
            print(f"Created additional indices: {indices}")

    if not reset_project:
        check_query = f"SELECT COUNT(*) FROM {genesis_db}.{pm_bot_id.upper().replace('-','_')}_WORKSPACE.{project_id.upper().replace('-','_')}_REQUIREMENTS"
        res = run_snowflake_query(client, check_query, bot_id)
        if isinstance(res, list) and len(res) > 0:
            print("Table already exists, checking gsheet location...")
            # Load gsheet location from file
            try:
                with open(f'tmp/gsheet_location_{project_id}.txt', 'r') as f:
                    gsheet_location = f.read().strip()
                    print(f"Read gsheet location: {gsheet_location}")
            except Exception as e:
                print(f"Error reading gsheet location: {e}")
                gsheet_location = None
            # Load index ID from temp file
            try:
                with open(f'tmp/{project_id}_index_id.txt', 'r') as f:
                    index_id = f.read().strip()
                    print(f"Read index ID: {index_id}")
            except Exception as e:
                print(f"Error reading index ID: {e}")
                index_id = None
            return gsheet_location, index_id
    
    # initialize projects
    initialize_project(client, pm_bot_id, project_id) # requirements mapping project
    initialize_project(client, data_connector_bot_id, data_connector_project_id, project_name=f"DEng: {data_connector_project_id}", project_description="Project for connecting source system tables to Snowflake tables via Iceberg")
    
    index_id = initialize_document_index(client, pm_bot_id, project_id)

    push_knowledge_files_to_git_and_index(client, eve_bot_id, past_projects_dir, past_projects_git_path, index_id)


    #index_input_files(client, pm_bot_id, project_id, input_files_dir)
    #push_knowledge_files_to_git(client, eve_bot_id, os.path.join('api_examples/data_engineering', 'knowledge/eval_answers'), "data_engineering/eval_answers/")

    # Check if the schema exists, if not, create it
    schema_name = f"{genesis_db}.{pm_bot_id.upper()}_WORKSPACE".replace("-", "_")
    check_schema_query = f"SHOW SCHEMAS LIKE '{schema_name}'"
    schema_exists = run_snowflake_query(client, check_schema_query, bot_id)

    if not schema_exists:
        print(f"Schema {schema_name} does not exist. Creating schema...")
        create_schema_query = f"CREATE SCHEMA IF NOT EXISTS {schema_name}"
        run_snowflake_query(client, create_schema_query, bot_id)
    else:
        print(f"Schema {schema_name} already exists.")

    # Create table if query failed
    print(f"Creating table {requirements_table_name}...")
    create_query = f"""
        create or replace TABLE {requirements_table_name} (
        LOGICAL_TABLE_NAME VARCHAR(16777216),
        TABLE_DESCRIPTION VARCHAR(16777216),
        PHYSICAL_TABLE_NAME VARCHAR(16777216),
        PHYSICAL_COLUMN_NAME VARCHAR(16777216),
        LOGICAL_COLUMN_NAME VARCHAR(16777216),
        COLUMN_DESCRIPTION VARCHAR(16777216),
        DATA_TYPE VARCHAR(16777216),
        LENGTH VARCHAR(16777216),
        DECIMAL VARCHAR(16777216),
        LIST_OF_VALUES VARCHAR(16777216),
        CORRECT_ANSWER_FOR_EVAL VARCHAR(16777216),
        SOURCE_RESEARCH VARCHAR(16777216),
        CONFIDENCE_SCORE VARCHAR(16777216),
        CONFIDENCE_OUTPUT VARCHAR(16777216),
        PROPOSAL_MADE VARCHAR(16777216),
        QUESTIONS VARCHAR(16777216),
        MAPPING_PROPOSAL VARCHAR(16777216),
        UPSTREAM_DB_CONNECTION VARCHAR(16777216),
        UPSTREAM_TABLE VARCHAR(16777216),
        UPSTREAM_COLUMN VARCHAR(16777216),
        TRANSFORMATION_LOGIC VARCHAR(16777216),
        STATUS VARCHAR(16777216),
        MAPPING_CORRECT_FLAG VARCHAR(10),
        MAPPING_ISSUES VARCHAR(16777216),
        CORRECT_TO_DECLINE_FLAG VARCHAR(10),
        OVERALL_CORRECT_FLAG VARCHAR(10)
        );   """
    run_snowflake_query(client, create_query, bot_id)
    
    # Load data from JSON file
    json_path = requirements_file_name
    with open(json_path, 'r') as f:
        requirements = json.load(f)
        
    # Insert data - using the actual fields from the requirements
    print(f"Populating initial data into {genesis_db}.{pm_bot_id.upper()}_WORKSPACE.{project_id}_REQUIREMENTS...")

    for req in requirements:
        insert_query = f"""
        INSERT INTO {requirements_table_name}
        (LOGICAL_TABLE_NAME, TABLE_DESCRIPTION, PHYSICAL_TABLE_NAME,
         PHYSICAL_COLUMN_NAME, LOGICAL_COLUMN_NAME, COLUMN_DESCRIPTION, 
         DATA_TYPE, LENGTH, LIST_OF_VALUES, STATUS, CORRECT_ANSWER_FOR_EVAL)
        VALUES (
            '{req.get('LOGICAL_TABLE_NAME', '').replace("'", "''")}',
            '{req.get('TABLE_DESCRIPTION', '').replace("'", "''")}',
            '{req.get('PHYSICAL_TABLE_NAME', '').replace("'", "''")}',
            '{req.get('PHYSICAL_COLUMN_NAME', '').replace("'", "''")}',
            '{req.get('LOGICAL_COLUMN_NAME', '').replace("'", "''")}',
            '{req.get('COLUMN_DESCRIPTION', '').replace("'", "''")}',
            '{req.get('DATA_TYPE', '').replace("'", "''")}',
            '{str(req.get('LENGTH', '')).replace("'", "''")}',
            '{str(req.get('LIST_OF_VALUES', '')).replace("'", "''")}',
            'NEW',
            '{req.get('CORRECT_ANSWER_FOR_EVAL', '').replace("'", "''")}'
        )
        """
        run_snowflake_query(client, insert_query, bot_id)
    from datetime import datetime
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M")
    gsheet_location = export_table_to_gsheets(client, requirements_table_name, f'Data Engineering {project_id} - {current_time}', pm_bot_id)
    # Save gsheet location to file
    # Ensure tmp directory exists
    os.makedirs('tmp', exist_ok=True)
    with open(f'tmp/gsheet_location_{project_id}.txt', 'w') as f:
        f.write(str(gsheet_location['file_url']))

    print("Adding todos...")
    add_requirements_todos(client, requirements, pm_bot_id, project_id=project_id, max_todos=req_max, root_folder=root_folder)
    add_data_connector_todos(client, data_connector_bot_id, project_id=data_connector_project_id)



    return gsheet_location['file_url'], index_id


def update_todo_status(client, todo_id, new_status, bot_id, work_description=None, work_results=None):
    """
    Update the status of a todo item.
    
    Args:
        client: Genesis API client instance
        todo_id: ID of the todo to update
        new_status: New status to set (NEW, IN_PROGRESS, ON_HOLD, COMPLETED, CANCELLED)
        bot_id: Bot ID to use for the update
        work_description: Optional description of work done
        work_results: Optional results of work done
        
    Returns:
        Result of the status update operation
    """
    try:
        todo_details = {
            "new_status": new_status,
            "work_description": work_description,
            "work_results": work_results
        }
        
        result = client.run_genesis_tool(
            tool_name="manage_todos",
            params={
                "action": "CHANGE_STATUS",
                "todo_id": todo_id,
                "todo_details": todo_details,
                "bot_id": bot_id
            },
            bot_id=bot_id
        )
        
        if result.get("success"):
            print(f"Successfully updated todo {todo_id} status to {new_status}")
        else:
            print(f"Failed to update todo status: {result.get('error')}")
            
        return result
        
    except Exception as e:
        print(f"Error updating todo status: {e}")
        raise e


def get_todos(client, project_id, bot_id, todo_id=None):
    """
    Get all open todos for the project.
    
    Args:
        client: Genesis API client instance
        project_id: ID of the project to get todos for
        bot_id: Bot ID to use for the query
        
    Returns:
        List of todo items that are not completed or cancelled
    """
    try:
        todos = client.run_genesis_tool(
            tool_name="get_project_todos",
            params={
                "project_id": project_id,
                "bot_id": bot_id
            },
            bot_id=bot_id
        )
        
        if todo_id:
            todos = [todo for todo in todos['todos'] if todo['todo_id'] == todo_id]
            if todos:
                print(f"Found specified todo with ID {todo_id}:")
                for todo in todos:
                    print(f"- {todo.get('todo_name')} (Status: {todo.get('current_status')})")
            else:
                print(f"No todo found with ID {todo_id}")
            return todos
        else:
            # Filter for open todos (not COMPLETED or CANCELLED)
            open_todos = [
                todo for todo in todos['todos'] if todo['current_status'] not in ['COMPLETED', 'CANCELLED']
            ]
        
        print(f"\nFound {len(open_todos)} open todos:")
        for todo in open_todos:
            print(f"- {todo.get('todo_name')} (Status: {todo.get('current_status')})")
        return open_todos
        
    except Exception as e:
        print(f"Error getting open todos: {e}")
        raise e


def main():
    """Main execution flow.
    """
    args = parse_arguments()

    server_proxy = build_server_proxy(args.server_url, args.snowflake_conn_args, args.genesis_db)

    global genesis_api_client
    genesis_api_client = GenesisAPI(server_proxy=server_proxy)
    client = genesis_api_client

    global eve_bot_id
    if args.genesis_db == 'GENESIS_BOTS_ALPHA':
        eve_bot_id = 'Eve-nEwEve1'
    else:
        eve_bot_id = 'Eve'

    # Update root folder to include data_engineering
    root_folder = args.base_file_path if args.base_file_path else 'api_examples/data_engineering'
    project_id = args.project_id if args.project_id else 'deng_requirements_mapping'
    data_connector_project_id = args.data_connector_project_id if args.data_connector_project_id else 'deng_data_connector'

    bot_team_path = os.path.join(root_folder, 'bot_team')
    past_projects_dir = os.path.join(root_folder, 'knowledge/past_projects')
    past_projects_git_path = f'{project_id}/knowledge/past_projects'
    requirements_file_name = os.path.join(root_folder, 'field_requirements.json')

    # LOAD AND ACTIVATE BOTS FROM YAML FILES
    pm_bot_id = 'DEng-requirementsPM'
    source_research_bot_id = 'DEng-sourceResearchBot'
    mapping_proposer_bot_id = 'DEng-mappingProposerBot'
    confidence_analyst_bot_id = 'DEng-confidenceAnalystBot'
    dbt_engineer_bot_id = 'DEng-DBT-EngineerBot'
    data_connector_bot_id = 'DEng-dataConnectorBot'
    skip_confidence = True
    requirements_table_name = f"{args.genesis_db}.{pm_bot_id.upper()}_WORKSPACE.{project_id.upper()}_REQUIREMENTS".replace("-", "_")

    # Load project config if exists
    deng_project_config = None
    try:
        with open(os.path.join(root_folder, 'deng_project_config.json'), 'r') as f:
            deng_project_config = json.loads(f.read())
    except Exception as e:
        print(f"Project config not loaded from {root_folder}/deng_project_config.json {e}")
        pass


    if not args.todo_id:
        # If a specific todo_id is provided, filter the todos to only include this one
    
        load_bots = False
        reset_project = False
        index_files = True
        if load_bots:
            # make the runner_id overrideable
            load_bots_from_yaml(client=client, bot_team_path=bot_team_path) # , onlybot=source_research_bot_id)  # takes bot definitions from yaml files at the specified path and injects/updates those bots into the running local server
        else:
            print("!!! Skipping bot loading, using existing bots")

        # Initialize requirements table if not exists 
        req_max = -1
    else:
        reset_project = False  # do not change 
        index_files = False # do not change
        req_max = -1

    gsheet_location, index_id = initialize_system(
        client=client,
        pm_bot_id=pm_bot_id,
        genesis_db=args.genesis_db,
        reset_project=reset_project,
        index_files=index_files,
        req_max=req_max,
        past_projects_dir=past_projects_dir,
        past_projects_git_path=past_projects_git_path,
        project_id=project_id,
        eve_bot_id=eve_bot_id,
        data_connector_bot_id=data_connector_bot_id,
        data_connector_project_id=data_connector_project_id,
        requirements_file_name=requirements_file_name,
        requirements_table_name=requirements_table_name,
        root_folder=root_folder,
        project_config = deng_project_config
    )
    todos = get_todos(client, project_id, pm_bot_id, todo_id=args.todo_id) 

   # past_projects_list = get_past_project_git_locations(client, past_projects_dir, past_projects_git_path, pm_bot_id)

    run_number = 1;

    requirements_query = f"SELECT * FROM {requirements_table_name}"
    requirements = run_snowflake_query(client, requirements_query, eve_bot_id, max_rows=1000)

    for todo in todos:
        # Check if todo_id is not provided
        if not args.todo_id:
            # Generate a filename with the current timestamp
            unique_id = str(uuid.uuid4())
            filename = f"tmp/mapping_research_and_proposal_{unique_id}.txt"
            with open(filename, 'w') as f:
                with stdout_redirector(f, sys.stdout):
                    process_todo_item(todo, client, requirements, pm_bot_id, run_number, project_id, deng_project_config, gsheet_location, requirements_table_name, args, source_research_bot_id, mapping_proposer_bot_id, confidence_analyst_bot_id, skip_confidence, filename=filename, index_id=index_id)
        else:
            process_todo_item(todo, client, requirements, pm_bot_id, run_number, project_id, deng_project_config, gsheet_location, requirements_table_name, args, source_research_bot_id, mapping_proposer_bot_id, confidence_analyst_bot_id, skip_confidence, index_id=index_id)

    # Return success status
    return {
        "success": True
    }

def process_todo_item(todo, client, requirements, pm_bot_id, run_number, project_id, deng_project_config, gsheet_location, requirements_table_name, args, source_research_bot_id, mapping_proposer_bot_id, confidence_analyst_bot_id, skip_confidence, filename=None, index_id=None):
    field_name = todo['todo_name'][11:]
    print(f"\033[34mProcessing todo item: {todo['todo_name']}\033[0m")
    print(f"\033[34mLooking up requirement for field: {field_name}\033[0m")

    try:
        requirement = next((req for req in requirements if req['PHYSICAL_COLUMN_NAME'] == field_name), None)
        if requirement is None:
            raise Exception(f"Requirement not found for field: {field_name}")

        filtered_requirement = {
            'PHYSICAL_COLUMN_NAME': requirement.get('PHYSICAL_COLUMN_NAME', ''),
            'LOGICAL_COLUMN_NAME': requirement.get('LOGICAL_COLUMN_NAME', ''),
            'COLUMN_DESCRIPTION': requirement.get('COLUMN_DESCRIPTION', ''),
            'DATA_TYPE': requirement.get('DATA_TYPE', ''),
            'LENGTH': requirement.get('LENGTH', ''),
            'LIST_OF_VALUES': requirement.get('LIST_OF_VALUES', ''),
            'PHYSICAL_TABLE_NAME': requirement.get('PHYSICAL_TABLE_NAME', ''),
            'TABLE_DESCRIPTION': requirement.get('TABLE_DESCRIPTION', '')
        }
        # set status to in progress?

        print("\033[34mWorking on requirement:", filtered_requirement, "\033[0m")

        update_todo_status(client=client, todo_id=todo['todo_id'], new_status='IN_PROGRESS', bot_id=pm_bot_id)
        
        paths = setup_paths(requirement["PHYSICAL_COLUMN_NAME"], run_number=run_number, genesis_db = args.genesis_db, project_id=project_id)

        # create and tag the git project assets for this requirement

        source_research_results = perform_source_research_v2(client, filtered_requirement, paths, source_research_bot_id, pm_bot_id=pm_bot_id, project_id=project_id, deng_project_config=deng_project_config, index_id=index_id)

        if source_research_results.get('success'):
            source_research = source_research_results['contents']['source_research']
           # source_research_path = source_research_results['git_file_paths']['source_research']
           # source_research_thread = source_research_results['thread']

            # These steps are now handled in perform_source_research_v2
            mapping_proposal = source_research_results['contents']['mapping_proposal']
         #   mapping_proposal_path = source_research_results['git_file_paths']['mapping_proposal']
         #   mapping_proposal_thread = source_research_results['thread']
            confidence_report = source_research_results['contents']['confidence_report']

            proposal_made = source_research_results['contents']['proposal_made']
            questions = source_research_results['contents']['questions']
    
            # Get the full content of each file from git
            source_research_content = source_research
            mapping_proposal_content = mapping_proposal        

            # Only perform PM summary if a mapping was proposed
            if proposal_made:
                summary_results = perform_pm_summary(client, filtered_requirement, paths, pm_bot_id, skip_confidence)
                summary = summary_results['summary']
            else:
                # Set all summary fields to None if no mapping was proposed
                summary = {
                    'UPSTREAM_DB_CONNECTION': None,
                    'UPSTREAM_TABLE': None,
                    'UPSTREAM_COLUMN': None,
                    'TRANSFORMATION_LOGIC': None,
                    'CONFIDENCE_SCORE': None
                }

            # Evaluate results
            if requirement['CORRECT_ANSWER_FOR_EVAL'] and proposal_made:
                evaluation, eval_json, thread = evaluate_results(client, filtered_requirement=filtered_requirement, pm_bot_id=pm_bot_id, mapping_proposal_content=mapping_proposal_content, correct_answer_for_eval=requirement['CORRECT_ANSWER_FOR_EVAL'])
                record_work(client=client, todo_id=todo['todo_id'], description=f"Completed evaluation for column: {requirement['PHYSICAL_COLUMN_NAME']}, via thread: {thread}", bot_id=pm_bot_id, results=evaluation, thread_id=thread)     
            else:
                evaluation = None
                eval_json = None
            
            # Prepare fields for database update
            db_fields = {
                'proposal_made': proposal_made,
                'upstream_table': summary['UPSTREAM_TABLE'],
                'upstream_column': summary['UPSTREAM_COLUMN'],
                'upstream_db_connection': summary['UPSTREAM_DB_CONNECTION'],
                'source_research': source_research_content,
                'mapping_proposal': mapping_proposal_content,
                'confidence_output': confidence_report,
                'confidence_score': summary['CONFIDENCE_SCORE'],
                'transformation_logic': summary['TRANSFORMATION_LOGIC'],
                'mapping_correct_flag': eval_json['MAPPING_CORRECT_FLAG'] if eval_json and eval_json['MAPPING_CORRECT_FLAG'] is not None else '',
                'mapping_issues': eval_json['MAPPING_ISSUES'] if eval_json and eval_json['MAPPING_ISSUES'] is not None else '',
                'status': 'READY_FOR_REVIEW' if proposal_made else 'QUESTIONS_POSED',
                'questions': questions,
                'proposal_made': proposal_made
            }

            # Save results of work to database
            save_pm_summary_to_requirements(
                requirement['PHYSICAL_COLUMN_NAME'],
                db_fields,
                requirements_table_name
            )
            print("\033[32mSuccessfully saved results to database for requirement:", requirement['PHYSICAL_COLUMN_NAME'], "\033[0m")
            record_work(client=client, todo_id=todo['todo_id'], description=f"Completed database update for column: {requirement['PHYSICAL_COLUMN_NAME']}", bot_id=pm_bot_id, results=None, thread_id=None)     

            # if correct, ready for review, otherwise needs help
            # Update todo status to complete
            update_todo_status(client=client, todo_id=todo['todo_id'], new_status='COMPLETED', bot_id=pm_bot_id)

        else:
            raise('Error performing source research')
        # Update the Google Sheet with mapping results

        try:
            if gsheet_location:
                update_gsheet_with_mapping(
                    client=client,
                    filtered_requirement=filtered_requirement,
                    summary=summary,
                    gsheet_location=gsheet_location,
                    pm_bot_id=pm_bot_id,
                    source_research_content=source_research_content,
                    mapping_proposal_content=mapping_proposal_content,
                    mapping_correct_flag=eval_json['MAPPING_CORRECT_FLAG'] if eval_json and eval_json['MAPPING_CORRECT_FLAG'] is not None else '',
                    mapping_issues=eval_json['MAPPING_ISSUES'] if eval_json and eval_json['MAPPING_ISSUES'] is not None else '',
                    confidence_output=confidence_report,
                    questions=questions,
                    proposal_made=proposal_made
                )
                print(f"\033[32mSuccessfully updated Google Sheet for requirement: {requirement['PHYSICAL_COLUMN_NAME']}\033[0m")
                record_work(
                    client=client,
                    todo_id=todo['todo_id'],
                    description=f"Updated [Google Sheet]({gsheet_location}) for column: {requirement['PHYSICAL_COLUMN_NAME']}. ",
                    bot_id=pm_bot_id,
                    results=None
                )
            else:
                print("\033[33mSkipping Google Sheet update - no sheet location provided\033[0m")
            
        except Exception as e:
            print(f"\033[31mError updating Google Sheet: {e}\033[0m")
            record_work(
                client=client,
                todo_id=todo['todo_id'],
                description=f"Error updating Google Sheet: {e}",
                bot_id=pm_bot_id,
                results=None
            )

        if filename:
            print(f"\033[32mFull output saved to: {filename}\033[0m")
            record_work(
                client=client,
                todo_id=todo['todo_id'],
                description=f"Full output saved to: {filename}",
                bot_id=pm_bot_id,
                results=None
            )

    except Exception as e:
        print(f"\033[31mError occurred: {e}\033[0m")
        record_work(client=client, todo_id=todo['todo_id'], description=f"Error occurred: {e}", bot_id=pm_bot_id, results=None, thread_id=None)     

        # Save error state to database for this field
        error_fields = {
            'upstream_table': None,
            'upstream_column': None,
            'upstream_db_connection': None,
            'source_research': str(e),
            'mapping_proposal': None,
            'confidence_output': None,
            'confidence_score': 0,
            'transformation_logic': None,
            'mapping_correct_flag': 'error',
            'mapping_issues': f'Error: {str(e)}',
            'status': 'ERROR'
        }

        if requirement is not None and 'PHYSICAL_COLUMN_NAME' in requirement:
            save_pm_summary_to_requirements(
                requirement['PHYSICAL_COLUMN_NAME'],
                error_fields,
                requirements_table_name
            )
        # Update the Google Sheet with error state
            print(f"\033[33mSaved error state to database for requirement: {requirement['PHYSICAL_COLUMN_NAME']}\033[0m")
            record_work(client=client, todo_id=todo['todo_id'], description=f"Saved error state to database for requirement: {requirement['PHYSICAL_COLUMN_NAME']}", bot_id=pm_bot_id, results=None, thread_id=None)     

        # Update todo status to complete
        update_todo_status(client=client, todo_id=todo['todo_id'], new_status='ON_HOLD', bot_id=pm_bot_id)

def initialize_document_indices(client, bot_id, project_id, project_config):
    """
    Initialize multiple document indices based on configuration and populate them with files.
    Preserves subdirectory structure when saving to git and indexing.
    
    Args:
        client: Genesis API client instance
        bot_id: Bot ID to use for operations
        project_id: Project ID
        deng_project_config: Project configuration containing document_indices
        
    Returns:
        dict: Mapping of index names to their IDs
    """
    if not project_config or 'document_indices' not in project_config:
        print("No document indices configured in project config")
        return {}

    indices = {}
    
    for index_config in project_config['document_indices']:
        index_name = index_config['index_name']
        path_to_files = index_config['path_to_files']
        
        # Create index name with project prefix
        full_index_name = f"{index_name}"
        
        try:
            # Check if index exists
            result = client.run_genesis_tool(
                tool_name="document_index",
                params={
                    "action": "CREATE_INDEX",
                    "index_name": full_index_name,
                    "bot_id": bot_id
                },
                bot_id=bot_id
            )
            
            if 'Error' in result and result['Error'] == 'Index with the same name already exists':
                print(f"Document index {full_index_name} already exists, removing")
                client.run_genesis_tool(
                    tool_name="document_index",
                    params={
                        "action": "DELETE_INDEX",
                        "index_name": full_index_name,
                        "bot_id": bot_id
                    },
                    bot_id=bot_id
                )
                # Recreate the index
                client.run_genesis_tool(
                    tool_name="document_index",
                    params={
                        "action": "CREATE_INDEX",
                        "index_name": full_index_name,
                        "bot_id": bot_id
                    },
                    bot_id=bot_id
                )
            
            print(f"Created document index: {full_index_name}")
            
            # Walk through directory and add files
            i = 0
            for root, dirs, files in os.walk(path_to_files):
                for file in files:
                    i += 1
                    # Skip hidden files
                    if file.startswith('.'):
                        continue

                    local_path = os.path.join(root, file)
                    
                    # Preserve subdirectory structure relative to path_to_files
                    rel_path = os.path.relpath(root, path_to_files)
                    if rel_path == '.':
                        rel_path = ''
                        
                    # Construct git path preserving subdirectories
                    git_path = os.path.join(
                        project_id,
                        "input_files",
                        index_name,
                        rel_path
                    ).replace('\\', '/')  # Ensure forward slashes for git paths
                    
                    # Put file in git, preserving directory structure
                    put_git_file(
                        client=client,
                        local_file=local_path,
                        git_file_path=git_path + '/',
                        file_name=file,
                        bot_id=bot_id,
                    )
                    
                    # Add to index with full path
                    full_git_path = f"{git_path}/{file}".replace('//', '/')
                    resp = client.run_genesis_tool(
                        tool_name="document_index",
                        params={
                            "action": "ADD_DOCUMENTS",
                            "index_name": full_index_name,
                            "filepath": f"BOT_GIT:{full_git_path}",
                        },
                        bot_id=bot_id
                    )
                    
                    if resp['Success'] == True:
                        print(f'...indexed file {local_path} in {full_index_name}, and saved it to git at {full_git_path} (#{i})')
                    else:
                        print(f'\033[31mError indexing file {local_path} in {full_index_name}: {resp["Error"]}\033[0m')
                        a = input('Error indexing, press return to continue...')
            
            indices[index_name] = full_index_name
            
        except Exception as e:
            print(f"Error creating/populating index {full_index_name}: {e}")
            continue
    
    # Save indices mapping to temp file for later use
    temp_dir = os.path.join(os.getcwd(), 'tmp')
    os.makedirs(temp_dir, exist_ok=True)
    temp_file = os.path.join(temp_dir, f'{project_id}_indices.json')
    with open(temp_file, 'w') as f:
        json.dump(indices, f)
    print(f"Saved document indices mapping to {temp_file}")
    
    return indices

if __name__ == "__main__":
    main()

