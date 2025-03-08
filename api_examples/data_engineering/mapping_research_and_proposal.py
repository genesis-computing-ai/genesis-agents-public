import uuid
import json
import yaml
import argparse
from   genesis_bots.api         import GenesisAPI, build_server_proxy
from   genesis_bots.api.utils   import add_default_argparse_options
import os

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

def setup_paths(physical_column_name, run_number = 1, genesis_db = 'GENESIS_BOTS'):
    """Setup file paths and names for a given requirement."""
    stage_base = f"@{genesis_db}.APP1.bot_git/"
    base_git_path = f'requirements/run{run_number}/'

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
        # Read local file
        with open(local_file, 'r') as f:
            content = f.read()
            
        res = client.gitfiles.write(f"{git_file_path}{file_name}", content, bot_id=bot_id)
        return res
        
    except Exception as e:
        print(f"Error putting file to git: {str(e)}")
        return False



def perform_pm_summary(client, requirement, paths, bot_id,skip_confidence = False):
    """Have PM bot analyze results and provide structured summary."""
    print("\033[34mGetting PM summary...\033[0m")

    pm_prompt = f'''{message_prefix}Here are requirements for a target field I want you to work on: {requirement}\n
    The mapping proposer bot has saved its results in git at: {paths["base_git_path"]}{paths["mapping_proposal_file"]}\n'''

    if not skip_confidence:
        pm_prompt += f'''The confidence analyst bot has saved its results in git at: {paths["base_git_path"]}{paths["confidence_report_file"]}\n'''

    pm_prompt += f'''
    Please review above documents and provide a JSON response with the following fields:
    - UPSTREAM_TABLE: List of source table(s) needed for the data, with schema prefixes
    - UPSTREAM_COLUMN: List of source column(s) for the mapping
    - TRANSFORMATION_LOGIC: SQL snippet or natural language description of any needed transformations
    - CONFIDENCE_SCORE: Score from confidence bot report (if available)
    - CONFIDENCE_SUMMARY: Brief explanation of the confidence score (if available)
    - PM_BOT_COMMENTS: Your brief statement about the team's work and what specific feedback would be helpful from human reviewers

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
        required_fields = ["UPSTREAM_TABLE", "UPSTREAM_COLUMN", "TRANSFORMATION_LOGIC",
                         "CONFIDENCE_SCORE", "CONFIDENCE_SUMMARY", "PM_BOT_COMMENTS"]
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
                upstream_table = %(upstream_table)s,
                upstream_column = %(upstream_column)s,
                source_research = %(source_research)s,
                mapping_proposal = %(mapping_proposal)s,
                confidence_output = %(confidence_output)s,
                confidence_score = %(confidence_score)s,
                confidence_summary = %(confidence_summary)s,
                PM_BOT_COMMENTS = %(pm_bot_comments)s,
                transformation_logic = %(transformation_logic)s,
                status = %(status)s,
                which_mapping_correct = %(which_mapping_correct)s,
                primary_issues = %(primary_issues)s,
                secondary_issues = %(secondary_issues)s,
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
                escaped_value = str(value).replace("'", "''")
                query_with_params = query_with_params.replace(f'%({key})s', f"'{escaped_value}'")

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


def update_gsheet_with_mapping(client, filtered_requirement, summary, gsheet_location, pm_bot_id, source_research_content, mapping_proposal_content, which_mapping_correct, primary_issues, secondary_issues):
    """
    Update Google Sheet with mapping information from PM summary.

    Args:
        client: Genesis API client
        filtered_requirement: The requirement being processed
        summary: PM summary containing mapping details
        gsheet_location: URL/ID of Google Sheet to update
        pm_bot_id: ID of PM bot to use
        source_research_content: Content of source research
        mapping_proposal_content: Content of mapping proposal
        which_mapping_correct: Which mapping is correct
        correct_answer: The correct answer
        primary_issues: Primary issues identified
        secondary_issues: Secondary issues identified
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
            {'column': 'UPSTREAM_TABLE', 'value': summary['UPSTREAM_TABLE']},
            {'column': 'UPSTREAM_COLUMN', 'value': summary['UPSTREAM_COLUMN']},
            {'column': 'TRANSFORMATION_LOGIC', 'value': summary['TRANSFORMATION_LOGIC']},
            {'column': 'CONFIDENCE_SCORE', 'value': summary['CONFIDENCE_SCORE']},
            {'column': 'CONFIDENCE_SUMMARY', 'value': summary['CONFIDENCE_SUMMARY']},
            {'column': 'PM_BOT_COMMENTS', 'value': summary['PM_BOT_COMMENTS']},
            {'column': 'SOURCE_RESEARCH', 'value': source_research_content},
            {'column': 'MAPPING_PROPOSAL', 'value': mapping_proposal_content},
            {'column': 'WHICH_MAPPING_CORRECT', 'value': which_mapping_correct},
            {'column': 'PRIMARY_ISSUES', 'value': primary_issues},
            {'column': 'SECONDARY_ISSUES', 'value': secondary_issues},
            {'column': 'STATUS', 'value': 'READY_FOR_REVIEW'}
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

def evaluate_results(client, filtered_requirement=None, pm_bot_id=None, mapping_proposal_content=None, correct_answer_for_eval=None):
    """
    Evaluate the mapping results against known correct answers.

    Args:
        client: Genesis API client
        filtered_requirement: The requirement being processed
        paths: Dictionary of file paths
        pm_bot_id: ID of the PM bot to use
        source_research_content: Content of source research report
        mapping_proposal_content: Content of mapping proposal
        confidence_output_content: Content of confidence analysis
        summary: PM summary of the mapping

    Returns:
        Dictionary containing evaluation results
    """
    try:
        # Get the correct answers file
        correct_answer = correct_answer_for_eval

        # Now prepare full evaluation message
        message = {
            "requirement": filtered_requirement,
            "mapping_proposal": mapping_proposal_content,
            "correct_answer": correct_answer,
            "instruction": f"""{message_prefix}
                Please evaluate the mapping proposal results against the correct answer for this field.

                Compare the following aspects:
                1. Is the Primary mapping fully correct?
                2. Are the Primary option identified source tables/columns correct?
                3. Is the Primary option transformation logic correct?

                If the Primary option is not fully correct, check the Secondary option, if one is provided, and perform the same analysis on that option.

                Provide a detailed analysis of any discrepancies found and identify
                likely sources of errors in the process.

                Format your response as a JSON with these fields as follows:

                ### JSON Response

                ```json
                {{
                    "WHICH_MAPPING_CORRECT": "primary, secondary, or neither",
                    "CORRECT_ANSWER": "what is the correct answer text",
                    "PRIMARY_ISSUES": "'NONE' if primary is correct, or detailed text with the problems with primary if not correct",
                    "SECONDARY_ISSUES": "'NONE' if secondary is correct, detailed text with the problems with secondary if not correct",
                }}
                ```
                This is being run by an automated process so do not repeat these instructions back to me, and do not stop to ask for futher permission to proceed.
            """
        }

        # Send to PM bot for evaluation
        message_str = json.dumps(message)
        thread = str(uuid.uuid4())
        evaluation = call_genesis_bot(client, pm_bot_id, message_str, thread=thread)

        json_str = evaluation.split("```json\n")[-1].split("\n```")[0].strip()
        response = json_str
        # Basic validation that we got JSON back
        try:
            summary = json.loads(response)
            required_fields = ["WHICH_MAPPING_CORRECT", "CORRECT_ANSWER", "PRIMARY_ISSUES", "SECONDARY_ISSUES"]
            for field in required_fields:
                if field not in summary:
                    raise Exception(f"Missing required field {field} in PM summary")
        except json.JSONDecodeError:
            raise Exception("PM bot did not return valid JSON")

        print("\033[93mEvaluation results:\033[0m")
        print("\033[96m" + "-"*80 + "\033[0m")  # Cyan separator
        print(json.dumps(summary, indent=4, sort_keys=True, ensure_ascii=False))
        print("\033[96m" + "-"*80 + "\033[0m")  # Cyan separator
        if summary["WHICH_MAPPING_CORRECT"] != "neither":
            print("\033[92m" + "🎉🎉🎉🎉🎉🎉🎉🎉🎉🎉" + "\033[0m")
        else:
            print("\033[91m" + "🛑🛑🛑🛑🛑🛑🛑🛑🛑🛑" + "\033[0m")


        return evaluation, summary, thread

    except Exception as e:
        print(f"\033[31mError in evaluation: {str(e)}\033[0m")
        raise e


def port_bot(client_source, client_target, bot_id, suffix='local', remove_slack=True):
    """
    Port bots from remote to local environment.

    Args:
        client: Remote Genesis API client
        client_local: Local Genesis API client

    Use example:

        ## port bots from another server
        # client_remote = GenesisAPI("remote-snowflake", scope="GENESIS_BOTS_ALPHA")
        #  bots = client_remote.get_all_bots()
        #  print("Remote bots: ",bots)
        #  bots_to_port = [
        #       'RequirementsPM-72dj5k',
        #       'sourceResearchBot-d3k9m1',
        #       'mappingProposerBot-4fj7kf',
        #      'confidenceanalyst-xYzAb9'
        #   ]
        #  for bot_id in bots_to_port:
        #      port_bot(client_source=client_remote, client_target=client, bot_id=bot_id, suffix='jllocal')

    """

    # Get bot definition from remote
    remote_bot = client_source.get_bot(bot_id)

    new_bot = {
        "BOT_ID": f"{bot_id.split('-')[0]}-{suffix}",
        "BOT_NAME": remote_bot.BOT_NAME,
        "BOT_IMPLEMENTATION": remote_bot.BOT_IMPLEMENTATION,
        "FILES": remote_bot.FILES,
        "AVAILABLE_TOOLS": remote_bot.AVAILABLE_TOOLS,
    #  "BOT_AVATAR_IMAGE": remote_bot.BOT_AVATAR_IMAGE,
        "BOT_AVATAR_IMAGE": None,
        "BOT_INSTRUCTIONS": remote_bot.BOT_INSTRUCTIONS,
        "BOT_INTRO_PROMPT": remote_bot.BOT_INTRO_PROMPT,
        "DATABASE_CREDENTIALS": remote_bot.DATABASE_CREDENTIALS,
        "RUNNER_ID": os.getenv("RUNNER_ID", "genesis-runner-jl"),
        "UDF_ACTIVE": remote_bot.UDF_ACTIVE
    }

    bot_name = new_bot["BOT_NAME"]
    file_path = f"./tmp/ported_bots/{bot_name}.yaml"

    os.makedirs(os.path.dirname(file_path), exist_ok=True)

    with open(file_path, 'w') as file:
        yaml.dump(new_bot, file)

    print(f"New bot details saved to {file_path} for bot {new_bot['BOT_NAME']}")

    if remote_bot:
        # Register bot locally
        client_target.register_bot(new_bot)
        print(f"Registered bot {bot_id} locally")
    else:
        print(f"Could not find bot {bot_id} on remote server")


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

def perform_source_research_new(client, requirement, paths, bot_id, pm_bot_id=None, past_projects_list=None, project_id=None):
    """Execute source research step and validate results."""
    try:
        print("\033[34mExecuting source research...\033[0m")

        research_prompt = f'''{message_prefix}Here are requirements for a target field I want you to work on: {requirement}\n
        Save the results in git at: {paths["base_git_path"]}{paths["source_research_file"]}\n

        First explore the available data for fields that may be useful using data_explorer function.
        You may want to try a couple different search terms to make sure your search is comprehensive.
'''

        past_projects_str = ""
        if past_projects_list:
            for project in past_projects_list:
                past_projects_str += f"git_action(action='read_file',file_path='{project}')\n"

        research_prompt += f'''
        Then, consider these past projects in your past project consideration step, stored in git. Get it by calling:
        {past_projects_str}

        Be sure to use the git_action function, do NOT just hallucinate the contents of these files.
        Make SURE that you have read BOTH of these past project files, not just one of them.
     
        It is important to analyze BOTH the data explorer results, and ALSO the past project, and to discuss both in your report.
        When discussing past project in your report, describe their sources and transforms independently, don't say things like 'in the same way as described above', referring to the other project, even if you have to repeat things.

        When you're done, be sure to save your detailed results in git using the git_action function at {paths["base_git_path"]}{paths["source_research_file"]}
        Be sure to put a full copy of the contents of your research into that git location, not just a reference to "what you did above."
        Make sure your report contains the results of both your data exploration, and your analysis of **both** past projects.

        *** MAKE YOUR REPORT EXTREMELY DETAILED, WITH FULL DDL OF POTENTIAL SOURCE TABLES, AND FULL PAST PROJECT EXAMPLES OF SIMILAR FIELDS (SOURCING AND TRANSFORMS) ***
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
            retry_prompt = f'''I don't see the full results of your research saved at {paths["base_git_path"]}{paths["source_research_file"]} in git.  Please complete your analysis, and then save your work again using the git_action function.'''
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

def perform_mapping_proposal_new(client, requirement, paths, bot_id, pm_bot_id=None, project_id=None):
    f"""{message_prefix}Execute mapping proposal step and validate results."""
    print("\033[34mExecuting mapping proposal...\033[0m")

    mapping_prompt = f'''Here are requirements for a target field I want you to work on a mapping proposal for: {requirement}

    The source research bot has already run and saved its results at this git. First, read its report by calling:
    git_action(action='read_file',file_path='{paths["base_git_path"]}{paths["source_research_file"]}')

    Now, make a mapping proposal for this field.
    If there are two options that both look good, label the best as Primary and the other as Secondary (in a separate section, but also with full details.)

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
        retry_prompt = f'''I don't see the full results of your mapping proposal saved at {paths["base_git_path"]}{paths["mapping_proposal_file"]} in git, please complete your work, and then try the save using the git_action function.'''
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

def perform_confidence_analysis_new(client, requirement, paths, bot_id):
    """Execute confidence analysis step and validate results."""
    print("\033[34mExecuting confidence analysis...\033[0m")

    confidence_prompt = f'''Here are requirements for a target field I want you to analyze confidence for: {requirement}

    First read the source research report by calling:
    git_action(action='read_file',file_path='{paths["base_git_path"]}{paths["source_research_file"]}')

    Then read the mapping proposal by calling:
    git_action(action='read_file',file_path='{paths["base_git_path"]}{paths["mapping_proposal_file"]}')

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
    return parser.parse_args()


def run_snowflake_query(client, query, bot_id=None):
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
            "bot_id": bot_id
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

def push_knowledge_files_to_git(client, bot_id, past_projects_dir, past_projects_git_path):
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


def initialize_project(client, bot_id, project_id="de_requirements_mapping"):
    """Initialize project by deleting if exists and creating new."""
    print(f"\nInitializing project '{project_id}'...")

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
                    "project_name": "Data Engineering Requirements Mapping",
                    "description": "Project for mapping source system fields to target system requirements for Data Engineering"
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


def add_todos(client, requirements, bot_id, project_id="de_requirements_mapping", max_todos=-1):
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
Use _run_program tool to run the mapping_research_and_proposal program referencing this Todo's id.
            
Field Details:
- Physical Name: {req['PHYSICAL_COLUMN_NAME']}
- Logical Name: {req['LOGICAL_COLUMN_NAME']}
- Description: {req['COLUMN_DESCRIPTION']}
- Data Type: {req['DATA_TYPE']}
- Length: {req['LENGTH']}
- List of Values: {req['LIST_OF_VALUES']}
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

def record_work(client, todo_id, description, bot_id, results=None):
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
        result = client.run_genesis_tool(
            tool_name="record_todo_work",
            params={
                "todo_id": todo_id,
                "work_description": description,
                "work_results": str(results),
                "bot_id": bot_id
            },
            bot_id=bot_id
        )
        return result
    except Exception as e:
        print(f"Error recording work: {e}")
        raise e


def initialize_system(client, bot_id=None, pm_bot_id=None, genesis_db='GENESIS_BOTS', reset_all=False, folder_id=None, req_max=-1, project_id="de_requirements_mapping", past_projects_dir=None, past_projects_git_path=None, eval_answers_dir=None, eval_answers_git_path=None, eve_bot_id=None):
    """
    Initialize the requirements table in Snowflake if it doesn't exist.
    Loads data from test_requirements.json if table needs to be created.
    
    Args:
        client: Genesis API client instance
        bot_id: Bot ID to use for database operations (defaults to eve_bot_id)
    """
    # Try to query table to check if exists
    
    # Check if source data table exists
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
        harvested_databases = [entry['DATABASE_NAME'] for entry in harvest_data ]
        
        if 'HCLS' not in harvested_databases:
            print("\033[31mHCLS database is not being harvested. Adding it to harvest control.\033[0m")
            # Add HCLS database to harvest control
            try:
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
                print(f"\033[31mError adding HCLS to harvest control: {e}\033[0m")
                raise e

            
    except Exception as e:
        print(f"\033[31mError checking harvest control data: {e}\033[0m")
        raise e

    
    if not reset_all:
        check_query = f"SELECT COUNT(*) FROM {genesis_db}.{pm_bot_id.upper().replace('-','_')}_WORKSPACE.{project_id.upper().replace('-','_')}_REQUIREMENTS"
        res = run_snowflake_query(client, check_query, bot_id)
        if isinstance(res, list) and len(res) > 0:
            print("Table already exists, checking gsheet location...")
            # Load gsheet location from file
            try:
                with open('tmp/gsheet_location.txt', 'r') as f:
                    gsheet_location = f.read().strip()
                    print(f"Read gsheet location: {gsheet_location}")
            except Exception as e:
                print(f"Error reading gsheet location: {e}")
                gsheet_location = None
            return gsheet_location
    
    # initialize project
    initialize_project(client, pm_bot_id, project_id)

    push_knowledge_files_to_git(client, eve_bot_id, past_projects_dir, past_projects_git_path)

    push_knowledge_files_to_git(client, eve_bot_id, "data_engineering/eval_answers/", "data_engineering/eval_answers/")

    requirements_table_name = f"{genesis_db}.{pm_bot_id.upper()}_WORKSPACE.{project_id}_REQUIREMENTS".replace("-", "_")

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
        UPSTREAM_TABLE VARCHAR(16777216),
        UPSTREAM_COLUMN VARCHAR(16777216),
        SOURCE_RESEARCH VARCHAR(16777216),
        MAPPING_PROPOSAL VARCHAR(16777216),
        CONFIDENCE_OUTPUT VARCHAR(16777216),
        CONFIDENCE_SCORE VARCHAR(16777216),
        CONFIDENCE_SUMMARY VARCHAR(16777216),
        PM_BOT_COMMENTS VARCHAR(16777216),
        TRANSFORMATION_LOGIC VARCHAR(16777216),
        STATUS VARCHAR(16777216),
        WHICH_MAPPING_CORRECT VARCHAR(10),
        PRIMARY_ISSUES VARCHAR(16777216),
        SECONDARY_ISSUES VARCHAR(16777216),
        CORRECT_ANSWER_FOR_EVAL VARCHAR(16777216)
        );   """
    run_snowflake_query(client, create_query, bot_id)
    
    # Load data from JSON file
    json_path = os.path.join(os.path.dirname(__file__), 'field_requirements.json')
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
            '{req['LOGICAL_TABLE_NAME'].replace("'", "''")}',
            '{req['TABLE_DESCRIPTION'].replace("'", "''")}',
            '{req['PHYSICAL_TABLE_NAME'].replace("'", "''")}',
            '{req['PHYSICAL_COLUMN_NAME'].replace("'", "''")}',
            '{req['LOGICAL_COLUMN_NAME'].replace("'", "''")}',
            '{req['COLUMN_DESCRIPTION'].replace("'", "''")}',
            '{req['DATA_TYPE'].replace("'", "''")}',
            '{str(req['LENGTH']).replace("'", "''")}',
            '{str(req['LIST_OF_VALUES']).replace("'", "''")}',
            'NEW',
            '{req['CORRECT_ANSWER_FOR_EVAL'].replace("'", "''")}'
        )
        """
        run_snowflake_query(client, insert_query, bot_id)
    gsheet_location = export_table_to_gsheets(client, requirements_table_name, 'Data Engineering Requirements Mapping', pm_bot_id)
    # Save gsheet location to file
    # Ensure tmp directory exists
    os.makedirs('tmp', exist_ok=True)
    with open('tmp/gsheet_location.txt', 'w') as f:
        f.write(str(gsheet_location['file_url']))

    print("Adding todos...")
    add_todos(client, requirements, pm_bot_id, max_todos=req_max)

    return gsheet_location['file_url']


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

    # LOAD AND ACTIVATE BOTS FROM YAML FILES
    pm_bot_id = 'DEng-requirementsPM'
    source_research_bot_id = 'DEng-sourceResearchBot'
    mapping_proposer_bot_id = 'DEng-mappingProposerBot'
    confidence_analyst_bot_id = 'DEng-confidenceAnalystBot'
    etl_engineer_bot_id = 'DEng-ETLEngineerBot'
    project_id = 'de_requirements_mapping'

    skip_confidence = True

    bot_team_path = os.path.join(os.path.dirname(__file__), 'bot_team')
    past_projects_dir =os.path.join(os.path.dirname(__file__), 'knowledge/past_projects')
    past_projects_git_path = 'DEng/knowledge/past_projects'
    eval_answers_dir = os.path.join(os.path.dirname(__file__), 'knowledge/eval_answers')
    eval_answers_git_path = 'DEng/knowledge/eval_answers'
    

    if not args.todo_id:
        # If a specific todo_id is provided, filter the todos to only include this one
    
        load_bots = False
        if load_bots:
            # make the runner_id overrideable
            load_bots_from_yaml(client=client, bot_team_path=bot_team_path) # , onlybot=source_research_bot_id)  # takes bot definitions from yaml files at the specified path and injects/updates those bots into the running local server
        else:
            print("Skipping bot loading, using existing bots")

        # Initialize requirements table if not exists 

        reset_all = False
        req_max = -1
    else:
        reset_all = False
        req_max = -1
    
    gsheet_location = initialize_system(client, pm_bot_id, genesis_db = args.genesis_db, pm_bot_id = pm_bot_id, reset_all = reset_all, req_max = req_max, past_projects_dir = past_projects_dir, past_projects_git_path = past_projects_git_path, project_id = project_id, eval_answers_dir = eval_answers_dir, eval_answers_git_path = eval_answers_git_path, eve_bot_id = eve_bot_id)
    
    todos = get_todos(client, "de_requirements_mapping", pm_bot_id, todo_id=args.todo_id) 

    past_projects_list = get_past_project_git_locations(client, past_projects_dir, past_projects_git_path, pm_bot_id)

    run_number = 1;

    requirements_table_name = f"{args.genesis_db}.{pm_bot_id.upper()}_WORKSPACE.{project_id.upper()}_REQUIREMENTS".replace("-", "_")

    requirements_query = f"SELECT * FROM {requirements_table_name}"
    requirements = run_snowflake_query(client, requirements_query, eve_bot_id)

    for todo in todos:
        field_name = todo['todo_name'][11:]
        print(f"\033[34mProcessing todo item: {todo['todo_name']}\033[0m")
        print(f"\033[34mLooking up requirement for field: {field_name}\033[0m")

        requirement = next((req for req in requirements if req['PHYSICAL_COLUMN_NAME'] == field_name), None)
        try:
            filtered_requirement = {
                'PHYSICAL_COLUMN_NAME': requirement['PHYSICAL_COLUMN_NAME'],
                'LOGICAL_COLUMN_NAME': requirement['LOGICAL_COLUMN_NAME'],
                'COLUMN_DESCRIPTION': requirement['COLUMN_DESCRIPTION'],
                'DATA_TYPE': requirement['DATA_TYPE'],
                'LENGTH': requirement['LENGTH'],
                'LIST_OF_VALUES': requirement['LIST_OF_VALUES']
            }
            # set status to in progress?

            print("\033[34mWorking on requirement:", filtered_requirement, "\033[0m")

            update_todo_status(client=client, todo_id=todo['todo_id'], new_status='IN_PROGRESS', bot_id=pm_bot_id)
            
            paths = setup_paths(requirement["PHYSICAL_COLUMN_NAME"], run_number=run_number, genesis_db = args.genesis_db)

            # create and tag the git project assets for this requirement

            source_research_results = perform_source_research_new(client, filtered_requirement, paths, source_research_bot_id, pm_bot_id=pm_bot_id, past_projects_list=past_projects_list, project_id=project_id)
            #source_research_results = {
            #    'success': True,
            #    'contents': 'source research contents',
            #    'git_file_path': 'source_research_file.txt',
            #    'thread': 'source_research_thread'
            #}

        
            if source_research_results.get('success'):  
                source_research = source_research_results['contents']
                git_file_path = source_research_results['git_file_path']
                source_research_thread = source_research_results['thread']
                # add ability to tag the threads in message log that the work was done by          
                record_work(client=client, todo_id=todo['todo_id'], description=f"Completed source research for column: {requirement['PHYSICAL_COLUMN_NAME']}, results in: {git_file_path}, via thread: {source_research_thread}", bot_id=pm_bot_id, results=source_research)     

            mapping_proposal_results = perform_mapping_proposal_new(client, filtered_requirement, paths, mapping_proposer_bot_id, pm_bot_id=pm_bot_id, project_id=project_id)
            #mapping_proposal_results = {
            #    'success': True,
            #    'contents': 'mapping proposal contents',
            #    'git_file_path': 'mapping_proposal_file.txt',
            #    'thread': 'mapping_proposal_thread'
            #}

            if mapping_proposal_results.get('success'):
                mapping_proposal = mapping_proposal_results['contents']
                git_file_path = mapping_proposal_results['git_file_path'] 
                mapping_proposal_thread = mapping_proposal_results['thread']

                record_work(client=client, todo_id=todo['todo_id'], description=f"Completed mapping proposal for column: {requirement['PHYSICAL_COLUMN_NAME']}, results in: {git_file_path}, via thread: {mapping_proposal_thread}", bot_id=pm_bot_id, results=mapping_proposal)     

            if not skip_confidence:
                confidence_report = perform_confidence_analysis_new(client, filtered_requirement, paths, confidence_analyst_bot_id)
                record_work(client=client, todo_id=todo['todo_id'], description=f"Completed confidence analysis for column: {requirement['PHYSICAL_COLUMN_NAME']}", bot_id=pm_bot_id, results=confidence_report)     

            summary_results = perform_pm_summary(client, filtered_requirement, paths, pm_bot_id, skip_confidence)
            summary = summary_results['summary']
            record_work(client=client, todo_id=todo['todo_id'], description=f"Completed PM summary for column: {requirement['PHYSICAL_COLUMN_NAME']}, via thread: {summary_results['thread']}", bot_id=pm_bot_id, results=summary_results)     
        
            # Get the full content of each file from git
            source_research_content = source_research
            mapping_proposal_content = mapping_proposal
            
            confidence_output_content = 'bypassed currently while we improve this bot'
            # Evaluate results
            if requirement['CORRECT_ANSWER_FOR_EVAL']:
                evaluation, eval_json, thread = evaluate_results(client, filtered_requirement=filtered_requirement, pm_bot_id=pm_bot_id, mapping_proposal_content=mapping_proposal_content, correct_answer_for_eval=requirement['CORRECT_ANSWER_FOR_EVAL'])
                record_work(client=client, todo_id=todo['todo_id'], description=f"Completed evaluation for column: {requirement['PHYSICAL_COLUMN_NAME']}, via thread: {thread}", bot_id=pm_bot_id, results=evaluation)     
            else:
                evaluation = None
                eval_json = None
            
            # Prepare fields for database update
            db_fields = {
                'upstream_table': summary['UPSTREAM_TABLE'],
                'upstream_column': summary['UPSTREAM_COLUMN'],
                'source_research': source_research_content,
                'mapping_proposal': mapping_proposal_content,
                'confidence_output': confidence_output_content,
                'confidence_score': summary['CONFIDENCE_SCORE'],
                'confidence_summary': summary['CONFIDENCE_SUMMARY'],
                'pm_bot_comments': summary['PM_BOT_COMMENTS'],
                'transformation_logic': summary['TRANSFORMATION_LOGIC'],
                'which_mapping_correct': eval_json['WHICH_MAPPING_CORRECT'] if eval_json and eval_json['WHICH_MAPPING_CORRECT'] is not None else '',
                'primary_issues': eval_json['PRIMARY_ISSUES'] if eval_json and eval_json['PRIMARY_ISSUES'] is not None else '',
                'secondary_issues': eval_json['SECONDARY_ISSUES'] if eval_json and eval_json['SECONDARY_ISSUES'] is not None else '',
                'status': 'READY_FOR_REVIEW'
            }

            # Save results of work to database
            save_pm_summary_to_requirements(
                requirement['PHYSICAL_COLUMN_NAME'],
                db_fields,
                requirements_table_name
            )
            print("\033[32mSuccessfully saved results to database for requirement:", requirement['PHYSICAL_COLUMN_NAME'], "\033[0m")
            record_work(client=client, todo_id=todo['todo_id'], description=f"Completed database update for column: {requirement['PHYSICAL_COLUMN_NAME']}", bot_id=pm_bot_id, results=None)     

            # if correct, ready for review, otherwise needs help
            # Update todo status to complete
            update_todo_status(client=client, todo_id=todo['todo_id'], new_status='COMPLETED', bot_id=pm_bot_id)

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
                        which_mapping_correct=eval_json['WHICH_MAPPING_CORRECT'] if eval_json and eval_json['WHICH_MAPPING_CORRECT'] is not None else '',
                        primary_issues=eval_json['PRIMARY_ISSUES'] if eval_json and eval_json['PRIMARY_ISSUES'] is not None else '',
                        secondary_issues=eval_json['SECONDARY_ISSUES'] if eval_json and eval_json['SECONDARY_ISSUES'] is not None else ''
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

        except Exception as e:
            print(f"\033[31mError occurred: {e}\033[0m")
            record_work(client=client, todo_id=todo['todo_id'], description=f"Error occurred: {e}", bot_id=pm_bot_id, results=None)     

            # Save error state to database for this field
            error_fields = {
                'upstream_table': None,
                'upstream_column': None,
                'source_research': str(e),
                'mapping_proposal': None,
                'confidence_output': None,
                'confidence_score': 0,
                'confidence_summary': None,
                'pm_bot_comments': f'Error: {str(e)}',
                'transformation_logic': None,
                'which_mapping_correct': 'error',
                'primary_issues': f'Error: {str(e)}',
                'secondary_issues': None,
                'status': 'ERROR'
            }

            save_pm_summary_to_requirements(
                requirement['PHYSICAL_COLUMN_NAME'],
                error_fields,
                requirements_table_name
            )
            # Update the Google Sheet with error state
            print(f"\033[33mSaved error state to database for requirement: {requirement['PHYSICAL_COLUMN_NAME']}\033[0m")
            record_work(client=client, todo_id=todo['todo_id'], description=f"Saved error state to database for requirement: {requirement['PHYSICAL_COLUMN_NAME']}", bot_id=pm_bot_id, results=None)     

            # Update todo status to complete
            update_todo_status(client=client, todo_id=todo['todo_id'], new_status='ON_HOLD', bot_id=pm_bot_id)

    # Return success status
    return {
        "success": True
    }

                    # update the row in the g-sheet with the mapping and results

        #i = input('press return to continue (this is for runaway prevention when testing...) ')


if __name__ == "__main__":
    main()

