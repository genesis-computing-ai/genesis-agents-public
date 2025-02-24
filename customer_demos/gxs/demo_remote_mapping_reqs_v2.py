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
    # Get git base path from environment variable, default to ./bot_git

  #  res = client.run_genesis_tool(tool_name="git_action", params={"action": "read_file", "file_path": f"{paths['base_git_path']}{file_name}" }, bot_id=bot_id)

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

    response = call_genesis_bot(client, bot_id, pm_prompt)

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
    return summary


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
                git_source_research_stage_link = %(git_source_research_stage_link)s,
                mapping_proposal = %(mapping_proposal)s,
                git_mapping_proposal_stage_link = %(git_mapping_proposal_stage_link)s,
                confidence_output = %(confidence_output)s,
                git_confidence_output_stage_link = %(git_confidence_output_stage_link)s,
                confidence_score = %(confidence_score)s,
                confidence_summary = %(confidence_summary)s,
                PM_BOT_COMMENTS = %(pm_bot_comments)s,
                transformation_logic = %(transformation_logic)s,
                status = %(status)s,
                evaluation_results = %(evaluation_results)s,
                which_mapping_correct = %(which_mapping_correct)s,
                correct_answer = %(correct_answer)s,
                primary_issues = %(primary_issues)s,
                secondary_issues = %(secondary_issues)s
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


def evaluate_results(client, paths, filtered_requirement, pm_bot_id, source_research_content, mapping_proposal_content, confidence_output_content, summary):
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

        answers_content = check_git_file(client, {"base_git_path": "knowledge/flexicard_eval_answers/"}, file_name="flexicard_answers_clean2.txt", bot_id=eve_bot_id)

        # First get just the correct answer for this field
        message = {
            "requirement": filtered_requirement,
            "correct_answers": answers_content,
            "instruction": f"""{message_prefix}
                You are helping to evaluate a ETL mapping that has been proposed by another AI Bot.

                To help in that evaluation, please extract ONLY the correct answer for this specific field from the correct_answers field I provdier above (NOT from your uploaded documents.)

                The field name to look for is in the requirement.  Return both the correct database, schema, table and column for any source columns,
                and also any required transformation logic.

                Format your response as a simple JSON with one field:
                {{
                    "correct_answer": "the correct answer text for this specific field, including DATABASE.SCHEMA.TABLE and Column information and any required mappings or transformations"
                }}

                We will then use your extracted correct answer to judge the output of the other bot.
            """
        }

        # Get the correct answer first
        message_str = json.dumps(message)
        correct_answer = call_genesis_bot(client, pm_bot_id, message_str)

        # Now prepare full evaluation message
        message = {
            "requirement": filtered_requirement,
       #     "source_research": source_research_content,
            "mapping_proposal": mapping_proposal_content,
       #     "confidence_report": confidence_output_content,
      #      "pm_summary": summary,
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
        evaluation = call_genesis_bot(client, pm_bot_id, message_str)

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


        return evaluation, summary

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

def perform_source_research_new(client, requirement, paths, bot_id):
    """Execute source research step and validate results."""
    try:
        print("\033[34mExecuting source research...\033[0m")

        # Create placeholder file
   #     success = write_file_to_stage(f'{paths["stage_base"]}{paths["base_git_path"]}',
   #                                  paths["source_research_file"],
   #                                  "Placeholder for source research")
   #     if not success:
   #         raise Exception("Failed to put placeholder source research file to stage")



        research_prompt = f'''{message_prefix}Here are requirements for a target field I want you to work on: {requirement}\n
        Save the results in git at: {paths["base_git_path"]}{paths["source_research_file"]}\n

        First explore the available data for fields that may be useful using data_explorer function.
        You may want to try a couple different search terms to make sure your search is comprehensive.

        HINT: We are only interested in data sourced from these tables (this is my Focus Tables List):

        bronze.lending_loan_core.asset_account
        bronze.core_accounts_service.customer_account
        bronze.lending_loan_core.card_summary
        bronze.lending_loan_core.billing_cycle_detail
        bronze.lending_loan_core.billing_cycle_payment_info
        bronze.cards_digicard_core.card
        bronze.lending_loan_app.application
        bronze.lending_loan_app.applicant_scoring_model_details
        bronze.lending_cr_decision_eng.application_credit_decision
        bronze.lending_loan_core.asset_account_parameter
        bronze.lending_loan_core.asset_account_restriction

        Then, consider these TWO past projects to in your past project consideration step, stored in git. Get it by calling:
        1. git_action(action='read_file',file_path='knowledge/past_projects/loan_data_project_clean2.txt')
        2. git_action(action='read_file',file_path='knowledge/past_projects/loan_lending_project_clean2.txt')

        Be sure to use the git_action function, do NOT just halucinate the contents of these files.
        Make SURE that you have ready BOTH of these past project files, not just one of them.
        HINT: This past project is useful mostly to see the logic and transforms used for similar fields, but is a different kind of loan (installment vs card) so a lot of
        the source tables we need to use for our project will not be the same, and the logic may vary for our project as its for credit cards not installment loans.
        Use your intelligence to determine when a past mapping may apply vs not apply due to the differences between installment loans and credit cards and highlight any such thoughts.

        It is important to analyze BOTH the data explorer results, and ALSO the past project, and to discuss both in your report.
        When discussing past project in your report, describe their sources and transforms independently, don't say thing like 'in the same way as described above', referring to the other project, even if you have to repeat things.

        When you're done, be sure to save your detailed results in git using the git_action function at {paths["base_git_path"]}{paths["source_research_file"]}
        Be sure to put a full copy of the contents of your research into that git location, not just a reference to "what you did above."
        Make sure your report contains the results of both your data exploration, and your analysis of **both** past projects.

        *** MAKE YOUR REPORT EXTREMELY DETAILED, WITH FULL DDL OF POTENTIAL SOURCE TABLES, AND FULL PAST PROJECT EXAMPLES OF SIMILAR FIELDS (SOURCING AND TRANSFORMS) ***
        *** THE READER OF THIS REPORT WILL NOT HAVE ACCESS TO ANY OTHER RESOURCES AND IT WILL NEED TO PROPOSE MAPPINGS BASED SOLELY ON YOUR REPORT ***

        This is being run by an automated process, so do not repeat these instructions back to me, simply proceed to execute them without asking for further approval.
        '''

        thread = str(uuid.uuid4())
        response = call_genesis_bot(client, bot_id, research_prompt, thread = thread)


        contents = check_git_file(client,paths=paths, file_name=paths["source_research_file"], bot_id=eve_bot_id)

        if not contents or len(contents) < 100 or isinstance(contents, dict) and 'error' in contents and 'File not found' in contents['error']:
            retry_prompt = f'''I don't see the full results of your research saved at {paths["base_git_path"]}{paths["source_research_file"]} in git.  Please complete your analysis, and then save your work again using the git_action function.'''
            response = call_genesis_bot(client, bot_id, retry_prompt, thread = thread)
            contents = check_git_file(client,paths=paths, file_name=paths["source_research_file"], bot_id=eve_bot_id)
            if not contents or len(contents) < 100 or isinstance(contents, dict) and 'error' in contents and 'File not found' in contents['error']:
                raise Exception('Source research file not found or contains only placeholder')
        print_file_contents("SOURCE RESEARCH",
                           f"{paths['base_git_path']}{paths['source_research_file']}",
                           contents)
        return contents

    except Exception as e:
        raise e

#  HINT: When past projects conflict on how to map this field, consider favoring the loan_lending_project.txt project as it is a closer analogue for this project at hand as the Primary option.

def perform_mapping_proposal_new(client, requirement, paths, bot_id):
    f"""{message_prefix}Execute mapping proposal step and validate results."""
    print("\033[34mExecuting mapping proposal...\033[0m")

    mapping_prompt = f'''Here are requirements for a target field I want you to work on a mapping proposal for: {requirement}

    The source research bot has already run and saved its results at this git. First, read its report by calling:
    git_action(action='read_file',file_path='{paths["base_git_path"]}{paths["source_research_file"]}')

    Now, make a mapping proposal for this field.
    If there are two options that both look good, label the best as Primary and the other as Secondary (in a separate section, but also with full details.)

    HINT: Do NOT suggest mappings based on any of these tables, as they are related to installment loans not Cards:
        LOAN_DATA, LOAN_SUMMARY, LOAN_REPAYMENT_SCHEDULE, LOAN_REPAYMENT_DETAIL, LOAN_RECOVERY_DETAIL, LOAN_DISBURSEMENT_DETAIL, LOAN_TRANSACTION_REPAYMENT_SCHEDULE_MAPPING
        **DO NOT USE LOAN_DATA AS A SOURCE FOR YOUR MAPPINGS, IT IS NOT A VALID SOURCE FOR THIS PROJECT**    HINT: Use COALESCE() in your mappings to handle potential null values on numerical (but not date) fields, often for numbers for example a NULL would imply a 0.

    HINT: If you need details about how acct_block_code is generated, you can read a special report about that field by calling:
    git_action(action='read_file',file_path='knowledge/past_projects/acct_block_code.txt')

    Then save your full results at this git location using git_action: {paths["base_git_path"]}{paths["mapping_proposal_file"]}
    Don't forget use use git_action to save your full and complete mapping results.  Don't just put "see above" or similar as this file will be read by another bot who will not see your full completion output.

    This is being run by an automated process, so do not repeat these instructions back to me, simply proceed to execute them without asking for further approval.'''

    thread = str(uuid.uuid4())
    response = call_genesis_bot(client, bot_id, mapping_prompt, thread=thread)

    contents = check_git_file(client,paths=paths, file_name=paths["mapping_proposal_file"], bot_id=eve_bot_id)

    if not contents or len(contents) < 100 or isinstance(contents, dict) and 'error' in contents and 'File not found' in contents['error']:
        retry_prompt = f'''I don't see the full results of your mapping proposal saved at {paths["base_git_path"]}{paths["mapping_proposal_file"]} in git, please complete your work, and then try the save using the git_action function.'''
        response = call_genesis_bot(client, bot_id, retry_prompt, thread=thread)
        contents = check_git_file(client,paths=paths, file_name=paths["mapping_proposal_file"], bot_id=eve_bot_id)
        if not contents or len(contents) < 100 or isinstance(contents, dict) and 'error' in contents and 'File not found' in contents['error']:
            raise Exception('Mapping proposal file not found or contains only placeholder')

    print_file_contents("MAPPING PROPOSAL",
                       f"{paths['base_git_path']}{paths['mapping_proposal_file']}",
                       contents)
    return contents

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
            "max_rows": 100
        }, 
        bot_id=bot_id
    )
    print("Sent to G-sheets results:")
    print(res)
    return res

def push_knowledge_files_to_git(client, bot_id):
    """
    Push knowledge base files to git repository.
    
    Args:
        client: Genesis API client instance
        bot_id: Bot ID to use for git operations
    """
    files_to_push = {
        "./customer_demos/gxs/knowledge/flexicard_eval_answers/flexicard_answers_clean2.txt": 
            "knowledge/flexicard_eval_answers/flexicard_answers_clean2.txt",
        "./customer_demos/gxs/knowledge/past_projects/loan_data_project_clean2.txt":
            "knowledge/past_projects/loan_data_project_clean2.txt",
        "./customer_demos/gxs/knowledge/past_projects/loan_lending_project_clean2.txt":
            "knowledge/past_projects/loan_lending_project_clean2.txt"
    }

    for local_path, git_path in files_to_push.items():
        put_git_file(
            client=client,
            local_file=local_path,
            git_file_path=os.path.dirname(git_path) + "/",
            file_name=os.path.basename(git_path),
            bot_id=bot_id
        )


def initialize_requirements_table(client, bot_id=None, genesis_db = 'GENESIS_BOTS'):
    """
    Initialize the requirements table in Snowflake if it doesn't exist.
    Loads data from test_requirements.json if table needs to be created.
    
    Args:
        client: Genesis API client instance
        bot_id: Bot ID to use for database operations (defaults to eve_bot_id)
    """
    # Try to query table to check if exists
    check_query = f"SELECT COUNT(*) FROM {genesis_db}.REQUIREMENTSPM_GXS_WORKSPACE.test_requirements"
    res = run_snowflake_query(client, check_query, bot_id)
   
    if isinstance(res, list) and len(res) > 0:
        return
    # Create table if query failed
    print(f"Creating table {genesis_db}.REQUIREMENTSPM_GXS_WORKSPACE.test_requirements...")
    create_query = f"""
        create or replace TABLE {genesis_db}.REQUIREMENTSPM_GXS_WORKSPACE.test_requirements (
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
        GIT_SOURCE_RESEARCH_STAGE_LINK VARCHAR(16777216),
        MAPPING_PROPOSAL VARCHAR(16777216),
        GIT_MAPPING_PROPOSAL_STAGE_LINK VARCHAR(16777216),
        CONFIDENCE_OUTPUT VARCHAR(16777216),
        GIT_CONFIDENCE_OUTPUT_STAGE_LINK VARCHAR(16777216),
        CONFIDENCE_SCORE VARCHAR(16777216),
        CONFIDENCE_SUMMARY VARCHAR(16777216),
        PM_BOT_COMMENTS VARCHAR(16777216),
        TRANSFORMATION_LOGIC VARCHAR(16777216),
        STATUS VARCHAR(16777216),
        EVALUATION_RESULTS VARCHAR(16777216),
        WHICH_MAPPING_CORRECT VARCHAR(10),
        CORRECT_ANSWER VARCHAR(16777216),
        PRIMARY_ISSUES VARCHAR(16777216),
        SECONDARY_ISSUES VARCHAR(16777216)
        );   """
    run_snowflake_query(client, create_query, bot_id)
    
    # Load data from JSON file
    json_path = os.path.join(os.path.dirname(__file__), 'test_requirements.json')
    with open(json_path, 'r') as f:
        requirements = json.load(f)
        
    # Insert data - using the actual fields from the requirements
    print(f"Populating initial data into {genesis_db}.REQUIREMENTSPM_GXS_WORKSPACE.test_requirements...")
    for req in requirements:
        insert_query = f"""
        INSERT INTO {genesis_db}.REQUIREMENTSPM_GXS_WORKSPACE.test_requirements
        (LOGICAL_TABLE_NAME, TABLE_DESCRIPTION, PHYSICAL_TABLE_NAME,
         PHYSICAL_COLUMN_NAME, LOGICAL_COLUMN_NAME, COLUMN_DESCRIPTION, 
         DATA_TYPE, LENGTH, LIST_OF_VALUES, STATUS)
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
            'NEW'
        )
        """
        run_snowflake_query(client, insert_query, bot_id)

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
    pm_bot_id = 'requirementsPM-GXS'
    source_research_bot_id = 'sourceResearchBot-GXS'
    mapping_proposer_bot_id = 'mappingProposerBot-GXS'
    confidence_analyst_bot_id = 'confidenceAnalystBot-GXS'

    bot_team_path = os.path.join(os.path.dirname(__file__), 'bot_team')

    load_bots = True
    if load_bots:
        load_bots_from_yaml(client=client, bot_team_path=bot_team_path) # , onlybot=source_research_bot_id)  # takes bot definitions from yaml files at the specified path and injects/updates those bots into the running local server
    else:
        print("Skipping bot loading, using existing bots")


    # Initialize requirements table if not exists 
    initialize_requirements_table(client, pm_bot_id, genesis_db = args.genesis_db)
    # Push project files to git
    push_knowledge_files_to_git(client, eve_bot_id)

    # MAIN WORKFLOW 
    try:
        run_number = 4;
        table_name = f"{args.genesis_db}.REQUIREMENTSPM_GXS_WORKSPACE.test_requirements"  
        
        # this will run it just for the CUSTOMER_ID field, for initial testing
        focus_field = 'BILLING_CYCLE_DATE';
        #focus_field = None
        skip_confidence = True
   
        #to test g-sheets export without running the rest of the workflow
        #export_table_to_gsheets(client, table_name, f'Mapping Results, Run# {run_number}', eve_bot_id)
        
        #Allows you to re-run a specific mapping when testing
        if focus_field:

            reset_sql = f"""
                update {table_name}
                set upstream_table = null,
                    upstream_column = null,
                    source_research = null,
                    git_source_research_stage_link = null,
                    mapping_proposal = null,
                    git_mapping_proposal_stage_link = null,
                    confidence_output = null,
                    git_confidence_output_stage_link = null,
                    confidence_summary = null,
                    pm_bot_comments = null,
                    transformation_logic = null,
                    evaluation_results=null,
                    which_mapping_correct = null,
                    correct_answer = null,
                    confidence_score = null,
                    primary_issues = null,
                    secondary_issues = null,
                    status = 'NEW'
                where physical_column_name = '{focus_field}'
                ;
            """
            run_snowflake_query(client, reset_sql, eve_bot_id)
            requirements_query = f'''SELECT * FROM {table_name} WHERE physical_column_name = '{focus_field}' '''
        else:
            requirements_query = f"SELECT * FROM {table_name} WHERE status = 'NEW'"

        requirements = run_snowflake_query(client, requirements_query, eve_bot_id)

        print("Found", len(requirements), "requirements with NEW status:")
        #requirements = None

        if not requirements:
            requirements = [{
                'PHYSICAL_COLUMN_NAME': 'CUSTOMER_ID',
                'LOGICAL_COLUMN_NAME': 'Customer ID',
                'COLUMN_DESCRIPTION': 'Unique identifier for the customer',
                'DATA_TYPE': 'VARCHAR',
                'LENGTH': 20,
                'LIST_OF_VALUES': None,
                'STATUS': 'NEW'
            }]
        # loop over the work to do

        for requirement in requirements:

            try:
                filtered_requirement = {
                    'PHYSICAL_COLUMN_NAME': requirement['PHYSICAL_COLUMN_NAME'],
                    'LOGICAL_COLUMN_NAME': requirement['LOGICAL_COLUMN_NAME'],
                    'COLUMN_DESCRIPTION': requirement['COLUMN_DESCRIPTION'],
                    'DATA_TYPE': requirement['DATA_TYPE'],
                    'LENGTH': requirement['LENGTH'],
                    'LIST_OF_VALUES': requirement['LIST_OF_VALUES']
                }
                print("\033[34mWorking on requirement:", filtered_requirement, "\033[0m")

                paths = setup_paths(requirement["PHYSICAL_COLUMN_NAME"], run_number=run_number, genesis_db = args.genesis_db)

                source_research = perform_source_research_new(client, filtered_requirement, paths, source_research_bot_id)
                
                mapping_proposal = perform_mapping_proposal_new(client, filtered_requirement, paths, mapping_proposer_bot_id)

                if not skip_confidence:
                    confidence_report = perform_confidence_analysis_new(client, filtered_requirement, paths, confidence_analyst_bot_id)

                summary = perform_pm_summary(client, filtered_requirement, paths, pm_bot_id, skip_confidence)

            
                # Get the full content of each file from git
                source_research_content = source_research
                mapping_proposal_content = mapping_proposal
                
                confidence_output_content = 'bypassed currently while we improve this bot'
                # Evaluate results
                evaluation, eval_json  = evaluate_results(client, paths, filtered_requirement, pm_bot_id, source_research_content, mapping_proposal_content, confidence_output_content, summary)

                # Prepare fields for database update
                db_fields = {
                    'upstream_table': summary['UPSTREAM_TABLE'],
                    'upstream_column': summary['UPSTREAM_COLUMN'],
                    'source_research': source_research_content,
                    'git_source_research_stage_link': f"{paths['stage_base']}{paths['base_git_path']}{paths['source_research_file']}",
                    'mapping_proposal': mapping_proposal_content,
                    'git_mapping_proposal_stage_link': f"{paths['stage_base']}{paths['base_git_path']}{paths['mapping_proposal_file']}",
                    'confidence_output': confidence_output_content,
                    'git_confidence_output_stage_link': f"{paths['stage_base']}{paths['base_git_path']}{paths['confidence_report_file']}",
                    'confidence_score': summary['CONFIDENCE_SCORE'],
                    'confidence_summary': summary['CONFIDENCE_SUMMARY'],
                    'pm_bot_comments': summary['PM_BOT_COMMENTS'],
                    'transformation_logic': summary['TRANSFORMATION_LOGIC'],
                    'evaluation_results': evaluation,
                    'which_mapping_correct': eval_json['WHICH_MAPPING_CORRECT'],
                    'correct_answer': eval_json['CORRECT_ANSWER'],
                    'primary_issues': eval_json['PRIMARY_ISSUES'],
                    'secondary_issues': eval_json['SECONDARY_ISSUES'],
                    'status': 'READY_FOR_REVIEW'
                }

                # Save results of work to database
                save_pm_summary_to_requirements(
                    requirement['PHYSICAL_COLUMN_NAME'],
                    db_fields,
                    table_name
                )
                print("\033[32mSuccessfully saved results to database for requirement:", requirement['PHYSICAL_COLUMN_NAME'], "\033[0m")

                # prevent unintentional runaway runs while developing/testing
                i = input('press return to continue (this is for runaway prevention when testing...) ')

            except Exception as e:
                print(f"\033[31mError occurred: {e}\033[0m")

                # Save error state to database for this field
                error_fields = {
                    'upstream_table': None,
                    'upstream_column': None,
                    'source_research': str(e),
                    'git_source_research_stage_link': None,
                    'mapping_proposal': None,
                    'git_mapping_proposal_stage_link': None,
                    'confidence_output': None,
                    'git_confidence_output_stage_link': None,
                    'confidence_score': 0,
                    'confidence_summary': None,
                    'pm_bot_comments': f'Error: {str(e)}',
                    'transformation_logic': None,
                    'evaluation_results': None,
                    'which_mapping_correct': 'error',
                    'correct_answer': None,
                    'primary_issues': f'Error: {str(e)}',
                    'secondary_issues': None,
                    'status': 'ERROR'
                }

                save_pm_summary_to_requirements(
                    requirement['PHYSICAL_COLUMN_NAME'],
                    error_fields,
                    table_name
                )
                print(f"\033[33mSaved error state to database for requirement: {requirement['PHYSICAL_COLUMN_NAME']}\033[0m")

            # Export results to Google Sheets
            i = input('All requirements processed, press return to export results to Google Sheets')
            try:
                export_table_to_gsheets(client, table_name, f'Mapping Results, Run# {run_number}', eve_bot_id)
            except Exception as e:
                print(f"\033[31mError occurred exporting to Google Sheets, but results are in the table at: {table_name}. Error: {e}\033[0m")

    except Exception as e:
            raise e

if __name__ == "__main__":
    main()
