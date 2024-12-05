import time
from api.genesis_api import GenesisAPI
from langsmith import Client
from langsmith import traceable

import snowflake.connector
import json 

import os, yaml

# Initialize LangSmith client at the top of your file
langsmith_client = Client()

conn = snowflake.connector.connect(
    account=os.environ['SNOWFLAKE_ACCOUNT_OVERRIDE'],
    user=os.environ['SNOWFLAKE_USER_OVERRIDE'],
    password=os.environ['SNOWFLAKE_PASSWORD_OVERRIDE'],
    database=os.environ['SNOWFLAKE_DATABASE_OVERRIDE'],
    warehouse=os.environ['SNOWFLAKE_WAREHOUSE_OVERRIDE'],
    role=os.environ['SNOWFLAKE_ROLE_OVERRIDE']
)

def get_file_from_stage(stage_path, file_name):
    """
    Retrieve file contents from a Snowflake stage location.
    Args:
        stage_path: Full path to file in stage, e.g. '@my_stage/path/to/file.txt'
    Returns:
        String contents of the file
    """
    try:
        cursor = conn.cursor()
        get_file_cmd = f"GET {stage_path}{file_name} file://./tmp/;"
        res = cursor.execute(get_file_cmd)
        
        with open(f'./tmp/{file_name}', 'r') as f:
            contents = f.read()
            
        return contents
    except Exception as e:
        print(f"Error retrieving file from stage: {e}")
        return None
    finally:
        cursor.close()

def remove_file_from_stage(stage_path, file_name):
    """
    Remove a file from a Snowflake stage location if it exists.
    Args:
        stage_path: Full path to stage, e.g. '@my_stage/path/to/'
        file_name: Name of file to remove
    Returns:
        True if file was removed successfully, False otherwise
    """
    try:
        cursor = conn.cursor()
        # Check if file exists first
        list_cmd = f"LIST {stage_path}{file_name};"
        res = cursor.execute(list_cmd).fetchall()
        
        if len(res) > 0:
            # File exists, remove it
            remove_cmd = f"REMOVE {stage_path}{file_name};"
            cursor.execute(remove_cmd)
            return True
        return False
    except Exception as e:
        print(f"Error removing file from stage: {e}")
        return False
    finally:
        cursor.close()

def write_file_to_stage(stage_path, file_name, contents):
    """
    Write file contents to a Snowflake stage location.
    Args:
        stage_path: Full path to stage, e.g. '@my_stage/path/to/'
        file_name: Name of file to write
        contents: String contents to write to file
    Returns:
        True if file was written successfully, False otherwise
    """
    try:
        cursor = conn.cursor()
        
        # Write contents to temporary local file
        with open(f'./tmp/{file_name}', 'w') as f:
            f.write(contents)
            
        # Put file to stage
        put_cmd = f"PUT file://./tmp/{file_name} {stage_path};"
        cursor.execute(put_cmd)
        
        return True
    except Exception as e:
        print(f"Error writing file to stage: {e}")
        return False
    finally:
        cursor.close()


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


@traceable(name="genesis_bot_call")
def call_genesis_bot(client, bot_id, request):
    """Wait for a complete response from the bot, streaming partial results."""
    try:
        print(f"\n\033[94m{'='*80}\033[0m")  # Blue separators
        print(f"\033[92mBot:\033[0m {bot_id}")  # Green label
        print(f"\033[92mPrompt:\033[0m {request}")  # Green label
        print(f"\033[94m{'-'*80}\033[0m")  # Blue separator
        print("\033[92mResponse:\033[0m")  # Green label for response section

        request = client.add_message(bot_id, request)
        response = client.get_response(bot_id, request["request_id"])
        
        print(f"\n\033[94m{'-'*80}\033[0m")  # Blue separator
        print("\033[93mResponse complete\033[0m")  # Yellow status
        print(f"\033[94m{'='*80}\033[0m\n")  # Blue separator
        return response
    except Exception as e:
        raise e

def setup_paths(physical_column_name):
    """Setup file paths and names for a given requirement."""
    stage_base = '@genesis_bots_alpha.app1.bot_git/'
    base_git_path = 'requirements/run1/'
    
    return {
        'stage_base': stage_base,
        'base_git_path': base_git_path,
        'source_research_file': f"{physical_column_name}__source_research.txt",
        'mapping_proposal_file': f"{physical_column_name}__source_research.txt",
        'confidence_report_file': f"{physical_column_name}__confidence_report.txt"
    }

@traceable(name="source_research")
def perform_source_research(client, requirement, paths, bot_id):
    """Execute source research step and validate results."""
    try:
        print("\033[34mExecuting source research...\033[0m")
        
        # Create placeholder file
        success = write_file_to_stage(f'{paths["stage_base"]}{paths["base_git_path"]}',
                                     paths["source_research_file"], 
                                     "Placeholder for source research")
        if not success:
            raise Exception("Failed to put placeholder source research file to stage")

        research_prompt = f'''Here are requirements for a target field I want you to work on: {requirement}\n
        Delegate to the SourceResourceBot microbot and tell them to research this field and save the results in git at: {paths["base_git_path"]}{paths["source_research_file"]}\n
        Then validate the microbot has saved the report in the right place, if so return SUCCESS.  If not return FAILURE.
        This is being run by an automated process, so do not repeat these instructions back to me, simply proceed to execute them without asking for further approval.'''
        
        response = call_genesis_bot(client, bot_id, research_prompt)
        if 'SUCCESS' not in response:
            raise Exception('Error on source research')

        contents = get_file_from_stage(f'{paths["stage_base"]}{paths["base_git_path"]}', 
                                     paths["source_research_file"])
        if not contents or contents.startswith('Placeholder '):
            raise Exception('Source research file not found or contains only placeholder')
        
        print_file_contents("SOURCE RESEARCH", 
                           f"{paths['base_git_path']}{paths['source_research_file']}", 
                           contents)
        return contents
            
    except Exception as e:
        raise e

def perform_mapping_proposal(client, requirement, paths, bot_id):
    """Execute mapping proposal step and validate results."""
    print("\033[34mExecuting mapping proposal...\033[0m")
    
    success = write_file_to_stage(f'{paths["stage_base"]}{paths["base_git_path"]}',
                                 paths["mapping_proposal_file"], 
                                 "Placeholder for mapping proposal")
    if not success:
        raise Exception("Failed to put placeholder mapping proposal file to stage")

    mapping_prompt = f'''Here are requirements for a target field I want you to work on: {requirement}\n
    The source research bot has already run and saved its results at this git location: {paths["base_git_path"]}{paths["source_research_file"]}\n
    Now call the mapping proposer microbot and have it perform a mapping proposal for this field, give it the requirements and the git location of the source research file (including the path), and save its results at this git location: {paths["base_git_path"]}{paths["mapping_proposal_file"]}\n
    If it has trouble finding the source research document, make sure it is using the correct GIT path, or if that fails, get it yourself and provide the text of it it in your prompt to the mapping proposer bot.
    Then validate the microbot has saved the report in the right place, if so return SUCCESS.  If not return FAILURE.
    This is being run by an automated process, so do not repeat these instructions back to me, simply proceed to execute them without asking for further approval.'''
    
    response = call_genesis_bot(client, bot_id, mapping_prompt)
    if 'SUCCESS' not in response:
        raise Exception('Error on mapping proposal')

    contents = get_file_from_stage(f'{paths["stage_base"]}{paths["base_git_path"]}',
                                 paths["mapping_proposal_file"])
    if not contents or contents.startswith('Placeholder '):
        raise Exception('Mapping proposal file not found or contains only placeholder')
    
    print_file_contents("MAPPING PROPOSAL",
                       f"{paths['base_git_path']}{paths['mapping_proposal_file']}", 
                       contents)
    return contents

def perform_confidence_analysis(client, requirement, paths, bot_id):
    """Execute confidence analysis step and validate results."""
    print("\033[34mExecuting confidence analysis...\033[0m")
    
    success = write_file_to_stage(f'{paths["stage_base"]}{paths["base_git_path"]}',
                                 paths["confidence_report_file"], 
                                 "Placeholder for confidence report")
    if not success:
        raise Exception("Failed to put placeholder confidence report file to stage")

    confidence_prompt = f'''Here are requirements for a target field I want you to work on: {requirement}\n
    The source research bot has saved its results in git at: {paths["base_git_path"]}{paths["source_research_file"]}\n
    The mapping proposer bot has saved its results in git at: {paths["base_git_path"]}{paths["mapping_proposal_file"]}\n
    Now call the confidence analyst microbot and have it analyze the confidence level of this mapping proposal.
    Tell it to use git_action to retrieve both of the files listed above.
    Have it review both the source research and mapping proposal documents from the git locations provided above.
    If it has trouble finding these documents, explain in mode detail how to find them using git_action, or if needed
    you can get them and provide the full contents to the confidence bot yourself.
    Tell it to save its confidence analysis report at this git location: {paths["base_git_path"]}{paths["confidence_report_file"]}\n
    Then validate the microbot has saved the report in the right place, if so return SUCCESS. If not return FAILURE.
    This is being run by an automated process, so do not repeat these instructions back to me, simply proceed to execute them without asking for further approval.'''
    
    response = call_genesis_bot(client, bot_id, confidence_prompt)
    if 'SUCCESS' not in response:
        raise Exception('Error on confidence analysis')

    contents = get_file_from_stage(f'{paths["stage_base"]}{paths["base_git_path"]}',
                                 paths["confidence_report_file"])
    if not contents or contents.startswith('Placeholder '):
        raise Exception('Confidence report file not found or contains only placeholder')
    
    print_file_contents("CONFIDENCE REPORT",
                       f"{paths['base_git_path']}{paths['confidence_report_file']}", 
                       contents)
    return contents


def perform_pm_summary(client, requirement, paths, bot_id):
    """Have PM bot analyze results and provide structured summary."""
    print("\033[34mGetting PM summary...\033[0m")

    pm_prompt = f'''Here are requirements for a target field I want you to work on: {requirement}\n
    The mapping proposer bot has saved its results in git at: {paths["base_git_path"]}{paths["mapping_proposal_file"]}\n
    The confidence analyst bot has saved its results in git at: {paths["base_git_path"]}{paths["confidence_report_file"]}\n
    
    Please review both documents and provide a JSON response with the following fields:
    - UPSTREAM_TABLE: List of source table(s) needed for the data, with schema prefixes
    - UPSTREAM_COLUMN: List of source column(s) for the mapping
    - TRANSFORMATION_LOGIC: SQL snippet or natural language description of any needed transformations
    - CONFIDENCE_SCORE: Score from confidence bot report
    - CONFIDENCE_SUMMARY: Brief explanation of the confidence score
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
                git_source_research = %(git_source_research)s,
                mapping_proposal = %(mapping_proposal)s,
                git_mapping_proposal = %(git_mapping_proposal)s,
                confidence_output = %(confidence_output)s,
                git_confidence_output = %(git_confidence_output)s,
                confidence_score = %(confidence_score)s,
                confidence_summary = %(confidence_summary)s,
                PM_BOT_COMMENTS = %(pm_bot_comments)s,
                transformation_logic = %(transformation_logic)s,
                status = 'READY_FOR_REVIEW'
            WHERE physical_column_name = %(physical_column_name)s
        """
        
        params = {
            **flattened_fields,  # Use the flattened fields instead of raw summary_fields
            'physical_column_name': physical_column_name
        }
        
        cursor = conn.cursor()
        cursor.execute(update_query, params)
        conn.commit()
        cursor.close()
        
    except Exception as e:
        conn.rollback()
        raise Exception(f"Failed to update requirement for column {physical_column_name}: {str(e)}")


def port_bot(client_source, client_target, bot_id, suffix='local', remove_slack=True):
    """
    Port bots from remote to local environment.
    
    Args:
        client: Remote Genesis API client
        client_local: Local Genesis API client
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


def load_bots_from_yaml(client,bot_team_path):
    """
    Load and register bots from YAML files in the specified directory.
    
    Args:
        bot_team_path: Path to directory containing bot YAML files
    """
   # client = GenesisAPI("local-snowflake", scope="GENESIS_TEST", sub_scope="GENESIS_JL")
    
    # Get all YAML files in directory
    yaml_files = [f for f in os.listdir(bot_team_path) if f.endswith('.yaml')]
    
    for yaml_file in yaml_files:
        file_path = os.path.join(bot_team_path, yaml_file)
        
        # Load bot config from YAML
        with open(file_path, 'r') as file:
            bot_config = yaml.safe_load(file)
            
        print(f"Registering bot from {file_path}")
        
        try:
            # Register bot with API
            client.register_bot(bot_config)
            print(f"Successfully registered bot {bot_config['BOT_NAME']}")
        except Exception as e:
            print(f"Failed to register bot from {file_path}: {str(e)}")


def main():
    """Main execution flow."""

    # start these bots if they exist (if you're not loading/refreshing from YAMLS below) 
    local_bots = []

    client = GenesisAPI("local-snowflake", scope="GENESIS_TEST", sub_scope="GENESIS_JL", 
                        bot_list=local_bots) # ["marty-l6kx7d"]

    bots = client.get_all_bots()
    print("Existing local bots: ",bots)

            # port bots from another server
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

    # LOAD AND ACTIVATE BOTS FROM YAML FILES
    # adds or updates bots defined in YAML to metadata and activates the listed bots
    bot_team_path = './demo/bot_team'
    load_bots_from_yaml(client=client, bot_team_path=bot_team_path)

    # MAIN WORKFLOW
    try:
        cursor = conn.cursor()
        table_name = "genesis_gxs.requirements.flexicard_pm_jl"  # Changed from genesis_gxs.requirements.flexicard_pm
        cursor.execute(f"SELECT * FROM {table_name} WHERE status = 'NEW'")
        results = cursor.fetchall()
        columns = [col[0] for col in cursor.description]

        # Convert results to array of JSON objects
        requirements = []
        for row in results:
            requirement = {}
            for i, value in enumerate(row):
                requirement[columns[i]] = value
            requirements.append(requirement)

        print("Found", len(requirements), "requirements with NEW status:")
        
        for requirement in requirements:
            filtered_requirement = {
                'PHYSICAL_COLUMN_NAME': requirement['PHYSICAL_COLUMN_NAME'],
                'LOGICAL_COLUMN_NAME': requirement['LOGICAL_COLUMN_NAME'], 
                'COLUMN_DESCRIPTION': requirement['COLUMN_DESCRIPTION'],
                'DATA_TYPE': requirement['DATA_TYPE'],
                'LENGTH': requirement['LENGTH'],
                'LIST_OF_VALUES': requirement['LIST_OF_VALUES']
            }
            print("\033[34mWorking on requirement:", filtered_requirement, "\033[0m")

            paths = setup_paths(requirement["PHYSICAL_COLUMN_NAME"])
            
            pm_bot_id = 'RequirementsPM-jllocal'
            source_research = perform_source_research(client, filtered_requirement, paths, pm_bot_id)
            mapping_proposal = perform_mapping_proposal(client, filtered_requirement, paths, pm_bot_id)
            confidence_report = perform_confidence_analysis(client, filtered_requirement, paths, pm_bot_id)
            summary = perform_pm_summary(client, filtered_requirement, paths, pm_bot_id)

            # Get the full content of each file from git
            source_research_content = get_file_from_stage(
                f"{paths['stage_base']}{paths['base_git_path']}", 
                paths["source_research_file"]
            )
            mapping_proposal_content = get_file_from_stage(
                f"{paths['stage_base']}{paths['base_git_path']}", 
                paths["mapping_proposal_file"]
            )
            confidence_output_content = get_file_from_stage(
                f"{paths['stage_base']}{paths['base_git_path']}", 
                paths["confidence_report_file"]
            )

            # Prepare fields for database update
              # Prepare fields for database update
            db_fields = {
                'upstream_table': summary['UPSTREAM_TABLE'],
                'upstream_column': summary['UPSTREAM_COLUMN'],
                'source_research': source_research_content,
                'git_source_research': f"{paths['stage_base']}{paths['base_git_path']}{paths['source_research_file']}", 
                'mapping_proposal': mapping_proposal_content,
                'git_mapping_proposal': f"{paths['stage_base']}{paths['base_git_path']}{paths['mapping_proposal_file']}",
                'confidence_output': confidence_output_content,
                'git_confidence_output': f"{paths['stage_base']}{paths['base_git_path']}{paths['confidence_report_file']}",
                'confidence_score': summary['CONFIDENCE_SCORE'],
                'confidence_summary': summary['CONFIDENCE_SUMMARY'],
                'pm_bot_comments': summary['PM_BOT_COMMENTS'],
                'transformation_logic': summary['TRANSFORMATION_LOGIC']
            }

            # Save to database
            save_pm_summary_to_requirements(
                requirement['PHYSICAL_COLUMN_NAME'], 
                db_fields,
                table_name
            )
            print("\033[32mSuccessfully saved results to database for requirement:", requirement['PHYSICAL_COLUMN_NAME'], "\033[0m")

            i = input('next? ')
    except Exception as e:
        print(f"\033[31mError occurred: {e}\033[0m")
        raise e

    finally:
        client.shutdown()

if __name__ == "__main__":
    main()
