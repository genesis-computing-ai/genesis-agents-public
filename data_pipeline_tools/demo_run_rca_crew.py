# Must precede any llm module imports
from   langtrace_python_sdk     import langtrace
langtrace.init(api_key = 'f2cc4c71dbd567098789b764b8b4e50308046824ddadfe2844c4e3560a5fb317',
                #disable_instrumentations = dict( only=['crewai']) # Disable crewai oinstrumentation - it has a bug with task with no agents (hirahierarhical)
)



from   crewai                   import Agent, Crew, LLM, Process, Task
from   crewai.tools             import tool
from   langchain_openai         import ChatOpenAI
import logging
import os
from   pathlib                  import Path
import yaml

from   data_pipeline_tools      import gc_dagster
import datetime


# log raw request/response
#litellm.log_raw_request_response = True
#
# litellm.set_verbose=True

# https://app.langtrace.ai/ Key: f2cc4c71dbd567098789b764b8b4e50308046824ddadfe2844c4e3560a5fb317




# # Configure OpenTelemetry Tracer Provider
# trace.set_tracer_provider(TracerProvider())
# tracer = trace.get_tracer(__name__)

# # Add a Console Span Exporter for debugging purposes
# span_processor = BatchSpanProcessor(ConsoleSpanExporter())
# trace.get_tracer_provider().add_span_processor(span_processor)





# class FetchDagsterRunDebug(BaseTool):
#     name: str = "dagster_run_explorer"
#     description: str = (
#         "Fetch information from the Dagster Cloud server about a specific run, given its run_id."
#     )
#     class _FetchDagsterRunDebugArgs(BaseModel):
#         run_id: str = Field(..., description="Ther run_id of the Dagster run to fetch data for.")

#     args_schema = _FetchDagsterRunDebugArgs

#     def _run(self, run_id: str) -> str:
#         return _get_dagster_run_debug_dump(run_id)


@tool
def get_dagster_run_debug_dump(run_id: str) -> str:
    '''
    Fetch full (long, detailed) debug information from the Dagster Cloud server about a specific run, given its run_id.
    This is equivanet of downloading a full debug file information from dasgter cloud.

    Returns a JSON string representing the result of a dagster GraphQL query.
    '''
    res = gc_dagster.get_dagster_run_debug_dump(run_id)
    logging.info(f"Result of get_dagster_run_debug_dump({run_id}) : \n {res}")
    return res


@tool
def get_dagster_asset_definition_and_overview(asset_key: str) -> str:
    '''
    Fetch rich information about the given asset, which includes the following:
     - latest materlization information (time, run_id)
     - Description
     - Raw SQL (for DBT-wrapped assets)
     - Schema of the asset (e.g. columns for table/view assets)
     - Metadata for the assets recognized by Dagster

     Arguments:
       asset_key: the asset key, using "/" as path separeator (e.g. foo/bar for asset key ['foo', 'bar'])     

    Returns a JSON string representing the result of a dagster GraphQL query.       
    '''
    res = gc_dagster.get_dagster_asset_definition_and_overview(asset_key)
    logging.info(f"Result of get_dagster_asset_definition_and_overview({asset_key}) : \n {res}")
    return res


@tool
def get_dagster_asset_lineage_graph() -> str:
    '''
    Fetch asset lineage for the entire dagster repository.

    Returns a JSON string representing the result of a dagster GraphQL query.       
    '''
    res = gc_dagster.get_dagster_asset_lineage_graph()
    logging.info(f"Result of get_dagster_asset_lineage_graph() : \n {res}")
    return res


@tool
def run_snowflake_sql_query(query_text: str) -> str:
    '''
    Run the given SQL query text against the databases where dagster assets are hosted. ALWAYS Use fully qualified object names.

    Arguments:
       query_text: a valid Snowfake SQL query text.

    Returns a JSON string representing resultset, or an error message.
    '''
    import os
    import snowflake.connector
    import json

    try:
        # Retrieve Snowflake connection parameters from environment variables
        account = os.getenv('SNOWFLAKE_ACCOUNT_OVERRIDE')
        user = os.getenv('SNOWFLAKE_USER_OVERRIDE')
        password = os.getenv('SNOWFLAKE_PASSWORD_OVERRIDE')
        warehouse = os.getenv('SNOWFLAKE_WAREHOUSE_OVERRIDE')
        role = os.getenv("SNOWFLAKE_ROLE_OVERRIDE")

        # Establish a connection to Snowflake
        with snowflake.connector.connect(
            account=account,
            user=user,
            password=password,
            warehouse=warehouse,
            role=role,
        ) as conn:

            # Execute the query
            with conn.cursor() as cursor:
                cursor.execute(query_text)

                # Fetch the results
                result = cursor.fetchall()

                # Convert the result to a JSON string
                result_json = json.dumps(result)

        res = result_json

    except Exception as e:
        res = json.dumps({"error": str(e)})

    logging.info(f"Result of get_dagster_asset_lineage_graph() : \n {res}")
    return res


# # Uncomment the following line to use an example of a custom tool
# # from websearch_example.tools.custom_tool import MyCustomTool

# # Check our tools documentations for more information on how to use them
# # from crewai_tools import SerperDevTool

# @CrewBase
# class DagsterRunRCACrew():

#     agents_config = 'agents.yaml'
#     tasks_config = 'tasks.yaml'

#     @before_kickoff
#     def before_kickoff_function(self, inputs):
#         print(f"\nBefore kickoff function with inputs: {inputs}")
#         return inputs # You can return the inputs or modify them as needed

#     @after_kickoff
#     def after_kickoff_function(self, result):
#         print(f"\nAfter kickoff function with result: {result}")
#         return result # You can return the result or modify it as needed


#     @agent
#     def dagster_run_debug_analyzer(self) -> Agent:
#         return Agent(
#             config=self.agents_config['dagster_run_debug_analyzer'],
#             tools=[DagsterRunExplorer()],
#             verbose=False
#         )

#     # @agent
#     # def reporting_analyst(self) -> Agent:
#     #     return Agent(
#     #         config=self.agents_config['reporting_analyst'],
#     #         verbose=True
#     #     )

#     @task
#     def analyze_dagster_run_failure(self) -> Task:
#         return Task(
#             config=self.tasks_config['analyze_dagster_run_failure'],
#         )

#     # @task
#     # def reporting_task(self) -> Task:
#     #     return Task(
#     #         config=self.tasks_config['reporting_task'],
#     #         output_file='report.md'
#     #     )

#     @crew
#     def crew(self) -> Crew:
#         """Creates the DagsterRunRCACrew crew"""
#         return Crew(
#             agents=self.agents, # Automatically created by the @agent decorator
#             tasks=self.tasks, # Automatically created by the @task decorator
#             process=Process.sequential,
#             verbose=True,
#             output_log_file=True # should go to logs.txt?
#             # process=Process.hierarchical, # In case you wanna use that instead https://docs.crewai.com/how-to/Hierarchical/
#         )

#------------------------------------------



def _load_yaml_file(fn, cwd=None):
    if cwd is None:
        cwd = os.path.dirname(__file__)
    file_path = os.path.join(cwd, fn)
    with open(file_path) as fh:
        return yaml.safe_load(fh)

agents_config = _load_yaml_file("agents.yaml")
tasks_config = _load_yaml_file("tasks.yaml")

dagster_explorer = Agent(
    config=agents_config['dagster_explorer'],
    #verbose=True,
    llm = LLM(model="gpt-4o", temperature=0.0),
)

rca_director = Agent(
    config=agents_config['rca_director'],
    #allow_delegation=True,
    #verbose=True,
    llm = LLM(model="gpt-4o", temperature=0.2),
)

tasks = [
    Task(
        config=tasks_config['fetch_failed_run_debug_information'],
        #agent=dagster_explorer,
        agent=rca_director,
        tools=[get_dagster_run_debug_dump],
        # output_file="crewai_outputs/task_analyze_dagster_run_failure.json"
    ),

    Task(
        config=tasks_config['assert_failure_persists'],
        agent=rca_director, # dagster_explorer,
        tools=[get_dagster_asset_definition_and_overview, run_snowflake_sql_query],
    ),
    Task(
        config=tasks_config['trace_upstream_dependencies'],
        agent=rca_director,
        tools=[get_dagster_asset_lineage_graph, get_dagster_asset_definition_and_overview, run_snowflake_sql_query],
    ),
    # Task(
    #     config=tasks_config['correlate_findings'],
    #     agent=rca_director,
    #     tools=[],
    # ),
    Task(
        config=tasks_config['generate_rca_report'],
        agent=rca_director,
        tools=[],
    ),
]

def create_crew(crewai_log_file=None):
    crew4tasks = Crew(
        agents=[#dagster_explorer,
                rca_director,
                ]
        ,
        tasks=tasks,
        process=Process.sequential,
        verbose=True,
        output_log_file=str(crewai_log_file),
        # process=Process.hierarchical, # In case you wanna use that instead https://docs.crewai.com/how-to/Hierarchical/
    )


    crew2tasks = Crew(
        agents=[dagster_explorer,
                rca_director,
                ]
        ,
        tasks= [
            Task(
                config=tasks_config['fetch_failed_run_debug_information'],
                agent=dagster_explorer,
                tools=[get_dagster_run_debug_dump],
            ),
            Task(
                config=tasks_config['generate_rca_report_one_shot'],
                agent=rca_director,
                tools=[get_dagster_asset_lineage_graph, get_dagster_asset_definition_and_overview, run_snowflake_sql_query],
            )
        ],
        process=Process.sequential,
        verbose=True,
        output_log_file=str(crewai_log_file),
    )

    # Try a hierarchial model with just one 'worker' agent and two tasks - fetch the run debug data and then RCA
    # Looks like the tools MUST be specified at the Agwnt level in this case (tools dont get delegated to the agent if defined at
    # the task level)
    crew_2t_hier = Crew(
        agents= [
            Agent(config=agents_config['dagster_explorer'],
                  #verbose=True,
                  llm = LLM(model="gpt-4o", temperature=0.0),
                  tools = [get_dagster_run_debug_dump, get_dagster_asset_lineage_graph, get_dagster_asset_definition_and_overview, run_snowflake_sql_query],
            ),
        ],
        tasks=  [
            Task(
                config=tasks_config['fetch_failed_run_debug_information'],
                #agent=dagster_explorer,
                #tools = [get_dagster_run_debug_dump,],
            ),
            Task(
                config=tasks_config['generate_rca_report_one_shot'],
                #agent=rca_director,
                #tools=[get_dagster_asset_lineage_graph, get_dagster_asset_definition_and_overview, run_snowflake_sql_query],
            )
        ],
        process=Process.hierarchical,
        manager_llm=ChatOpenAI(model="gpt-4o"),
        verbose=True,
        output_log_file=str(crewai_log_file),
    )

    #return crew4tasks
    return crew2tasks
    #return crew_2t_hier



#------------------- testing the crew ------------------

def make_crew_run_dir():
    timestamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%d_%H%M%S")
    run_dir = f".personal/crewai_runs/{timestamp}"
    if not os.path.exists(run_dir):
        os.makedirs(run_dir)
    return Path(run_dir)


INPUT_PARAMS = dict(
    run_id = "975f848d-11f2-4655-8970-740bbf66edda"
)

def run():
    """
    Run the crew.
    """
    import datetime


    run_dir = make_crew_run_dir()
    crewai_log_path = run_dir / "crewai.log"
    debug_log_path = None #run_dir / "raw.log"

    print("CREWAI Log dump: ", crewai_log_path)
    print("DEBUG Log dump: ", debug_log_path)

    # Set up debug logging
    if debug_log_path:
        logging.basicConfig(filename=debug_log_path, level=logging.DEBUG)

    # create crew for this run
    crew = create_crew(crewai_log_path)

    # kickoff!
    result = crew.kickoff(inputs=INPUT_PARAMS)

    print("\n\n ================================= CREW RESULT: ================================")
    print(result.raw)
    print("\n\n ===============================================================================")
    print("CREWAI Log dump: ", crewai_log_path)
    print("DEBUG Log dump: ", debug_log_path)


if __name__ == "__main__":
    run()