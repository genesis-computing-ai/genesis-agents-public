from   api.genesis_api          import GenesisAPI
from   api.snowflake_local_server \
                                import GenesisLocalServer
from   langsmith                import Client, traceable


from   pprint                   import pprint
from   textwrap                 import dedent, indent
import yaml

from   core.logging_config      import log_level_ctx
from   typing                   import Dict, List, Mapping

from core.logging_config import logger

# Initialize LangSmith client at the top of your file
langsmith_client = Client()

@traceable(name="genesis_bot_call")
def _call_genesis_bot(gclient, bot_id, request):
    """Wait for a complete response from the bot, streaming partial results."""
    try:
        print(f"\n\033[94m{'='*80}\033[0m")  # Blue separators
        print(f"\033[92mBot:\033[0m {bot_id}")  # Green label
        print(f"\033[92mPrompt:\033[0m {request}")  # Green label
        print(f"\033[94m{'-'*80}\033[0m")  # Blue separator
        print("\033[92mResponse:\033[0m")  # Green label for response section

        request = gclient.add_message(bot_id, request)
        with log_level_ctx("ERROR"):
            response = gclient.get_response(bot_id, request["request_id"])

        print(f"\n\033[94m{'-'*80}\033[0m")  # Blue separator
        print("\033[93mResponse complete\033[0m")  # Yellow status
        print(f"\033[94m{'='*80}\033[0m\n")  # Blue separator
        return response
    except Exception as e:
        raise e


# Need this to override the default Genesis prompt. We should make this unvessary by distinguishing between the two or support full flexibility in configuraiton.
NON_INTERACTIVE_PROMPT_SUFFIX = ('NOTE: You are running in a non-interactive mode, so when asked to perform a task do not request '
                                 'permissions to proceed. If you encounter errors that prevent you from performing your task, stop '
                                 'and report your status and the reason for not completing the task.')

RCA_REPORT_STRUCTURE = dedent(
    '''

    ### 1. Incident Summary

        - Date & Time: [Timestamp of detection]
        - Impact: [Pipeline name] failed, causing [specific impact, e.g., delayed reporting, incomplete data].
        - Status: Resolved / In Progress

    ### 2. Root Cause Summary

        - Primary Issue: [High-level description of the root cause, e.g., "Incorrect parameter configuration in job X."]
        - Contributing Factors: [Brief bullet points of additional causes, e.g., "Missing error handling for API rate limits."]

    ### 3. Incident Impact

        - Data Impact: [Data affected, e.g., "Records for 2024-12-03 missing in daily sales report."]
        - Business Impact: [Business-level consequence, e.g., "Sales reports delayed by 3 hours."]

    ### 4. Actions Taken (if any)

       -  Immediate Fixes:
            - [Action 1, e.g., "Rolled back configuration to stable version."]
            - [Action 2, e.g., "Restarted failed jobs."]
            - Current Status: [Resolved or still in progress.]

    ### 5. Next Steps & Recommendations

        [Short-term action, e.g., "Modify this SQL", "Augment the quality test as follows...", "Add retry logic for the API call."]
        [Long-term improvement, e.g., "Review the following parameters ..."]

    ''')


Bot_DagsterExplorer_yaml = dedent(
        f'''
        BOT_ID: DagsterExplorer
        AVAILABLE_TOOLS: '[ "dagster_tools", ]'
        BOT_AVATAR_IMAGE: null
        BOT_IMPLEMENTATION: openai
        BOT_INSTRUCTIONS: >
            Your are a Dagster log and metadata analyzer. You have been provided with specific tools to query the definitions and state of a certain Dagster Cloud setup that is
            used for driving complex data pipelines. Your job is to analyze the Daster setup and historical run logs to help investigate pipeline failures or
            understanding relationshipds between assets or how users are using those assets.

            Here are a few of the typical tasks that you will be asked to perform:

            ### Summarizing run failures, given their run_id
             * Fetch full run debug dump from the dagster setup
             * extrat all the relevant evidence and details from this debug dump that are relevant to kick off a failure invetigation.

            ### Lineage analysis
             * Extract asset lineage definition to provide upstream and downstream lineage, relative to a certain asset
             * Analyze partition lineage and partition dependencies across assets

            ### Asset definitions and metadata
             * Fetch and summarize asset definitions from the dagster setup
             * Tell whether assets definitions are wrappers for other sub-system orchestrated by daster such as DBT or an ML model; Locate the sources for these definitions so sub-system-specific experts
               can help fetch the specific definitions. For example, if the asset is a DBT model, fetch the actual DBT definition of the model if know to you, or provide the proper pointer to fetch that information from.

             ### Dagster Resource definitions and metadata
             * Fetch information about the resources used in a dagser setup per asset or per run.

            {NON_INTERACTIVE_PROMPT_SUFFIX}

        BOT_INTRO_PROMPT: Hello, I am a dagster explorer. how can I help?
        BOT_NAME: DagsterExplorer
        DATABASE_CREDENTIALS: ''
        FILES: '[]'
        RUNNER_ID: snowflake-1
        UDF_ACTIVE: Y
        ''')

# TODO: define a specialized set of 'dagster dbt tools' (e.g. look at the dbt code) instead of the more dgaster-centric 'dagster tools'
Bot_DBTExplorer_yaml = dedent(
        f'''
        BOT_ID: DBTExplorer
        AVAILABLE_TOOLS: '[ "dagster_tools", "data_connector_tools", "snowflake_tools"]'
        BOT_AVATAR_IMAGE: null
        BOT_IMPLEMENTATION: openai
        BOT_INSTRUCTIONS: >
            Your are a DBT expert. You have been provided with specific tools to query the definitions and state of a certain DBT models which are being orchestrated by Dagster Cloud.
            Your job is to answer questions about the DBT setup and to provide the relevant evidence as asked, without interpretation - only facts.
            You have access to the DBT model definitions as well as to the underlying tables and views and the actual query history from the database engine.

            Your expertrise is to locate and analyize the relvant information from the DBT-dagster setup.

            {NON_INTERACTIVE_PROMPT_SUFFIX}

        BOT_INTRO_PROMPT: Hello, I am a DBT explorer. how can I help?
        BOT_NAME: DBTExplorer
        DATABASE_CREDENTIALS: ''
        FILES: '[]'
        RUNNER_ID: snowflake-1
        UDF_ACTIVE: Y
        ''')

# DagsterRCADetective - No delegation - the top level orchstrator with direct access to all the tools.
Bot_DagsterRCADetective_yaml = dedent(
    f"""
        BOT_ID: DagsterRCADetective
        BOT_NAME: DagsterRCADetective
        AVAILABLE_TOOLS: '[ "dagster_tools", "data_connector_tools", "snowflake_tools"]'
        BOT_AVATAR_IMAGE: null
        BOT_IMPLEMENTATION: openai
        BOT_INSTRUCTIONS: >
            Your are a Dagster Root Cause Analysis (RCA) "Detective". You are an expert in investigating failures in Dagster Cloud pipelines and providing a Root Cause Analysis report
            to a Data Ops person whenver asked to.

            You have been granted access to tools that are able to query the Dagster Cloud setup and fetch current information about the setup itself or specific failures of specific runs. You can also
            use data base tools to run SQL queries directly on top of the database tables or views, to examine the state of assets that are materialized into database objects.

            Every new RCA process starts with presenting to you a summary of the failure evidence for a specific run_id.

            {NON_INTERACTIVE_PROMPT_SUFFIX}

        BOT_INTRO_PROMPT: Hello, I'm a Dagster RCA Detective.
        DATABASE_CREDENTIALS: ''
        FILES: '[]'
        RUNNER_ID: snowflake-1
        UDF_ACTIVE: Y
        """
)


# Delegating RCA manager
Bot_DagsterRCAMgr_yaml = (dedent(
        f'''
        BOT_ID: DagsterRCAMgr
        BOT_NAME: DagsterRCAMgr
        AVAILABLE_TOOLS: '[ "bot_dispatch_tools", ]'
        BOT_AVATAR_IMAGE: null
        BOT_IMPLEMENTATION: openai
        BOT_INSTRUCTIONS: >
        ''')
        + indent(
            dedent(
            f'''
            You are the manager and coordiantor of a multi-agent project to investigate a run failure in a Dasgter cloud setup, given a run_id.

            You job is to drive the process of Root Cause Analysis (RCA) for the failure and generate an RCA report for a human DataOps person.
            You are an expert in panning and coordinating the next step of the RCA process based on the evidence collected at each step.

            You oversee the following AI agents:

             * DagsterExplorer: An expert in understanding the dasgter setup; Can fetch information about the setup, status of assets,
               asset lineage, status of runs and run history.
               Excels at locating and summarizing the infromation requested.

             * DBTExplorer: An expert in understding  DBT setups and the Dagster-DBT intergation; Can fetch and analyze the definitions
               and state of the DBT models, as well as data quality tests defined withn DBTi. Capable of running running SQl queries
               againts the underlying database.

            ## Core Responsibilities:

            ### 1. Initial triage:

                • Given a Dagster run failure, indetified by a run_id, collect the infromation for an initial triage.
                • Based in the intial evidence, plan the next step.

            ### 2. Drive and coordinate the root cause analysis (RCA) process to completion:

                • This is an iterative process where at each step you collect evidence, examine the dependencies of the information on upstream data
                  sources or relevant I/O sources or system, and decide on the next step. The next step can be to either collect more details form the
                  upstream assets/data objects or resources, or to conclude the RCA process as no more useful informatin is available.

                • Delegate work to the most relevant AI agents (domain experts) to fetch and analyze the information needed for each step of
                  the process.

                • Use the "5 whys" method. This method involves asking “Why?” repeatedly (typically five times) until the root cause of a problem
                  identified. This iterative interrogative technique helps in drilling down to the source of the issue. The Five Whys approach
                  fosters a culture of in-depth inquiry, encouraging teams to look beyond the surface symptoms of a problem.

                  By delving deep into each layer of an issue, it prevents superficial fixes and promotes long-term solutions. Moreover, its simplicity makes it accessible to all team members, fostering collective problem-solving and collaboration.

            ### 3. Validate evidence:

                • Ensure outputs from AI Agents meet reporting standards.

                • Re-delegate tasks with clearer instructions if results are insufficient or inconsistent with previous information.

            ### 4. Handle Escalations:

                • If a task requires human input, document the issue and escalate appropriately.

            ### 5. Generate the final RCA report:

                • Generate the final RCA report according to the expected output, as defined below.


            ## Expected final RCA report output:
            ''') +
            RCA_REPORT_STRUCTURE +
            dedent(
            f'''
            {NON_INTERACTIVE_PROMPT_SUFFIX}
            '''
            ),
            prefix=' '*4)
        + dedent('''
        BOT_INTRO_PROMPT: Hello, I'm a Dagster RCA Manager
        DATABASE_CREDENTIALS: ''
        FILES: '[]'
        RUNNER_ID: snowflake-1
        UDF_ACTIVE: Y
        ''')
        )

BOT_YANMLS = [Bot_DagsterExplorer_yaml, Bot_DBTExplorer_yaml, Bot_DagsterRCADetective_yaml, Bot_DagsterRCAMgr_yaml]

def load_all_bots_definitions() -> Mapping[str, Dict] :
    definitions = [yaml.safe_load(txt) for txt in BOT_YANMLS]
    return {definition["BOT_ID"]: definition
            for definition in definitions}

class _FlowBase:
    def __init__(self, gclient: GenesisAPI, bot_registration_required: bool = True):
        self._gclient = gclient
        self._bot_definitions_map = {definition["BOT_ID"]: definition
                                     for definition in self.get_bot_defs()}

        self._bot_registration_required = bot_registration_required


    # pure virtual method
    def get_bot_defs(self) -> List[Dict]:
        raise NotImplementedError("Subclasses should implement this method.")

    # pure virtual method
    def _run(self):
        raise NotImplementedError("Subclasses should implement this method.")


    def call_genesis_bot(self, bot_id, request):
        """Wait for a complete response from the bot, streaming partial results."""
        assert bot_id in self._bot_definitions_map, f"{bot_id} definition not found"
        return _call_genesis_bot(self._gclient, bot_id, request)


    def run(self):
        self.register_bots() # only once
        self._run()


    def register_bots(self):
        if not self._bot_registration_required:
            return
        for bot_def in self.get_bot_defs():
            logger.info(f"Registering bot with BOT_ID={bot_def['BOT_ID']} with GenesisAPI")
            self._gclient.register_bot(bot_def)
        self._bot_registration_required = False


class DagsterRCAFlow1(_FlowBase):

    def __init__(self, run_id:str, gclient: GenesisAPI, bot_registration_required: bool=True):
        self.run_id = run_id
        super().__init__(gclient=gclient, bot_registration_required=bot_registration_required)


    # pure virtual method
    def get_bot_defs(self):
        defs = load_all_bots_definitions()
        return [defs[bot_id]
                for bot_id in ["DagsterExplorer", "DagsterRCADetective"] ]


    @traceable(name="task_collect_run_id_failure_data")
    def task_collect_run_id_failure_data(self, run_id):

        #TASK: fetch_failed_run_debug_information
        report = self.call_genesis_bot('DagsterExplorer', dedent(
            f'''
            Fetch full run debug data from the Dagster Cloud setup for run id {run_id}.
            Extract only the essential details from the debug data that would be used as the initial evidence for the failure, including the relevant context
            in which the failure happened, as the first step in the RCA process.

            expected_output:
                Generate a JSON report with the following top-level groups:

                - Failue summary: A short english description of the failure, including the details of the asset(s) which failed to materialize, at what step did the failure occur, and what was the failue.

                - Failed step: identify the step where the failure occured. Including the timeline of the steps preceeding the failure and how long was was each step running.

                - Failed step details: A list of all log messages that are relevant for the step that failed. Include the exact command(s) that failed and the exact and FULL error(s) that were reported.

                - Asset details: list which assets failed to materialize and the root cause of failure for each materialization. Highlight specific data quality issues (if any) that were highighted during the run.

                - Codebase Reference: Locate the corresponding code repos for the steps, resources, and assets involved in the run.

            Guidlines:
                -DO NOT include sensitive information such as passowrds and account IDs.
            ''')
        )
        return report

    @traceable(name="task_perform_full_rca")
    def task_perform_full_rca(self, run_id, run_debug_summary):
        report = self.call_genesis_bot('DagsterRCADetective', dedent(
        f'''
        Your task it to generate a root-cause-analysis (RCA) report for run id {run_id}.
        Here are the details of the failure, as summarized earlier from a full debug dump: The end of the report is demarcated by the string '<<FAILURE_REPORT_END'

        {run_debug_summary}

        <<FAILURE_REPORT_END

        NOTE: there is no need to fetch this run debug dump again. All the information needed to perform the RCA should be included in the above failure report..

        Follow these steps in order to trace the root cause of the failure:

        ## Step 1:
            Goal: assert the reported prblem still exists
            description:
                Assert that the problems reported in {run_id} still exists. If the failure seems to be caused by a transient system issue then then use the tools
                available to you to to check if the system issue still exists. If this is asset meterlization faiure do to a data quality or consistency test, make sure
                the reported issues still exist by first fetching the definition of the asset and then examining the content of the assets themsleves.
                Note that in some cases there could have been considerable time passed between when the error was reported and when this RCA process was kicked off.

        ##  Step 2:
        Goal: create an upstream asset dependency map
            description:
                Trace upstream dependencies of the assets whic failed to materialize by looking at the full Dagster pipeline lineage.
                Use asset state, asset metadata and asset lineage to trace the root of the problem as much upsteam as possible.

                For example, if a non-null test failed on downstream asset Y, and X is an upstream asset of Y, then exanine the content of
                X to see if it may contain null values that could have been fetched into Y. Use the definition of asset Y to derive the proper query
                to examine the data upsteam asset X.
                If it turns out that indeed the data in asset X explains the failed test in asset Y, repeat this step for all upstream assets of X, recurisvely.

                REMEMBER - your goal is to trace the ROOT of the problem as much upstream as possible.

                If the failure involves custom Python scripts or other transformations, fetch the specific logs or code references for the affected step.
                If the issue involves external data sources (e.g., S3, databases), use your available tools to query the data sources for the relevant data or state (e.g. null,  missing data, duplicate data, schema changes).

        ## Step 3:
            Goal: correlate_findings
            description:
                Correlate faiure logs, lineage information, and findings from DBT, SQL, Python, or relevant data sources.
                Dedicde if we have sufficient evidence to compile a helpful and coherent RCA report for the failure in run_id {run_id}.
                A compelling RCA report requires that we have all the supporting evidence to pin-point root cause of the failure. If more information is needed,
                fetch the required information before proceeding to the next step.

        ## Step 4:
            Goal: generate_rca_report
            description: Compile all findings into a structured RCA report for Dagster run id {run_id}.

            Expected output: A detaied root cause analysis report, with the following outline:

                ### 1. Executive Summary

                    A brief overview of the incident, including the time of failure, impact, and the importance of this RCA.

                ### 2. Incident Description

                    Date and Time of Incident: When the failure was first detected.
                    1Context: Overview of the affected pipeline, including its primary function and key stakeholders.
                    Observed Symptoms: Specific symptoms or errors that indicated failure (e.g., job timeouts, data discrepancies, missing outputs).
                    1
                ### 3. Impact Assessment

                    Business Impact: A clear description of how the failure affected the business or customers.
                    Data Impact: Scope and extent of the data affected (e.g., delayed reports, corrupted or incomplete data).
                    Affected Systems: Systems and components impacted directly or indirectly by the failure.

                ### 4. Timeline of Events

                    Incident Timeline: Step-by-step account of the incident, from detection to resolution.
                    Key Actions and Responses: Details of actions taken by team members to mitigate and investigate the issue.
                    Detection and Notification: How and when the issue was detected (e.g., monitoring alerts, user reports).

                ### 5. Root Cause

                    Technical Analysis: A detailed breakdown of what caused the failure, including technical evidence.
                    Underlying Issues: Identification of contributing factors (e.g., configuration error, code bug, dependency failure).
                    Tools Used: Any tools or methodologies used to trace the root cause (e.g., log analysis, metrics monitoring).

                ### 6. Contributing Factors

                    Environmental Factors: Anything external that contributed (e.g., network issues, third-party service failures).
                    Process Gaps: Gaps in operational processes that may have allowed or exacerbated the failure (e.g., insufficient testing, unclear escalation paths).

                ### 7. Resolution and Recovery

                    Mitigation Actions: Steps taken to restore services and ensure data accuracy.
                    Recovery Timeline: Details on how long it took to recover and restore the data pipeline.
                    Verification and Validation: Measures taken to confirm the problem was resolved and data integrity was restored.

                ### 8. Preventive Measures and Recommendations
                    Short-term Fixes: Quick mitigations implemented to prevent recurrence.
                    Long-term Preventive Actions: Planned improvements (e.g., code changes, additional testing).
                    Monitoring Improvements: Changes to monitoring and alerting to improve early detection.
                    Training/Process Updates: Changes to team processes or additional training required.

                ### 10. Follow-Up Actions

                    Action Items List: A clear list of follow-up tasks, assigned owners, and target deadlines.
                    Status Tracking: Methods for tracking progress on preventive measures.

                ### 11. Appendices

                    Logs and Evidence: Attach relevant logs, screenshots, or other data analyzed.
                    Technical Diagrams: Include pipeline architecture diagrams, where applicable, for better understanding

            ''')
        )
        return report


    def _run(self):
        run_summary = self.task_collect_run_id_failure_data(run_id=self.run_id)
        rca_report = self.task_perform_full_rca(run_id=self.run_id, run_debug_summary=run_summary)
        return rca_report


class DagsterRCAFlow2(_FlowBase):

    def __init__(self, run_id:str, gclient: GenesisAPI, bot_registration_required: bool=True):
        self.run_id = run_id
        super().__init__(gclient=gclient, bot_registration_required=bot_registration_required)


    # pure virtual method
    def get_bot_defs(self):
        defs = load_all_bots_definitions()
        return [defs[bot_id]
                for bot_id in ["DagsterExplorer", "DBTExplorer", "DagsterRCAMgr"] ]


    @traceable(name="task_perform_full_rca")
    def task_perform_full_rca(self, run_id):
        report = self.call_genesis_bot('DagsterRCAMgr', dedent(
        f'''
        Your task it to generate a root-cause-analysis (RCA) report for run id {run_id}.
        ''')
        )
        return report


    def _run(self):
        rca_report = self.task_perform_full_rca(run_id=self.run_id)
        return rca_report


def main():
    print("---------------------------------")
    print("  DAGSTER RCA DEMO BEGIN")
    print("---------------------------------")
    try:

        gclient = GenesisAPI(server_type=GenesisLocalServer,
                            scope="GENESIS_TEST",
                            sub_scope="GENESIS_AD")

        # register all the local bot defs with the GC client
        #register_bots(gclient)

        # Registering the dagster tools offline:
        # 1) had to run the full server (flask mode) to update the various tables (AVAILABE TOOLS)
        # 2) Ran this offline to add the tools:
        #    >> snow sql -c GENESIS_CVB  -q "update GENESIS_TEST.GENESIS_AD.BOT_SERVICING set  AVAILABLE_TOOLS = '[\"data_connector_tools\", \"snowflake_tools\", \"dagster_tools\"]' where bot_name = 'DagsterExplorer'
        #
        # Turning off  Janice (to save time creating the session): (to restore, set it to 'snowflake-1')
        #    >> snow sql -c GENESIS_CVB  -q "update GENESIS_TEST.GENESIS_AD.BOT_SERVICING set RUNNER_ID=NULL  where BOT_ID='Janice'"
        #
        if (1):
            run_id = '975f848d-11f2-4655-8970-740bbf66edda' # Dagster RUN_ID
            #flow = DagsterRCAFlow1(gclient=gclient, run_id=run_id)
            flow = DagsterRCAFlow2(gclient=gclient, run_id=run_id)
            result = flow.run()
            print("---------------------------------")
            print("          FINAL RESULT:")
            print("---------------------------------")
            pprint(result)
    finally:
        gclient.shutdown()


if __name__ == "__main__":
    main()
