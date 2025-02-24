import json
from typing import Optional, Dict, List, Any
from jira import JIRA  # You'll need to pip install jira
from genesis_bots.connectors.snowflake_connector.snowflake_connector import SnowflakeConnector

from genesis_bots.core.bot_os_tools2 import (
    BOT_ID_IMPLICIT_FROM_CONTEXT,
    THREAD_ID_IMPLICIT_FROM_CONTEXT,
    ToolFuncGroup,
    ToolFuncParamDescriptor,
    gc_tool,
)

class JiraConnector:
    def __init__(self):
        """Initialize JiraConnector with None values, will be set during connection."""
        self.jira_url = None
        self.username = None
        self.api_token = None
        self.client = None
        # Get connection parameters on initialization
        self._initialize_connection_params()

    def _initialize_connection_params(self):
        """Initialize connection parameters from Snowflake."""
        try:
            db_adapter = SnowflakeConnector(connection_name="Snowflake")
            jira_config_params = db_adapter.get_jira_config_params()

            # Extract the 'Data' field, which is a JSON string
            data_json_str = jira_config_params['Data']

            # Parse the JSON string into a Python list of dictionaries
            data_list = json.loads(data_json_str)

            # Convert list of dictionaries to a single dictionary
            params_dict = {item['parameter']: item['value'] for item in data_list}

            # Set instance variables
            self.jira_url = params_dict['jira_url']
            self.username = params_dict['jira_email']
            self.api_token = params_dict['jira_api_key']

        except Exception as e:
            raise Exception(f"Failed to initialize Jira connection parameters: {str(e)}")

    def connect(self):
        """Establish connection to Jira."""
        if not all([self.jira_url, self.username, self.api_token]):
            raise Exception("Connection parameters not properly initialized")

        try:
            self.client = JIRA(
                server=self.jira_url,
                basic_auth=(self.username, self.api_token)
            )
            return True
        except Exception as e:
            print(f"Failed to connect to Jira: {str(e)}")
            return False

    def jira_connector(
                self,
                action: str,
                project_key: Optional[str] = None,
                summary: Optional[str] = None,
                description: Optional[str] = None,
                status: Optional[str] = None,
                issue_key: Optional[str] = None,
                issue_type: Optional[str] = None,
                priority: Optional[str] = None,
                jql: Optional[str] = None,
                user_name: Optional[str] = None,
                thread_id = None
            ) -> Dict[str, Any]:
        """
        Main interface for Jira operations.

        Args:
            action: One of CREATE_ISSUE, UPDATE_ISSUE, GET_ISSUE, or SEARCH_ISSUES
            project_key: The Jira project key (e.g., 'DATA', 'DEV')
            summary: Issue summary/title for CREATE_ISSUE action
            description: Detailed description for CREATE_ISSUE or UPDATE_ISSUE actions
            status: Jira issue status to be updated exactly as requested for CREATE_ISSUE or UPDATE_ISSUE actions
            issue_key: The Jira issue key for UPDATE_ISSUE or GET_ISSUE actions
            issue_type: The Jira issue type for UPDATE_ISSUE or CREATE_ISSUE actions
            priority: The Jira issue priority (e.g. 'Low','High','Highest') for CREATE_ISSUE or UPDATE_ISSUE actions
            jql: JQL query string for SEARCH_ISSUES action
            user_name: Jira user name for SEARCH_ISSUES, CREATE_ISSUE, or UPDATE_ISSUE actions

        Returns:
            Dict containing operation results
        """

        action = action.upper()

        if action == "CREATE_ISSUE":
            success = False
            if project_key and summary and description and issue_type:
                result = self._create_issue(project_name=project_key, summary=summary, description=description, issue_type=issue_type, priority=priority, user_name=user_name)
                if result:
                    success = True
            else:
                result = "Issue details not provided"

            return {
                "Success": success,
                "Message": "Jira issue created successfully.",
                "Suggestion": "Show the details of the new issue. Offer to perform another action",
                "result": result,
            }

        if action == "GET_ISSUE":
            success = False
            if issue_key:
                result = self._get_jira_issue(issue_name=issue_key)
                if result:
                    success = True
            else:
                result = "Issue key not provided"

            return {
                "Success": success,
                "Message": "Jira issue found successfully.",
                "Suggestion": "Offer to perform another action",
                "result": result,
            }

        if action == "SEARCH_ISSUES":
            success = False
            # TODO get user name by name or other attribute from Jira?
            if user_name:
                result = self._get_issues_by_user(user_name=user_name)
                if result:
                    success = True
            elif description or summary or status or issue_type or priority or project_key:
                result = self._search_issues(description=description, summary=summary, status=status, issue_type=issue_type, priority=priority, project_key=project_key)
                if result:
                    success = True
            else:
                result = "Search attribute(s) not provided"

            return {
                "Success": success,
                "Message": f"Jira issues found successfully.",
                "Suggestion": "Offer to perform another action",
                "result": result,
            }

        if action == "UPDATE_ISSUE":
            success = False
            result = ""
            if issue_key:
                if status:
                    status_result = self._set_issue_status(issue_name=issue_key, status_text=status)
                    if status_result:
                        success = True
                        message = "Jira status updated successfully"
                        suggestion = "Offer to perform another action"
                        result += f" {status_result}"
                    else:
                        success = False
                        message = "Jira status not updated properly"
                        suggestion = "Ensure you have the correct issue key and allowed status"
                        return {
                            "Success": success,
                            "Message": message,
                            "Suggestion": suggestion,
                            "result": status_result,
                        }
                if description:
                    comment_result = self._set_issue_comment(issue_name=issue_key, comment=description)
                    if comment_result:
                        success = True
                        message = "Jira comment added successfully"
                        suggestion = "Offer to perform another action"
                        result += f" {comment_result}"
                    else:
                        success = False
                        message = "Jira comment not added properly"
                        suggestion = "Ensure you have the correct issue key and have entered a comment"
                        return {
                            "Success": success,
                            "Message": message,
                            "Suggestion": suggestion,
                            "result": comment_result,
                        }
                if user_name:
                    assign_user_result = self._set_issue_assigned_user(issue_name=issue_key, user_name=user_name)
                    if assign_user_result:
                        success = True
                        message = "Jira user assigned successfully"
                        suggestion = "Offer to perform another action"
                        result += f" {assign_user_result}"
                    else:
                        success = False
                        message = "Jira user not assigned properly"
                        suggestion = "Ensure you have the correct issue key and existing user name"
                        return {
                            "Success": success,
                            "Message": message,
                            "Suggestion": suggestion,
                            "result": assign_user_result,
                        }
                if result:
                    success = True
            else:
                result = "Issue key was not provided"

            return {
                "Success": success,
                "Message": message,
                "Suggestion": suggestion,
                "result": result.strip(),
            }

        if not action:
            # TODO: Implement actual Jira operations
            return {
                "status": "not_implemented",
                "message": f"Action {action} not implemented yet",
                "data": None
            }

    def _jira_api_connector(self):
        """Get a connected JiraConnector instance."""
        try:
            # Since we already have the connection parameters, just connect
            if self.connect():
                return self
            raise Exception("Failed to connect to Jira")
        except Exception as e:
            return {"error connecting to JIRA": str(e)}

    def _create_issue(self, project_name, issue_type, summary, description, user_name=None, priority=None, thread_id=None):

        try:
            jira_connector = self._jira_api_connector()
            jira = jira_connector.connect()

            result_output = {
                "issue_data": None,
                "success": False,
                "message": None,
            }

            if jira == True:

                # Step 1: Validate the project
                projects = jira_connector.client.projects()
                project_keys = [project.key for project in projects]

                if project_name not in project_keys:
                    result_output["message"] = f"Invalid project key: {project_name}"
                else:
                    # Step 2: Validate the issue type for the project
                    issue_types = jira_connector.client.issue_types()
                    issue_type_names = [issue_type.name for issue_type in issue_types]

                    if issue_type not in issue_type_names:
                        result_output["message"] = f"Invalid issue type: {issue_type}"
                    else:
                        if priority:
                            # Fetch the available values for the Priority field from the issue metadata
                            metadata = jira_connector.client.createmeta(projectKeys=project_name, issuetypeNames=issue_type, expand='projects.issuetypes.fields')
                            fields = metadata['projects'][0]['issuetypes'][0]['fields']

                            # Get allowed values for the Priority field
                            priority_field = fields.get('priority')
                            if priority_field and 'allowedValues' in priority_field:
                                allowed_priorities = [p['name'] for p in priority_field['allowedValues']]

                                # Check if the passed priority is valid
                                if priority not in allowed_priorities:
                                    result_output["message"] = f"Invalid priority value: {priority}. Allowed values are: {allowed_priorities}"
                                    return json.dumps(result_output, indent=4)
                            else:
                                result_output["message"] = "Priority field is not available in this issue type or project."
                                return json.dumps(result_output, indent=4)

                        users = jira_connector.client.search_users(query=user_name)
                        if users:
                            assignee = users[0].accountId
                        else:
                            assignee = None

                        issue_dict = {
                            'project': {'key': project_name},  # Replace 'PROJ' with your project key
                            'summary': summary,
                            'description': description,
                            'priority': {'name': priority},
                            'issuetype': {'name': issue_type},  # Replace 'Task' with the appropriate issue type
                            'assignee': {'id': assignee},
                        }

                        new_issue = jira_connector.client.create_issue(fields=issue_dict)

                        issue_link = jira_connector.jira_url + '/browse/' + new_issue.key

                        # Fetch details of the created issue to return as JSON
                        issue_data = {
                            "id": new_issue.id,
                            "key": new_issue.key,
                            "link": issue_link,
                            "fields": {
                                "summary": new_issue.fields.summary,
                                "description": new_issue.fields.description,
                                "status": new_issue.fields.status.name,
                                "priority": new_issue.fields.priority.name,
                                "issuetype": new_issue.fields.issuetype.name,
                            }
                        }
                        result_output["success"] = True
                        result_output["message"] = "Issue successfully created"
                        result_output["issue_data"] = issue_data

                # Print or return the issue data as JSON
                content_json = json.dumps(result_output, indent=4)

            else:
                content_json = {"Unable to get connection to jira api": project_name}
            return content_json
        except Exception as e:
            return {"error updating comment for issue": str(e)}

    def _set_issue_assigned_user(self, issue_name, user_name, thread_id=None):
        try:
            jira_connector = self._jira_api_connector()
            jira = jira_connector.connect()

            if jira == True:

                # JSON output structure
                reassign_output = {
                    "issue_key": issue_name,
                    "new_assignee": user_name,
                    "success": False,
                    "message": None,
                }

                try:
                    # Update the assignee of the issue
                    issue = jira_connector.client.issue(issue_name)

                    users = jira_connector.client.search_users(query=user_name)
                    if users:
                        account_id = users[0].accountId
                        issue.update(fields={"assignee": {"accountId": account_id}})
                        reassign_output["success"] = True
                        reassign_output["message"] = "User successfully assigned to the Jira issue"
                    else:
                        reassign_output["message"] = f"No user found"

                except Exception as e:
                    reassign_output["message"] = str(e)

                # Convert to JSON and print
                content_json = json.dumps(reassign_output, indent=4)
            else:
                content_json = {"Unable to get connection to jira api": issue_name}
            return content_json
        except Exception as e:
            return {"error updating comment for issue": str(e)}

    def _set_issue_comment(self, issue_name, comment, thread_id=None):
        try:
            jira_connector = self._jira_api_connector()
            jira = jira_connector.connect()

            if jira == True:

                # JSON output structure
                comment_output = {
                    "issue_key": issue_name,
                    "comment": comment,
                    "success": False,
                    "message": None,
                }

                # Add the comment to the specified issue
                jira_connector.client.add_comment(issue_name, comment)
                comment_output["success"] = True
                comment_output["message"] = f"Comment added to {issue_name}"

                # Convert to JSON and print
                content_json = json.dumps(comment_output, indent=4)
            else:
                content_json = {"Unable to get connection to jira api": issue_name}
            return content_json
        except Exception as e:
            return {"error updating comment for issue": str(e)}

    def _set_issue_status(self, issue_name, status_text, thread_id=None):
        try:
            jira_connector = self._jira_api_connector()
            jira = jira_connector.connect()
            if jira == True:

                # Retrieve available transitions
                issue = jira_connector.client.issue(issue_name)
                transitions = jira_connector.client.transitions(issue)

                # Create a comma-separated list of valid transition names
                valid_transitions = ", ".join(transition["name"] for transition in transitions)

                # Find the transition ID for the desired status
                transition_id = next((transition["id"] for transition in transitions if transition["name"] == status_text), None)

                # JSON output for status change
                status_output = {
                    "issue_key": issue_name,
                    "requested_status": status_text,
                    "valid_transitions": valid_transitions,
                    "success": False,
                    "message": None,
                }

                # Perform the transition if a valid ID was found
                if transition_id:
                    jira_connector.client.transition_issue(issue, transition_id)
                    status_output["success"] = True
                    status_output["message"] = f"Issue {issue_name} status changed to {status_text}"
                else:
                    status_output["message"] = f"Status '{status_text}' is not a valid transition for {issue_name}. The valid transitions are {valid_transitions}"

                # Convert to JSON and print
                content_json = json.dumps(status_output, indent=4)
            else:
                content_json = {"Unable to get connection to jira api": issue_name}
            return content_json
        except Exception as e:
            return {"error updating status for issue": str(e)}

    def _search_issues(self, description=None, summary=None, status=None, issue_type=None, priority=None, project_key=None):
        try:
            jira_connector = self._jira_api_connector()
            jira = jira_connector.connect()

            if jira == True:
                jql_parts = []

                if project_key:
                    jql_parts.append(f'project = "{project_key}"')

                if description:
                    jql_parts.append(f'description ~ "{description}"')

                if summary:
                    jql_parts.append(f'summary ~ "{summary}"')

                if issue_type:
                    jql_parts.append(f'issuetype = "{issue_type}"')

                if priority:
                    jql_parts.append(f'priority = "{priority}"')

                if status:
                    jql_parts.append(f'status = "{status}"')

                # Join all parts with AND operator
                jql_query = ' AND '.join(jql_parts) if jql_parts else 'created is not EMPTY'

                # Fetch the issues
                issues = jira_connector.client.search_issues(jql_query)

                # Display the issues found
                found_issues = [
                    {
                        "issue_key": issue.key,
                        "summary": issue.fields.summary,
                        "status": issue.fields.status.name,
                        "priority": issue.fields.priority.name,
                        "issuetype": issue.fields.issuetype.name,
                        "link": jira_connector.jira_url + '/browse/' + issue.key
                    }
                    for issue in issues
                ]

                # Convert to JSON and print
                content_json = json.dumps({"found_issues": found_issues}, indent=4)
            else:
                content_json = {"Unable to get connection to jira api": description}

            return content_json
        except Exception as e:
            return {"error getting all issues by user": str(e)}

    def _get_issues_by_user(self, user_name, thread_id=None):
        try:
            jira_connector = self._jira_api_connector()
            jira = jira_connector.connect()

            if jira == True:

                # JQL query to find all issues assigned to the specified user
                if user_name == 'Unassigned':
                    jql_query = 'assignee is EMPTY'
                else:
                    users = jira_connector.client.search_users(query=user_name)
                    if users:
                        account_id = users[0].accountId
                    else:
                        account_id = None
                    jql_query = f'assignee = "{account_id}"'

                # Fetch the issues
                issues = jira_connector.client.search_issues(jql_query)

                # Display or process the issues
                assigned_issues = [
                    {
                        "issue_key": issue.key,
                        "summary": issue.fields.summary,
                        "status": issue.fields.status.name,
                    }
                    for issue in issues
                ]

                # Convert to JSON and print
                content_json = json.dumps({"assigned_issues": assigned_issues}, indent=4)
            else:
                content_json = {"Unable to get connection to jira api": user_name}

            return content_json
        except Exception as e:
            return {"error getting all issues by user": str(e)}

    def _get_jira_issue(self, issue_name, thread_id=None):
        try:
            jira_connector = self._jira_api_connector()
            jira = jira_connector.connect()

            # jira = JIRA(server=jira_url, basic_auth=(jira_email, jira_api_key))
            def serialize_field(field_value):
                """Helper function to convert non-serializable objects to strings."""
                if hasattr(field_value, '__dict__'):
                    # Recursively convert objects with attributes to dictionaries
                    return {k: serialize_field(v) for k, v in field_value.__dict__.items()}
                elif isinstance(field_value, list):
                    # Convert lists by serializing each element
                    return [serialize_field(item) for item in field_value]
                elif isinstance(field_value, (int, float, str, bool)) or field_value is None:
                    return field_value  # Directly serializable types
                else:
                    # Convert any remaining object to a string representation
                    return str(field_value)

            # Retrieve the issue
            if jira:
                issue = jira_connector.client.issue(issue_name)

                # Store issue details in a JSON document
                content = {
                    "issue_key": issue.key,
                    "issue_id": issue.id,
                    "issue_url": issue.self,
                    "fields": {},
                    "comments": []
                }

                # Extract comments from the issue
                if issue.fields.comment:
                    for comment in issue.fields.comment.comments:
                        comment_data = {
                            "id": comment.id,
                            "author": comment.author.displayName,
                            "created": comment.created,
                            "updated": comment.updated,
                            "body": comment.body
                        }
                        content["comments"].append(comment_data)

                # Populate the fields in the content dictionary
                for field_name, field_value in issue.fields.__dict__.items():
                    content["fields"][field_name] = serialize_field(field_value)

                # Convert the dictionary to JSON format
                content_json = json.dumps(content, indent=4)

            return content_json
        except Exception as e:
            return {"error": str(e)}

jira_connector_tools = ToolFuncGroup(
    name="jira_connector_tools",
    description="Jira connector tools",
    lifetime="PERSISTENT",
)

@gc_tool(
    action="""The action to perform: CREATE_ISSUE, UPDATE_ISSUE, GET_ISSUE, or SEARCH_ISSUES. If asked to assign or update a user, or search for issues by user, capture the user_name.
            Do not capture description variable unless told to update or add a description or comment.
            If looking for issues that are unassigned, set user_name to Unassigned.
            """,
    project_key="The Jira project key (e.g., 'DATA', 'DEV')",
    summary="Issue summary/title for CREATE_ISSUE or SEARCH_ISSUE action",
    description="Detailed description or comment for CREATE_ISSUE or UPDATE_ISSUE or SEARCH_ISSUE actions. Use only the text entered by the user, do not auto create this field.",
    status="Jira issue status to be updated exactly as requested for CREATE_ISSUE or UPDATE_ISSUE or SEARCH_ISSUE actions.",
    issue_key="The Jira issue key for UPDATE_ISSUE or GET_ISSUE or SEARCH_ISSUE actions (e.g., 'DATA-123')",
    issue_type="The Jira issue type for UPDATE_ISSUE or CREATE_ISSUE or SEARCH_ISSUE actions (e.g., 'Task')",
    priority="The Jira issue priority or CREATE_ISSUE or UPDATE_ISSUE or SEARCH_ISSUE actions (e.g. 'Low','High','Highest')",
    jql="JQL query string optional for SEARCH_ISSUES action",
    user_name="Jira user name for SEARCH_ISSUES, CREATE_ISSUE, or UPDATE_ISSUE actions",
    thread_id=THREAD_ID_IMPLICIT_FROM_CONTEXT,
    # bot_id=BOT_ID_IMPLICIT_FROM_CONTEXT,
    _group_tags_=[jira_connector_tools],
)
def jira_connector(
    action: str,
    project_key: str = None,
    summary: str = None,
    description: str = None,
    status: str = None,
    issue_key: str = None,
    issue_type: str = None,
    priority: str = None,
    jql: str = None,
    user_name: str = None,
    thread_id: str = None,
) -> Dict[str, Any]:
    """
    Main interface for Jira operations.

    Args:
        action: One of CREATE_ISSUE, UPDATE_ISSUE, GET_ISSUE, or SEARCH_ISSUES
        project_key: The Jira project key (e.g., 'DATA', 'DEV')
        summary: Issue summary/title for CREATE_ISSUE action
        description: Detailed description for CREATE_ISSUE or UPDATE_ISSUE actions
        status: Jira issue status to be updated exactly as requested for CREATE_ISSUE or UPDATE_ISSUE actions
        issue_key: The Jira issue key for UPDATE_ISSUE or GET_ISSUE actions
        issue_type: The Jira issue type for UPDATE_ISSUE or CREATE_ISSUE actions
        priority: The Jira issue priority (e.g. 'Low','High','Highest') for CREATE_ISSUE or UPDATE_ISSUE actions
        jql: JQL query string for SEARCH_ISSUES action
        user_name: Jira user name for SEARCH_ISSUES, CREATE_ISSUE, or UPDATE_ISSUE actions

    Returns:
        Dict containing operation results
    """
    return JiraConnector().jira_connector(
        action=action,
        project_key=project_key,
        summary=summary,
        description=description,
        status=status,
        issue_key=issue_key,
        issue_type=issue_type,
        priority=priority,
        jql=jql,
        user_name=user_name,
        thread_id=thread_id
    )

jira_connector_functions = [jira_connector,]

# Called from bot_os_tools.py to update the global list of functions
def get_jira_connector_functions():
    return jira_connector_functions
