import json
from typing import Optional, Dict, List, Any
from github import Github, Auth
from genesis_bots.connectors.snowflake_connector.snowflake_connector import SnowflakeConnector
from   genesis_bots.core.logging_config \
                                import logger
from genesis_bots.core.bot_os_tools2 import (
    BOT_ID_IMPLICIT_FROM_CONTEXT,
    THREAD_ID_IMPLICIT_FROM_CONTEXT,
    ToolFuncGroup,
    ToolFuncParamDescriptor,
    gc_tool,
)

class GithubConnector:
    def __init__(self):
        """Initialize GithubConnector with None values, will be set during connection."""
        self.github_token = None
        self.client = None
        # Get connection parameters on initialization
        self._initialize_connection_params()

    def _initialize_connection_params(self):
        """Initialize connection parameters from Snowflake."""
        try:
            db_adapter = SnowflakeConnector(connection_name="Snowflake")
            github_config_params = db_adapter.get_github_config_params()

            # Extract the 'Data' field, which is a JSON string
            data_json_str = github_config_params['Data']

            # Parse the JSON string into a Python list of dictionaries
            data_list = json.loads(data_json_str)

            # Convert list of dictionaries to a single dictionary
            params_dict = {item['parameter']: item['value'] for item in data_list}

            # Set instance variables
            self.github_token = params_dict['github_token']

        except Exception as e:
            raise Exception(f"Failed to initialize GitHub connection parameters: {str(e)}")

    def connect(self):
        """Establish connection to GitHub."""
        if not self.github_token:
            raise Exception("Connection parameters not properly initialized")

        try:
            auth = Auth.Token(self.github_token)
            self.client = Github(auth=auth, per_page=100)
            
            # Verify connection and permissions
            try:
                # Test private repo access
                user = self.client.get_user()
                private_repos = list(user.get_repos(visibility='private'))
                logger.info(f"Successfully connected with access to {len(private_repos)} private repositories")
                return True
            except Exception as e:
                raise Exception(f"Authentication successful but repository access failed: {str(e)}")
            
        except Exception as e:
            logger.error(f"Failed to connect to GitHub: {str(e)}")
            return False

    def github_connector(
                self,
                action: str,
                repo_name: Optional[str] = None,
                owner: Optional[str] = None,
                title: Optional[str] = None,
                body: Optional[str] = None,
                branch: Optional[str] = None,
                issue_number: Optional[int] = None,
                pr_number: Optional[int] = None,
                labels: Optional[List[str]] = None,
                assignees: Optional[List[str]] = None,
                file_path: Optional[str] = None,
                content: Optional[str] = None,
                file_content: Optional[str] = None,
                commit_message: Optional[str] = None,
                thread_id = None
            ) -> Dict[str, Any]:
        """
        Main interface for GitHub operations.
        """
        if not self.client:
            if not self.connect():
                return {"error": "Failed to connect to GitHub"}

        try:
            # Get repository reference if repo_name is provided
            repo = None
            if repo_name and owner:
                repo = self.client.get_repo(f"{owner}/{repo_name}")

            # Repository Operations
            if action == "GET_REPO_INFO":
                return {
                    "name": repo.name,
                    "description": repo.description,
                    "stars": repo.stargazers_count,
                    "forks": repo.forks_count,
                    "url": repo.html_url
                }
            elif action == "CREATE_REPO":
                new_repo = self.client.get_user().create_repo(
                    name=repo_name,
                    description=body,
                    private=True
                )
                return {"url": new_repo.html_url, "name": new_repo.name}
            elif action == "DELETE_REPO":
                repo.delete()
                return {"status": "deleted"}

            # Issue Operations
            elif action == "CREATE_ISSUE":
                issue = repo.create_issue(
                    title=title,
                    body=body,
                    labels=labels,
                    assignees=assignees
                )
                return {"number": issue.number, "url": issue.html_url}
            elif action == "UPDATE_ISSUE":
                issue = repo.get_issue(number=issue_number)
                issue.edit(title=title, body=body, labels=labels, assignees=assignees)
                return {"number": issue.number, "url": issue.html_url}
            elif action == "CLOSE_ISSUE":
                issue = repo.get_issue(number=issue_number)
                issue.edit(state='closed')
                return {"status": "closed", "number": issue.number}

            # Pull Request Operations
            elif action == "CREATE_PR":
                pr = repo.create_pull(
                    title=title,
                    body=body,
                    head=branch,
                    base='main'
                )
                return {"number": pr.number, "url": pr.html_url}
            elif action == "UPDATE_PR":
                pr = repo.get_pull(pr_number)
                pr.edit(title=title, body=body)
                return {"number": pr.number, "url": pr.html_url}
            elif action == "MERGE_PR":
                pr = repo.get_pull(pr_number)
                pr.merge()
                return {"status": "merged", "number": pr.number}

            # Branch Operations
            elif action == "LIST_BRANCHES":
                branches = list(repo.get_branches())
                return {"branches": [b.name for b in branches]}
            elif action == "CREATE_BRANCH":
                source = repo.get_branch("main")
                repo.create_git_ref(f"refs/heads/{branch}", source.commit.sha)
                return {"name": branch, "status": "created"}
            elif action == "DELETE_BRANCH":
                ref = repo.get_git_ref(f"heads/{branch}")
                ref.delete()
                return {"name": branch, "status": "deleted"}

            # User Operations
            elif action == "GET_USER_INFO":
                user = self.client.get_user(owner) if owner else self.client.get_user()
                return {
                    "login": user.login,
                    "name": user.name,
                    "email": user.email,
                    "bio": user.bio
                }
            elif action == "LIST_USER_REPOS":
                user = self.client.get_user(owner) if owner else self.client.get_user()
                repos = list(user.get_repos())
                return {"repos": [{"name": r.name, "url": r.html_url} for r in repos]}

            # File Operations
            elif action == "CREATE_FILE":
                if not all([file_path, branch]):
                    return {"error": "file_path and branch are required for CREATE_FILE"}
                
                # Use file_content if provided, fall back to content
                content_to_write = file_content if file_content is not None else content
                if content_to_write is None:
                    return {"error": "Either content or file_content is required for CREATE_FILE"}

                try:
                    # First ensure the branch exists
                    try:
                        repo.get_branch(branch)
                    except:
                        # Create branch if it doesn't exist
                        source = repo.get_branch("main")
                        repo.create_git_ref(f"refs/heads/{branch}", source.commit.sha)
                    
                    # Create or update file
                    commit_msg = commit_message or f"Add/Update {file_path}"
                    try:
                        # Try to get existing file
                        contents = repo.get_contents(file_path, ref=branch)
                        repo.update_file(contents.path, commit_msg, content_to_write, contents.sha, branch=branch)
                    except:
                        # File doesn't exist, create it
                        repo.create_file(file_path, commit_msg, content_to_write, branch=branch)
                    return {"status": "success", "path": file_path}
                except Exception as e:
                    return {"error": f"Failed to create/update file: {str(e)}"}

            else:
                return {"error": f"Unknown action: {action}"}

        except Exception as e:
            return {"error": str(e)}

# ... rest of the implementation will follow ... 

github_connector_tools = ToolFuncGroup(
    name="github_connector_tools",
    description="GitHub connector tools",
    lifetime="PERSISTENT",
)

@gc_tool(
    action=ToolFuncParamDescriptor(
        name="action",
        description="The GitHub action to perform",
        required=True,
        llm_type_desc=dict(
            type="string",
            enum=["GET_REPO_INFO", "CREATE_REPO", "DELETE_REPO", "CREATE_ISSUE", 
                  "UPDATE_ISSUE", "CLOSE_ISSUE", "CREATE_PR", "UPDATE_PR", "MERGE_PR", 
                  "LIST_BRANCHES", "CREATE_BRANCH", "DELETE_BRANCH", "GET_USER_INFO", 
                  "LIST_USER_REPOS", "CREATE_FILE"],
        ),
    ),
    repo_name=ToolFuncParamDescriptor(
        name="repo_name",
        description="The name of the repository",
        required=False,
        llm_type_desc=dict(type="string"),
    ),
    owner=ToolFuncParamDescriptor(
        name="owner",
        description="The owner (user or organization) of the repository",
        required=False,
        llm_type_desc=dict(type="string"),
    ),
    title=ToolFuncParamDescriptor(
        name="title",
        description="Title for issue or PR",
        required=False,
        llm_type_desc=dict(type="string"),
    ),
    body=ToolFuncParamDescriptor(
        name="body",
        description="Body content for issue, PR, or repo description",
        required=False,
        llm_type_desc=dict(type="string"),
    ),
    branch=ToolFuncParamDescriptor(
        name="branch",
        description="Branch name for PR or branch operations",
        required=False,
        llm_type_desc=dict(type="string"),
    ),
    issue_number=ToolFuncParamDescriptor(
        name="issue_number",
        description="Issue number for operations on existing issues",
        required=False,
        llm_type_desc=dict(type="integer"),
    ),
    pr_number=ToolFuncParamDescriptor(
        name="pr_number",
        description="PR number for operations on existing PRs",
        required=False,
        llm_type_desc=dict(type="integer"),
    ),
    labels=ToolFuncParamDescriptor(
        name="labels",
        description="List of labels to apply to issues",
        required=False,
        llm_type_desc=dict(type="array", items=dict(type="string")),
    ),
    assignees=ToolFuncParamDescriptor(
        name="assignees",
        description="List of users to assign to issues",
        required=False,
        llm_type_desc=dict(type="array", items=dict(type="string")),
    ),
    file_path=ToolFuncParamDescriptor(
        name="file_path",
        description="Path to the file in the repository",
        required=False,
        llm_type_desc=dict(type="string"),
    ),
    content=ToolFuncParamDescriptor(
        name="content",
        description="Content to write to the file",
        required=False,
        llm_type_desc=dict(type="string"),
    ),
    file_content=ToolFuncParamDescriptor(
        name="file_content",
        description="Content from an uploaded file",
        required=False,
        llm_type_desc=dict(type="string"),
    ),
    commit_message=ToolFuncParamDescriptor(
        name="commit_message",
        description="Message to use when committing changes",
        required=False,
        llm_type_desc=dict(type="string"),
    ),
    thread_id=THREAD_ID_IMPLICIT_FROM_CONTEXT,
    _group_tags_=[github_connector_tools],
)
def github_connector(
    action: str,
    repo_name: str = None,
    owner: str = None,
    title: str = None,
    body: str = None,
    branch: str = None,
    issue_number: int = None,
    pr_number: int = None,
    labels: List[str] = None,
    assignees: List[str] = None,
    file_path: str = None,
    content: str = None,
    file_content: str = None,
    commit_message: str = None,
    thread_id: str = None,
) -> Dict[str, Any]:
    """
    Main interface for GitHub operations.

    Args:
        action: GitHub operation (GET_REPO_INFO, CREATE_REPO, etc.)
        repo_name: Repository name
        owner: Repository owner
        title: Issue/PR title
        body: Content for issue/PR/repo
        branch: Branch name
        issue_number: Issue number
        pr_number: PR number
        labels: Issue labels
        assignees: Issue assignees
        file_path: File path
        content: File content
        file_content: File content
        commit_message: Commit message
        thread_id: Thread ID

    Returns:
        Dict containing operation results
    """
    return GithubConnector().github_connector(
        action=action,
        repo_name=repo_name,
        owner=owner,
        title=title,
        body=body,
        branch=branch,
        issue_number=issue_number,
        pr_number=pr_number,
        labels=labels,
        assignees=assignees,
        file_path=file_path,
        content=content,
        file_content=file_content,
        commit_message=commit_message,
        thread_id=thread_id
    )

github_connector_functions = [github_connector,]

def get_github_connector_functions():
    return github_connector_functions 