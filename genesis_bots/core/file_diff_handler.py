import os
import time
import yaml
from git import Repo
from datetime import datetime
from difflib import unified_diff
from typing import List, Dict, Optional, Union


class GitFileManager:

    @classmethod
    def get_default_git_repo_path(cls):
        return os.path.join(os.getcwd(), 'bot_git')


    def __init__(self, repo_path: str = None):
        """Initialize GitFileManager with a repository path"""

        self.repo_path = os.getenv('GIT_PATH', self.get_default_git_repo_path())

        try:
            # Create directory if it doesn't exist
            os.makedirs(self.repo_path, exist_ok=True)

            # Try to initialize repository
            try:
                self.repo = Repo(self.repo_path)
            except:
                # If repository doesn't exist, initialize it
                self.repo = Repo.init(self.repo_path)

                # Configure git user for initial commit
                with self.repo.config_writer() as git_config:
                    git_config.set_value('user', 'email', 'bot@example.com')
                    git_config.set_value('user', 'name', 'Bot')

                # Create an initial empty commit
                # First create an empty file to commit
                readme_path = os.path.join(self.repo_path, 'README.md')
                with open(readme_path, 'w') as f:
                    f.write('# Git Repository\nInitialized by Bot')

                # Stage and commit
                self.repo.index.add(['README.md'])
                self.repo.index.commit("Initial commit")

        except Exception as e:
            raise Exception(f"Failed to initialize git repository: {str(e)}")

    def list_files(self, path: str = None) -> List[str]:
        """List all tracked files in the repository or specific path"""
        if path:
            full_path = os.path.join(self.repo_path, path)
            return [str(item[0]) for item in self.repo.index.entries.keys() if str(item[0]).startswith(path)]
        return [str(item[0]) for item in self.repo.index.entries.keys()]

    def read_file(self, file_path: str) -> str:
        """Read contents of a file from the repository"""
        full_path = os.path.join(self.repo_path, file_path)
        if not os.path.exists(full_path):
            raise FileNotFoundError(f"File not found: {file_path}")

        with open(full_path, 'r') as f:
            return f.read()

    def write_file(self, file_path: str, content: str, commit_message: str = None) -> Dict:
        """Write content to a file and optionally commit changes"""
        try:
            # Ensure directory exists
            os.makedirs(os.path.dirname(os.path.join(self.repo_path, file_path)), exist_ok=True)

            # Write the file
            with open(os.path.join(self.repo_path, file_path), 'w') as f:
                f.write(content)

            # Add to git
            self.repo.index.add([file_path])

            # Prepare response
            result = {
                "success": True,
                "reminder": "If this file is related to any project, remember to record it as a project asset using _manage_project_assets"
            }

            # Add commit-specific message if applicable
            if commit_message:
                self.commit_changes(commit_message)
                result["message"] = "File written and changes committed successfully"
            else:
                result["message"] = "File written successfully. Remember to commit changes with git_action('commit', message='your message')"

            return result
        except Exception as e:
            return {"success": False, "error": str(e)}

    def generate_diff(self, old_content: str, new_content: str, context_lines: int = 3) -> str:
        """Generate a unified diff between two content strings"""
        old_lines = old_content.splitlines(keepends=True)
        new_lines = new_content.splitlines(keepends=True)

        return ''.join(unified_diff(
            old_lines, new_lines,
            fromfile='old',
            tofile='new',
            n=context_lines
        ))

    def validate_diff_format(self, diff_content: str) -> bool:
        """Validate that the provided content follows unified diff format"""
        try:
            lines = diff_content.splitlines()
            if not lines:
                return False

            # Basic unified diff format validation
            # Should start with --- and +++ lines
            has_header = False
            has_changes = False

            for line in lines:
                if line.startswith('--- '):
                    has_header = True
                    continue
                if line.startswith('+++ '):
                    has_header = True
                    continue
                if line.startswith('@@ '):
                    has_changes = True
                    continue
                if line.startswith('+') or line.startswith('-'):
                    has_changes = True

            return has_header and has_changes
        except:
            return False

    def apply_diff(self, file_path: str, diff_content: str, commit_message: str = None) -> Dict:
        """Apply a diff to a file"""
        try:
            from difflib import unified_diff, restore

            full_path = os.path.join(self.repo_path, file_path)

            # Read the current file content
            with open(full_path, 'r') as f:
                current_content = f.read()

            # Split content into lines
            current_lines = current_content.splitlines(True)

            # Parse and apply the diff
            patch_lines = diff_content.splitlines(True)
            new_lines = list(restore(patch_lines, 1))  # 1 means to apply the changes
            new_content = ''.join(new_lines)

            # Write the new content
            with open(full_path, 'w') as f:
                f.write(new_content)

            # Add and commit if requested
            self.repo.index.add([file_path])
            if commit_message:
                self.commit_changes(commit_message)
                return {
                    "success": True,
                    "message": "Diff applied and changes committed successfully",
                    "content": new_content
                }
            else:
                return {
                    "success": True,
                    "message": "Diff applied successfully. Remember to commit changes with git_action('commit', message='your message')",
                    "content": new_content
                }
        except Exception as e:
            return {"success": False, "error": str(e)}

    def commit_changes(self, message: str) -> Dict:
        """Commit staged changes to the repository"""
        try:
            self.repo.index.commit(message)
            return {"success": True, "message": "Changes committed successfully"}
        except Exception as e:
            return {"success": False, "error": str(e)}

    def get_commit_history(self, file_path: str = None, max_count: int = 10) -> List[Dict]:
        """Get commit history for the repository or specific file"""
        try:
            if file_path:
                commits = list(self.repo.iter_commits(paths=file_path, max_count=max_count))
            else:
                commits = list(self.repo.iter_commits(max_count=max_count))

            return [{
                "hash": str(commit.hexsha),
                "message": commit.message,
                "author": str(commit.author),
                "date": datetime.fromtimestamp(commit.committed_date),
                "files": list(commit.stats.files.keys())
            } for commit in commits]
        except Exception as e:
            return []

    def create_branch(self, branch_name: str) -> Dict:
        """Create a new branch"""
        try:
            new_branch = self.repo.create_head(branch_name)
            new_branch.checkout()
            return {"success": True, "message": f"Branch '{branch_name}' created and checked out"}
        except Exception as e:
            return {"success": False, "error": str(e)}

    def switch_branch(self, branch_name: str) -> Dict:
        """Switch to an existing branch"""
        try:
            self.repo.heads[branch_name].checkout()
            return {"success": True, "message": f"Switched to branch '{branch_name}'"}
        except Exception as e:
            return {"success": False, "error": str(e)}

    def get_current_branch(self) -> str:
        """Get the name of the current branch"""
        return self.repo.active_branch.name

    def get_file_status(self, file_path: str = None) -> Dict:
        """Get the status of files in the repository"""
        try:
            if file_path:
                status = self.repo.git.status('--porcelain', file_path)
            else:
                status = self.repo.git.status('--porcelain')

            modified = []
            untracked = []
            staged = []

            for line in status.splitlines():
                if line:
                    status_code = line[:2]
                    path = line[3:]
                    if status_code == '??':
                        untracked.append(path)
                    elif status_code == ' M' or status_code == 'M ':
                        modified.append(path)
                    elif status_code == 'A ':
                        staged.append(path)

            result = {
                "success": True,
                "modified": modified,
                "untracked": untracked,
                "staged": staged
            }

            # Add reminder if there are uncommitted changes
            if modified or untracked or staged:
                result["message"] = "There are uncommitted changes. Remember to commit them with git_action('commit', message='your message')"

            return result
        except Exception as e:
            return {"success": False, "error": str(e)}

    def git_action(self, action: str, **kwargs) -> Dict:
        """
        Unified interface for all git operations.
        
        Actions:
            - list_files: List all tracked files (optional: path)
            - read_file: Read file contents (requires: file_path)
            - write_file: Write content to file (requires: file_path, content; optional: commit_message)
            - generate_diff: Generate diff between contents (requires: old_content, new_content; optional: context_lines)
            - apply_diff: Apply a unified diff to a file (requires: file_path, diff_content; optional: commit_message)
            - commit: Commit changes (requires: message)
            - get_history: Get commit history (optional: file_path, max_count)
            - create_branch: Create new branch (requires: branch_name)
            - switch_branch: Switch to branch (requires: branch_name)
            - get_branch: Get current branch name
            - get_status: Get file status (optional: file_path)
        
        Returns:
            Dict containing operation result and any relevant data
        """

        try:
            action = action.lower()



            if action == "list_files":
                path = kwargs.get("path")
                files = self.list_files(path)
                return {"success": True, "files": files}

            elif action == "read_file":
                if "file_path" not in kwargs:
                    return {"success": False, "error": "file_path is required"}
                # Check if file_path starts with / and return error if it does
                if kwargs["file_path"].startswith('/'):
                    return {"success": False, "error": "Please provide a relative file path without leading /"}
                content = self.read_file(kwargs["file_path"])
                return {"success": True, "content": content}

            elif action == "write_file":
                if "file_content" in kwargs and "content" not in kwargs:
                    kwargs["content"] = kwargs["file_content"]
                if "content" not in kwargs and "new_content" in kwargs:
                    kwargs["content"] = kwargs["new_content"]
                if "file_path" not in kwargs or "content" not in kwargs:
                    return {"success": False, "error": "file_path and content are required"}
                # Check if file_path starts with / and return error if it does
                if kwargs["file_path"].startswith('/'):
                    return {"success": False, "error": "Please provide a relative file path without leading /"}
                return self.write_file(
                    kwargs["file_path"],
                    kwargs["content"],
                    kwargs.get("commit_message")
                )

            elif action == "generate_diff":
                if "old_content" not in kwargs or "new_content" not in kwargs:
                    return {"success": False, "error": "old_content and new_content are required"}
                diff = self.generate_diff(
                    kwargs["old_content"],
                    kwargs["new_content"],
                    kwargs.get("context_lines", 3)
                )
                return {"success": True, "diff": diff}

            elif action == "apply_diff":
                if "file_path" not in kwargs or "diff_content" not in kwargs:
                    return {
                        "success": False,
                        "error": "file_path and diff_content are required"
                    }
                return self.apply_diff(
                    kwargs["file_path"],
                    kwargs["diff_content"],
                    kwargs.get("commit_message")
                )

            elif action == "commit":
                if "message" not in kwargs:
                    return {"success": False, "error": "commit message is required"}
                return self.commit_changes(kwargs["message"])

            elif action == "get_history":
                history = self.get_commit_history(
                    kwargs.get("file_path"),
                    kwargs.get("max_count", 10)
                )
                return {"success": True, "history": history}

            elif action == "create_branch":
                if "branch_name" not in kwargs:
                    return {"success": False, "error": "branch_name is required"}
                return self.create_branch(kwargs["branch_name"])

            elif action == "switch_branch":
                if "branch_name" not in kwargs:
                    return {"success": False, "error": "branch_name is required"}
                return self.switch_branch(kwargs["branch_name"])

            elif action == "get_branch":
                branch = self.get_current_branch()
                return {"success": True, "branch": branch}

            elif action == "get_status":
                status = self.get_file_status(kwargs.get("file_path"))
                return status

            else:
                return {"success": False, "error": f"Unknown action: {action}"}

        except Exception as e:
            return {"success": False, "error": str(e)}
