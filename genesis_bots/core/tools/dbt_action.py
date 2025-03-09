import os
import yaml
import shutil
import json
from pathlib import Path
from typing import Dict, List, Any, Optional, Union, Tuple
from dbt.cli.main import dbtRunner, dbtRunnerResult
from genesis_bots.core.logging_config import logger

from genesis_bots.core.bot_os_tools2 import (
    BOT_ID_IMPLICIT_FROM_CONTEXT,
    THREAD_ID_IMPLICIT_FROM_CONTEXT,
    ToolFuncGroup,
    ToolFuncParamDescriptor,
    gc_tool,
)

class DBTTools:
    """
    A comprehensive toolkit for AI bots to interact with dbt programmatically.
    Provides methods for creating, configuring, and running dbt projects without CLI.
    """
    
    def __init__(self, workspace_dir: Optional[str] = None):
        """
        Initialize the DBT Tools with a workspace directory.
        
        Args:
            workspace_dir: Directory where dbt projects will be created and managed.
                          If None, uses the current working directory.
        """
        self.workspace_dir = Path(workspace_dir) if workspace_dir else Path.cwd()
        self.workspace_dir.mkdir(parents=True, exist_ok=True)
        self.runner = dbtRunner()
        self.current_project = None
    
    def action(self, action: str, **kwargs) -> Dict[str, Any]:
        """
        Main entry point for AI bots to interact with dbt.
        
        Args:
            action: The action to perform (create_project, add_model, run, test, etc.)
            **kwargs: Additional parameters specific to the action
            
        Returns:
            Dict containing the result of the action
        """
        actions = {
            # Project management
            "create_project": self.create_project,
            "use_project": self.use_project,
            "delete_project": self.delete_project,
            "list_projects": self.list_projects,
            
            # Configuration
            "setup_profile": self.setup_profile,
            "get_profile_info": self.get_profile_info,
            "update_project_config": self.update_project_config,
            
            # Models and resources
            "add_model": self.add_model,
            "add_source": self.add_source,
            "add_seed": self.add_seed,
            "add_snapshot": self.add_snapshot,
            "add_test": self.add_test,
            "add_macro": self.add_macro,
            "add_analysis": self.add_analysis,
            
            # Project operations
            "run": self.run_models,
            "test": self.run_tests,
            "seed": self.run_seeds,
            "snapshot": self.run_snapshots,
            "build": self.build,
            "generate_docs": self.generate_docs,
            
            # Information and metadata
            "list_models": self.list_models,
            "list_sources": self.list_sources,
            "list_seeds": self.list_seeds,
            "get_model_sql": self.get_model_sql,
            "execute_query": self.execute_query,
            "get_manifest": self.get_manifest,
            "get_catalog": self.get_catalog,
            
            # Dependencies
            "add_package": self.add_package,
            "install_deps": self.install_deps,
        }
        
        if action not in actions:
            return {"success": False, "error": f"Unknown action: {action}. Available actions: {list(actions.keys())}"}
        
        try:
            return actions[action](**kwargs)
        except Exception as e:
            return {"success": False, "error": str(e)}
    
    # --------- Project Management ---------
    
    def create_project(self, project_name: str, profile_name: Optional[str] = None) -> Dict[str, Any]:
        """
        Create a new dbt project
        
        Args:
            project_name: Name of the project
            profile_name: Profile name to use (defaults to project_name)
            
        Returns:
            Dict with project information
        """
        if not profile_name:
            profile_name = project_name
        
        project_dir = self.workspace_dir / project_name
        
        # Check if project already exists
        if project_dir.exists():
            return {"success": False, "error": f"Project '{project_name}' already exists at {project_dir}"}
        
        # Create project directory
        project_dir.mkdir(parents=True, exist_ok=True)
        
        # Create dbt_project.yml
        dbt_project = {
            "name": project_name,
            "version": "1.0.0",
            "config-version": 2,
            "profile": profile_name,
            "model-paths": ["models"],
            "seed-paths": ["seeds"],
            "test-paths": ["tests"],
            "analysis-paths": ["analyses"],
            "macro-paths": ["macros"],
            "snapshot-paths": ["snapshots"],
            "target-path": "target",
            "clean-targets": [
                "target",
                "dbt_packages"
            ],
            "models": {
                project_name: {
                    "+materialized": "view"
                }
            }
        }
        
        # Write dbt_project.yml
        with open(project_dir / "dbt_project.yml", "w") as f:
            yaml.dump(dbt_project, f, default_flow_style=False, sort_keys=False)
        
        # Create required directories
        dirs = ["models", "seeds", "tests", "macros", "snapshots", "analyses"]
        for dir_name in dirs:
            (project_dir / dir_name).mkdir(exist_ok=True)
        
        # Create packages.yml
        with open(project_dir / "packages.yml", "w") as f:
            yaml.dump({"packages": []}, f, default_flow_style=False)
        
        # Set as current project
        self.current_project = project_dir
        
        return {
            "success": True, 
            "project_name": project_name,
            "project_dir": str(project_dir),
            "message": f"dbt project '{project_name}' created successfully"
        }
    
    def use_project(self, project_name: str) -> Dict[str, Any]:
        """
        Set the current project to work with
        
        Args:
            project_name: Name of the project
            
        Returns:
            Dict with project information
        """
        project_dir = self.workspace_dir / project_name
        
        if not project_dir.exists():
            return {"success": False, "error": f"Project '{project_name}' does not exist at {project_dir}"}
        
        if not (project_dir / "dbt_project.yml").exists():
            return {"success": False, "error": f"Directory exists but is not a valid dbt project (missing dbt_project.yml)"}
        
        self.current_project = project_dir
        
        return {
            "success": True,
            "project_name": project_name,
            "project_dir": str(project_dir),
            "message": f"Now using dbt project '{project_name}'"
        }
    
    def delete_project(self, project_name: str, confirm: bool = False) -> Dict[str, Any]:
        """
        Delete a dbt project
        
        Args:
            project_name: Name of the project to delete
            confirm: Must be True to confirm deletion
            
        Returns:
            Dict with deletion result
        """
        if not confirm:
            return {"success": False, "error": "Set confirm=True to confirm project deletion"}
        
        project_dir = self.workspace_dir / project_name
        
        if not project_dir.exists():
            return {"success": False, "error": f"Project '{project_name}' does not exist at {project_dir}"}
        
        # Reset current project if it's the one being deleted
        if self.current_project == project_dir:
            self.current_project = None
        
        # Delete the project directory
        shutil.rmtree(project_dir)
        
        return {
            "success": True,
            "message": f"Project '{project_name}' deleted successfully"
        }
    
    def list_projects(self) -> Dict[str, Any]:
        """
        List all dbt projects in the workspace
        
        Returns:
            Dict with list of projects
        """
        projects = []
        
        for item in self.workspace_dir.iterdir():
            if item.is_dir() and (item / "dbt_project.yml").exists():
                project_file = item / "dbt_project.yml"
                with open(project_file, "r") as f:
                    project_config = yaml.safe_load(f)
                
                projects.append({
                    "name": project_config.get("name"),
                    "directory": str(item),
                    "profile": project_config.get("profile")
                })
        
        current_project = None
        if self.current_project:
            current_project = self.current_project.name
        
        return {
            "success": True,
            "projects": projects,
            "current_project": current_project
        }
    
    # --------- Configuration ---------
    
    def setup_profile(self, profile_name: str, target_name: str = "dev", 
                     adapter_type: str = "postgres", credentials: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        Set up a dbt profile
        
        Args:
            profile_name: Name of the profile
            target_name: Name of the target connection (default: dev)
            adapter_type: dbt adapter type (postgres, snowflake, bigquery, etc.)
            credentials: Database connection credentials
            
        Returns:
            Dict with profile setup result
        """
        if not credentials:
            return {"success": False, "error": "Credentials are required"}
        
        # Default credentials by adapter type
        default_creds = {
            "postgres": {
                "type": "postgres",
                "host": "localhost",
                "user": "postgres",
                "password": "",
                "port": 5432,
                "dbname": "postgres",
                "schema": "public",
                "threads": 4
            },
            "snowflake": {
                "type": "snowflake",
                "account": "",
                "user": "",
                "password": "",
                "role": "",
                "database": "",
                "warehouse": "",
                "schema": "",
                "threads": 4
            },
            "bigquery": {
                "type": "bigquery",
                "method": "service-account",
                "project": "",
                "dataset": "",
                "threads": 4,
                "keyfile": ""
            },
            "redshift": {
                "type": "redshift",
                "host": "",
                "user": "",
                "password": "",
                "port": 5439,
                "dbname": "",
                "schema": "",
                "threads": 4
            }
        }
        
        if adapter_type not in default_creds:
            return {"success": False, "error": f"Unsupported adapter type: {adapter_type}"}
        
        # Merge provided credentials with defaults
        merged_creds = default_creds[adapter_type].copy()
        merged_creds.update(credentials)
        
        # Get profiles.yml location
        profiles_dir = Path.home() / ".dbt"
        profiles_dir.mkdir(exist_ok=True)
        profiles_path = profiles_dir / "profiles.yml"
        
        # Read existing profiles if file exists
        if profiles_path.exists():
            with open(profiles_path, "r") as f:
                profiles = yaml.safe_load(f) or {}
        else:
            profiles = {}
        
        # Add or update profile
        profiles[profile_name] = {
            "target": target_name,
            "outputs": {
                target_name: merged_creds
            }
        }
        
        # Write updated profiles.yml
        with open(profiles_path, "w") as f:
            yaml.dump(profiles, f, default_flow_style=False)
        
        return {
            "success": True,
            "profile_name": profile_name,
            "target": target_name,
            "adapter_type": adapter_type,
            "profiles_path": str(profiles_path),
            "message": f"Profile '{profile_name}' with target '{target_name}' set up successfully"
        }
    
    def get_profile_info(self, profile_name: Optional[str] = None) -> Dict[str, Any]:
        """
        Get information about dbt profiles
        
        Args:
            profile_name: Specific profile to get info for (if None, returns all)
            
        Returns:
            Dict with profile information
        """
        profiles_path = Path.home() / ".dbt" / "profiles.yml"
        
        if not profiles_path.exists():
            return {"success": False, "error": "profiles.yml not found"}
        
        with open(profiles_path, "r") as f:
            profiles = yaml.safe_load(f) or {}
        
        if profile_name:
            if profile_name not in profiles:
                return {"success": False, "error": f"Profile '{profile_name}' not found"}
            
            # Copy profile but remove sensitive information
            profile_info = json.loads(json.dumps(profiles[profile_name]))
            
            for target, config in profile_info.get("outputs", {}).items():
                if "password" in config:
                    config["password"] = "********"
                if "keyfile" in config:
                    config["keyfile"] = "********"
            
            return {
                "success": True,
                "profile_name": profile_name,
                "profile_info": profile_info
            }
        else:
            # Get all profile names and their targets
            profile_info = {}
            for name, config in profiles.items():
                targets = list(config.get("outputs", {}).keys())
                default_target = config.get("target")
                profile_info[name] = {
                    "targets": targets,
                    "default_target": default_target
                }
            
            return {
                "success": True,
                "profiles": profile_info
            }
    
    def update_project_config(self, config_updates: Dict[str, Any]) -> Dict[str, Any]:
        """
        Update the dbt_project.yml configuration
        
        Args:
            config_updates: Dict with configuration updates
            
        Returns:
            Dict with update result
        """
        self._ensure_project_selected()
        
        project_file = self.current_project / "dbt_project.yml"
        
        with open(project_file, "r") as f:
            project_config = yaml.safe_load(f)
        
        # Update configuration recursively
        def update_dict(d, updates):
            for k, v in updates.items():
                if isinstance(v, dict) and k in d and isinstance(d[k], dict):
                    update_dict(d[k], v)
                else:
                    d[k] = v
        
        update_dict(project_config, config_updates)
        
        with open(project_file, "w") as f:
            yaml.dump(project_config, f, default_flow_style=False)
        
        return {
            "success": True,
            "message": "Project configuration updated successfully",
            "updated_config": project_config
        }
    
    # --------- Models and Resources ---------
    
    def add_model(self, model_name: str, sql_content: str, materialized: str = "view", 
                 schema: Optional[str] = None, subdirectory: Optional[str] = None) -> Dict[str, Any]:
        """
        Add a model to the project
        
        Args:
            model_name: Name of the model (without .sql extension)
            sql_content: SQL content for the model
            materialized: Materialization type (view, table, incremental, ephemeral)
            schema: Custom schema name
            subdirectory: Subdirectory within models/ to place the model
            
        Returns:
            Dict with model creation result
        """
        self._ensure_project_selected()
        
        # Determine model path
        models_dir = self.current_project / "models"
        if subdirectory:
            model_dir = models_dir / subdirectory
            model_dir.mkdir(parents=True, exist_ok=True)
        else:
            model_dir = models_dir
        
        model_path = model_dir / f"{model_name}.sql"
        
        # Add config block if materialized is not view (the default)
        # or if schema is specified
        config_lines = []
        
        if materialized != "view" or schema:
            config_lines.append("{{")
            config_lines.append("  config(")
            
            config_params = []
            if materialized != "view":
                config_params.append(f"    materialized='{materialized}'")
            if schema:
                config_params.append(f"    schema='{schema}'")
            
            config_lines.append(",\n".join(config_params))
            config_lines.append("  )")
            config_lines.append("}}")
            config_lines.append("")
        
        # Write the model file
        with open(model_path, "w") as f:
            if config_lines:
                f.write("\n".join(config_lines) + "\n")
            f.write(sql_content)
        
        return {
            "success": True,
            "model_name": model_name,
            "model_path": str(model_path),
            "materialized": materialized,
            "schema": schema,
            "message": f"Model '{model_name}' created successfully"
        }
    
    def add_source(self, source_name: str, tables: List[Dict[str, Any]], 
                  schema: str, database: Optional[str] = None,
                  loader: Optional[str] = None) -> Dict[str, Any]:
        """
        Add a source definition to the project
        
        Args:
            source_name: Name of the source
            tables: List of tables in the source
            schema: Schema where the source tables are located
            database: Database where the source tables are located
            loader: Loader that loads the source
            
        Returns:
            Dict with source creation result
        """
        self._ensure_project_selected()
        
        # Create sources.yml in models directory if it doesn't exist
        sources_path = self.current_project / "models" / "sources.yml"
        
        if sources_path.exists():
            with open(sources_path, "r") as f:
                sources = yaml.safe_load(f) or {}
        else:
            sources = {"version": 2, "sources": []}
        
        # Define the new source
        new_source = {
            "name": source_name,
            "schema": schema,
            "tables": tables
        }
        
        if database:
            new_source["database"] = database
        
        if loader:
            new_source["loader"] = loader
        
        # Add or update source
        source_exists = False
        for i, source in enumerate(sources.get("sources", [])):
            if source.get("name") == source_name:
                sources["sources"][i] = new_source
                source_exists = True
                break
        
        if not source_exists:
            if "sources" not in sources:
                sources["sources"] = []
            sources["sources"].append(new_source)
        
        # Write sources.yml
        with open(sources_path, "w") as f:
            yaml.dump(sources, f, default_flow_style=False, sort_keys=False)
        
        return {
            "success": True,
            "source_name": source_name,
            "source_path": str(sources_path),
            "message": f"Source '{source_name}' with {len(tables)} tables created successfully"
        }
    
    def add_seed(self, seed_name: str, csv_content: str) -> Dict[str, Any]:
        """
        Add a seed file (CSV) to the project
        
        Args:
            seed_name: Name of the seed (without .csv extension)
            csv_content: CSV content as a string
            
        Returns:
            Dict with seed creation result
        """
        self._ensure_project_selected()
        
        # Determine seed path
        seeds_dir = self.current_project / "seeds"
        seed_path = seeds_dir / f"{seed_name}.csv"
        
        # Write the seed file
        with open(seed_path, "w", newline='') as f:
            f.write(csv_content)
        
        return {
            "success": True,
            "seed_name": seed_name,
            "seed_path": str(seed_path),
            "message": f"Seed '{seed_name}' created successfully"
        }
    
    def add_snapshot(self, snapshot_name: str, sql_content: str, 
                    unique_key: str, strategy: str = "timestamp",
                    updated_at: Optional[str] = None,
                    check_cols: Optional[Union[str, List[str]]] = None) -> Dict[str, Any]:
        """
        Add a snapshot to the project
        
        Args:
            snapshot_name: Name of the snapshot (without .sql extension)
            sql_content: SQL content for the snapshot
            unique_key: Column(s) that uniquely identify a record
            strategy: Snapshot strategy (timestamp or check)
            updated_at: Column name for timestamp strategy
            check_cols: Column(s) to check for changes (for check strategy)
            
        Returns:
            Dict with snapshot creation result
        """
        self._ensure_project_selected()
        
        # Validate parameters
        if strategy == "timestamp" and not updated_at:
            return {"success": False, "error": "updated_at column is required for timestamp strategy"}
        
        if strategy == "check" and not check_cols:
            return {"success": False, "error": "check_cols is required for check strategy"}
        
        # Determine snapshot path
        snapshots_dir = self.current_project / "snapshots"
        snapshot_path = snapshots_dir / f"{snapshot_name}.sql"
        
        # Create snapshot configuration
        config_lines = ["{% snapshot " + snapshot_name + " %}"]
        config_lines.append("{{")
        config_lines.append("    config(")
        config_lines.append(f"      target_database=target.database,")
        config_lines.append(f"      target_schema=target.schema,")
        config_lines.append(f"      unique_key='{unique_key}',")
        config_lines.append(f"      strategy='{strategy}',")
        
        if strategy == "timestamp":
            config_lines.append(f"      updated_at='{updated_at}'")
        elif strategy == "check":
            if isinstance(check_cols, str):
                config_lines.append(f"      check_cols='{check_cols}'")
            else:
                config_lines.append(f"      check_cols={check_cols}")
        
        config_lines.append("    )")
        config_lines.append("}}")
        config_lines.append("")
        config_lines.append(sql_content)
        config_lines.append("")
        config_lines.append("{% endsnapshot %}")
        
        # Write the snapshot file
        with open(snapshot_path, "w") as f:
            f.write("\n".join(config_lines))
        
        return {
            "success": True,
            "snapshot_name": snapshot_name,
            "snapshot_path": str(snapshot_path),
            "strategy": strategy,
            "message": f"Snapshot '{snapshot_name}' created successfully"
        }
    
    def add_test(self, test_name: str, sql_content: str, 
                generic: bool = False) -> Dict[str, Any]:
        """
        Add a test to the project
        
        Args:
            test_name: Name of the test (without .sql extension)
            sql_content: SQL content for the test
            generic: Whether this is a generic test (macro) or a singular test
            
        Returns:
            Dict with test creation result
        """
        self._ensure_project_selected()
        
        if generic:
            # Generic tests go in macros directory
            test_dir = self.current_project / "macros"
            test_path = test_dir / f"{test_name}.sql"
            
            # Add macro wrapper
            test_content = f"""
{{% macro test_{test_name}(model, column_name) %}}

{sql_content}

{{% endmacro %}}
""".strip()
        else:
            # Singular tests go in tests directory
            test_dir = self.current_project / "tests"
            test_path = test_dir / f"{test_name}.sql"
            test_content = sql_content
        
        # Write the test file
        with open(test_path, "w") as f:
            f.write(test_content)
        
        return {
            "success": True,
            "test_name": test_name,
            "test_path": str(test_path),
            "generic": generic,
            "message": f"{'Generic' if generic else 'Singular'} test '{test_name}' created successfully"
        }
    
    def add_macro(self, macro_name: str, macro_content: str) -> Dict[str, Any]:
        """
        Add a macro to the project
        
        Args:
            macro_name: Name of the macro (without .sql extension)
            macro_content: Jinja/SQL content for the macro
            
        Returns:
            Dict with macro creation result
        """
        self._ensure_project_selected()
        
        # Determine macro path
        macros_dir = self.current_project / "macros"
        macro_path = macros_dir / f"{macro_name}.sql"
        
        # Write the macro file
        with open(macro_path, "w") as f:
            f.write(macro_content)
        
        return {
            "success": True,
            "macro_name": macro_name,
            "macro_path": str(macro_path),
            "message": f"Macro '{macro_name}' created successfully"
        }
    
    def add_analysis(self, analysis_name: str, sql_content: str) -> Dict[str, Any]:
        """
        Add an analysis to the project
        
        Args:
            analysis_name: Name of the analysis (without .sql extension)
            sql_content: SQL content for the analysis
            
        Returns:
            Dict with analysis creation result
        """
        self._ensure_project_selected()
        
        # Determine analysis path
        analyses_dir = self.current_project / "analyses"
        analysis_path = analyses_dir / f"{analysis_name}.sql"
        
        # Write the analysis file
        with open(analysis_path, "w") as f:
            f.write(sql_content)
        
        return {
            "success": True,
            "analysis_name": analysis_name,
            "analysis_path": str(analysis_path),
            "message": f"Analysis '{analysis_name}' created successfully"
        }
    
    # --------- Project Operations ---------
    
    def run_models(self, models: Optional[List[str]] = None, exclude: Optional[List[str]] = None, 
                 full_refresh: bool = False, vars: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Run dbt models
        
        Args:
            models: List of models to run (if None, runs all models)
            exclude: List of models to exclude
            full_refresh: Whether to perform a full refresh
            vars: Variables to pass to the run
            
        Returns:
            Dict with run result
        """
        self._ensure_project_selected()
        
        # Change to project directory
        original_dir = os.getcwd()
        os.chdir(self.current_project)
        
        try:
            # Build command
            command = ["run"]
            
            if models:
                command.extend(["--select", " ".join(models)])
            
            if exclude:
                command.extend(["--exclude", " ".join(exclude)])
            
            if full_refresh:
                command.append("--full-refresh")
            
            if vars:
                vars_str = " ".join([f"{k}={v}" for k, v in vars.items()])
                command.extend(["--vars", vars_str])
            
            # Run command
            result = self.runner.invoke(command)
            
            return {
                "success": result.success,
                "logs": result.stdout,
                "command": " ".join(command),
                "message": "Models run completed successfully" if result.success else "Models run failed"
            }
        finally:
            # Restore original directory
            os.chdir(original_dir)
    
    def run_tests(self, models: Optional[List[str]] = None, tests: Optional[List[str]] = None, 
                 vars: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Run dbt tests
        
        Args:
            models: List of models to test (if None, tests all models)
            tests: List of specific tests to run
            vars: Variables to pass to the tests
            
        Returns:
            Dict with test result
        """
        self._ensure_project_selected()
        
        # Change to project directory
        original_dir = os.getcwd()
        os.chdir(self.current_project)
        
        try:
            # Build command
            command = ["test"]
            
            if models:
                command.extend(["--models", " ".join(models)])
            
            if tests:
                command.extend(["--select", " ".join(tests)])
            
            if vars:
                vars_str = " ".join([f"{k}={v}" for k, v in vars.items()])
                command.extend(["--vars", vars_str])
            
            # Run command
            result = self.runner.invoke(command)
            
            return {
                "success": result.success,
                "logs": result.stdout,
                "command": " ".join(command),
                "message": "Tests completed successfully" if result.success else "Tests failed"
            }
        finally:
            # Restore original directory
            os.chdir(original_dir)
    
    def run_seeds(self, select: Optional[List[str]] = None,
                 full_refresh: bool = False) -> Dict[str, Any]:
        """
        Run dbt seeds
        
        Args:
            select: List of seeds to run (if None, runs all seeds)
            full_refresh: Whether to perform a full refresh
            
        Returns:
            Dict with seed result
        """
        self._ensure_project_selected()
        
        # Change to project directory
        original_dir = os.getcwd()
        os.chdir(self.current_project)
        
        try:
            # Build command
            command = ["seed"]
            
            if select:
                command.extend(["--select", " ".join(select)])
            
            if full_refresh:
                command.append("--full-refresh")
            
            # Run command
            result = self.runner.invoke(command)
            
            return {
                "success": result.success,
                "logs": result.stdout,
                "command": " ".join(command),
                "message": "Seeds loaded successfully" if result.success else "Seeds failed to load"
            }
        finally:
            # Restore original directory
            os.chdir(original_dir)
    
    def run_snapshots(self, select: Optional[List[str]] = None) -> Dict[str, Any]:
        """
        Run dbt snapshots
        
        Args:
            select: List of snapshots to run (if None, runs all snapshots)
            
        Returns:
            Dict with snapshot result
        """
        self._ensure_project_selected()
        
        # Change to project directory
        original_dir = os.getcwd()
        os.chdir(self.current_project)
        
        try:
            # Build command
            command = ["snapshot"]
            
            if select:
                command.extend(["--select", " ".join(select)])
            
            # Run command
            result = self.runner.invoke(command)
            
            return {
                "success": result.success,
                "logs": result.stdout,
                "command": " ".join(command),
                "message": "Snapshots completed successfully" if result.success else "Snapshots failed"
            }
        finally:
            # Restore original directory
            os.chdir(original_dir)
    
    def build(self, select: Optional[List[str]] = None, exclude: Optional[List[str]] = None,
             full_refresh: bool = False, vars: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Run dbt build (runs, tests, seeds, and snapshots)
        
        Args:
            select: List of resources to build (if None, builds all)
            exclude: List of resources to exclude
            full_refresh: Whether to perform a full refresh
            vars: Variables to pass to the build
            
        Returns:
            Dict with build result
        """
        self._ensure_project_selected()
        
        # Change to project directory
        original_dir = os.getcwd()
        os.chdir(self.current_project)
        
        try:
            # Build command
            command = ["build"]
            
            if select:
                command.extend(["--select", " ".join(select)])
            
            if exclude:
                command.extend(["--exclude", " ".join(exclude)])
            
            if full_refresh:
                command.append("--full-refresh")
            
            if vars:
                vars_str = " ".join([f"{k}={v}" for k, v in vars.items()])
                command.extend(["--vars", vars_str])
            
            # Run command
            result = self.runner.invoke(command)
            
            return {
                "success": result.success,
                "logs": result.stdout,
                "command": " ".join(command),
                "message": "Build completed successfully" if result.success else "Build failed"
            }
        finally:
            # Restore original directory
            os.chdir(original_dir)
    
    def generate_docs(self, compile_only: bool = False) -> Dict[str, Any]:
        """
        Generate dbt documentation
        
        Args:
            compile_only: Whether to compile docs without serving them
            
        Returns:
            Dict with docs generation result
        """
        self._ensure_project_selected()
        
        # Change to project directory
        original_dir = os.getcwd()
        os.chdir(self.current_project)
        
        try:
            # Build command for docs
            command = ["docs", "generate"]
            
            # Run command
            result = self.runner.invoke(command)
            
            if not result.success:
                return {
                    "success": False,
                    "logs": result.stdout,
                    "command": " ".join(command),
                    "message": "Failed to generate documentation"
                }
            
            if compile_only:
                return {
                    "success": True,
                    "logs": result.stdout,
                    "command": " ".join(command),
                    "docs_path": str(self.current_project / "target" / "index.html"),
                    "message": "Documentation generated successfully"
                }
            
            # Serve the docs
            command = ["docs", "serve"]
            
            # For this specific command, we don't wait for it to complete
            # Instead, we just return information about how to access it
            # You could use subprocess.Popen to run it in the background
            
            return {
                "success": True,
                "logs": result.stdout,
                "command": " ".join(command),
                "docs_path": str(self.current_project / "target" / "index.html"),
                "message": "Documentation generated successfully. To serve, run 'dbt docs serve'"
            }
        finally:
            # Restore original directory
            os.chdir(original_dir)

dbt_action_grp = ToolFuncGroup(
    name="dbt_action",
    description="DBT project management and operations",
    lifetime="PERSISTENT",
)

@gc_tool(
    action=ToolFuncParamDescriptor(
        name="action",
        description="The DBT action to perform",
        required=True,
        llm_type_desc=dict(
            type="string",
            enum=[
                # Project management
                "create_project", "use_project", "delete_project", "list_projects",
                # Configuration
                "setup_profile", "get_profile_info", "update_project_config",
                # Models and resources
                "add_model", "add_source", "add_seed", "add_snapshot", "add_test",
                "add_macro", "add_analysis",
                # Project operations
                "run", "test", "seed", "snapshot", "build", "generate_docs",
                # Information and metadata
                "list_models", "list_sources", "list_seeds", "get_model_sql",
                "execute_query", "get_manifest", "get_catalog",
                # Dependencies
                "add_package", "install_deps"
            ]
        ),
    ),
    workspace_dir=ToolFuncParamDescriptor(
        name="workspace_dir",
        description="Directory where dbt projects will be created and managed",
        required=False,
        llm_type_desc=dict(type="string"),
    ),
    project_name=ToolFuncParamDescriptor(
        name="project_name",
        description="Name of the DBT project",
        required=False,
        llm_type_desc=dict(type="string"),
    ),
    profile_name=ToolFuncParamDescriptor(
        name="profile_name",
        description="Name of the DBT profile",
        required=False,
        llm_type_desc=dict(type="string"),
    ),
    sql_content=ToolFuncParamDescriptor(
        name="sql_content",
        description="SQL content for models, tests, etc.",
        required=False,
        llm_type_desc=dict(type="string"),
    ),
    materialized=ToolFuncParamDescriptor(
        name="materialized",
        description="Materialization type for models",
        required=False,
        llm_type_desc=dict(type="string"),
    ),
    adapter_type=ToolFuncParamDescriptor(
        name="adapter_type",
        description="Type of database adapter",
        required=False,
        llm_type_desc=dict(type="string"),
    ),
    credentials=ToolFuncParamDescriptor(
        name="credentials",
        description="Database connection credentials",
        required=False,
        llm_type_desc=dict(type="object"),
    ),
    config_updates=ToolFuncParamDescriptor(
        name="config_updates",
        description="Updates to project configuration",
        required=False,
        llm_type_desc=dict(type="object"),
    ),
    kwargs=ToolFuncParamDescriptor(
        name="kwargs",
        description="Additional arguments needed for the specific action",
        required=True,
        llm_type_desc=dict(type="object"),
    ),
    bot_id=BOT_ID_IMPLICIT_FROM_CONTEXT,
    thread_id=THREAD_ID_IMPLICIT_FROM_CONTEXT,
    _group_tags_=[dbt_action_grp],
)
def dbt_action(
    action: str,
    workspace_dir: str = None,
    project_name: str = None,
    profile_name: str = None,
    sql_content: str = None,
    materialized: str = None,
    adapter_type: str = None,
    credentials: Dict[str, Any] = None,
    config_updates: Dict[str, Any] = None,
    bot_id: str = None,
    thread_id: str = None,
    **kwargs: Any,
) -> Dict[str, Any]:
    """
    Wrapper for DBT operations

    Args:
        action: The DBT action to perform
        workspace_dir: Directory for DBT projects
        project_name: Name of the DBT project
        profile_name: Name of the DBT profile
        sql_content: SQL content for models, tests, etc.
        materialized: Materialization type for models
        adapter_type: Type of database adapter
        credentials: Database connection credentials
        config_updates: Updates to project configuration
        bot_id: Bot identifier
        thread_id: Thread identifier
        kwargs: Additional arguments needed for the specific action

    Returns:
        Dict containing operation result and any relevant data
    """
    # Initialize DBTTools if not already initialized
    if not hasattr(dbt_action, '_dbt_tools'):
        dbt_action._dbt_tools = DBTTools(workspace_dir)

    # Combine all parameters into a single dict
    params = {
        'workspace_dir': workspace_dir,
        'project_name': project_name,
        'profile_name': profile_name,
        'sql_content': sql_content,
        'materialized': materialized,
        'adapter_type': adapter_type,
        'credentials': credentials,
        'config_updates': config_updates,
        'bot_id': bot_id,
        'thread_id': thread_id,
        **kwargs
    }
    # Remove None values
    params = {k: v for k, v in params.items() if v is not None}
    
    return dbt_action._dbt_tools.action(action, **params)

# Define as a list explicitly
dbt_action_functions = [dbt_action]

def get_dbt_action_functions():
    try:
        # You might want to add checks here to ensure DBT is available
        return list(dbt_action_functions)
    except Exception as e:
        logger.warning(f"DBT tools initialization failed: {str(e)}. DBT action tools will not be registered.")
        return []