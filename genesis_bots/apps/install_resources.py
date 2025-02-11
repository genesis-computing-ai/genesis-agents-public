import os
import shutil
from pathlib import Path

def copy_resources(base_dir=None, verbose=False):
    """Copy demo files and demo data to the current directory."""

    def trace_action(message):
        if verbose:
            print(" -->", message)

    # Get the current working directory
    if base_dir is None:
        base_dir = Path.cwd()
    else:
        base_dir = Path(base_dir).resolve()

    # Get the source directory (to the root of the genesis_bots package)
    root_pkg_source_dir = Path(__file__).parent.parent  # Go up one level from apps/install_resources.py

    # Define source and destination paths
    demo_src = root_pkg_source_dir / "apps" / "demos"
    demo_dest = base_dir / "apps" / "demos"
    golden_src = root_pkg_source_dir / "genesis_bots" / "golden_defaults"
    golden_dest = base_dir / "genesis_bots" / "golden_defaults"

    # Create destination directories
    demo_data_dest = demo_dest / "demo_data"
    db_demos_dest = demo_dest / "database_demos"

    for directory in [demo_data_dest, db_demos_dest, golden_dest]:
        directory.mkdir(parents=True, exist_ok=True)
        trace_action(f"Created/Using directory: {directory}")

    # Copy demo data files
    demo_data_src = demo_src / "demo_data"
    if demo_data_src.exists():
        for file_name in ["workspace.sqlite", "baseball.sqlite", "formula_1.sqlite",
                         "postgres_travel.sql", "demo_harvest_results.json"]:
            src_file = demo_data_src / file_name
            dst_file = demo_data_dest / file_name
            if src_file.exists():
                shutil.copy2(src_file, dst_file)

    # Copy database demo files
    db_demos_src = demo_src / "database_demos"
    if db_demos_src.exists():
        for file_name in ["oracle.md", "oracle_demo.sql"]:
            src_file = db_demos_src / file_name
            dst_file = db_demos_dest / file_name
            if src_file.exists():
                shutil.copy2(src_file, dst_file)

    # Copy golden defaults structure
    if golden_src.exists():
        # Copy golden_notes
        golden_notes_src = golden_src / "golden_notes"
        golden_notes_dest = golden_dest / "golden_notes"
        golden_notes_dest.mkdir(exist_ok=True)
        trace_action(f"Created/Using directory: {golden_notes_dest}")
        if golden_notes_src.exists():
            for file_name in ["__init__.py", "golden_notes.yaml"]:
                src_file = golden_notes_src / file_name
                dst_file = golden_notes_dest / file_name
                if src_file.exists():
                    shutil.copy2(src_file, dst_file)

        # Copy golden_processes
        golden_processes_src = golden_src / "golden_processes"
        golden_processes_dest = golden_dest / "golden_processes"
        golden_processes_dest.mkdir(exist_ok=True)
        trace_action(f"Created/Using directory: {golden_processes_dest}")
        if golden_processes_src.exists():
            for file_name in ["__init__.py", "janice_processes_default.yaml"]:
                src_file = golden_processes_src / file_name
                dst_file = golden_processes_dest / file_name
                if src_file.exists():
                    shutil.copy2(src_file, dst_file)

    # Copy API documentation files
    api_src = root_pkg_source_dir / "genesis_bots" / "api"
    api_dest = base_dir / "genesis_bots" / "api"
    api_dest.mkdir(parents=True, exist_ok=True)
    trace_action(f"Created/Using directory: {api_dest}")

    # Copy README.md and LICENSE
    for file_name in ["README.md", "LICENSE"]:
        src_file = api_src / file_name
        dst_file = api_dest / file_name
        if src_file.exists():
            shutil.copy2(src_file, dst_file)

if __name__ == '__main__':
    copy_resources(verbose=True)
    print("Resources copied successfully.")