import os
import shutil
from pathlib import Path

def copy_resources():
    """Copy demo files and demo data to the current directory."""
    # Get the package directory where the demo files are stored
    package_dir = Path(__file__).parent

    # Define source and destination paths for demos
    demo_src = package_dir / "demos"
    demo_dest = Path.cwd() / "apps" / "demos"

    # Create destination directories if they don't exist
    demo_dest.mkdir(parents=True, exist_ok=True)
    (demo_dest / "database_demos").mkdir(exist_ok=True)
    (demo_dest / "demo_data").mkdir(exist_ok=True)

    # Copy database demos
    print("\nCopying demo files...")
    for demo_file in ["oracle.md", "oracle_demo.sql"]:
        src = demo_src / "database_demos" / demo_file
        dst = demo_dest / "database_demos" / demo_file
        if src.exists():
            shutil.copy2(src, dst)
            print(f"Copying demo file: {src} -> {dst}")

    # Copy demo data files
    print("\nCopying demo_data files...")
    demo_data_files = [
        "workspace.sqlite",
        "baseball.sqlite",
        "formula_1.sqlite",
        "postgres_travel.sql",
        "demo_harvest_results.json"
    ]
    
    for data_file in demo_data_files:
        src = demo_src / "demo_data" / data_file
        dst = demo_dest / "demo_data" / data_file
        if src.exists():
            shutil.copy2(src, dst)
            print(f"Copying demo_data file: {src} -> {dst}")

if __name__ == '__main__':
    try:
        copy_resources()
    except Exception as e:
        print(f"Error copying resources: {str(e)}") 