import os
import shutil
import compileall
import zipfile
import tempfile

def compile_to_bytecode(project_dir, exclude=None, include=None, output_archive="project.zip"):
    """
    Compiles Python files to bytecode, organizes them, and creates a zip package.
    Processes __pycache__ directories explicitly to ensure .pyc files are included.

    Args:
        project_dir (str): Path to the project directory.
        exclude (list): List of files or directories to exclude.
        include (list): List of specific .py files to include in the final zip.
        output_archive (str): Name of the output zip archive.
    """
    exclude = exclude or []
    include = include or []

    # Normalize paths
    #exclude = [os.path.abspath(path) for path in exclude]
    #include = [os.path.abspath(path) for path in include]
    project_dir = os.path.abspath(project_dir)

    # Step 1: Copy project to a temporary directory
    temp_dir = tempfile.mkdtemp()
    temp_project_dir = os.path.join(temp_dir, os.path.basename(project_dir))
    shutil.copytree(project_dir, temp_project_dir, dirs_exist_ok=True)
    print(f"Copied project to temporary directory: {temp_project_dir}")

    # Step 2: Compile all .py files to bytecode
    print("Compiling Python files to bytecode...")
    compileall.compile_dir(temp_project_dir, force=True, quiet=1)

    # Step 3: Process __pycache__ explicitly and organize .pyc files
    def process_pycache(temp_project_dir):
        for dirpath, dirnames, filenames in os.walk(temp_project_dir):
            for dirname in dirnames:
                if dirname == "__pycache__":
                    pycache_path = os.path.join(dirpath, dirname)
                    for pyc_file in os.listdir(pycache_path):
                        if pyc_file.endswith(".pyc"):
                            # Move .pyc file to parent directory
                            src = os.path.join(pycache_path, pyc_file)
                            dest_name = pyc_file.split(".")[0] + ".pyc"
                            dest = os.path.join(dirpath, dest_name)
                            shutil.move(src, dest)
                    # Remove the now-empty __pycache__ directory
                    shutil.rmtree(pycache_path)

    process_pycache(temp_project_dir)

    # Step 4: Remove source .py files except included ones
    for dirpath, _, filenames in os.walk(temp_project_dir):
        for filename in filenames:
            filepath = os.path.abspath(os.path.join(dirpath, filename))
            if filename.endswith(".py") and filename not in include:
                os.remove(filepath)

    # Step 5: Create a zip archive of the compiled project
    print(f"Packaging compiled files into {output_archive}...")
    with zipfile.ZipFile(output_archive, "w", zipfile.ZIP_DEFLATED) as archive:
        for dirpath, _, filenames in os.walk(temp_project_dir):
            for filename in filenames:
                filepath = os.path.abspath(os.path.join(dirpath, filename))
                if filename in exclude:
                    continue
                arcname = os.path.relpath(filepath, temp_project_dir)
                print(f"Adding {filepath} to archive as {arcname}")
                archive.write(filepath, arcname)

    # Step 6: Cleanup temporary directory
    shutil.rmtree(temp_dir)
    print("Temporary directory cleaned up.")
    print("Packaging complete!")
    print(f"Output archive: {output_archive}")


# Example Usage:
if __name__ == "__main__":
    project_directory = "./api"  # Replace with your project directory
    excluded_items = [
        "snowflake_local_server.py",  # Example file to exclude
        "snowflake_local_server.pyc",  # Example directory to exclude
    ]
    included_items = [
        "genesis_api.py",  # Example file to include in zip
        "demo_remote_api_o1.py",  # Example file to include in zip
    ]
    output_zip = "genesis_api.zip"

    compile_to_bytecode(
        project_dir=project_directory,
        exclude=excluded_items,
        include=included_items,
        output_archive=output_zip
    )
