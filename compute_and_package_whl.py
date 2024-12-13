from distutils.dir_util import copy_tree
from distutils.extension import Extension
import os
import re
import shutil
import tempfile
from Cython.Build import cythonize

def compile_and_package(project_dir, public_files, exclude=None, output_dir="dist", package_name="compiled_whl", public_package_name="public_package", version="1.0.0"):
    """
    Compiles internal Python files into C extensions as a .whl package and creates a public Python package
    that depends on the compiled .whl package.

    Args:
        project_dir (str): Path to the project directory.
        public_files (list): List of public .py files to remain uncompiled.
        exclude (list): List of files or directories to exclude from the package.
        output_dir (str): Directory to store the output .whl files.
        package_name (str): Name of the compiled .whl package.
        public_package_name (str): Name of the public Python package.
        version (str): Version of the package.
    """
    exclude = exclude or []
    exclude.append(os.path.join(project_dir, output_dir))

    public_files = public_files or []
    project_dir = os.path.abspath(project_dir)

    # Create a temporary directory for the compiled package
    temp_dir = tempfile.mkdtemp()
    temp_project_dir = os.path.join(temp_dir, package_name)

    # Update exclude and public_files to reference paths in the temporary directory
    exclude = [
        os.path.join(project_dir, os.path.relpath(path, project_dir))
        for path in exclude
    ]
    public_files = [
        os.path.join(project_dir, file)
        for file in public_files
    ]
    tmp_public_files = [
        os.path.join(temp_project_dir, os.path.relpath(file, project_dir))
        for file in public_files
    ]

    def should_copy(path):
        """Check if the given path should be copied based on exclusions."""
        #if ".venv" in path:
        #    print(f"should be excluded: {path}")
        for excluded in exclude:
            if os.path.abspath(path).startswith(os.path.abspath(excluded)):
                return False
        return True

    def sanitize_filename(filename):
        """Convert invalid filenames to valid Python module names."""
        return re.sub(r"[^a-zA-Z0-9_]", "_", filename)

    
    def copy_filtered(src, dest):
        """Copy the source directory to the destination, filtering out excluded paths."""
        for root, dirs, files in os.walk(src):
            # Filter out excluded directories
            dirs[:] = [d for d in dirs if should_copy(os.path.join(root, d))]
            for file in files:
                src_path = os.path.join(root, file)
                if should_copy(src_path):
                    sanitized_file = sanitize_filename(file)
                    dest_path = os.path.join(dest, os.path.relpath(root, src), sanitized_file)
                    os.makedirs(os.path.dirname(dest_path), exist_ok=True)
                    shutil.copy2(src_path, dest_path)
                else:
                    print(f"Excluding {src_path}")

    print(f"Copying project to temporary directory: {temp_project_dir}")
    copy_filtered(project_dir, temp_project_dir)

    # Identify Python files to compile
    extensions = []
    for dirpath, _, filenames in os.walk(temp_project_dir):
        for filename in filenames:
            filepath = os.path.abspath(os.path.join(dirpath, filename))
            if filepath in exclude:
                continue
            if filepath.endswith(".py") and filepath not in tmp_public_files:
                module_path = os.path.relpath(filepath, temp_project_dir).replace(os.path.sep, ".")[:-3]
                extensions.append(Extension(module_path, [filepath]))

    # Create a setup.py for the compiled package
    compiled_setup_script = os.path.join(temp_project_dir, "setup.py")
    with open(compiled_setup_script, "w") as f:
        extensions_str = ",\n    ".join(
            f"Extension('{ext.name}', {ext.sources})" for ext in extensions
        )
        f.write(f"""
from setuptools import setup, Extension
from Cython.Build import cythonize

setup(
    name="{package_name}",
    version="{version}",
    description="Compiled internal logic package",
    ext_modules=cythonize([
        {extensions_str}
    ], compiler_directives={{"language_level": "3"}}),
    packages=[""],
    package_data={{"": ["*.so"]}},
    include_package_data=True,
)
    """)

    # Compile the project and create the .whl package
    print("Compiling internal logic to C extensions...")
    os.system(f"python {compiled_setup_script} bdist_wheel --dist-dir {output_dir}")

    compiled_whl_path = os.path.join(output_dir, f"{package_name}-{version}-py3-none-any.whl")
    print(f"Compiled package saved to: {compiled_whl_path}")

     # Create the public Python package
    public_package_dir = os.path.join(temp_dir, public_package_name)
    if os.path.exists(public_package_dir):
        shutil.rmtree(public_package_dir)

    os.makedirs(os.path.join(public_package_dir, public_package_name), exist_ok=True)

    # Add an empty __init__.py file to the public package
    init_file = os.path.join(public_package_dir, public_package_name, "__init__.py")
    with open(init_file, "w") as f:
        pass

    # Create setup.py for the public package
    public_setup_script = os.path.join(public_package_dir, "setup.py")
    with open(public_setup_script, "w") as f:
        f.write(f"""
from setuptools import setup

setup(
    name="{public_package_name}",
    version="{version}",
    description="Public API package depending on compiled internal logic",
    packages=["{public_package_name}"],
    package_dir={{"{public_package_name}": "{public_package_name}"}},  # Map only the public package
    package_data={{"{public_package_name}": ["*"]}},  # Include all files in the public_package_name directory
    install_requires=[
        "{package_name} @ file://{compiled_whl_path}"
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.6",
)
    """)

    # Ensure public package directory contains only the intended files
    if os.path.exists(public_package_dir):
        shutil.rmtree(public_package_dir)
    os.makedirs(os.path.join(public_package_dir, public_package_name), exist_ok=True)

    # Add __init__.py to the public package
    init_file = os.path.join(public_package_dir, public_package_name, "__init__.py")
    with open(init_file, "w") as f:
        pass

    # Copy public files to the public package directory
    for public_file in public_files:
        dest_path = os.path.join(public_package_dir, public_package_name, os.path.basename(public_file))
    shutil.copy2(public_file, dest_path)

    # Debug: Print directory contents
    print("Public package directory contents:")
    for root, dirs, files in os.walk(public_package_dir):
        print(f"Directory: {root}")
        for file in files:
            print(f"  {file}")


# Example Usage
if __name__ == "__main__":
    project_directory = "."  # Replace with your project directory

    # Public files to remain as .py
    public_api_files = [
        "api/genesis_api.py",  # Public API file to expose
        "api/demo_remote_api_01.py",
        "api/demo_local_api_01.py",
        "api/snowflake_local_server.py",
        "api/snowflake_remote_server.py",
    ]

    # Excluded files or directories, including .venv
    excluded_items = [
        os.path.join(project_directory, ".venv"),  # Exclude virtual environment
    ]

    # Output directory for the .whl files
    output_directory = "dist"

    # Call the function to create both packages
    compile_and_package(
        project_dir=project_directory,
        public_files=public_api_files,
        exclude=excluded_items,
        output_dir=output_directory,
        package_name="genesis_api_whl",
        public_package_name="genesis_api_public",
        version="1.0.0",
    )
