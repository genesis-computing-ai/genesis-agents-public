import os
import re
import shutil
import tempfile
import subprocess
from distutils.extension import Extension
from Cython.Build import cythonize

def compile_and_package(project_dir, public_files, exclude=None, output_dir="dist", package_name="compiled_whl", public_package_name="public_package", version="1.0.0"):
    exclude = exclude or []
    # Ensure output_dir is excluded so we don't copy previous builds
    exclude.append(os.path.join(project_dir, output_dir))

    public_files = public_files or []
    project_dir = os.path.abspath(project_dir)
    output_dir = os.path.abspath(output_dir)

    # Create a temporary directory for building
    temp_dir = tempfile.mkdtemp()
    temp_project_dir = os.path.join(temp_dir, package_name)

    # Compute absolute excluded paths
    abs_exclude = [os.path.abspath(x) for x in exclude]

    # `public_files` are given relative to project_dir, store them as abs paths
    public_files = [os.path.abspath(os.path.join(project_dir, file)) for file in public_files]

    # public files in tmp structure
    tmp_public_files = [
        os.path.join(temp_project_dir, os.path.relpath(file, project_dir))
        for file in public_files
    ]

    def should_copy(path):
        for exc in abs_exclude:
            if os.path.abspath(path).startswith(exc):
                return False
        return True

    def sanitize_filename(filename):
    # Replace invalid chars but keep extension intact
        base, ext = os.path.splitext(filename)
        base = re.sub(r'[^a-zA-Z0-9_]', '_', base)
        return base + ext

    def copy_filtered(src, dest):
        """Copy the source directory to the destination, filtering out excluded paths."""
        for root, dirs, files in os.walk(src):
            # Filter directories
            pre_filtered_dirs = dirs
            dirs[:] = [d for d in dirs if should_copy(os.path.join(root, d))]
            #print(f"Excluded directories in {root}: {set(pre_filtered_dirs) - set(dirs)}")
            #print(f"Copied directories in {root}: {set(dirs)}")
            for file in files:
                src_path = os.path.join(root, file)
                if should_copy(src_path):
                    sanitized_file = sanitize_filename(file)
                    dest_path = os.path.join(dest, os.path.relpath(root, src), sanitized_file)
                    os.makedirs(os.path.dirname(dest_path), exist_ok=True)
                    shutil.copy2(src_path, dest_path)
                else:   
                    print(f"Skipping {src_path} because it should be excluded")

    print(f"Copying project from {project_dir} to temporary directory: {temp_project_dir}")
    copy_filtered(project_dir, temp_project_dir)

    compiled_whl_path = os.path.join(output_dir, f"{package_name}-{version}-py3-none-any.whl")
    create_public_package(temp_dir, public_package_name, public_files, output_dir, version, compiled_package_name=package_name, compiled_whl_path=compiled_whl_path)

    # Identify Python files to compile (not public, not excluded)
    extensions = []
    for dirpath, _, filenames in os.walk(temp_project_dir):
        for filename in filenames:
            filepath = os.path.join(dirpath, filename)
            if not should_copy(filepath):
                continue
        # Adjust your condition as needed. For example, if you're reverting back to ".py":
            if filepath.endswith(".py") and filepath not in tmp_public_files:
                module_path = os.path.relpath(filepath, temp_project_dir).replace(os.path.sep, ".")[:-3]
                # Convert to relative path for the Extension source
                source_rel = os.path.relpath(filepath, temp_project_dir)
                extensions.append(Extension(module_path, [source_rel]))
            else:
                print(f"Skipping {filepath} because it should be excluded")

    if not extensions:
        print("No files to compile into C extensions. Ensure some .py files are not public or excluded.")
    else:
        print(f"Found {len(extensions)} extensions to compile.")

    # Create setup.py for the compiled package
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

    # Build the compiled wheel
    if extensions:
        print("Compiling internal logic to C extensions...")
        subprocess.check_call(
            ["python", compiled_setup_script, "bdist_wheel", "--dist-dir", output_dir],
            cwd=temp_project_dir
        )
        compiled_whl_path = os.path.join(output_dir, f"{package_name}-{version}-py3-none-any.whl")
        print(f"Compiled package saved to: {compiled_whl_path}")
    else:
        # If no extensions, still create a wheel with no compiled code (if desired)
        print("No compiled extensions. Creating empty compiled package wheel.")
        subprocess.check_call(["python", compiled_setup_script, "bdist_wheel", "--dist-dir", output_dir])

    # Cleanup
    shutil.rmtree(temp_dir)
    print("Temporary directory cleaned up.")

def create_public_package(temp_dir, public_package_name, public_files, output_dir, version, compiled_package_name, compiled_whl_path):
    # Now create the public package
    public_package_dir = os.path.join(temp_dir, public_package_name)

    # Clean and create directory structure for public package
    if os.path.exists(public_package_dir):
        shutil.rmtree(public_package_dir)
    os.makedirs(os.path.join(public_package_dir, public_package_name), exist_ok=True)

    # Add __init__.py
    init_file = os.path.join(public_package_dir, public_package_name, "__init__.py")
    with open(init_file, "w") as f:
        pass

    # Copy only the public files
    for public_file in public_files:
        dest_path = os.path.join(public_package_dir, public_package_name, os.path.basename(public_file))
        shutil.copy2(public_file, dest_path)

    # Create setup.py for the public package AFTER copying files
    public_setup_script = os.path.join(public_package_dir, "setup.py")
    with open(public_setup_script, "w") as f:
        f.write(f"""
from setuptools import setup

setup(
    name="{public_package_name}",
    version="{version}",
    description="Public API package depending on compiled internal logic",
    packages=["{public_package_name}"],
    package_dir={{"{public_package_name}": "{public_package_name}"}},
    package_data={{"{public_package_name}": ["*"]}},
    install_requires=[
        "{compiled_package_name} @ file://{compiled_whl_path}"
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.6",
)
    """)

    # Build the public wheel
    print("Building public API package...")
    subprocess.check_call(["python", public_setup_script, "bdist_wheel", "--dist-dir", output_dir], 
                          cwd=public_package_dir)
    public_whl_path = os.path.join(output_dir, f"{public_package_name}-{version}-py3-none-any.whl")
    print(f"Public package saved to: {public_whl_path}")



# Example usage
if __name__ == "__main__":
    project_directory = "."  # current directory
    public_api_files = [
        "api/genesis_api.py",
        "api/demo_remote_api_01.py",
        "api/demo_local_api_01.py",
        "api/snowflake_local_server.py",
        "api/snowflake_remote_server.py",
    ]
    excluded_items = [
        os.path.join(project_directory, ".venv"),
        os.path.join(project_directory, ".git"),
        os.path.join(project_directory, "build"),
        #os.path.join(project_directory, "spider_load"),
        os.path.join(project_directory, "bot_git"),
        os.path.join(project_directory, "genesis_api_whl"),    
        os.path.join(project_directory, "app engine"),
        os.path.join(project_directory, "experimental"),
        os.path.join(project_directory, "teams/app.py"),
        os.path.join(project_directory, "tests/hello_world_regtest_01_local.py"),
    ]
    output_directory = "dist"

    compile_and_package(
        project_dir=project_directory,
        public_files=public_api_files,
        exclude=excluded_items,
        output_dir=output_directory,
        package_name="genesis_api_whl",
        public_package_name="genesis_api_public",
        version="1.0.0",
    )