import os
import re
import shutil
import tempfile
import subprocess
from distutils.extension import Extension
from Cython.Build import cythonize
import glob

def should_copy(path, abs_exclude):
    for exc in abs_exclude:
        if os.path.abspath(path).startswith(exc) or "__pycache__" in path:
            return False
    return True

def compile_and_package(project_dir, public_files, exclude=None, output_dir="dist", package_name="compiled_whl", public_package_name="public_package", version="1.0.0"):
    exclude = exclude or []
    # Ensure output_dir is excluded so we don't copy previous builds
    exclude.append(os.path.join(project_dir, output_dir))

    public_files = public_files or []
    project_dir = os.path.abspath(project_dir)
    output_dir = os.path.abspath(output_dir)
    os.makedirs(output_dir, exist_ok=True)

    # Create a temporary directory for building
    temp_dir = tempfile.mkdtemp()
    temp_project_dir = os.path.join(temp_dir, package_name)
    print(f"temp_project_dir: {temp_project_dir}")

    # Compute absolute excluded paths
    abs_exclude = [os.path.abspath(x) for x in exclude]

    # `public_files` are given relative to project_dir, store them as abs paths
    public_files = [os.path.abspath(os.path.join(project_dir, file)) for file in public_files]

    def sanitize_filename(filename):
    # Replace invalid chars but keep extension intact
        base, ext = os.path.splitext(filename)
        base = re.sub(r'[^a-zA-Z0-9_]', '_', base)
        return base + ext

    def copy_filtered(src, dest):
        """Copy the source directory to the destination, filtering out excluded paths."""
        for root, dirs, files in os.walk(src):
            # Filter directories
            dirs[:] = [d for d in dirs if should_copy(os.path.join(root, d), abs_exclude)]
            for file in files:
                src_path = os.path.join(root, file)
                if should_copy(src_path, abs_exclude):
                    sanitized_file = sanitize_filename(file)
                    dest_path = os.path.join(dest, os.path.relpath(root, src), sanitized_file)
                    os.makedirs(os.path.dirname(dest_path), exist_ok=True)
                    shutil.copy2(src_path, dest_path)
                else:   
                    print(f"Skipping {src_path} because it should be excluded")

    print(f"Copying project from {project_dir} to temporary directory: {temp_project_dir}")
    copy_filtered(project_dir, temp_project_dir)

    # Build the compiled package
    build_compiled_package(temp_project_dir, output_dir, package_name, version, public_files, project_dir, abs_exclude)
    
    # After building the compiled wheel build the public package
    whl_files = glob.glob(os.path.join(output_dir, f"{package_name}-{version}-*.whl"))
    if len(whl_files) == 1:
        post_build_compiled_whl_path = whl_files[0]
    else:
        # Handle error: either no files or multiple matches
        raise RuntimeError("Could not uniquely identify the compiled wheel.")
    
    create_public_package(temp_dir, public_package_name, public_files, output_dir, version, compiled_package_name=package_name, compiled_whl_path=post_build_compiled_whl_path)

    # Cleanup
    print(f"Cleaning up temporary directory: {temp_dir}")
    #shutil.rmtree(temp_dir)
    #print("Temporary directory cleaned up.")

def build_compiled_package(temp_project_dir, output_dir, package_name, version, public_files, project_dir, abs_exclude):
    # public files in tmp structure
    tmp_public_files = [
        os.path.join(temp_project_dir, os.path.relpath(file, project_dir))
        for file in public_files
    ]
    # Identify Python files to compile (not public, not excluded)
    extensions = []
    for dirpath, _, filenames in os.walk(temp_project_dir):
        for filename in filenames:
            filepath = os.path.join(dirpath, filename)
            if not should_copy(filepath, abs_exclude):
                continue
        # Adjust your condition as needed. For example, if you're reverting back to ".py":
            if filepath.endswith(".py") and filepath not in tmp_public_files and filename != "__init__.py":
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
    compiled_setup_script = os.path.join(temp_project_dir, "compile_setup.py")
    wheel_setup_script = os.path.join(temp_project_dir, "wheel_setup.py")

    #extensions = extensions[:10]
    extensions_str = ",\n    ".join(
        f"Extension('{ext.name}', {ext.sources})" for ext in extensions
    )
    setup_py_template = """
from setuptools import setup
from setuptools.command.build_ext import build_ext
from setuptools.extension import Extension
from Cython.Build import cythonize

setup(
    name="{package_name}",
    version="{version}",
    description="Compiled internal logic package",
    ext_modules=cythonize(
        [{extensions_str}],
        compiler_directives={{"language_level": "3"}}
    ),
    #packages=[""],
    package_data={{
        '': ['**/*.yaml', '**/*.so', 'requirements.txt'],  # Include all YAML and .so files in any package
    }},
    include_package_data=True,
)
"""
    build_setup_py_content = setup_py_template.format(
        package_name=package_name,
        version=version,
        extensions_str=extensions_str
    )

    with open(compiled_setup_script, "w") as f:
        f.write(build_setup_py_content)

    wheel_setup_template = """
from setuptools import setup, find_packages
from wheel.bdist_wheel import bdist_wheel as _bdist_wheel
import os

this_directory = os.path.abspath(os.path.dirname(__file__))

with open(os.path.join(this_directory, "requirements.txt")) as f:
    required = f.read().splitlines()
                
class bdist_wheel(_bdist_wheel):
    def finalize_options(self):
        super().finalize_options()
        self.root_is_pure = False  # Mark the wheel as not pure, making it platform-specific

setup(
    name="{package_name}",
    version="{version}",
    description="Compiled internal logic package (packaging stage)",
    packages=find_packages(),       # Automatically find all packages with __init__.py
    include_package_data=True,      # Include additional data as specified
    package_data={{
        '': ['**/*.yaml', '**/*.so'],  # Include all YAML and .so files in any package
    }},
    cmdclass={{'bdist_wheel': bdist_wheel}},  # Use the customized bdist_wheel
    install_requires=required,
)
"""
    wheel_setup_py_content = wheel_setup_template.format(
        package_name=package_name,
        version=version,
        #extensions_str=extensions_str
    )

    with open(wheel_setup_script, "w") as f:
        f.write(wheel_setup_py_content)

    # Build the compiled wheel
    # 1. Run build_ext to compile .py to .so
    subprocess.check_call(["python", compiled_setup_script, "build_ext", "--inplace"], cwd=temp_project_dir)

    # 2. Remove all .py files except __init__.py
    for dirpath, dirs, files in os.walk(temp_project_dir):
        for file in files:
            if file.endswith(".py") and file != "__init__.py" and not file.endswith("setup.py") or file.endswith(".pyc") or file.endswith(".c"):
                os.remove(os.path.join(dirpath, file))

    subprocess.check_call(["python", wheel_setup_script, "bdist_wheel", "--dist-dir", output_dir], cwd=temp_project_dir)

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
import os

this_directory = os.path.abspath(os.path.join(os.path.dirname(__file__), "{public_package_name}"))

setup(
    name="{public_package_name}",
    version="{version}",
    description="Public API package depending on compiled internal logic",
    packages=["{public_package_name}"],
    package_dir={{"{public_package_name}": "{public_package_name}"}},
    package_data={{"{public_package_name}": ["*"]}},
    install_requires=["snowflake_connector_python==3.12.3"],  # Adjust version constraints as needed,
    classifiers=[
        "Programming Language :: Python :: 3",
        #"License :: OSI Approved :: Server Side Public License (SSPL)",
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
        #"api/snowflake_local_server.py",
        "api/snowflake_remote_server.py",
        "api/genesis_base.py",
        "requirements.txt",
    ]
    excluded_items = [
        os.path.join(project_directory, ".venv"),
        os.path.join(project_directory, ".git"),
        os.path.join(project_directory, "build"),
        os.path.join(project_directory, "bot_git"),
        os.path.join(project_directory, "genesis_api_whl"),    
        os.path.join(project_directory, "app engine"),
        os.path.join(project_directory, "experimental"),
        os.path.join(project_directory, "teams/app.py"),
        os.path.join(project_directory, "tests/hello_world_regtest_01_local.py"),
        os.path.join(project_directory, "demo/bot_os_streamlit.py"),
        os.path.join(project_directory, "app_engine"),
        #os.path.join(project_directory, "google_sheets"),
    ]
    output_directory = "dist"

    compile_and_package(
        project_dir=project_directory,
        public_files=public_api_files,
        exclude=excluded_items,
        output_dir=output_directory,
        package_name="genesis_api_whl",
        public_package_name="genesis_api_public",
        version="1.0.2",
    )
