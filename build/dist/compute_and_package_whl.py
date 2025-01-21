import os
import re
import shutil
import tempfile
import subprocess
from setuptools.extension import Extension
from Cython.Build import cythonize
import glob

def should_copy(path, abs_exclude):
    for exc in abs_exclude:
        if os.path.abspath(path).startswith(exc) or "__pycache__" in path:
            return False
    return True

def compile_and_package(project_dir, public_files, exclude=None, output_dir="dist", package_name="genesis_bots", version="1.0.0"):
    exclude = exclude or []
    # Ensure output_dir is excluded so we don't copy previous builds
    exclude.append(os.path.join(project_dir, output_dir))

    public_files = public_files or []
    project_dir = os.path.abspath(project_dir)
    output_dir = os.path.abspath(output_dir)
    os.makedirs(output_dir, exist_ok=True)

    # Create a temporary directory for building
    temp_dir = tempfile.mkdtemp()
    temp_project_dir = os.path.join(temp_dir, "build")
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
                    rel_path = os.path.relpath(root, src)
                    # Ensure we maintain the genesis_bots package structure
                    if not rel_path.startswith("genesis_bots"):
                        rel_path = os.path.join("genesis_bots", rel_path)
                    dest_path = os.path.join(dest, rel_path, sanitized_file)
                    os.makedirs(os.path.dirname(dest_path), exist_ok=True)
                    shutil.copy2(src_path, dest_path)
                else:
                    print(f"Skipping {src_path} because it should be excluded")

    print(f"Copying project from {project_dir} to temporary directory: {temp_project_dir}")
    copy_filtered(project_dir, temp_project_dir)

    build_package(temp_project_dir, output_dir, package_name, version, public_files, project_dir, abs_exclude)

    # Cleanup
    user_input = input(f"Do you want to cleanup the temporary directory {temp_dir}? (y/n, default=n): ")
    if user_input.lower() == 'y':
        shutil.rmtree(temp_dir)
        print("Temporary directory cleaned up.")
    else:
        print("Temporary directory not cleaned up.")

def build_package(temp_project_dir, output_dir, package_name, version, public_files, project_dir, abs_exclude):
    package_dir = os.path.join(temp_project_dir, "genesis_bots")

    # Ensure required directories exist
    api_dir = os.path.join(package_dir, 'api')
    os.makedirs(api_dir, exist_ok=True)

    # Add __init__.py files if they don't exist
    if not os.path.exists(os.path.join(package_dir, "__init__.py")):
        with open(os.path.join(package_dir, "__init__.py"), "w") as f:
            f.write("from .api.genesis_api import GenesisAPI\n\n")
            f.write("from .api.snowflake_remote_server import GenesisSnowflakeServer\n")
            f.write("__all__ = ['GenesisAPI']\n")

    if not os.path.exists(os.path.join(api_dir, "__init__.py")):
        with open(os.path.join(api_dir, "__init__.py"), "w") as f:
            f.write("")

    # Handle extensions for Cython compilation
    extensions = []
    for dirpath, _, filenames in os.walk(temp_project_dir):
        for filename in filenames:
            filepath = os.path.join(dirpath, filename)
            if not should_copy(filepath, abs_exclude):
                continue
            if filepath.endswith(".py") and filename != "__init__.py":
                # Ensure module path starts from genesis_bots
                rel_path = os.path.relpath(filepath, temp_project_dir)
                if not rel_path.startswith("genesis_bots"):
                    rel_path = os.path.join("genesis_bots", rel_path)
                module_path = rel_path.replace(os.path.sep, ".")[:-3]
                source_rel = os.path.relpath(filepath, temp_project_dir)
                extensions.append(Extension(module_path, [source_rel]))

    if not extensions:
        print("No files to compile into C extensions. Ensure some .py files are not public or excluded.")
    else:
        print(f"Found {len(extensions)} extensions to compile.")

    # Create setup scripts
    compiled_setup_script = os.path.join(temp_project_dir, "compile_setup.py")
    wheel_setup_script = os.path.join(temp_project_dir, "wheel_setup.py")

    extensions_str = ",\n    ".join(
        f"Extension('{ext.name}', {ext.sources})" for ext in extensions
    )

    setup_py_template = """
from setuptools import setup, find_namespace_packages
from setuptools.command.build_ext import build_ext
from setuptools.extension import Extension
from Cython.Build import cythonize

setup(
    name="{package_name}",
    version="{version}",
    description="Genesis Bots Package",
    ext_modules=cythonize(
        [{extensions_str}],
        compiler_directives={{"language_level": "3"}}
    ),
    packages=find_namespace_packages(include=[
        'genesis_bots',
        'genesis_bots.*',
    ]),
    package_dir={{"": "."}},
    package_data={{
         'genesis_bots': ['**/*.yaml', '**/*.so', '**/*.py', 'requirements.txt'],
    }},
    include_package_data=True,
    install_requires=["snowflake_connector_python==3.12.3",
                     "urllib3==1.26.19",
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: Other/Proprietary License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.6",
)
"""

    wheel_setup_template = """
from setuptools import setup, find_namespace_packages
from wheel.bdist_wheel import bdist_wheel as _bdist_wheel
import os

this_directory = os.path.join(os.path.abspath(os.path.dirname(__file__)), "genesis_bots")

with open(os.path.join(this_directory, "requirements.txt")) as f:
    required = f.read().splitlines()

class bdist_wheel(_bdist_wheel):
    def finalize_options(self):
        super().finalize_options()
        self.root_is_pure = False

setup(
    name="{package_name}",
    version="{version}",
    description="Genesis Bots Package",
    packages=find_namespace_packages(include=[
        'genesis_bots',
        'genesis_bots.*',
    ]),
    package_dir={{"": "."}},
    package_data={{
         'genesis_bots': ['**/*.yaml', '**/*.so', '**/*.py'],
    }},
    data_files=[],
    zip_safe=False,
    include_package_data=True,
    cmdclass={{'bdist_wheel': bdist_wheel}},
    install_requires=required,
)
"""

    with open(compiled_setup_script, "w") as f:
        f.write(setup_py_template.format(
            package_name=package_name,
            version=version,
            extensions_str=extensions_str
        ))

    with open(wheel_setup_script, "w") as f:
        f.write(wheel_setup_template.format(
            package_name=package_name,
            version=version
        ))

    # Build steps
    print("Compiling extensions...")
    subprocess.check_call(["python", compiled_setup_script, "build_ext", "--inplace"], cwd=temp_project_dir)

    # Remove source files except API and __init__
    for dirpath, dirs, files in os.walk(temp_project_dir):
        for file in files:
            filepath = os.path.join(dirpath, file)
            if filepath.endswith(".py"):
                if file != "__init__.py" and not file.endswith("setup.py"):
                    os.remove(filepath)
            elif file.endswith((".pyc", ".c")):
                os.remove(filepath)

    print("Building wheel...")
    subprocess.check_call(["python", wheel_setup_script, "bdist_wheel", "--dist-dir", output_dir], cwd=temp_project_dir)

if __name__ == "__main__":
    project_directory = "."  # current directory
    public_api_files = [
        "genesis_bots/api/genesis_api.py",
        "genesis_bots/api/snowflake_remote_server.py",
        "genesis_bots/api/genesis_base.py",
        "genesis_bots/api/control.py",
    ]
    excluded_items = [
        os.path.join(project_directory, ".venv"),
        os.path.join(project_directory, ".git"),
        os.path.join(project_directory, "build"),
        os.path.join(project_directory, "bot_git"),
        os.path.join(project_directory, "backup"),
        os.path.join(project_directory, "default_files"),
        os.path.join(project_directory, "genesis_api_whl"),
        os.path.join(project_directory, "app engine"),
        os.path.join(project_directory, "experimental"),
        os.path.join(project_directory, "genesis_bots/teams/app.py"),
        os.path.join(project_directory, "tests"),
        os.path.join(project_directory, "test_services"),
        os.path.join(project_directory, "genesis_bots/demo/bot_os_streamlit.py"),
        os.path.join(project_directory, "app_engine"),
        os.path.join(project_directory, "generated_modules"),
        os.path.join(project_directory, "genesis_voice"),
        os.path.join(project_directory, "spider_load"),
        os.path.join(project_directory, "nodetest"),
        os.path.join(project_directory, "compute_and_package_whl2.py"),
    ]
    output_directory = "dist"

    compile_and_package(
        project_dir=project_directory,
        public_files=public_api_files,
        exclude=excluded_items,
        output_dir=output_directory,
        package_name="genesis_bots",
        version="1.0.8",
    )