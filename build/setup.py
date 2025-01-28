from setuptools import setup, find_namespace_packages
from wheel.bdist_wheel import bdist_wheel as _bdist_wheel
import os
from build_config import IGNORE_DIRS, IGNORE_FILES, VERSION, PUBLIC_API_FILES
import glob

class bdist_wheel(_bdist_wheel):
    def finalize_options(self):
        super().finalize_options()
        self.root_is_pure = False

def find_packages_excluding(exclude_dirs, exclude_files):
    """Custom package finder that excludes specified directories and files"""
    def is_excluded(package):
        # Check if package is in excluded directories
        if any(exclude_dir in package.split('.') for exclude_dir in exclude_dirs):
            return True
        
        # Check if package corresponds to excluded files
        package_path = package.replace('.', os.path.sep) + '.py'
        if package_path in exclude_files:
            return True
        
        return False
    
    # Make sure we're including both genesis_bots and apps packages
    all_packages = find_namespace_packages(include=[
        'genesis_bots',
        'genesis_bots.*',
        'apps',
        'apps.*'
    ])
    return [pkg for pkg in all_packages if not is_excluded(pkg)]

def get_package_data():
    """Get package data, excluding .py files that have corresponding .so files"""
    def should_include_py(filepath):
        # Always include __init__.py files
        if os.path.basename(filepath) == '__init__.py':
            return True
            
        # Always include files in PUBLIC_API_FILES
        relative_path = filepath.replace('\\', '/').replace('./', '')
        if relative_path in PUBLIC_API_FILES:
            return True
            
        # Check if there's a corresponding .so file
        so_path = os.path.splitext(filepath)[0] + '.cpython-*-darwin.so'
        so_files = glob.glob(so_path)
        return len(so_files) == 0

    # Initialize package data with non-Python files
    package_data = {
        'genesis_bots': [
            '**/*.yaml',
            '**/*.so',
            '**/*.conf',
            '**/*.json',
            'requirements.txt',
            'default_config/*'
        ],
        'apps': [
            '**/*.yaml', 
            '**/*.so', 
            '**/*.conf',
            '**/*.json',
            'demos/**/*',
            'streamlit_gui/**/*',
            'sdk_examples/**/*'
        ]
    }
    
    # Add Python files selectively
    for package in ['genesis_bots', 'apps']:
        py_files = glob.glob(f'{package}/**/*.py', recursive=True)
        included_py = [f[len(package)+1:] for f in py_files if should_include_py(f)]
        if included_py:
            package_data[package].extend(included_py)
    
    return package_data

# Get the project root directory
root_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
with open(os.path.join(root_dir, "requirements.txt")) as f:
    required = f.read().splitlines()

setup(
    name="genesis_bots",
    version=VERSION,
    description="Genesis Bots Package",
    packages=find_packages_excluding(IGNORE_DIRS, IGNORE_FILES),
    package_dir={
        "": ".",  # Look for packages in the current directory
        "genesis_bots.apps": "apps",  # Map apps to genesis_bots.apps
    },
    py_modules=['apps'],  # Explicitly include apps as a module
    package_data=get_package_data(),  # Use the function instead of hardcoded dict
    data_files=[],
    zip_safe=False,
    include_package_data=True,
    cmdclass={'bdist_wheel': bdist_wheel},
    install_requires=required,
) 