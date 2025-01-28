from setuptools import setup, find_namespace_packages
from wheel.bdist_wheel import bdist_wheel as _bdist_wheel
import os
import platform
from build_config import IGNORE_DIRS, IGNORE_FILES, VERSION, PUBLIC_API_FILES
import glob

# Add this import
COMPILE_CYTHON = os.getenv('COMPILE_CYTHON', 'false').lower() == 'true'

class bdist_wheel(_bdist_wheel):
    def finalize_options(self):
        super().finalize_options()
        if COMPILE_CYTHON:
            # Platform-specific tag for Cython builds
            self.root_is_pure = False
            # Ensure proper platform tag
            if platform.system() == 'Linux':
                self.plat_name = f"manylinux1_{platform.machine()}"
            elif platform.system() == 'Darwin':
                self.plat_name = f"macosx_10_9_{platform.machine()}"
            # Windows will use default platform tag
        else:
            # Platform-independent for pure Python builds
            self.root_is_pure = True

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
        # Always include __init__.py files and PUBLIC_API_FILES
        if os.path.basename(filepath) == '__init__.py':
            return True
            
        # Always include files in PUBLIC_API_FILES
        relative_path = filepath.replace('\\', '/').replace('./', '')
        if relative_path in PUBLIC_API_FILES:
            return True
            
        # Check for corresponding binary files across different platforms
        base_path = os.path.splitext(filepath)[0]
        binary_patterns = [
            base_path + '.*.so',  # Linux/Mac
            base_path + '.*.pyd',  # Windows
            base_path + '.*.dylib'  # MacOS
        ]
        
        for pattern in binary_patterns:
            if glob.glob(pattern):
                return False
        return True

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
    install_requires=required + ['ngrok'],
) 