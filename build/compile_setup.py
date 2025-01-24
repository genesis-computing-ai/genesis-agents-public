from setuptools import setup, find_namespace_packages
from setuptools.command.build_ext import build_ext
from setuptools.extension import Extension
from Cython.Build import cythonize
import glob
import os
from build_config import PUBLIC_API_FILES, IGNORE_DIRS, IGNORE_FILES, VERSION

# Collect all .py files except those in PUBLIC_API_FILES and __init__.py files
extensions = []
for root, dirs, files in os.walk('genesis_bots'):
    # Remove ignored directories from the walk
    dirs[:] = [d for d in dirs if d not in IGNORE_DIRS]
    
    for file in files:
        if file.endswith('.py'):
            path = os.path.join(root, file)
            # Skip public API files, __init__.py, and ignored files
            if (path not in PUBLIC_API_FILES and 
                file != '__init__.py' and 
                path not in IGNORE_FILES):
                module_path = path[:-3].replace(os.path.sep, '.')
                extensions.append(Extension(module_path, [path]))

setup(
    name="genesis_bots",
    version=VERSION,
    description="Genesis Bots Package",
    ext_modules=cythonize(
        extensions,
        compiler_directives={"language_level": "3"}  # Simplified directives
    ),
    packages=find_namespace_packages(include=[
        'genesis_bots',
        'genesis_bots.*',
        'apps',
        'apps.*'
    ]),
    package_dir={
        "": ".",
        "genesis_bots.apps": "apps",  # Map apps to genesis_bots.apps namespace
    },
    package_data={
         'genesis_bots': ['**/*.yaml', '**/*.so', '**/*.py', 'requirements.txt'],
         'apps': ['**/*.yaml', '**/*.so', '**/*.py', 'demos/demo_data/*'],
    },
    include_package_data=True,
    install_requires=[
        "snowflake_connector_python==3.12.3",
        "urllib3==1.26.19",
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: Other/Proprietary License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.6",
) 