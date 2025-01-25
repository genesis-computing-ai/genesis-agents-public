from setuptools import setup, find_namespace_packages
import os
import shutil
from build_config import PUBLIC_API_FILES, IGNORE_DIRS, IGNORE_FILES, VERSION

# Check environment variable for Cython compilation
COMPILE_CYTHON = os.getenv('COMPILE_CYTHON', 'false').lower() == 'true'

# Only import Cython and set up extensions if we're compiling
extensions = None
if COMPILE_CYTHON:
    from setuptools.command.build_ext import build_ext
    from setuptools.extension import Extension
    from Cython.Build import cythonize
    
    # Disable multiprocessing completely for Cython
    os.environ['CYTHON_PARALLEL'] = '0'
    
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
    
    # Configure Cython compilation with single thread
    extensions = cythonize(
        extensions,
        compiler_directives={
            'language_level': "3",
            'always_allow_keywords': True
        },
        nthreads=1,  # Force single-threaded compilation
        force=True   # Force recompilation
    )

setup(
    name="genesis_bots",
    version=VERSION,
    description="Genesis Bots Package",
    ext_modules=extensions,  # Will be None if COMPILE_CYTHON is False
    packages=find_namespace_packages(include=[
        'genesis_bots*',
        'apps*'
    ]),
    package_dir={
        "": ".",
    },
    package_data={
         'genesis_bots': ['**/*.yaml', '**/*.so', '**/*.py', 'requirements.txt'],
         'apps': [
             '**/*.yaml', 
             '**/*.so', 
             '**/*.py', 
             'demos/**/*',  # Include all files in demos directory
             'streamlit_gui/**/*',
             'streamlit_gui/**/*.png',  # Explicitly include PNG files
         ],
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
    entry_points={
        'console_scripts': [
            'install-genesis-demos=genesis_bots.install_demos:copy_demos_to_documents',
        ],
    },
) 