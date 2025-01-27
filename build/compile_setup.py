from setuptools import setup, find_namespace_packages
import os
import shutil
from build_config import PUBLIC_API_FILES, IGNORE_DIRS, IGNORE_FILES, VERSION
from multiprocessing import freeze_support

# Check environment variable for Cython compilation
COMPILE_CYTHON = os.getenv('COMPILE_CYTHON', 'false').lower() == 'true'

def main():
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
            dirs[:] = [d for d in dirs if d not in IGNORE_DIRS]
            
            for file in files:
                if file.endswith('.py'):
                    path = os.path.join(root, file)
                    if (path not in PUBLIC_API_FILES and 
                        file != '__init__.py' and 
                        path not in IGNORE_FILES):
                        module_path = path[:-3].replace(os.path.sep, '.')
                        extension = Extension(
                            module_path, 
                            [path],
                            extra_compile_args=['-O2'],
                            define_macros=[('NPY_NO_DEPRECATED_API', 'NPY_1_7_API_VERSION')]
                        )
                        extensions.append(extension)
        
        # Simplified cythonize configuration matching the original file
        extensions = cythonize(
            extensions,
            compiler_directives={"language_level": "3"},
            nthreads=1,
            force=True
        )

    setup(
        name="genesis_bots",
        version=VERSION,
        description="Genesis Bots Package",
        ext_modules=extensions,
        packages=find_namespace_packages(include=[
            'genesis_bots*',
            'apps*'
        ]),
        package_dir={
            "": ".",
        },
        package_data={
            'genesis_bots': [
                '**/*.yaml',
                '**/*.so',
                '**/*.py',
                '**/*.conf',
                '**/*.json',
                'default_config/*',
                'requirements.txt'
            ],
            'apps': [
                '**/*.yaml', 
                '**/*.so', 
                '**/*.py', 
                '**/*.conf',
                '**/*.json',
                'demos/**/*',
                'streamlit_gui/**/*',
                'sdk_examples/**/*'
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
                'install-genesis-resources=genesis_bots.install_resources:install_resources',
            ],
        },
    )

if __name__ == '__main__':
    freeze_support()
    main() 