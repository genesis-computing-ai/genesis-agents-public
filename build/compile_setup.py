from setuptools import setup
import os
from build_config import IGNORE_DIRS, IGNORE_FILES, is_public_api_file
from multiprocessing import freeze_support
import platform

# Check environment variable for Cython compilation
COMPILE_CYTHON = os.getenv('COMPILE_CYTHON', 'false').lower() == 'true'

def main():
    # Only import Cython and set up extensions if we're compiling
    extensions = None
    if COMPILE_CYTHON:
        from setuptools.extension import Extension
        from Cython.Build import cythonize

        # Disable multiprocessing completely for Cython
        os.environ['CYTHON_PARALLEL'] = '0'

        # Platform-specific compiler arguments
        if platform.system() == 'Windows':
            extra_compile_args = [
                '/O2',  # Optimization flag
                '/wd4702',  # Suppress unreachable code warnings
                '/wd4457'   # Suppress fallthrough warnings
            ]
        else:
            # Add flags to suppress specific warnings on Unix-like systems
            extra_compile_args = [
                '-O2',
                '-Wno-unreachable-code',
                '-Wno-unreachable-code-fallthrough'
            ]

        # Define packages to skip during compilation based on platform
        SKIP_COMPILE_PACKAGES = {
            'Windows': [
                'dagster==1.9.5',
                'dagster-graphql',
                'dagster-spark',
                'dagster-dbt',
                'dagster-sdf',
                'annoy==1.17.3',
            ],
            'Linux': [],
            'Darwin': [],  # macOS
        }

        # Get skip list for current platform
        skip_packages = SKIP_COMPILE_PACKAGES.get(platform.system(), [])

        extensions = []
        compiled_files = []  # Keep track of files we've compiled
        cwd = os.getcwd()
        for root, dirs, files in os.walk('genesis_bots'):
            dirs[:] = [d for d in dirs if d not in IGNORE_DIRS]

            for file in files:
                if file.endswith('.py'):
                    path = os.path.join(root, file)
                    
                    # Skip compilation if file is in a package we want to skip
                    if any(pkg in path for pkg in skip_packages):
                        print(f"Skipping compilation for {path} (platform-specific exclusion)")
                        continue
                        
                    if (not is_public_api_file(cwd, path) and
                        file != '__init__.py' and
                        path not in IGNORE_FILES):
                        module_path = path[:-3].replace(os.path.sep, '.')
                        extension = Extension(
                            module_path,
                            [path],
                            extra_compile_args=extra_compile_args,
                            define_macros=[('NPY_NO_DEPRECATED_API', 'NPY_1_7_API_VERSION')]
                        )
                        extensions.append(extension)
                        compiled_files.append(path)

        # Cythonize all extensions
        extensions = cythonize(
            extensions,
            compiler_directives={"language_level": "3"},
            nthreads=1,
            force=True
        )

        # Remove original .py files after successful compilation
        for py_file in compiled_files:
            try:
                os.remove(py_file)
                print(f"Removed source file: {py_file}")
            except OSError as e:
                print(f"Error removing {py_file}: {e}")

    # Modify the setup call to include entry_points
    setup(
        ext_modules=extensions,
        entry_points={
            'console_scripts': [
                'genesis=genesis_bots.apps.cli:main',
            ],
        }
    )

if __name__ == '__main__':
    freeze_support()
    main()