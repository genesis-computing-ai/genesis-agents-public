from setuptools import setup
import os
from build_config import VERSION, PUBLIC_API_FILES, IGNORE_DIRS, IGNORE_FILES
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
        extra_compile_args = ['/O2'] if platform.system() == 'Windows' else ['-O2']
        
        extensions = []
        compiled_files = []  # Keep track of files we've compiled
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

    # Minimal setup call since most config is in pyproject.toml
    setup(
        name="genesis_bots",
        version=VERSION,
        license="SSPL",
        classifiers=[
            "License :: Other/Proprietary License",
        ],
        ext_modules=extensions,
    )

if __name__ == '__main__':
    freeze_support()
    main() 