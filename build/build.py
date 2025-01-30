import os
import shutil
import subprocess
from build_config import IGNORE_DIRS, IGNORE_FILES, PUBLIC_API_FILES
from multiprocessing import freeze_support
import platform
import argparse

# Check environment variable for Cython compilation
COMPILE_CYTHON = os.getenv('COMPILE_CYTHON', 'false').lower() == 'true'

def setup_build_environment():
    """Ensure build environment has all necessary dependencies."""
    print("Setting up build environment...")
    subprocess.run(['pip', 'install', '--upgrade', 'pip'], check=True)
    subprocess.run(['pip', 'install', 'setuptools>=40.8.0'], check=True)
    subprocess.run(['pip', 'install', 'wheel>=0.37.0'], check=True)
    subprocess.run(['pip', 'install', 'Cython'], check=True)
    subprocess.run(['pip', 'install', 'numpy>=1.7.0'], check=True)
    subprocess.run(['pip', 'install', 'cmake>=3.1.0'], check=True)
    subprocess.run(['pip', 'install', 'annoy'], check=True)

def create_build_directory(version):
    """Create a clean build directory with copied source files."""
    # Create dist directory if it doesn't exist
    dist_dir = os.path.join('build', 'dist')
    os.makedirs(dist_dir, exist_ok=True)
    
    # Create a new build directory name with version
    build_dir = os.path.join(dist_dir, f"genesis_bots_build_{version}")
    
    # Remove the build directory if it exists
    if os.path.exists(build_dir):
        print(f"Removing existing build directory: {build_dir}")
        shutil.rmtree(build_dir)
    
    print(f"Creating new build directory: {build_dir}")
    os.makedirs(build_dir)
    
    def ignore_patterns(path, names):
        # Check if any parent directory is in IGNORE_DIRS
        path_parts = path.replace('\\', '/').split('/')
        for i in range(len(path_parts)):
            current_path = '/'.join(path_parts[i:])
            if current_path in IGNORE_DIRS:
                return names  # Ignore everything in this directory
                
        relative_paths = [os.path.join(path, name).replace('\\', '/') for name in names]
        return [n for n, p in zip(names, relative_paths) 
                if n in IGNORE_DIRS or 
                n.endswith('.pyc') or 
                p in IGNORE_FILES]
    
    print("Copying source files...")
    # Copy genesis_bots directory
    shutil.copytree(
        'genesis_bots', 
        os.path.join(build_dir, 'genesis_bots'),
        ignore=ignore_patterns
    )
    
    # Copy apps directory
    print("Copying apps directory...")
    shutil.copytree(
        'apps',
        os.path.join(build_dir, 'apps'),
        ignore=ignore_patterns
    )
    
    # Copy build files
    build_files = ['build_config.py', 'compile_setup.py', 'setup.py', 'cleanup.py']
    for file in build_files:
        shutil.copy2(os.path.join('build', file), os.path.join(build_dir, file))
    
    # Copy pyproject.toml
    shutil.copy2('pyproject.toml', os.path.join(build_dir, 'pyproject.toml'))
    
    return build_dir

def build_package(build_dir, args):
    """Run the build process in the specified directory."""
    original_dir = os.getcwd()
    try:
        # Setup build environment first
        setup_build_environment()
        
        os.chdir(build_dir)
        
        # Set version environment variable
        os.environ['PACKAGE_VERSION'] = args.version
        
        if COMPILE_CYTHON:
            print("\nCompiling extensions...")
            os.environ['CYTHON_PARALLEL'] = '0'
            
            # Build for current platform first
            print(f"\nBuilding wheel for {platform.system()}...")
            subprocess.run(['python', 'compile_setup.py', 'build_ext', '--inplace'], check=True)
            
            # Run cleanup and ensure we see the output
            print("\nRunning cleanup...")
            subprocess.run(['python', 'cleanup.py'], check=True)
            
            print("\nVerifying cleanup results...")
            issues_found = False
            for root, _, files in os.walk('.'):
                # Get all compiled files in current directory
                compiled_files = set(
                    os.path.splitext(f)[0] for f in files 
                    if f.endswith(('.pyd', '.so', '.dylib'))
                )
                
                # Check all .py files in this directory
                for file in files:
                    if file.endswith('.py'):
                        filepath = os.path.join(root, file)
                        basename = os.path.splitext(file)[0]
                        relative_path = os.path.join(root, file)[2:].replace('\\', '/')  # Remove ./ and normalize slashes
                        
                        # If there's a compiled version and it's not an exempted file
                        if (basename in compiled_files and 
                            file != '__init__.py' and 
                            relative_path not in PUBLIC_API_FILES):
                            print(f"WARNING: Found .py file with compiled version: {filepath}")
                            try:
                                os.remove(filepath)
                                print(f"         Removed {filepath}")
                            except Exception as e:
                                print(f"         Failed to remove {filepath}: {e}")
                            issues_found = True
            
            if issues_found:
                print("\nWARNING: Some .py files were found and removed.")
            else:
                print("\nCleanup verification passed: All expected .py files were removed correctly.")
            
            print("\nBuilding wheel...")
            subprocess.run(['python', 'setup.py', 'bdist_wheel'], check=True)
            
        else:
            print("\nSkipping Cython compilation...")
            print("\nBuilding platform-independent wheel...")
            subprocess.run(['python', 'setup.py', 'bdist_wheel'], check=True)
        
        # Move the wheel files to build/dist directory
        wheel_dir = os.path.join('dist')
        if os.path.exists(wheel_dir):
            for file in os.listdir(wheel_dir):
                if file.endswith('.whl'):
                    dest_path = os.path.join('..', file)
                    print(f"\nMoving wheel file to: {dest_path}")
                    shutil.move(
                        os.path.join(wheel_dir, file),
                        dest_path
                    )
    
    finally:
        # Always return to original directory
        os.chdir(original_dir)

def main():
    parser = argparse.ArgumentParser(description='Build the Genesis Bots package')
    parser.add_argument('--version', required=True, help='Version number for the build')
    args = parser.parse_args()

    print("Starting build process...")
    print(f"Cython compilation {'enabled' if COMPILE_CYTHON else 'disabled'}")
    build_dir = create_build_directory(args.version)
    build_package(build_dir, args)
    
    print("\nBuild process complete!")
    print(f"Build directory: {build_dir}")
    print("Wheel file has been moved to build/dist directory")

if __name__ == '__main__':
    freeze_support()
    main() 