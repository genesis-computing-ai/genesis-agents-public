import os
import shutil
import subprocess
from build_config import IGNORE_DIRS, IGNORE_FILES, VERSION

# Check environment variable for Cython compilation
COMPILE_CYTHON = os.getenv('COMPILE_CYTHON', 'false').lower() == 'true'

def create_build_directory():
    """Create a clean build directory with copied source files."""
    # Create dist directory if it doesn't exist
    dist_dir = os.path.join('build', 'dist')
    os.makedirs(dist_dir, exist_ok=True)
    
    # Create a new build directory name with version
    build_dir = os.path.join(dist_dir, f"genesis_bots_build_{VERSION}")
    
    # Remove the build directory if it exists
    if os.path.exists(build_dir):
        print(f"Removing existing build directory: {build_dir}")
        shutil.rmtree(build_dir)
    
    print(f"Creating new build directory: {build_dir}")
    os.makedirs(build_dir)
    
    def ignore_patterns(path, names):
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
    
    return build_dir

def build_package(build_dir):
    """Run the build process in the specified directory."""
    original_dir = os.getcwd()
    try:
        # Change to build directory
        os.chdir(build_dir)
        
        if COMPILE_CYTHON:
            print("\nCompiling extensions...")
            subprocess.run(['python', 'compile_setup.py', 'build_ext', '--inplace'], check=True)
        else:
            print("\nSkipping Cython compilation...")
        
        print("\nBuilding wheel...")
        subprocess.run(['python', 'setup.py', 'bdist_wheel'], check=True)
        
        if COMPILE_CYTHON:
            print("\nCleaning up compiled files...")
            subprocess.run(['python', 'cleanup.py'], check=True)
        else:
            print("\nSkipping cleanup of Python source files...")
        
        # Move the wheel file to build/dist directory
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
    print("Starting build process...")
    print(f"Cython compilation {'enabled' if COMPILE_CYTHON else 'disabled'}")
    build_dir = create_build_directory()
    build_package(build_dir)
    
    print("\nBuild process complete!")
    print(f"Build directory: {build_dir}")
    print("Wheel file has been moved to build/dist directory")

if __name__ == '__main__':
    main() 