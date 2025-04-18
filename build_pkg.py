import requests
import subprocess
import sys
import re
from packaging import version

def get_latest_version():
    """Get the latest version of genesis-bots from PyPI."""
    package_name = "genesis-bots"
    response = requests.get(f"https://pypi.org/pypi/{package_name}/json")
    if response.status_code == 200:
        return response.json()["info"]["version"]
    else:
        print(f"Error: Could not fetch package info. Status code: {response.status_code}")
        sys.exit(1)

def increment_patch(version_str):
    """Increment the patch number of a version string."""
    v = version.parse(version_str)
    # For a proper version object with major, minor, patch
    if hasattr(v, 'major') and hasattr(v, 'minor') and hasattr(v, 'micro'):
        return f"{v.major}.{v.minor}.{v.micro + 1}"
    # For a simple version string, split by dots
    else:
        parts = version_str.split('.')
        if len(parts) >= 3:
            parts[-1] = str(int(parts[-1]) + 1)
            return '.'.join(parts)
        else:
            # If version doesn't have three parts, append a patch number
            return f"{version_str}.1"

def update_pyproject_toml(new_version):
    """Update version in pyproject.toml."""
    try:
        with open('pyproject.toml', 'r') as f:
            content = f.read()
        
        # Replace version in pyproject.toml using a more general pattern
        updated_content = re.sub(
            r'version\s*=\s*["\'](.*?)["\']',
            f'version = "{new_version}"',
            content
        )
        
        if updated_content != content:
            with open('pyproject.toml', 'w') as f:
                f.write(updated_content)
            print(f"Updated version in pyproject.toml to {new_version}")
            return True
        else:
            print("Warning: No version pattern found in pyproject.toml")
            return False
    except FileNotFoundError:
        print("Warning: pyproject.toml not found")
        return False

def update_github_workflow(new_version, current_version):
    """Update version in GitHub workflow file."""
    workflow_path = '.github/workflows/compile_package.yml'
    try:
        with open(workflow_path, 'r') as f:
            content = f.read()
        
        # Replace version in GitHub workflow
        updated_content = re.sub(
            r'PACKAGE_VERSION:\s*["\']' + re.escape(current_version) + r'["\']',
            f"PACKAGE_VERSION: '{new_version}'",
            content
        )
        
        if updated_content != content:
            with open(workflow_path, 'w') as f:
                f.write(updated_content)
            print(f"Updated version in {workflow_path} to {new_version}")
            return True
        else:
            print(f"Warning: No version pattern found in {workflow_path}")
            return False
    except FileNotFoundError:
        print(f"Warning: {workflow_path} not found")
        return False

def run_build():
    """Run the build command."""
    result = subprocess.run(["python", "-m", "build", "--sdist"], capture_output=True, text=True)
    if result.returncode == 0:
        print("Build successful!")
        print(result.stdout)
    else:
        print("Build failed!")
        print(result.stderr)

if __name__ == "__main__":
    package_name = "genesis-bots"
    
    if len(sys.argv) > 1:
        # Use the version provided as command-line argument
        new_version = sys.argv[1]
        print(f"Using provided version: {new_version}")
    else:
        # Original behavior - get and increment current version
        current_version = get_latest_version()
        print(f"Current version of {package_name}: {current_version}")
        new_version = increment_patch(current_version)
        print(f"New version will be: {new_version}")
    
    # Update version in files
    pyproject_updated = update_pyproject_toml(new_version)
    
    if pyproject_updated:
        # Run build
        print("Running build command...")
        run_build()
    else:
        print("Failed to update version in any files. Build aborted.")