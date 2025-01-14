import os
import subprocess
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

def run_command(command):
    result = subprocess.run(command, shell=True, capture_output=True, text=True)
    return result

def main():
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = f"/Users/alexuslau/Genesis/tacotuesday_{timestamp}.txt"
    with open(output_file, "w") as file:  # Open the file in write mode
    
        GITHUB_USERNAME = os.getenv("GITHUB_USERNAME")
        GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")

        if not GITHUB_USERNAME or not GITHUB_TOKEN:
            print("Error: GITHUB_USERNAME or GITHUB_TOKEN environment variable is not set.")
            return

        repo_url = f"https://{GITHUB_USERNAME}:{GITHUB_TOKEN}@github.com/genesis-gh-jlangseth/genesis"
        repo_dir = "/Users/alexuslau/Genesis/genesis/"
        parent_dir = "/Users/alexuslau/Genesis/"
    
        if not os.path.exists(parent_dir):
            os.makedirs(parent_dir)
            print(f"Created directory: {parent_dir}")

        # Remove any existing directory
        if os.path.exists(repo_dir):
            print("Cleaning up existing repo...")
            run_command(f"rm -rf {repo_dir}")

        # Clone the repository
        print("Cloning the repository...")
        clone_result = run_command(f"git clone -b main {repo_url} {repo_dir}")

        # Check if the clone was successful
        if clone_result.returncode != 0:
            print(f"Error cloning repository: {clone_result.stderr}")
            return  # Exit if cloning fails

        # Write init file
        init_file_path = os.path.join(repo_dir, "__init__.py")
        if not os.path.exists(init_file_path):
            with open(init_file_path, "w") as init_file:
                init_file.write("# This file marks the directory as a Python package.\n")
            print(f"Created __init__.py file at {init_file_path}")

        # Change to the repo directory
        if os.path.exists(repo_dir):
            os.chdir(repo_dir)
            print("Changed directory to repo_dir.")
        else:
            print(f"Error: {repo_dir} was not created after cloning.")
            return

        # Run pylint
        print("Running pylint...")
        pylint_result = run_command("pylint --disable=W,R,C /Users/alexuslau/Genesis/genesis")
        file.write("Pylint output:")
        file.write(pylint_result.stdout)
        print(pylint_result.stdout or "No output from pylint.\n")
        if pylint_result.returncode != 0:
            print(f"Pylint reported issues:\n{pylint_result.stderr}\n")
        
        # Run safety
        print("Running safety...")
        safety_result = run_command("safety check")
        print("Safety output:")
        file.write("Safety output:\n")
        file.write(safety_result.stdout)
        if safety_result.returncode != 0:
            file.write(f"Safety reported issues:\n{safety_result.stderr}")

        # Run bandit
        print("Running bandit...")
        bandit_result = run_command("bandit -r .")
        print("Bandit output:")
        file.write("Bandit output:\n")
        file.write(bandit_result.stdout)
        if bandit_result.returncode != 0:
            file.write(f"Bandit reported issues:\n{bandit_result.stderr}")

if __name__ == "__main__":
    main()
