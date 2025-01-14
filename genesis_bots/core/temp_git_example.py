import os
import yaml
from git import Repo
from datetime import datetime
from difflib import unified_diff

class MappingTracker:
    def __init__(self):
        self.mapping_file = "mappings/column_mapping.yml"
        self.repo_path = "."
        
        # Initialize repository if it doesn't exist
        if not os.path.exists(".git"):
            self.repo = Repo.init(self.repo_path)
        else:
            self.repo = Repo(self.repo_path)
            
        # Create initial mapping file if it doesn't exist
        if not os.path.exists(self.mapping_file):
            os.makedirs("mappings", exist_ok=True)
            self.create_initial_mapping()
            self.commit_changes("Initial empty mapping")

    def create_initial_mapping(self):
        initial_mapping = {
            "version": 2,
            "mappings": []
        }
        self.save_mapping(initial_mapping)

    def save_mapping(self, mapping_data):
        with open(self.mapping_file, 'w') as f:
            yaml.dump(mapping_data, f, sort_keys=False)

    def load_mapping(self):
        with open(self.mapping_file, 'r') as f:
            return yaml.safe_load(f)

    def generate_diff(self, old_content, new_content):
        old_lines = old_content.splitlines(keepends=True)
        new_lines = new_content.splitlines(keepends=True)
        
        diff = ''.join(unified_diff(
            old_lines, new_lines,
            fromfile='old',
            tofile='new',
            n=3
        ))
        return diff

    def add_column_mapping(self, source_table, source_column, target_column, description):
        # Load current mapping
        current_mapping = self.load_mapping()
        current_content = yaml.dump(current_mapping, sort_keys=False)
        
        # Add new mapping
        new_mapping = {
            "source_table": source_table,
            "source_column": source_column,
            "target_column": target_column,
            "description": description
        }
        current_mapping["mappings"].append(new_mapping)
        
        # Save new mapping
        self.save_mapping(current_mapping)
        new_content = yaml.dump(current_mapping, sort_keys=False)
        
        # Generate and print diff
        diff = self.generate_diff(current_content, new_content)
        print(f"\nDiff for adding {source_column} -> {target_column}:")
        print(diff)
        
        # Commit changes
        self.commit_changes(f"Add mapping: {source_column} -> {target_column}")

    def commit_changes(self, message):
        self.repo.index.add([self.mapping_file])
        self.repo.index.commit(message)

def main():
    tracker = MappingTracker()
    
    # Add several mappings
    mappings = [
        ("source_table1", "customer_id", "customer_key", "Primary customer identifier"),
        ("source_table1", "first_name", "customer_first_name", "Customer's first name"),
        ("source_table1", "last_name", "customer_last_name", "Customer's last name"),
        ("source_table1", "email", "customer_email", "Customer's email address"),
        ("source_table1", "phone", "customer_phone", "Customer's phone number"),
        ("source_table1", "address", "customer_address", "Customer's physical address"),
    ]
    
    for source_table, source_col, target_col, desc in mappings:
        tracker.add_column_mapping(source_table, source_col, target_col, desc)
        print(f"\nAdded mapping: {source_col} -> {target_col}")
        print("You can now check git log or use a Git GUI to see the changes")

if __name__ == "__main__":
    main() 