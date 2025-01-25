import os
import shutil

def install_resources():
    """Copy demo files, SDK examples, and config to current directory."""
    try:
        # Get the current working directory
        current_dir = os.getcwd()
        
        print(f"\n=== Installing Genesis Bots Files ===")
        print(f"Current directory: {current_dir}")
        
        # Setup paths for demos and sdk examples
        demo_src = os.path.join(current_dir, 'apps', 'demos')
        sdk_src = os.path.join(current_dir, 'apps', 'sdk_examples')
        os.makedirs(demo_src, exist_ok=True)
        os.makedirs(sdk_src, exist_ok=True)
        
        # Setup paths for config
        config_dest = os.path.join(current_dir, 'genesis_bots', 'default_config')
        os.makedirs(config_dest, exist_ok=True)
        
        # Get package directory where source files are installed
        package_dir = os.path.dirname(os.path.dirname(__file__))
        package_demos = os.path.join(package_dir, 'apps', 'demos')
        package_sdk = os.path.join(package_dir, 'apps', 'sdk_examples')
        package_config_dir = os.path.join(package_dir, 'genesis_bots', 'default_config')
        config_files = ['harvester_queries.conf', 'genesis_bots.config']
        
        # Copy demo files
        if os.path.exists(package_demos):
            print(f"\nCopying demo files to {demo_src}")
            for item in os.listdir(package_demos):
                src_path = os.path.join(package_demos, item)
                dst_path = os.path.join(demo_src, item)
                print(f"Copying {item}")
                if os.path.isdir(src_path):
                    shutil.copytree(src_path, dst_path, dirs_exist_ok=True)
                else:
                    shutil.copy2(src_path, dst_path)
        else:
            print(f"Warning: Demo source directory not found at {os.path.abspath(package_demos)}")
        
        # Copy SDK examples
        if os.path.exists(package_sdk):
            print(f"\nCopying SDK examples to {sdk_src}")
            for item in os.listdir(package_sdk):
                src_path = os.path.join(package_sdk, item)
                dst_path = os.path.join(sdk_src, item)
                print(f"Copying {item}")
                if os.path.isdir(src_path):
                    shutil.copytree(src_path, dst_path, dirs_exist_ok=True)
                else:
                    shutil.copy2(src_path, dst_path)
        else:
            print(f"Warning: SDK examples directory not found at {os.path.abspath(package_sdk)}")
        
        # Copy config files
        print(f"\nCopying config files to {config_dest}")
        for config_file in config_files:
            package_config = os.path.join(package_config_dir, config_file)
            if os.path.exists(package_config):
                shutil.copy2(package_config, os.path.join(config_dest, config_file))
                print(f"Config file {config_file} copied successfully!")
            else:
                print(f"Warning: Config file not found at {os.path.abspath(package_config)}")
            
        print("\nInstallation completed!")
            
    except Exception as e:
        print(f"Error during installation: {str(e)}")
        raise

if __name__ == '__main__':
    install_resources() 