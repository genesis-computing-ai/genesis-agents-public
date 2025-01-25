import os
import shutil

def copy_demos_to_documents():
    """Copy demo files to user's Documents directory."""
    try:
        # Get the package directory where demos are installed
        package_dir = os.path.dirname(os.path.dirname(__file__))
        demo_src = os.path.join(package_dir, 'apps', 'demos')
        
        # Use platform-appropriate user directory
        if os.name == 'nt':  # Windows
            base_dir = os.path.expanduser('~\\Documents')
        else:  # Unix-like systems
            base_dir = os.path.expanduser('~/Documents')
        
        demo_dest = os.path.join(base_dir, 'genesis_bots_demos')
        
        print(f"\n=== Installing Genesis Bots Demos ===")
        print(f"Source directory: {os.path.abspath(demo_src)}")
        print(f"Destination directory: {os.path.abspath(demo_dest)}")
        
        # Create destination directory if it doesn't exist
        os.makedirs(demo_dest, exist_ok=True)
        
        # Copy demo files
        if os.path.exists(demo_src):
            print(f"Copying demo files to {demo_dest}")
            for item in os.listdir(demo_src):
                src_path = os.path.join(demo_src, item)
                dst_path = os.path.join(demo_dest, item)
                print(f"Copying {item}")
                if os.path.isdir(src_path):
                    shutil.copytree(src_path, dst_path, dirs_exist_ok=True)
                else:
                    shutil.copy2(src_path, dst_path)
            print("Demo files installed successfully!")
        else:
            print(f"Warning: Demo source directory not found at {os.path.abspath(demo_src)}")
            print(f"Package directory contents: {os.listdir(package_dir)}")
            
    except Exception as e:
        print(f"Error during demo installation: {str(e)}")
        raise

if __name__ == '__main__':
    copy_demos_to_documents() 