"""
Monkey patch for the deprecated package to fix compatibility issues with Snowflake connector.
This module should be imported before any other imports that use the deprecated package.
"""
import sys
from functools import wraps

def apply_patch():
    """Apply the patch to the deprecated package."""
    if 'deprecated' in sys.modules:
        # Get the original deprecated module
        deprecated_module = sys.modules['deprecated']
        
        # Save the original deprecated function
        original_deprecated = deprecated_module.deprecated
        
        # Create a patched version that handles the 'name' parameter
        @wraps(original_deprecated)
        def patched_deprecated(*args, **kwargs):
            # Remove the 'name' parameter if present
            if 'name' in kwargs:
                del kwargs['name']
            return original_deprecated(*args, **kwargs)
        
        # Replace the original with our patched version
        deprecated_module.deprecated = patched_deprecated
        
        return True
    return False

# Apply the patch immediately when this module is imported
patched = apply_patch()
