# Import the patch first to fix the deprecated package
from .deprecated_patch import apply_patch
apply_patch()

# Now import the connector
from .snowflake_connector import SnowflakeConnector