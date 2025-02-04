from sqlalchemy import create_engine, text
import urllib.parse
import warnings
from sqlalchemy import exc as sa_exc
import boto3
from botocore.exceptions import ClientError
import os

# Only suppress specific transaction-related warnings
warnings.filterwarnings('ignore', 
                       message='.*BEGIN.*', 
                       category=sa_exc.SAWarning)
warnings.filterwarnings('ignore', 
                       message='.*ROLLBACK.*', 
                       category=sa_exc.SAWarning)

def get_required_env(name):
    """Get required environment variable or raise error"""
    value = os.getenv(name)
    if not value:
        raise ValueError(f"Missing required environment variable: {name}")
    return value

def get_aws_identity():
    """Get current AWS user identity and region"""
    session = boto3.session.Session()
    region = os.getenv('AWS_REGION') or session.region_name
    if not region:
        raise ValueError("AWS region not set. Please set AWS_REGION environment variable")
    
    sts = boto3.client('sts', region_name=region)
    caller_identity = sts.get_caller_identity()
    return caller_identity['Account'], region

def get_redshift_credentials(cluster_identifier, database, username='admin', region=None):
    """Get credentials for either Redshift Serverless or Provisioned"""
    if region is None:
        _, region = get_aws_identity()
    
    # Try serverless first
    try:
        client = boto3.client('redshift-serverless', region_name=region)
        credentials = client.get_credentials(
            workgroupName=cluster_identifier,
            dbName=database,
            durationSeconds=3600
        )
        return credentials['dbUser'], credentials['dbPassword']
    except ClientError as e:
        if 'WorkgroupNotFound' in str(e):
            # Try provisioned Redshift
            client = boto3.client('redshift', region_name=region)
            credentials = client.get_cluster_credentials(
                DbUser=username,
                DbName=database,
                ClusterIdentifier=cluster_identifier,
                DurationSeconds=3600
            )
            return credentials['DbUser'], credentials['DbPassword']
        raise

def get_redshift_endpoint(cluster_identifier, region=None):
    """Get the endpoint for either Redshift Serverless or Provisioned"""
    if region is None:
        account_id, region = get_aws_identity()
    
    # Try serverless first
    try:
        client = boto3.client('redshift-serverless', region_name=region)
        response = client.get_workgroup(
            workgroupName=cluster_identifier
        )
        return response['workgroup']['endpoint']['address']
    except ClientError as e:
        if 'WorkgroupNotFound' in str(e):
            # Try provisioned Redshift
            client = boto3.client('redshift', region_name=region)
            response = client.describe_clusters(
                ClusterIdentifier=cluster_identifier
            )
            return response['Clusters'][0]['Endpoint']['Address']
        raise

def get_aws_credentials(workgroup, database, username=None):
    """Get temporary AWS credentials for Redshift"""
    try:
        session = boto3.session.Session()
        region = os.getenv('AWS_REGION') or session.region_name
        if not region:
            raise ValueError("AWS region not set. Please set AWS_REGION environment variable")
        
        client = boto3.client('redshift-serverless', region_name=region)
        credentials = client.get_credentials(
            workgroupName=workgroup,
            dbName=database,
            durationSeconds=3600
        )
        return credentials['dbUser'], credentials['dbPassword']
    except Exception as e:
        print(f"Warning: Could not get AWS credentials: {e}")
        return None, None

def get_connection_string(conn_string=None):
    """
    Process a connection string, supporting both direct auth and IAM
    
    Example strings:
    Direct: "postgresql+psycopg2://username:password@host:5439/dev"
    IAM: "postgresql+psycopg2://iam@host:5439/dev"
    """
    if not conn_string:
        conn_string = get_required_env('REDSHIFT_CONNECTION')
    
    # Parse the connection string
    if '://' not in conn_string:
        conn_string = f"postgresql+psycopg2://{conn_string}"
    
    # Extract components
    prefix, rest = conn_string.split('://')
    if '@' in rest:
        auth, host_part = rest.split('@')
    else:
        raise ValueError("Connection string must include authentication part (user:password@ or iam@)")
    
    # Extract host and database
    if '/' not in host_part:
        raise ValueError("Connection string must include database name")
    host_port, database = host_part.split('/', 1)
    if '?' in database:
        database = database.split('?')[0]
    
    # Check if using IAM
    if auth.lower() == 'iam':
        # Get workgroup from host
        workgroup = host_port.split('.')[0]
        username, password = get_aws_credentials(workgroup, database)
        if not (username and password):
            raise ValueError("Could not get AWS credentials")
        auth = f"{urllib.parse.quote_plus(username)}:{urllib.parse.quote_plus(password)}"
    
    # Construct full connection string
    return (
        f"{prefix}://"
        f"{auth}@{host_port}/{database}"
        "?sslmode=verify-full"
        "&sslrootcert=system"
    )

if __name__ == "__main__":
    # Example connection strings
    direct_auth = "postgresql+psycopg2://username:password@default-workgroup.730335510242.us-east-2.redshift-serverless.amazonaws.com:5439/dev"
    iam_auth = "postgresql+psycopg2://iam@default-workgroup.730335510242.us-east-2.redshift-serverless.amazonaws.com:5439/dev"

    # Step 1: Get the full connection string
    print("\nStep 1: Getting connection string...")
    conn_string = get_connection_string(direct_auth)  # or direct_auth
    print("Connection string obtained\n")

    # Step 2: Use the connection string
    print("Step 2: Testing connection...")
    engine = create_engine(
        conn_string,
        isolation_level='AUTOCOMMIT'
    )

    with engine.connect() as conn:
        result = conn.execute(text("SELECT CURRENT_DATE"))
        print(f"Test query result: {result.fetchone()[0]}")