# Redshift Connection Guide

This guide explains how to use `redshift_test.py` to connect to Amazon Redshift using either direct authentication or IAM authentication.

## Prerequisites

* Python 3.6+
* Required packages: `sqlalchemy`, `psycopg2-binary`, `boto3`
* AWS credentials configured (for IAM authentication)

## Connection Methods

### 1. IAM Authentication

Uses AWS IAM credentials to generate temporary database credentials.

#### Required Environment Variables

##### AWS Configuration
```bash
export AWS_REGION="us-east-2"          # Your AWS region
export AWS_ACCESS_KEY_ID="YOUR_KEY"    # If not using AWS CLI config
export AWS_SECRET_ACCESS_KEY="YOUR_SECRET"  # If not using AWS CLI config
```

#### Connection String Format
```
postgresql+psycopg2://iam@<workgroup>.<account-id>.<region>.redshift-serverless.amazonaws.com:5439/<database>
```

#### Example

Set the connection string as an environment variable:
```bash
export REDSHIFT_CONNECTION="postgresql+psycopg2://iam@default-workgroup.730335510242.us-east-2.redshift-serverless.amazonaws.com:5439/dev"
```

Run the script:
```bash
python redshift_test.py
```

### 2. Direct Authentication

Uses username and password directly.

#### Required Environment Variables
None required.

#### Connection String Format
```
postgresql+psycopg2://<username>:<password>@<workgroup>.<account-id>.<region>.redshift-serverless.amazonaws.com:5439/<database>
```

#### Example

Set the connection string as an environment variable:
```bash
export REDSHIFT_CONNECTION="postgresql+psycopg2://your_username:your_password@default-workgroup.730335510242.us-east-2.redshift-serverless.amazonaws.com:5439/dev"
```

Run the script:
```bash
python redshift_test.py
```

## Connection String Components

* `postgresql+psycopg2://` - Required prefix
* `iam@` or `username:password@` - Authentication part
* `workgroup` - Your Redshift Serverless workgroup name
* `account-id` - Your AWS account ID
* `region` - AWS region (e.g., us-east-2)
* `:5439` - Port number (usually 5439)
* `/database` - Your database name

## Required AWS Permissions (for IAM auth)

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "redshift-serverless:GetCredentials",
                "redshift-serverless:GetWorkgroup"
            ],
            "Resource": "*"
        }
    ]
}
```

## Testing the Connection

The script will:
* Parse your connection string
* Get IAM credentials if using IAM authentication
* Establish a connection to Redshift
* Execute a test query (`SELECT CURRENT_DATE`)
* Print the result

## Troubleshooting

### IAM Authentication Fails
* Verify AWS credentials are configured
* Check AWS region is correct
* Ensure IAM permissions are correct

### Direct Authentication Fails
* Verify username and password
* Check if the database exists
* Confirm workgroup name is correct

### Connection Timeout
* Verify VPC/Security Group settings
* Check if port 5439 is open
* Confirm the workgroup endpoint is correct

## Example Usage in Code

```python
from redshift_test import get_connection_string
from sqlalchemy import create_engine

# IAM Authentication
conn_string = get_connection_string(
    "postgresql+psycopg2://iam@default-workgroup.730335510242.us-east-2.redshift-serverless.amazonaws.com:5439/dev"
)

# Direct Authentication
conn_string = get_connection_string(
    "postgresql+psycopg2://username:password@default-workgroup.730335510242.us-east-2.redshift-serverless.amazonaws.com:5439/dev"
)

engine = create_engine(conn_string, isolation_level='AUTOCOMMIT')
```