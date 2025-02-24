# BigQuery Connection Guide

This guide explains how to use `bigquery_test_auth.py` to connect to Google BigQuery using service account authentication.

## Authentication Setup

1. Go to Google Cloud Console (https://console.cloud.google.com)
2. Select/create your project
3. Navigate to "IAM & Admin" > "Service Accounts"
4. Click "Create Service Account"
5. Fill in details:
   - Name: (e.g., "genesis-bigquery-sa")
   - Description: Optional
6. Grant these roles:
   - BigQuery Data Viewer
   - BigQuery Job User
   - BigQuery User
7. Create and download JSON key file:
   - Go to "Keys" tab
   - "Add Key" > "Create new key"
   - Choose JSON format

The key file will contain your service account credentials including project_id, private_key, client_email, etc.

### Environment Variable Setup

Set `GOOGLE_APPLICATION_CREDENTIALS` to your key file path:

```bash
# Linux/MacOS
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/your/service-account-key.json"

# Windows PowerShell
$env:GOOGLE_APPLICATION_CREDENTIALS="C:\path\to\your\service-account-key.json"

# Windows CMD
set GOOGLE_APPLICATION_CREDENTIALS=C:\path\to\your\service-account-key.json
```

### Connection String Format
```
bigquery://<project-id>
```

## Testing

Run:
```bash
python bigquery_test_auth.py
```

The script will test the connection and run a sample query against Google Trends dataset.

## Troubleshooting

### Common Issues
- **Authentication Fails**: Check key file path and environment variable
- **Query Fails**: Verify project ID and permissions
- **"Could not automatically determine credentials"**: Environment variable not set
- **"Permission denied"**: Insufficient IAM roles
- **"Project not found"**: Invalid project ID or access

## Best Practices

1. Store service account keys securely
2. Use least privilege principle
3. Consider workload identity federation in production
4. Use parameterized queries
5. Configure appropriate timeouts and retries