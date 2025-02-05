from sqlalchemy import create_engine
import os
import argparse

def get_bigquery_engine(connection_string):
    """
    Get SQLAlchemy engine for BigQuery using GOOGLE_APPLICATION_CREDENTIALS
    environment variable
    """
    try:
        # Create engine
        engine = create_engine(connection_string)
        return engine
    except Exception as e:
        raise Exception(f"Failed to create BigQuery engine: {e}")

if __name__ == "__main__":
    # Set up argument parser
    parser = argparse.ArgumentParser(description='Test BigQuery connection')
    parser.add_argument('--project', type=str, help='Google Cloud project ID')
    args = parser.parse_args()

    # Get project ID from args or prompt user
    project_id = args.project
    if not project_id:
        project_id = input("Please enter your Google Cloud project ID: ")

    # Set credentials path if not already set
    if not os.environ.get('GOOGLE_APPLICATION_CREDENTIALS'):
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'tmp/genesis-bigquery.json'
    
    credentials_path = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')
    
    # Check if credentials file exists
    if not os.path.exists(credentials_path):
        print("\nError: Google Cloud credentials file not found!")
        print(f"Expected location: {credentials_path}")
        print("\nTo fix this:")
        print("1. Download your service account key file from Google Cloud Console")
        print("2. Either:")
        print(f"   a) Place it at: {credentials_path}")
        print("   b) Set GOOGLE_APPLICATION_CREDENTIALS environment variable to your key file location")
        print("\nFor more details, see the README.md file.")
        exit(1)

    connection_string = f"bigquery://{project_id}"
    print(f"\nUsing connection string: {connection_string}")
    print(f"Using credentials from: {credentials_path}")

    print("\nStep 1: Creating BigQuery engine...")
    

    engine = create_engine(connection_string)
    
    print("Step 2: Testing connection with timestamp query...")
    timestamp_query = "SELECT CURRENT_TIMESTAMP() as current_time"
    
    try:
        with engine.connect() as connection:
            result = connection.execute(timestamp_query).first()
            print(f"Test query result: {result.current_time}")
            print("\nConnection successful!")
            
            print("\nStep 3: Querying Google Trends data...")
            trends_query = """
            -- This query shows a list of the daily top Google Search terms.
            SELECT
                refresh_date AS Day,
                term AS Top_Term,
                -- These search terms are in the top 25 in the US each day.
                rank
            FROM `bigquery-public-data.google_trends.top_terms`
            WHERE
                rank = 1
                -- Choose only the top term each day.
                AND refresh_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 2 WEEK)
                -- Filter to the last 2 weeks.
            GROUP BY Day, Top_Term, rank
            ORDER BY Day DESC;
                -- Show the days in reverse chronological order.
            """
            
            results = connection.execute(trends_query)
            print("\nTop Google Search Terms (Last 2 Weeks):")
            print("Date\t\tTop Term")
            print("-" * 50)
            for row in results:
                print(f"{row.Day}\t{row.Top_Term}")
                
    except Exception as e:
        print(f"Error running query: {e}")