import snowflake.connector
from PIL import Image
import io
import os
import sys
import base64

# simple script to upload image files to a table

# TODO add region as env variable
conn = snowflake.connector.connect(
    user=os.getenv('SNOWFLAKE_USER_OVERRIDE',None),
    password=os.getenv('SNOWFLAKE_PASSWORD_OVERRIDE', None),
    account=os.getenv('SNOWFLAKE_ACCOUNT_OVERRIDE',None) + '.us-east-2.aws', # NOTE - hardcoded region
    database=os.getenv('SNOWFLAKE_DATABASE_OVERRIDE', None),
    schema=os.getenv('SNOWFLAKE_SCHEMA_OVERRIDE', 'PUBLIC'),
    warehouse=os.getenv('SNOWFLAKE_WAREHOUSE_OVERRIDE', None)
)


#TODO do we remove ID column?
def create_schema_and_table(conn):
    try:
        cursor = conn.cursor()
        cursor.execute("""
            CREATE SCHEMA IF NOT EXISTS GENESISAPP_MASTER.APP_SHARE;""")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS GENESISAPP_MASTER.APP_SHARE.IMAGES (
                id INTEGER AUTOINCREMENT,
                image_name STRING,
                bot_name STRING,
                image_data BINARY,
                encoded_image_data STRING,
                image_desc STRING
            );
        """)
        print("Schema and table created successfully")
        cursor.close()
    except Exception as e:
        print("Error creating schema and table: ", e)


#TODO modify to use merge statement
# Function to insert image into Snowflake
def insert_image(image_name, image_path, bot_name, conn):
    try:
        if not bot_name:
            image_desc = 'Genesis Logo ' + image_name
        else:
            image_desc = 'Genesis Bot ' + bot_name

        cursor = conn.cursor()
        with open(image_path, 'rb') as file:
            binary_data = file.read()
            encoded_data = base64.b64encode(binary_data).decode('utf-8')
        cursor.execute("INSERT INTO GENESISAPP_MASTER.APP_SHARE.IMAGES (image_name, bot_name, image_data, encoded_image_data, image_desc) VALUES (%s, %s, %s, %s, %s)", (image_name, bot_name, binary_data, encoded_data, image_desc))
        conn.commit()

        print(f"inserted {image_name}")
        # Close cursor and connection
        cursor.close()
    except Exception as e:
        print("error insert: ",e)


# insert_image(sys.argv[1], sys.argv[2], sys.argv[3], conn)

def insert_images_from_directory(directory_path, conn):
    for filename in os.listdir(directory_path):
        if filename.endswith(".png") or filename.endswith(".jpg"):
            bot_name = os.path.splitext(filename)[0]
            if bot_name in ['eve', 'eliza', 'stuart']:
                bot_name = bot_name.capitalize()
            elif bot_name == 'G-g':
                bot_name = 'Default'
            else:
                bot_name = ''
            image_path = os.path.join(directory_path, filename)
            insert_image(filename, image_path, bot_name, conn)
        else:
            continue

# create_temp_table(conn)
insert_images_from_directory(sys.argv[1], conn)
# create_schema_and_table(conn)

# Example usage:
# insert_images_from_directory('/Users/mrainey/Pictures', 'jeff', conn)

# insert_image('thedude.png', '/Users/mrainey/Pictures/thedude.png', 'jeff', conn)

conn.close()

