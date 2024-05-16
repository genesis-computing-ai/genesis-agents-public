import snowflake.connector
from PIL import Image
import io
import os
import sys
import base64

# simple script to upload image files to a temp table

conn = snowflake.connector.connect(
    user=os.getenv('SNOWFLAKE_USER_OVERRIDE',None),
    password=os.getenv('SNOWFLAKE_PASSWORD_OVERRIDE', None),
    account=os.getenv('SNOWFLAKE_ACCOUNT_OVERRIDE',None),
    database=os.getenv('SNOWFLAKE_DATABASE_OVERRIDE', None),
    schema=os.getenv('SNOWFLAKE_SCHEMA_OVERRIDE', 'PUBLIC'),
    warehouse=os.getenv('SNOWFLAKE_WAREHOUSE_OVERRIDE', None)
)



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
        cursor.execute("INSERT INTO GENESIS_TEST.PUBLIC.imagestest (image_name, bot_name, image_data, encoded_image_data, image_desc) VALUES (%s, %s, %s, %s, %s)", (image_name, bot_name, binary_data, encoded_data, image_desc))
        conn.commit()

        print("inserted")
        # Close cursor and connection
        cursor.close()
    except Exception as e:
        print("error insert: ",e)


insert_image(sys.argv[1], sys.argv[2], sys.argv[3], conn)

# insert_image('thedude.png', '/Users/mrainey/Pictures/thedude.png', 'jeff', conn)

conn.close()

