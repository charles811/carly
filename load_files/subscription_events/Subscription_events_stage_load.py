import mysql.connector
import json

with open('/opt/airflow/directory/sideprojects/mysql_config.json') as config_file:
    data = json.load(config_file)

user=data['user']
pwd=data['password']
host=data['host']
charles_db=data['charles_db']
stage_db=data['stage_db']

cnx = mysql.connector.connect(user=user, password=pwd,
                              host=host,
                              database=stage_db)   

# JSON file path
json_file_path = '/opt/airflow/directory/sideprojects/carly/input/sbscription_events.json'
cursor = cnx.cursor()


# Read JSON data from the file
with open(json_file_path, 'r') as json_file:
    data = json.load(json_file)

# Insert JSON data into the MySQL table
insert_query = """
INSERT INTO subscription_events (event_type, order_id, revenue, timestamp, customer_id)
VALUES (%s, %s, %s, %s, %s)
"""
for item in data:
    cursor.execute(insert_query, (
        item.get('event_type', None),
        item.get('order_id', None),
        item.get('revenue', None),
        item.get('timestamp', None),
        item.get('customer_id', None),
    ))

# Commit the changes and close the connection
cnx.commit()
cursor.close()
cnx.close()
