import json
import mysql.connector
import pandas as pd

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
# CSV file path
customer_file_path = '/opt/airflow/directory/sideprojects/carly/input/hardware_sales.csv'
df = pd.read_csv(customer_file_path)

table_name='hardware_sales'
cursor = cnx.cursor()
for _, row in df.iterrows():
    insert_query = "INSERT INTO {} (email, order_id, revenue,timestamp) VALUES ('{}', '{}', '{}', '{}')".format(
        table_name, row['email'], row['order_id'], row['revenue'], row['timestamp'])
    cursor.execute(insert_query)


cnx.commit()
cursor.close()
cnx.close()
