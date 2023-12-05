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

cursor = cnx.cursor()

insert_stage_to_core = "INSERT INTO `CORE`.subscription_events (customer_id, event_type, order_id,revenue, timestamp) " \
                       "SELECT customer_id, event_type,order_id, CAST(revenue AS DECIMAL(4,2)) AS revenue, " \
                       "FROM_UNIXTIME(CAST(timestamp AS UNSIGNED)) AS timestamp " \
                       "FROM subscription_events"


cursor.execute(insert_stage_to_core)

cnx.commit()
cursor.close()
cnx.close()