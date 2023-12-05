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

delete_stage="delete from subscription_events"
cursor.execute(delete_stage)

cnx.commit()
cursor.close()
cnx.close()