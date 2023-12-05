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
# insert_stage_to_core="insert into `CORE`.hardware_sales  (email,order_id,revenue,timestamp)                           select email , order_id, cast (revenue as DECIMAL(8,2))  as revenue, FROM_UNIXTIME(CAST(timestamp as UNSIGNED )) as timestamp from hardware_sales"

insert_stage_to_core = "INSERT INTO `CORE`.hardware_sales (email, order_id, revenue, timestamp) " \
                       "SELECT email, order_id, CAST(revenue AS DECIMAL(4,2)) AS revenue, " \
                       "FROM_UNIXTIME(CAST(timestamp AS UNSIGNED)) AS timestamp " \
                       "FROM hardware_sales"

cursor.execute(insert_stage_to_core)


cnx.commit()
cursor.close()
cnx.close()