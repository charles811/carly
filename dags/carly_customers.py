from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator

from datetime import datetime
from random import randint

with DAG("Carly_customers",
    start_date=datetime(2023, 1 ,1), 
    schedule_interval='30 0 * * *', 
    catchup=False) as dag:

    start=DummyOperator(task_id='start')
    end=DummyOperator(task_id='end')

    task1 = BashOperator(
            task_id='Customer_stage_load',
            bash_command='python3 /opt/airflow/directory/sideprojects/carly/customers/Customers_stage_load.py'
        )            
    
    task2 = BashOperator(
            task_id='Customer_stage_core',
            bash_command='python3 /opt/airflow/directory/sideprojects/carly/customers/Customer_stage_core.py'
        )

    task3 = BashOperator(
            task_id='Customers_stage_delete',
            bash_command='python3 /opt/airflow/directory/sideprojects/carly/customers/Customers_stage_delete.py'
        )
    
    start >> task1 >> task2 >> task3 >> end

