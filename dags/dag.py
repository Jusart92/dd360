"""DAG"""
# Ariflow
from airflow import DAG
from airflow.operators.python import PythonOperator
# Custom Operators
from scraper import ScraperOperator
from sql_worker import DatabaseOperator
# Utils
from datetime import datetime
import json
import os

def save_file(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(key='return_value', task_ids='scrapper')
    file_name = datetime.now().strftime('%Y%m%d_%H%M%S')
    with open(f'data/{file_name}.json', 'w') as file:
        json.dump(data, file)

def exec_script():
    python_file = os.path.join(os.getcwd(), 'dags/data_to_csv.py')

    exec(open(python_file).read())


with DAG(dag_id='scraper',
        description='Scrapper dag',
        schedule_interval='@hourly',
        # default_args = {'depends_on_past': True,},
        start_date=datetime(2023,6,17)) as dag:
    
    t1 = ScraperOperator(task_id="scrapper")
    t2 = PythonOperator(
        task_id='save_file',
        python_callable=save_file
    )
    t3 = DatabaseOperator(task_id='db')
    t4 = PythonOperator(
        task_id='exec_script',
        python_callable=exec_script
    )
    
    t1 >> [t2,t3] >> t4
    
