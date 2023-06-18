import unittest
from airflow import DAG
from airflow.models import Connection
from airflow.operators.python import PythonOperator
from datetime import datetime

def test_database_connection():
    conn = Connection.get_connection_from_secrets('mysql_conn')
    conn.test_connection()

class TestWeatherScraper(unittest.TestCase):
    def setUp(self):
        self.dag = DAG(
            'test_weather_scraper',
            start_date=datetime(2023, 6, 16),
            schedule_interval='@once'
        )

    def test_database_connection_task(self):
        database_connection_task = PythonOperator(
            task_id='database_connection_task',
            python_callable=test_database_connection,
            dag=self.dag
        )
        database_connection_task.execute(context={})

if __name__ == '__main__':
    unittest.main()