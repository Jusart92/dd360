import mysql.connector
from airflow.models.baseoperator import BaseOperator
from airflow.models import Connection
import json

class DatabaseOperator(BaseOperator):
    def __init__(self,**kwargs):
        super().__init__(**kwargs)
        self.conn = None
        self.cursor = None
    
    def execute(self, context):
        ti = context['ti']
        items = ti.xcom_pull(key='return_value', task_ids='scrapper')
        for item in list(items.values()):
            status = item['status_code']
            data = item.get('data',None)
            link = next(iter(item))
            self.save_data_to_database(
                status=status,
                data=data,
                link=link)
        

    def _connect(self):
        conn = Connection.get_connection_from_secrets('mysql_conn')
        self.conn = mysql.connector.connect(
            host=conn.host,
            user=conn.login,
            password=conn.password,
            database=conn.schema
        )
        self.cursor = self.conn.cursor()

    def _disconnect(self):
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()

    def save_data_to_database(self, data, status, link):
        self._connect()
        if not data:
            self.insert_http_request(
                status=status,
                link=link)
            return
        
        city_id = self.get_city_id(data['city'])
        if not city_id:
            city_id = self.insert_city(data['city'])
        data['city_id'] = city_id[0]
        self.insert_http_request(status,link,data)
        self.insert_successful_requests(data)

        self._disconnect()
    
    def get_city_id(self, city_name):
        self.cursor.execute(
            f'SELECT city_id \
            FROM cities \
            WHERE city_name="{city_name}"'
            )
        id = self.cursor.fetchone()
        return id
        

    def insert_http_request(self,status,link,data={}):
        city_id = data.get('city_id',None)
        if not city_id:
            self.cursor.execute(
            f'INSERT INTO \
                http_request (http_code,url_request) \
                VALUES ("{status}","{link}")'
                            )
        else:
            self.cursor.execute(
                f'INSERT INTO \
                http_request (city_id,http_code,url_request) \
                VALUES ("{city_id}","{status}","{link}")'
                            )
        self.conn.commit()
        
    def insert_successful_requests(self,data):
        city_id = data.pop('city_id')
        print(data)
        print(f'INSERT INTO successful_requests \
                (city_id,dist,temperature,humidity,last_updated) \
                VALUES ("{city_id}" \
                    ,"{data["dist"]}" \
                    ,"{data["temperatura"]}" \
                    ,"{data["humedad"]}" \
                    ,"{data["updated"]}")')
        self.cursor.execute(
            f'INSERT INTO successful_requests \
                (city_id,dist,temperature,humidity,last_updated) \
                VALUES ("{city_id}" \
                    ,"{data["dist"]}" \
                    ,"{data["temperatura"]}" \
                    ,"{data["humedad"]}" \
                    ,"{data["updated"]}")'   
        )
        self.conn.commit()
        
    def insert_city(self, name):
        self.cursor.execute(
            f'INSERT INTO cities (city_name) \
                VALUES ("{name}")'
        )
        self.conn.commit()
        return self.cursor.lastrowid,