from airflow.models.baseoperator import BaseOperator
from bs4 import BeautifulSoup
import requests
from datetime import datetime
import re

class ScraperOperator(BaseOperator):
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
    
    
    def execute(self, context):
        links = [
            'https://www.meteored.mx/ciudad-de-mexico/historico',
            'https://www.meteored.mx/monterrey/historico',
            'https://www.meteored.mx/merida/historico',
            'https://www.meteored.mx/wakanda/historico',
        ]
        runner = {}
        now = datetime.now()
        runner['date']: now.strftime('%Y/%m/%d')
        for link in links:
            data,status = self._scrape_ciudad(link)
            print(data.get('city',None))
            runner[link] = {'status_code':status, 'data':data}
        return runner
    
    
    def _scrape_ciudad(self,link):
        response = requests.get(link)
        data = dict()
        if response.status_code == 200:
            text = response.text
            
            regex = r"<title>Histórico del Clima en (.+?) - Meteored</title>"

            result_re = re.search(regex, text)
            if result_re:
                city_name = result_re.group(1)

            soup = BeautifulSoup(text, 'html.parser')
            dist = soup.find('span', id='dist_cant')
            updated = soup.find('span', id='fecha_act_dato')
            table = soup.find('table', id='tabla_actualizacion')
            rows = table.find_all('tr')
            bs_data = dict()
            for row in rows:
                columns = row.find_all('td')
                for i in range(0, len(columns), 2):
                    title = columns[i].text.strip()
                    value = columns[i+1].find('span').text.strip()
                    bs_data[title] = value

            # Crear un diccionario con los datos
            data = {
                'city': city_name,
                'dist': dist.text,
                'temperatura': bs_data['Temperatura actual'],
                'humedad': bs_data['Humedad Relativa'],
                'updated': updated.text
            }
            return data, 200

        else:
            status_code = response.status_code 
            return data,status_code