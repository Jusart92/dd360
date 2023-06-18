import pandas as pd
import mysql.connector
import pyarrow as pa
import pyarrow.parquet as pq
from airflow.models import Connection

# Conexión a la base de datos MySQL
conn = Connection.get_connection_from_secrets('mysql_conn')
conn = mysql.connector.connect(
            host=conn.host,
            user=conn.login,
            password=conn.password,
            database=conn.schema
)


query = """
SELECT sr.date_request, sr.dist, sr.temperature, sr.humidity, c.city_name
FROM successful_requests sr
JOIN cities c on c.city_id = sr.city_id
"""

# Ejecutar consulta y obtener los resultados en un DataFrame de pandas
df = pd.read_sql(query, conn)

# Cerrar la conexión a la base de datos
conn.close()


df['date_request'] = pd.to_datetime(df['date_request'])
df['dist'] = df['dist'].str.extract(r'(\d+\.?\d*)')
df['humidity'] = df['humidity'].replace('.', '', regex=False).astype(float)


# Particionar el DataFrame por la columna "date_request"
particiones = df.groupby(pd.Grouper(key='date_request', freq='D'))

# Crear un DataFrame resumen por ciudad y fecha
resumen = pd.DataFrame(columns=['Fecha', 'Ciudad', 'Temperatura Máxima', 'Temperatura Mínima', 'Temperatura Promedio',
                                'Humedad Máxima', 'Humedad Mínima', 'Humedad Promedio', 'Última Actualización'])


# Obtener los resúmenes por partición
for fecha, particion in particiones:
    ciudad_temperatura_max = particion.groupby('city_name')['temperature'].max()
    ciudad_temperatura_min = particion.groupby('city_name')['temperature'].min()
    ciudad_temperatura_promedio = particion.groupby('city_name')['temperature'].mean()
    ciudad_humedad_max = particion.groupby('city_name')['humidity'].max()
    ciudad_humedad_min = particion.groupby('city_name')['humidity'].min()
    ciudad_humedad_promedio = particion.groupby('city_name')['humidity'].mean()
    ultima_actualizacion = particion['date_request'].max()
    resumen = resumen.append({
        'Fecha': fecha,
        'Ciudad': ciudad_temperatura_max.index.tolist(),
        'Temperatura Máxima': ciudad_temperatura_max.values.tolist(),
        'Temperatura Mínima': ciudad_temperatura_min.values.tolist(),
        'Temperatura Promedio': ciudad_temperatura_promedio.values.tolist(),
        'Humedad Máxima': ciudad_humedad_max.values.tolist(),
        'Humedad Mínima': ciudad_humedad_min.values.tolist(),
        'Humedad Promedio': ciudad_humedad_promedio.values.tolist(),
        'Última Actualización': ultima_actualizacion
    }, ignore_index=True)


# Guardar el DataFrame en un archivo Parquet 
table = pa.Table.from_pandas(df)
pq.write_to_dataset(
    table,
    root_path='data/parquets',
    partition_cols=['date_request']
)