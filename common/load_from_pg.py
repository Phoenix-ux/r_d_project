import os
import psycopg2

from common.config import Config
from hdfs import InsecureClient
from datetime import datetime, date

config = Config('/home/user/airflow/dags/common/hw_config.yaml')
pg_con = config.get_config('pg_creds')
hdfs_con = config.get_config('hdfs_creds')

pg_creds = {
    'host': pg_con['host']
    , 'port': pg_con['port']
    , 'database': pg_con['database']
    , 'user': pg_con['user']
    , 'password': pg_con['password']
}

def read_from_db(table, load_date):
    l_date = datetime.strptime(load_date, '%Y-%m-%d').date()
    y_date = datetime.strptime(load_date, '%Y-%m-%d').strftime('%Y')
    m_date = datetime.strptime(load_date, '%Y-%m-%d').strftime('%Y%m')
    d_date = datetime.strptime(load_date, '%Y-%m-%d').strftime('%Y%m%d')
    
    hdfs_dir = f"/bronze/pg_data/{pg_con['database']}/{y_date}/{m_date}/{d_date}"
    client = InsecureClient(hdfs_con['host'], user=hdfs_con['user'])
    
    client.makedirs(hdfs_dir)

    with psycopg2.connect(**pg_creds) as pg_connection:
        cursor = pg_connection.cursor()
        with client.write(f'{hdfs_dir}/{table}.csv',) as csv_file:
            cursor.copy_expert(f'COPY public.{table} TO STDOUT WITH HEADER CSV', csv_file)