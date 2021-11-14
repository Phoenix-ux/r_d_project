import os
import requests
import json

from datetime import datetime, date
from requests.exceptions import HTTPError
from hdfs import InsecureClient

from common.config import Config

config = Config('/home/user/airflow/dags/common/hw_config.yaml')
api_con = config.get_config('stock_app')
hdfs_con = config.get_config('hdfs_creds')

def read_from_api(load_date):
    l_date = datetime.strptime(load_date, '%Y-%m-%d').date()
    y_date = datetime.strptime(load_date, '%Y-%m-%d').strftime('%Y')
    m_date = datetime.strptime(load_date, '%Y-%m-%d').strftime('%Y%m')
    d_date = datetime.strptime(load_date, '%Y-%m-%d').strftime('%Y%m%d')
    
    hdfs_dir = f"/bronze/api_data{api_con['data_endpoint']}/{y_date}/{m_date}/{d_date}"
    client = InsecureClient(hdfs_con['host'], user=hdfs_con['user'])
    
    client.makedirs(hdfs_dir)
    
    url = api_con['url']+api_con['auth_endpoint']
    headers = {"content-type": "application/json"}
    data = {"username": api_con['username'], "password": api_con['password']}
    r = requests.post(url, headers=headers, data=json.dumps(data), timeout=10)
    token = r.json()['access_token']
    
    url = api_con['url']+api_con['data_endpoint']
    headers = {"content-type": "application/json", "Authorization": "JWT " + token}
    data = {"date": str(l_date)}
    r = requests.get(url, headers=headers, data=json.dumps(data), timeout=10)
    r.raise_for_status()
    data = r.json()
    
    with client.write(f'{hdfs_dir}/product_stock.json', encoding='utf-8') as json_file:
        json.dump(data, json_file)