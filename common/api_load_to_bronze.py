import os
import requests
import json
import logging

from datetime import datetime, date
from requests.exceptions import HTTPError
from airflow.hooks.base_hook import BaseHook
from hdfs import InsecureClient

from common.config import Config

config = Config('/home/user/airflow/dags/common/hw_config.yaml')
api_config = config.get_config('stock_app')
api_con = BaseHook.get_connection('http_r_d')
hdfs_con = BaseHook.get_connection('hdfs_r_d')
hdfs_url = f"http://{hdfs_con.host}:{hdfs_con.port}/"

def read_from_api(**kwargs):
    ds = kwargs.get('ds', str(date.today()))    
    
    hdfs_dir = f"/datalake/bronze/api_data{api_config['data_endpoint']}/{ds}"
    client = InsecureClient(hdfs_url, user=hdfs_con.login)
    
    logging.info(f"Loading API data from {api_config['data_endpoint']} to Bronze on {hdfs_dir} started.")
    client.makedirs(hdfs_dir)
    
    url = api_config['url']+api_config['auth_endpoint']
    headers = {"content-type": "application/json"}
    data = {"username": api_con.login, "password": api_con.password}
    r = requests.post(url, headers=headers, data=json.dumps(data), timeout=10)
    token = r.json()['access_token']
    
    url = api_config['url']+api_config['data_endpoint']
    headers = {"content-type": "application/json", "Authorization": "JWT " + token}
    data = {"date": str(ds)}
    r = requests.get(url, headers=headers, data=json.dumps(data), timeout=10)
    logging.info(r.raise_for_status())
    data = r.json()
    
    with client.write(f'{hdfs_dir}/product_stock.json', encoding='utf-8') as json_file:
        json.dump(data, json_file)
                 
    logging.info(f"Loading API data from {api_config['data_endpoint']} to Bronze completed.")