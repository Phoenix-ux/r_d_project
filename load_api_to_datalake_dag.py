import os

from datetime import datetime

from common.api_load_to_bronze import read_from_api
from common.api_load_to_silver import load_stock_to_silver

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

dag = DAG(
    dag_id="data_api_load_to_datalake",
    description="Load data from API to datalake",
    start_date=datetime(2021, 11, 14, 17, 30, 25),
    end_date=datetime(2022, 11, 19, 14, 30, 25),
    schedule_interval='@daily'
)

    
load_api_to_bronze = PythonOperator(
    task_id="load_api_data_to_bronze",
    dag=dag,
    python_callable=read_from_api,
    provide_context=True
)

load_api_to_silver = PythonOperator(
    task_id="load_api_data_to_silver",
    dag=dag,
    python_callable=load_stock_to_silver,
    provide_context=True
)
    
load_api_to_bronze >> load_api_to_silver