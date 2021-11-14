import os
import psycopg2

from datetime import datetime

from common.load_from_pg import read_from_db
from common.load_from_api import read_from_api

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator

dag = DAG(
    dag_id="pg_data_export_dag",
    description="Export from PostgreSQL data to csv files",
    start_date=datetime(2021, 11, 7, 14, 30, 25),
    end_date=datetime(2022, 10, 19, 14, 30, 25),
    schedule_interval='@daily'
)

tables = ['aisles', 'clients', 'departments', 'orders', 'products']
run_day = "{{ ds }}"

dummy_start = DummyOperator(task_id='start_dag', dag=dag)
dummy_end = DummyOperator(task_id='end_dag', dag=dag)



table_tasks = []
for table in tables:
    table_tasks.append(
        PythonOperator(
            task_id=f"load_data_{table}",
            dag=dag,
            python_callable=read_from_db,
            op_kwargs={"table": table,"load_date": run_day})
    )   
    
load_api = PythonOperator(
    task_id="load_api_data",
    dag=dag,
    python_callable=read_from_api,
    op_args={run_day}
)    
    
dummy_start >> table_tasks >> load_api >> dummy_end