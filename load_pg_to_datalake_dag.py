import os

from datetime import datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from common.pg_load_to_bronze import load_to_bronze
from common.pg_load_to_silver import load_to_silver

dag = DAG(
    dag_id="data_pg_load_to_datalake",
    description="Load data from PostgreSQL to Bronze",
    start_date=datetime(2021, 11, 14, 14, 30, 25),
    end_date=datetime(2022, 11, 19, 14, 30, 25),
    schedule_interval='@daily'
)

dummy_start = DummyOperator(task_id='start_dag', dag=dag)
dummy_end = DummyOperator(task_id='end_dag', dag=dag)

tables_to_bronze = ['aisles','clients','departments','location_areas','products','store_types','stores','orders']

table_to_bronze_tasks = []
for table in tables_to_bronze:
    table_to_bronze_tasks.append(
        PythonOperator(
            task_id=f"{table}_load_to_bronze",
            dag=dag,
            python_callable=load_to_bronze,
            provide_context=True,
            op_kwargs={"table": table})
    ) 
    
load_to_silver = PythonOperator(    
            task_id=f"load_to_silver",
            dag=dag,
            python_callable=load_to_silver,
            provide_context=True)

    
dummy_start >> table_to_bronze_tasks >> load_to_silver >> dummy_end