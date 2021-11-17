import os

from datetime import datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from common.pg_load_to_bronze import load_to_bronze
from common.pg_load_to_silver import load_to_silver
from common.pg_load_to_gold import load_to_gold
from common.pg_load_to_gold import datamart_to_gold

dag = DAG(
    dag_id="data_pg_load_to_datalake",
    description="Load data from PostgreSQL to DataLake",
    start_date=datetime(2021, 11, 15, 23, 30, 25),
    end_date=datetime(2022, 11, 19, 14, 30, 25),
    schedule_interval='@daily'
)

dummy_start = DummyOperator(task_id='start_dag', dag=dag)

tables_to_load = ['aisles','clients','departments','location_areas','products','store_types','stores','orders']

table_to_bronze_tasks = []
for table in tables_to_load:
    table_to_bronze_tasks.append(
        PythonOperator(
            task_id=f"{table}_load_to_bronze",
            dag=dag,
            python_callable=load_to_bronze,
            provide_context=True,
            op_kwargs={"table": table})
    ) 

dummy1 = DummyOperator(task_id='end_of_bronze', dag=dag)
    
table_to_silver_tasks = []
for table in tables_to_load:
    table_to_silver_tasks.append(
        PythonOperator(
            task_id=f"{table}_load_to_silver",
            dag=dag,
            python_callable=load_to_silver,
            provide_context=True,
            op_kwargs={"table": table})
    )

dummy2 = DummyOperator(task_id='end_of_silver', dag=dag)
 
table_to_gold_tasks = []
for table in tables_to_load:
    table_to_gold_tasks.append(
        PythonOperator(
            task_id=f"{table}_load_to_gold",
            dag=dag,
            python_callable=load_to_gold,
            provide_context=True,
            op_kwargs={"table": table})
    )
    
load_datamart = PythonOperator(    
            task_id=f"load_datamart_to_gold",
            dag=dag,
            python_callable=datamart_to_gold,
            provide_context=True)    

    
dummy_start >> table_to_bronze_tasks >> dummy1 >> table_to_silver_tasks >> dummy2 >> table_to_gold_tasks >> load_datamart
