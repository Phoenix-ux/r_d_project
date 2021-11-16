import os
import logging

from datetime import date
from airflow.hooks.base_hook import BaseHook
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

def load_to_bronze(table, **kwargs):
    logging.info(f"Loading table '{table}' to Bronze")

    ds = kwargs.get('ds', str(date.today()))
    
    pg_conn = BaseHook.get_connection('postgres_conn')
    pg_url = f"jdbc:postgresql://{pg_conn.host}:{pg_conn.port}/{pg_conn.schema}"
    pg_creds = {"user": pg_conn.login, "password": pg_conn.password}
    
    spark = SparkSession.builder\
            .config('spark.driver.extraClassPath'
                    , '/home/user/shared_folder/postgresql-42.3.1.jar')\
            .master('local')\
            .appName("Load_to_Bronze")\
            .getOrCreate()
    
    table_df = spark.read.jdbc(pg_url, table=table, properties=pg_creds)
    
    table_df.write.option("header", True).csv(
        os.path.join('/','datalake','bronze','dshop',table,ds),
        mode='overwrite'    
    )
    
    logging.info(f"{table_df.count()} rows loaded.")
    logging.info(f"Row example: {table_df.first()}")
    logging.info(f"Loading table '{table}' to Bronze complited.")