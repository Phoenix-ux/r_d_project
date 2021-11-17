import os
import logging

from datetime import date
from airflow.hooks.base_hook import BaseHook
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

def load_to_gold(table, **kwargs):
    spark = SparkSession.builder\
            .config('spark.driver.extraClassPath'
                    , '/home/user/shared_folder/postgresql-42.3.1.jar')\
            .master('local')\
            .appName("Load_to_Gold")\
            .getOrCreate()
    
    gp_conn = BaseHook.get_connection('greenplum_olap')
    gp_url = f"jdbc:postgresql://{gp_conn.host}:{gp_conn.port}/{gp_conn.schema}"
    gp_creds = {"user": gp_conn.login, "password": gp_conn.password}
    
    
    silver_path = os.path.join('/','datalake','silver','dshop')
    
    logging.info(f"Loading table '{table}' to Gold")
    
    table_df = spark.read.parquet(
        os.path.join(silver_path, table)    
    )
    
    table_df.write.jdbc(gp_url, table=table, properties=gp_creds, mode='overwrite')
    
    logging.info(f"Loading table '{table}' to Gold completed")
    
def datamart_to_gold(**kwargs):
    spark = SparkSession.builder\
            .config('spark.driver.extraClassPath'
                    , '/home/user/shared_folder/postgresql-42.3.1.jar')\
            .master('local')\
            .appName("Vitrin_to_Gold")\
            .getOrCreate()
    
    gp_conn = BaseHook.get_connection('greenplum_olap')
    gp_url = f"jdbc:postgresql://{gp_conn.host}:{gp_conn.port}/{gp_conn.schema}"
    gp_creds = {"user": gp_conn.login, "password": gp_conn.password}
    
    
    silver_path = os.path.join('/','datalake','silver','dshop')
    
    logging.info(f"Loading Aggregated Datamart to Gold.")
    
    orders_df = spark.read.parquet(os.path.join(silver_path, 'orders'))
    products_df = spark.read.parquet(os.path.join(silver_path, 'products'))
    aisles_df = spark.read.parquet(os.path.join(silver_path, 'aisles'))
    store_types_df = spark.read.parquet(os.path.join(silver_path, 'store_types'))
    stores_df = spark.read.parquet(os.path.join(silver_path, 'stores'))
    departments_df = spark.read.parquet(os.path.join(silver_path, 'departments'))
    clients_df = spark.read.parquet(os.path.join(silver_path, 'clients'))
    
    datamart_df = orders_df.join(
    products_df
    , orders_df.product_id == products_df.product_id
    , 'left')\
    .join(
    clients_df
    , orders_df.client_id == clients_df.id
    , 'left')\
    .join(
    aisles_df
    , products_df.aisle_id == aisles_df.aisle_id
    , 'left')\
    .join(
    departments_df
    , products_df.department_id == departments_df.department_id
    , 'left')\
    .join(
    stores_df
    , orders_df.store_id == stores_df.store_id
    , 'left')\
    .join(
    store_types_df
    , stores_df.store_type_id == store_types_df.store_type_id
    , 'left')\
    .select(
    store_types_df.type
    , departments_df.department
    , aisles_df.aisle
    , products_df.product_name
    , clients_df.fullname
    , orders_df.quantity)\
    .groupby(store_types_df.type
             , departments_df.department
             , aisles_df.aisle
             , products_df.product_name
             , clients_df.fullname)\
    .sum()\
    .sort(F.desc("sum(quantity)"))\
    .withColumnRenamed("sum(quantity)", "product_quantity")\
    .withColumnRenamed("type", "store_type")\
    .withColumnRenamed("fullname", "client_name")
    
    datamart_df.write.jdbc(gp_url, table="datamart_for_products", properties=gp_creds, mode='overwrite')
    
    logging.info(f"Loading Aggregated Datamart to Gold completed")