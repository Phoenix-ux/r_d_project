import os
import logging

from datetime import datetime, date
from airflow.hooks.base_hook import BaseHook
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

def load_stock_to_gold(**kwargs):
    ds = kwargs.get('ds', str(date.today()))
    y_date = datetime.strptime(ds, '%Y-%m-%d').strftime('%Y')
    m_date = datetime.strptime(ds, '%Y-%m-%d').strftime('%Y%m')
    d_date = datetime.strptime(ds, '%Y-%m-%d').strftime('%Y%m%d')
    
    spark = SparkSession.builder\
            .config('spark.driver.extraClassPath'
                    , '/home/user/shared_folder/postgresql-42.3.1.jar')\
            .master('local')\
            .appName("Load_to_Gold")\
            .getOrCreate()
    
    gp_conn = BaseHook.get_connection('greenplum_olap')
    gp_url = f"jdbc:postgresql://{gp_conn.host}:{gp_conn.port}/{gp_conn.schema}"
    gp_creds = {"user": gp_conn.login, "password": gp_conn.password}
    
    
    silver_path = os.path.join('/','datalake','silver','api_data','out_of_stock')
    
    logging.info(f"Loading table 'Out_of_Stock' to Gold")
    
    table_df = spark.read.parquet(
        os.path.join(silver_path, y_date, m_date, d_date)    
    )
    
    table_df.write.jdbc(gp_url, 'out_of_stock', properties=gp_creds, mode='append')
    
    logging.info(f"Loading table 'Out_of_Stock' to Gold completed")