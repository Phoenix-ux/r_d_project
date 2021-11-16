import os
import logging

from datetime import datetime, date
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

def load_stock_to_silver(**kwargs):    
    ds = kwargs.get('ds', str(date.today()))
    y_date = datetime.strptime(ds, '%Y-%m-%d').strftime('%Y')
    m_date = datetime.strptime(ds, '%Y-%m-%d').strftime('%Y%m')
    d_date = datetime.strptime(ds, '%Y-%m-%d').strftime('%Y%m%d')
    
    bronze_path = os.path.join('/','datalake','bronze','api_data','out_of_stock',ds)
    silver_path = os.path.join('/','datalake','silver','api_data','out_of_stock')
    
    spark = SparkSession.builder\
            .master('local')\
            .appName("Load_api_to_Silver")\
            .getOrCreate()
    
    logging.info(f"Loading 'out_of_stock' data for {ds} to Silver")
    
    product_stock_df = spark.read\
        .option('header', True)\
        .option('inferSchema', True)\
        .json(os.path.join(bronze_path, 'product_stock.json'))
    
    product_stock_df = product_stock_df.dropDuplicates()
    
    product_stock_df.write.parquet(
        os.path.join(silver_path, y_date, m_date, d_date),
        mode='overwrite'    
    )
    logging.info(f"Loading 'out_of_stock' data to Silver completed.")