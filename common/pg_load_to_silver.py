import os
import logging

from datetime import date
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

def load_to_silver(table, **kwargs):
    ds = kwargs.get('ds', str(date.today()))
    bronze_path = os.path.join('/','datalake','bronze','dshop')
    silver_path = os.path.join('/','datalake','silver','dshop')
    
    spark = SparkSession.builder\
            .master('local')\
            .appName("Load_to_Silver")\
            .getOrCreate()
    
    logging.info(f"Loading table '{table}' to Silver")
    
    table_df = spark.read\
        .option('header', True)\
        .option('inferSchema', True)\
        .csv(os.path.join(bronze_path, table, ds))
    
    table_df = table_df.dropDuplicates()

    table_df.write.parquet(
        os.path.join(silver_path, table),
        mode='overwrite'    
    )
        
    logging.info(f"{table_df.count()} rows loaded.")
    logging.info(f"Loading table '{table}' to Silver completed.")