import os
import logging

from datetime import date
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

def load_to_silver(**kwargs):
    ds = kwargs.get('ds', str(date.today()))
    bronze_path = os.path.join('/','datalake','bronze','dshop')
    silver_path = os.path.join('/','datalake','silver','dshop')
    
    spark = SparkSession.builder\
            .master('local')\
            .appName("Load_to_Silver")\
            .getOrCreate()
    
    logging.info(f"Loading table 'aisles' to Silver.")
    aisles_df = spark.read\
        .option('header', True)\
        .option('inferSchema', True)\
        .csv(os.path.join(bronze_path, 'aisles', ds))
    
    aisles_df = aisles_df.dropDuplicates()

    aisles_df.write.parquet(
        os.path.join(silver_path, 'aisles'),
        mode='overwrite'    
    )
    logging.info(f"Loading table 'aisles' to Silver completed.")
    
    
    logging.info(f"Loading table 'clients' to Silver.")
    clients_df = spark.read\
        .option('header', True)\
        .option('inferSchema', True)\
        .csv(os.path.join(bronze_path, 'clients', ds))
    
    clients_df = clients_df.dropDuplicates()

    clients_df.write.parquet(
        os.path.join(silver_path, 'clients'),
        mode='overwrite'    
    )
    logging.info(f"Loading table 'clients' to Silver completed.")
    
    
    logging.info(f"Loading table 'departments' to Silver.")
    departments_df = spark.read\
        .option('header', True)\
        .option('inferSchema', True)\
        .csv(os.path.join(bronze_path, 'departments', ds))
    
    departments_df = departments_df.dropDuplicates()

    departments_df.write.parquet(
        os.path.join(silver_path, 'departments'),
        mode='overwrite'    
    )
    logging.info(f"Loading table 'departments' to Silver completed.")
    
    
    logging.info(f"Loading table 'location_areas' to Silver.")
    location_areas_df = spark.read\
        .option('header', True)\
        .option('inferSchema', True)\
        .csv(os.path.join(bronze_path, 'location_areas', ds))
    
    location_areas_df = location_areas_df.dropDuplicates()

    location_areas_df.write.parquet(
        os.path.join(silver_path, 'location_areas'),
        mode='overwrite'    
    )
    logging.info(f"Loading table 'location_areas' to Silver completed.")
    
    
    logging.info(f"Loading table 'products' to Silver.")
    products_df = spark.read\
        .option('header', True)\
        .option('inferSchema', True)\
        .csv(os.path.join(bronze_path, 'products', ds))
    
    products_df = products_df.dropDuplicates()

    products_df.write.parquet(
        os.path.join(silver_path, 'products'),
        mode='overwrite'    
    )
    logging.info(f"Loading table 'products' to Silver completed.")
    
    
    logging.info(f"Loading table 'store_types' to Silver.")
    store_types_df = spark.read\
        .option('header', True)\
        .option('inferSchema', True)\
        .csv(os.path.join(bronze_path, 'store_types', ds))
    
    store_types_df = store_types_df.dropDuplicates()

    store_types_df.write.parquet(
        os.path.join(silver_path, 'store_types'),
        mode='overwrite'    
    )
    logging.info(f"Loading table 'store_types' to Silver completed.")
    
    
    logging.info(f"Loading table 'stores' to Silver.")
    stores_df = spark.read\
        .option('header', True)\
        .option('inferSchema', True)\
        .csv(os.path.join(bronze_path, 'stores', ds))
    
    stores_df = stores_df.dropDuplicates()

    stores_df.write.parquet(
        os.path.join(silver_path, 'stores'),
        mode='overwrite'    
    )
    logging.info(f"Loading table 'stores' to Silver completed.")