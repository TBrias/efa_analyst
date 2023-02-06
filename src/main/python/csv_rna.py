import csv
import logging
import os
import sys
from datetime import datetime

import findspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, count, sum, desc
from pyspark.sql.utils import AnalysisException
from pyspark.sql import functions as F
from datetime import datetime
import geopandas as gpd

start_time = datetime.now()

logging.basicConfig(level=logging.INFO)

findspark.init("C:\\w\\source\\spark\\spark-3.3.0-bin-hadoop3")
resources_path = "C:\\w\\EthicsForAnimals\\src\\main\\resources\\code_bdd\\"
tmp_path = "C:\\w\\EthicsForAnimals\\src\\main\\tmp\\"
csv_rna = "C:\\w\\EthicsForAnimals\\src\\main\\resources\\code_bdd\\rna_waldec_20230201"
csv_efa_all = "C:\\w\\EthicsForAnimals\\src\\main\\tmp\\output_final_all.csv"
csv_out = "C:\\w\\EthicsForAnimals\\src\\main\\tmp\\output_rna.csv"

spark_session = (
    SparkSession.builder
    .appName("Ethics For Animals csv construct GEO")
    .master("local[*]")
    .config("spark.executor.memory", "12G")
    .config("spark.driver.memory", "12G")
    .config("spark.driver.maxResultSize", "20G")
    .getOrCreate()
)

def main ():
    logging.info("Starting ETL job")
    # Extract
    extract_data()

    end_time = datetime.now()
    print('Done: {}'.format(end_time))
    print('Duration: {}'.format(end_time - start_time))


def extract_data():

    try:
        df_rna = spark_session.read.option("header", True).csv(os.path.join(resources_path, csv_rna), sep=";")
        df_efa_all = spark_session.read.option("header", True).csv(os.path.join(tmp_path, csv_efa_all))
    except AnalysisException:
        logging.error(
            f"The file  does not exist or cannot be read")


    df_efa_all_filtered = df_efa_all.filter(F.col("Code_present").startswith("RNA"))

    print(df_efa_all_filtered.show(3))

    df_join = df_efa_all_filtered.join(
        df_rna,
        df_efa_all_filtered.Code_final.cast('String') == df_rna.id, 
        "left")

    print(f"df_efa_all_filtered.count {df_efa_all_filtered.count()}")
    print(f"df_join.count {df_join.count()}")

    df_join = df_join.select("Code_final", "Code_present", "CP", "Ville", "date_disso", "adrs_codepostal", "adrs_libcommune", 
    "adrg_complemid", "adrg_complemgeo", "adrg_libvoie", "adrg_distrib")

    #df_join.show(10, False)
    
    #df_dep = df_join.groupBy("nom_departement").agg(count("Code_final").alias("count_code")).orderBy(desc("count_code"))
    #df_dep.show(30, False)
    df_join.coalesce(1).write.option("header",True).mode("overwrite").csv(csv_out)

    return df_join

main()