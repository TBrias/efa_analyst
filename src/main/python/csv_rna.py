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
csv_siret = "C:\\w\\EthicsForAnimals\\src\\main\\resources\\code_bdd\\StockEtablissement_utf8"
csv_siren = "C:\\w\\EthicsForAnimals\\src\\main\\resources\\code_bdd\\StockUniteLegale_utf8"
csv_efa_all = "C:\\w\\EthicsForAnimals\\src\\main\\tmp\\output_final_all.csv"
csv_out = "C:\\w\\EthicsForAnimals\\src\\main\\tmp\\output_rna.csv"
csv_out_all = "C:\\w\\EthicsForAnimals\\src\\main\\tmp\\output_join_all.csv"

spark_session = (
    SparkSession.builder
    .appName("Ethics For Animals csv construct RNA join")
    .master("local[*]")
    .config("spark.executor.memory", "12G")
    .config("spark.driver.memory", "12G")
    .config("spark.driver.maxResultSize", "20G")
    .getOrCreate()
)

def main ():
    logging.info("Starting ETL job")
    # Extract
    df_efa_all = spark_session.read.option("header", True).csv(os.path.join(tmp_path, csv_efa_all))

    df_join_all = join_rna(df_efa_all)

    df_join_all = join_siren(df_join_all)

    df_join_all = join_siret(df_join_all)

    df_join_all.coalesce(1).write.option("header",True).mode("overwrite").csv(csv_out_all)

    end_time = datetime.now()
    print('Done: {}'.format(end_time))
    print('Duration: {}'.format(end_time - start_time))


def join_rna(df_efa_all):

    try:
        df_rna = spark_session.read.option("header", True).csv(os.path.join(resources_path, csv_rna), sep=";")
        
    except AnalysisException:
        logging.error(
            f"The file  does not exist or cannot be read")

    #df_efa_all_filtered = df_efa_all.filter(F.col("Code_present").startswith("RNA"))

    #print(df_efa_all.show(3))

    df_rna = df_rna.select("id", "date_disso").withColumn("rna_bdd", lit("OUI"))

    df_join = df_efa_all.join(
        df_rna,
        F.upper(df_efa_all.Code_final.cast('String')) == F.upper(df_rna.id), 
        "left")

    #print(f"df_efa_all.count join_rna {df_efa_all.count()}")
    #print(f"df_join.count {df_join.count()}")

    #df_join = df_join.select("Code_final", "Code_present", "CP", "Ville", "date_disso", "adrs_codepostal", "adrs_libcommune", 
    #"adrg_complemid", "adrg_complemgeo", "adrg_libvoie", "adrg_distrib")

    #print(f"df_efa_all.count {df_efa_all.count()}")
    #print(f"df_join all.count {df_join.count()}")

    #df_empty_lines = df_join.filter(F.col("date_disso").isNull())
    #print(f"df_join empty.count {df_empty_lines.count()}")


    #df_join.show(10, False)
    
    #df_dep.show(30, False)
    #df_join.coalesce(1).write.option("header",True).mode("overwrite").csv(csv_out)

    return df_join

def join_siren(df_efa_all):
    print('Starting join_siren: ' + format(datetime.now()))
    print('Duration: ' + format(datetime.now() - start_time))

    try:
        df_siren = spark_session.read.option("header", True).csv(os.path.join(resources_path, csv_siren), sep=",")
        
    except AnalysisException:
        logging.error(
            f"The file  does not exist or cannot be read")

    #df_efa_all_filtered = df_efa_all.filter(F.col("Code_present").startswith("RNA"))



    df_siren = df_siren.select("siren").withColumn("siren_bdd", lit("OUI"))

    df_join = df_efa_all.join(
        df_siren,
        F.upper(df_efa_all.Code_final.cast('String')) == F.upper(df_siren.siren), 
        "left")
    #print(df_join.show())
    #print(f"df_efa_all.count df_siren {df_join.count()}")

    return df_join

def join_siret(df_efa_all):
    print('Starting join_siret: ' + format(datetime.now()))
    print('Duration: ' + format(datetime.now() - start_time))

    try:
        df_siren = spark_session.read.option("header", True).csv(os.path.join(resources_path, csv_siret), sep=",")
        
    except AnalysisException:
        logging.error(
            f"The file  does not exist or cannot be read")

    #df_efa_all_filtered = df_efa_all.filter(F.col("Code_present").startswith("RNA"))



    df_siret = df_siren.select("siret").withColumn("siret_bdd", lit("OUI"))

    df_join = df_efa_all.join(
        df_siret,
        F.upper(df_efa_all.Code_final.cast('String')) == F.upper(df_siret.siret), 
        "left")
    #print(df_join.show())
    #print(f"df_efa_all.count df_siret {df_join.count()}")

    return df_join


main()