import csv
import logging
import os
import sys
from datetime import datetime

import findspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, count, sum, desc, concat
from pyspark.sql.utils import AnalysisException
from pyspark.sql import functions as F
from datetime import datetime
import geopandas as gpd

start_time = datetime.now()

logging.basicConfig(level=logging.INFO)

findspark.init("C:\\w\\source\\spark\\spark-3.3.0-bin-hadoop3")
resources_path = "C:\\w\\EthicsForAnimals\\src\\main\\resources\\code_bdd\\"
tmp_path = "C:\\w\\EthicsForAnimals\\src\\main\\tmp\\"
csv_siret = "C:\\w\\EthicsForAnimals\\src\\main\\resources\\code_bdd\\StockEtablissement_utf8"
csv_efa_all = "C:\\w\\EthicsForAnimals\\src\\main\\tmp\\output_final_all.csv"
csv_out = "C:\\w\\EthicsForAnimals\\src\\main\\tmp\\output_join_siret.csv"

spark_session = (
    SparkSession.builder
    .appName("Ethics For Animals csv construct SIRET Join")
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
    df_efa_all = df_efa_all.select("Code_final", "Type_acteur").filter(col("Code_present") == "SIRET")
    print(df_efa_all.count())
    df_join = join_siret(df_efa_all)

    df_join.coalesce(1).write.option("header",True).mode("overwrite").csv(csv_out)

    end_time = datetime.now()
    print('Done: {}'.format(end_time))
    print('Duration: {}'.format(end_time - start_time))


def join_siret(df_efa_all):

    print('Starting join_siret: ' + format(datetime.now()))
    print('Duration: ' + format(datetime.now() - start_time))

    try:
        df_siret = spark_session.read.option("header", True).csv(os.path.join(resources_path, csv_siret), sep=",")
    except AnalysisException:
        logging.error(
            f"The file  does not exist or cannot be read")

    df_siret = df_siret \
        .select("siret","numeroVoieEtablissement","typeVoieEtablissement","libelleVoieEtablissement","codePostalEtablissement","libelleCommuneEtablissement")

    df_join = df_efa_all.join(
        df_siret,
        F.upper(df_efa_all.Code_final.cast('String')) == F.upper(df_siret.siret), 
        "left")
    print(df_join.count())
    df_join = df_join \
        .withColumn("Adresse_complete", concat(col("numeroVoieEtablissement"),lit(" "),col("typeVoieEtablissement"),lit(" "),col("libelleVoieEtablissement"))) \
        .withColumnRenamed("codePostalEtablissement", "CP") \
        .withColumnRenamed("libelleCommuneEtablissement", "Ville") \
        .withColumn("Adresse_presente", when( F.col("Adresse_complete") != "", lit("OUI"))) \
        
        

    #print(df_join.show())
    #print(f"df_efa_all.count df_siret {df_join.count()}")

    return df_join

main()