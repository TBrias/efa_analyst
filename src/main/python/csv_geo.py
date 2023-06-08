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
main_path = "C:\\w\\EthicsForAnimals"
csv_france = "C:\\w\\EthicsForAnimals\\src\\main\\resources\\geo\\communes-departement-region.csv"
csv_efa_geo = "C:\\w\\EthicsForAnimals\\src\\main\\resources\\geo\\output_final_geo_all.csv"
csv_out = "C:\\w\\EthicsForAnimals\\src\\main\\resources\\geo\\output_join_geo_all.csv"
csv_in = "C:\\w\\EthicsForAnimals\\src\\main\\tmp\\output_clean.csv"

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
        #df_geo = spark_session.read.option("header", True).csv(os.path.join(main_path, csv_efa_geo))
        df_geo = spark_session.read.option("header", True).csv(os.path.join(main_path, csv_in))
        df_france = spark_session.read.option("header", True).csv(os.path.join(main_path, csv_france))
    except AnalysisException:
        logging.error(
            f"The file {csv_in} does not exist or cannot be read")

    #df_geo = df_geo.filter(col("CP").cast('float') == 62302)
    #print(df_geo.show())
    #df_france = df_france.filter(col("code_commune_INSEE").cast('float') == 62302)
    #print(df_france.show())

    df_france = df_france.dropDuplicates(["code_commune_INSEE"])
    #df_dep3 = df_geo.groupBy("Code_present").agg(count("Code_final").alias("count_code")).orderBy(desc("count_code"))
    #print(df_dep3.count())

    df_join = df_geo.join(
        df_france,
        df_geo.CP.cast('float') == df_france.code_commune_INSEE.cast('float'), 
        "left")

    print(df_join.count())

    df_join = df_join.select("Code_final", "Code_present", "CP", "Ville", "latitude", "longitude", "nom_departement", "nom_region")

    df_join.show(10, False)
    
    df_dep = df_join.groupBy("nom_departement").agg(count("Code_final").alias("count_code")).orderBy(desc("count_code"))
    #df_dep2 = df_join.groupBy("Code_present").agg(count("Code_final").alias("count_code")).orderBy(desc("count_code"))
    df_dep.show(30, False)
    print(df_dep.count())
    #print(df_dep2.count())
    df_join.coalesce(1).write.option("header",True).mode("overwrite").csv(csv_out)

    return df_join

main()