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
csv_france = "C:\w\EthicsForAnimals\geo\communes-departement-region.csv"
csv_geo = "C:\w\EthicsForAnimals\geo\output_final_geo_all.csv"
csv_out = "C:\w\EthicsForAnimals\geo\output_join_geo_all.csv"
csv_join = "C:\w\EthicsForAnimals\geo\geo_join_all.csv"

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
    df_extacted_data = extract_data()

    end_time = datetime.now()
    print('Done: {}'.format(end_time))
    print('Duration: {}'.format(end_time - start_time))


def extract_data():

    try:
        df_geo = spark_session.read.option("header", True).csv(os.path.join(main_path, csv_geo))
        df_france = spark_session.read.option("header", True).csv(os.path.join(main_path, csv_france))
    except AnalysisException:
        logging.error(
            f"The file {csv_in} does not exist or cannot be read")


    df_france = df_france.dropDuplicates(["code_postal"])

    df_join = df_geo.join(
        df_france,
        df_geo.CP.cast('float') == df_france.code_postal, 
        "left")

    print(df_join.count())

    df_join = df_join.select("Code_final", "Code_present", "CP", "Ville", "latitude", "longitude", "nom_departement", "nom_region")

    df_join.show(10, False)
    
    df_dep = df_join.groupBy("nom_departement").agg(count("Code_final").alias("count_code")).orderBy(desc("count_code"))
    df_dep.show(30, False)
    df_join.write.option("header",True).mode("overwrite").csv(csv_out)

    return df_join

main()