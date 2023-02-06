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
import clean

start_time = datetime.now()
logging.basicConfig(level=logging.INFO)

findspark.init("C:\\w\\source\\spark\\spark-3.3.0-bin-hadoop3")
main_path = "C:\\w\\EthicsForAnimals"
csv_in = "C:\\w\\EthicsForAnimals\\src\\main\\resources\\EFA_all.csv"
csv_out = "C:\\w\\EthicsForAnimals\\src\\main\\tmp\\output_final_all.csv"
csv_out_geo = "C:\\w\\EthicsForAnimals\\src\\main\\tmp\\output_final_geo_all.csv"

spark_session = (
    SparkSession.builder
    .appName("Ethics For Animals csv construct")
    .master("local[*]")
    .config("spark.executor.memory", "12G")
    .config("spark.driver.memory", "12G")
    .config("spark.driver.maxResultSize", "20G")
    .getOrCreate()
)

def main ():
    print('Starting: {}'.format(start_time))

    df_extacted_data = extract_data()

    write_geo(df_extacted_data)
    
    write_full_df(df_extacted_data)
    
    end_time = datetime.now()
    print('Done: {}'.format(end_time))
    print('Duration: {}'.format(end_time - start_time))


def extract_data():
    """ Read and clean data to specific columns
        Throw Exception if file does not exist
    """
    logging.info(f"Started retrieving data from {csv_in}")

    try:
        df_data = spark_session.read.option("header", True).csv(
            os.path.join(main_path, csv_in))
    except AnalysisException:
        logging.error(
            f"The file {csv_in} does not exist or cannot be read")


    #SIRET
    df_data = clean.clean_siret.handle_siret_cols(df_data)

    #SIREN
    df_data = clean.clean_siren.handle_siren_cols(df_data)

    #RNA
    df_data = clean.clean_rna.handle_rna_cols(df_data)

    #NUMAGRIN/NUMAGRIT
    df_data = clean.clean_numa.handle_numa_cols(df_data)
    
    #OTHER COLS
    df_data = clean.clean_others.handle_other_cols(df_data)
    
    #SUMMARY CODE
    df_data = clean.construct_code_cols.construct_cols(df_data)
    
    #EXTRACT GEO INFO
    df_data = clean.construct_geo.construct_geo_df(df_data)
    
    return df_data


def write_geo (df_data):
    print('Starting: Writing geo df')
    
    df_geo = df_data.select("Code_final", "Code_present", "CP", "Ville")

    df_geo.write.option("header",True).mode("overwrite").csv(csv_out_geo)


def write_full_df (df_data):
    print('Starting: Writing full df')
    
    df_data =  df_data.withColumn("Type_code", (F.regexp_extract("Code", "(^[A-Za-z]*)", 0)))
    #df_data = df_data.groupBy("Type_code").agg(count("Code_final").alias("count_code")).orderBy(desc("count_code"))

    df_data.coalesce(1).write.option("header",True).mode("overwrite").csv("C:\w\EthicsForAnimals\all_type_code")


main()