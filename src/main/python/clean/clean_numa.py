import csv
import os
import sys
from datetime import datetime

from pyspark.sql.functions import col, when, lit
from pyspark.sql import functions as F


def handle_numa_cols(df_data):
    print("Debut traitement des NUMAGRIT/NUMAGRIN")

    df_data = df_data.withColumn(
        'NUMAGRIT', F.regexp_extract("Code", "^((?i)[A]\d{11})", 0))

    df_data = df_data.withColumn('NUMAGRIN', when(
        F.col("NUMAGRIT") == "",
        F.regexp_extract("Code", "^((?i)[A]\d{9})", 0)).otherwise(lit(""))
    )

    return df_data
