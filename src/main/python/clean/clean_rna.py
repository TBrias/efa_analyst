import csv
import os
import sys
from datetime import datetime

from pyspark.sql.functions import col, when, lit
from pyspark.sql import functions as F


def handle_rna_cols(df_data):
    print("Debut traitement des RNA")

    df_data = df_data.withColumn(
        'RNA', F.regexp_extract("Code", "^(?i)[W][0-9]{9}", 0))

    df_data = df_data.withColumn('RNA8', when(
        F.col("RNA") == "",
        F.regexp_extract("Code", "^(?i)[W][0-9]{8}", 0)).otherwise(lit(""))
    )

    df_data = df_data.withColumn('RNA7', when(
        (F.col("RNA") == "") & (F.col("RNA8") == ""),
        F.regexp_extract("Code", "^(?i)[W][0-9]{7}", 0)).otherwise(lit(""))
    )

    df_data = df_data.withColumn('RNA6', when(
        (F.col("RNA") == "") & (F.col("RNA8") == "") & (F.col("RNA7") == ""),
        F.regexp_extract("Code", "^(?i)[W][0-9]{6}", 0)).otherwise(lit(""))
    )

    df_data = df_data.withColumn('RNA5', when(
        (F.col("RNA") == "") & (F.col("RNA8") == "") & (
            F.col("RNA7") == "") & (F.col("RNA6") == ""),
        F.regexp_extract("Code", "^(?i)[W][0-9]{5}", 0)).otherwise(lit(""))
    )

    return df_data