import csv
import os
import sys
from datetime import datetime

from pyspark.sql.functions import col, when, lit
from pyspark.sql import functions as F


def handle_siren_cols(df_data):
    print("Debut traitement des SIREN")

    df_data = df_data.withColumn('SIREN', when(
        ((F.col("SIRET") == "") & (F.col("SIRET13") == "") & (F.col("SIRET12") == "") & (F.col("SIRET11") == "")
         & (F.col("SIRET10") == "")),
        F.regexp_extract("Code", "^(\d{9})", 1)).otherwise(lit(""))
    )

    df_data = df_data.withColumn('SIREN8', when(
        ((F.col("SIRET") == "") & (F.col("SIRET13") == "") & (F.col("SIRET12") == "") & (F.col("SIRET11") == "")
         & (F.col("SIRET10") == "") & (F.col("SIREN") == "")),
        F.regexp_extract("Code", "^(\d{8})", 1)).otherwise(lit(""))
    )

    df_data = df_data.withColumn('SIREN7', when(
        ((F.col("SIRET") == "") & (F.col("SIRET13") == "") & (F.col("SIRET12") == "") & (F.col("SIRET11") == "")
         & (F.col("SIRET10") == "") & (F.col("SIREN8") == "")),
        F.regexp_extract("Code", "^(\d{7})", 1)).otherwise(lit(""))
    )

    df_data = df_data.withColumn('SIREN6', when(
        ((F.col("SIRET") == "") & (F.col("SIRET13") == "") & (F.col("SIRET12") == "") & (F.col("SIRET11") == "")
         & (F.col("SIRET10") == "") & (F.col("SIREN8") == "") & (F.col("SIREN7") == "")),
        F.regexp_extract("Code", "^(\d{6})", 1)).otherwise(lit(""))
    )

    df_data = df_data.withColumn('SIREN5', when(
        ((F.col("SIRET") == "") & (F.col("SIRET13") == "") & (F.col("SIRET12") == "") & (F.col("SIRET11") == "")
         & (F.col("SIRET10") == "") & (F.col("SIREN8") == "") & (F.col("SIREN7") == "") & (F.col("SIREN6") == "")),
        F.regexp_extract("Code", "^(\d{5})", 1)).otherwise(lit(""))
    )
