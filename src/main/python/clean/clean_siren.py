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



def handle_siren_cols(df_data):
    print("Debut traitement des SIREN")

    df_data = df_data.withColumn('SIREN_tmp', when(
        ((F.col("SIRET_tmp") == "") & (F.col("SIRET13") == "") & (F.col("SIRET12") == "") & (F.col("SIRET11") == "")
         & (F.col("SIRET10") == "")),
        F.regexp_extract("Code", "^(\d{9})", 1)).otherwise(lit(""))
    )

    df_data = df_data.withColumn('SIREN8', when(
        ((F.col("SIRET_tmp") == "") & (F.col("SIRET13") == "") & (F.col("SIRET12") == "") & (F.col("SIRET11") == "")
         & (F.col("SIRET10") == "") & (F.col("SIREN_tmp") == "")),
        F.regexp_extract("Code", "^(\d{8})", 1)).otherwise(lit(""))
    )

    df_data = df_data.withColumn('SIREN7', when(
        ((F.col("SIRET_tmp") == "") & (F.col("SIRET13") == "") & (F.col("SIRET12") == "") & (F.col("SIRET11") == "")
         & (F.col("SIRET10") == "") & (F.col("SIREN_tmp") == "") & (F.col("SIREN8") == "")),
        F.regexp_extract("Code", "^(\d{7})", 1)).otherwise(lit(""))
    )

    df_data = df_data.withColumn('SIREN6', when(
        ((F.col("SIRET_tmp") == "") & (F.col("SIRET13") == "") & (F.col("SIRET12") == "") & (F.col("SIRET11") == "")
         & (F.col("SIRET10") == "") & (F.col("SIREN_tmp") == "") & (F.col("SIREN8") == "") & (F.col("SIREN7") == "")),
        F.regexp_extract("Code", "^(\d{6})", 1)).otherwise(lit(""))
    )

    df_data = df_data.withColumn('SIREN5', when(
        ((F.col("SIRET_tmp") == "") & (F.col("SIRET13") == "") & (F.col("SIRET12") == "") & (F.col("SIRET11") == "")
         & (F.col("SIRET10") == "") & (F.col("SIREN_tmp") == "") & (F.col("SIREN8") == "") & (F.col("SIREN7") == "") & (F.col("SIREN6") == "")),
        F.regexp_extract("Code", "^(\d{5})", 1)).otherwise(lit(""))
    )

    return df_data
