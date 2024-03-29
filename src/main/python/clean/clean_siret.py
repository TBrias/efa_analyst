import csv
import os
import sys
from datetime import datetime

import findspark
from pyspark.sql.functions import col, when, lit, length
from pyspark.sql import functions as F


def handle_siret_cols(df_data):
    print("Debut traitement des SIRET")

    #df_data = df_data.withColumn('SIRET_tmp', F.regexp_extract("Code", "^(\d{14})", 1)) 
    df_data = df_data.withColumn('SIRET_tmp', when(length(F.trim(F.concat_ws("",F.expr(r"regexp_extract_all(Code, '[0-9]+', 0)")))) == 14,
        F.substring(F.trim(F.concat_ws("",F.expr(r"regexp_extract_all(Code, '[0-9]+', 0)"))), 1, 14)).otherwise(lit(""))
    )

                        


    df_data = df_data.withColumn('SIRET13', when(
            F.col("SIRET_tmp") == "", F.regexp_extract("Code", "^(\d{13})", 1)).otherwise(lit(""))
            )

    df_data = df_data.withColumn('SIRET12', when(
            ((F.col("SIRET_tmp") == "") & (F.col("SIRET13") == "")), 
            F.regexp_extract("Code", "^(\d{12})", 1)
            ).otherwise(lit(""))
            )

    df_data = df_data.withColumn('SIRET11', when(
            ((F.col("SIRET_tmp") == "") & (F.col("SIRET13") == "") & (F.col("SIRET12") == ""))
            , F.regexp_extract("Code", "^(\d{11})", 1)).otherwise(lit(""))
            )
            
    df_data = df_data.withColumn('SIRET10', when(
            ((F.col("SIRET_tmp") == "") & (F.col("SIRET13") == "") & (F.col("SIRET12") == "") & (F.col("SIRET11") == "")),
             F.regexp_extract("Code", "^(\d{10})", 1)).otherwise(lit(""))
            )

    return df_data
