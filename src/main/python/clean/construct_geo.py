import csv
import os
import sys
from datetime import datetime

from pyspark.sql.functions import col, when, lit
from pyspark.sql import functions as F


def construct_geo_df(df_data):
    print("Début traitement du code postal")

    df_data = df_data.withColumn('CP', 
        when((F.regexp_extract("Ville_1", "(\d{5})", 0).cast('int') > 1), F.regexp_extract("Ville_1", "(\d{5})", 0))
        .otherwise(when((F.regexp_extract("Rien3", "(\d{5})", 0).cast('int') > 1), F.regexp_extract("Rien3", "(\d{5})", 0))
        .otherwise(when((F.regexp_extract("Code_postal", "(\d{5})", 0).cast('int') > 1), F.regexp_extract("Code_postal", "(\d{5})", 0)) 
        .otherwise(when((F.regexp_extract("BP", "(\d{5})", 0).cast('int') > 1), F.regexp_extract("BP", "(\d{5})", 0)) 
        .otherwise(when((F.regexp_extract("Adresse", "(\d{5})", 0).cast('int') > 1), F.regexp_extract("Adresse", "(\d{5})", 0))
        .otherwise(when((F.regexp_extract("Num_voie", "(\d{5})", 0).cast('int') > 1), F.regexp_extract("Num_voie", "(\d{5})", 0))
        .otherwise("N/A"))))
            )
        )
    )

    print("Début traitement de la ville")


    df_data = df_data.withColumn('Ville', 
        when((F.trim(F.regexp_extract("Ville_1", "[^0-9]+", 0)).rlike("[^0-9]{3,}")), 
             F.trim(F.regexp_extract("Ville_1", "[^0-9]+", 0)))
        .otherwise(when((F.trim(F.regexp_extract("Rien3", "[^0-9]+", 0)).rlike("[^0-9]{3,}")), 
                        F.trim(F.regexp_extract("Rien3", "[^0-9]+", 0)))
        .otherwise(when((F.trim(F.regexp_extract("Code_postal", "[^0-9]+", 0)).rlike("[^0-9]{3,}")), 
                        F.trim(F.regexp_extract("Code_postal", "[^0-9]+", 0)))
        .otherwise(when((F.trim(F.regexp_extract("BP", "[^0-9]+", 0)).rlike("[^0-9]{3,}")), 
                        F.trim(F.regexp_extract("BP", "[^0-9]+", 0)))
        .otherwise(when((F.regexp_extract("Adresse", "[^0-9]+", 0).rlike("[^0-9]{3,}")), 
                        F.trim(F.regexp_replace(F.concat_ws(",",F.expr(r"regexp_extract_all(Adresse, '[^0-9]+', 0)")), ",", "")))
        .otherwise(when((F.regexp_extract("Num_voie", "[^0-9]+", 0).rlike("[^0-9]{3,}")), 
                        F.trim(F.regexp_replace(F.concat_ws(",",F.expr(r"regexp_extract_all(Num_voie, '[^0-9]+', 0)")), ",", "")))
        .otherwise(when((F.regexp_extract("Rien2", "[^0-9]+", 0).rlike("[^0-9]{3,}")), 
                        F.trim(F.regexp_replace(F.concat_ws(",",F.expr(r"regexp_extract_all(Rien2, '[^0-9]+', 0)")), ",", "")))
        .otherwise("N/A")))
            )
        )
    )))

    return df_data