import csv
import os

import sys

from datetime import datetime


from pyspark.sql.functions import col, when, lit

from pyspark.sql import functions as F


def handle_other_cols(df_data):

    #AUTRE_CODE

    print("Debut traitement des AUTRE_CODE")
    regex_letters = "(^[A-Za-z]*)"
    regex_letters_numbers = "(^[A-Za-z]*[0-9]*)"
    code = "Code"
    type_access = "Type_acces"
    
    df_data = df_data.withColumn('AUTRE_CODE', 
        when((F.trim(F.regexp_extract(code, regex_letters, 0)).startswith("MAI")),F.trim(F.regexp_extract(code, regex_letters_numbers, 0)))
        .otherwise(when((F.trim(F.regexp_extract(code, regex_letters, 0)).startswith("ELV")),F.trim(F.regexp_extract(code, regex_letters_numbers, 0)))
        .otherwise(when((F.trim(F.regexp_extract(code, regex_letters, 0)).startswith("AYD")),F.trim(F.regexp_extract(code, regex_letters_numbers, 0)))
        .otherwise(when((F.trim(F.regexp_extract(code, regex_letters, 0)).startswith("TATOU")),F.trim(F.regexp_extract(code, regex_letters_numbers, 0)))
        .otherwise(when((F.trim(F.regexp_extract(code, regex_letters, 0)).startswith("POL")),F.trim(F.regexp_extract(code, regex_letters_numbers, 0)))
        .otherwise(when((F.trim(F.regexp_extract(code, regex_letters, 0)).startswith("PRO")),F.trim(F.regexp_extract(code, regex_letters_numbers, 0)))
        .otherwise(when((F.trim(F.regexp_extract(code, regex_letters, 0)).startswith("DDPP")),F.trim(F.regexp_extract(code, regex_letters_numbers, 0)))
        .otherwise(when((F.trim(F.regexp_extract(code, regex_letters, 0)).startswith("DDCSPP")),F.trim(F.regexp_extract(code, regex_letters_numbers, 0)))
        .otherwise(when((F.trim(F.regexp_extract(code, regex_letters, 0)).startswith("HOTEL")),F.trim(F.regexp_extract(code, regex_letters_numbers, 0)))
        .otherwise(when((F.trim(F.regexp_extract(code, regex_letters, 0)).startswith("DDSV")),F.trim(F.regexp_extract(code, regex_letters_numbers, 0)))
        .otherwise(when((F.trim(F.regexp_extract(type_access, regex_letters, 0)).startswith("MAI")),F.trim(F.regexp_extract(code, regex_letters_numbers, 0)))
        .otherwise(when((F.trim(F.regexp_extract(type_access, regex_letters, 0)).startswith("ELV")),F.trim(F.regexp_extract(code, regex_letters_numbers, 0)))
        .otherwise(when((F.trim(F.regexp_extract(type_access, regex_letters, 0)).startswith("AYD")),F.trim(F.regexp_extract(code, regex_letters_numbers, 0)))
        .otherwise(when((F.trim(F.regexp_extract(type_access, regex_letters, 0)).startswith("TATOU")),F.trim(F.regexp_extract(code, regex_letters_numbers, 0)))
        .otherwise(when((F.trim(F.regexp_extract(type_access, regex_letters, 0)).startswith("POL")),F.trim(F.regexp_extract(code, regex_letters_numbers, 0)))
        .otherwise(when((F.trim(F.regexp_extract(type_access, regex_letters, 0)).startswith("PRO")),F.trim(F.regexp_extract(code, regex_letters_numbers, 0)))
        .otherwise(when((F.trim(F.regexp_extract(type_access, regex_letters, 0)).startswith("DDPP")),F.trim(F.regexp_extract(code, regex_letters_numbers, 0)))
        .otherwise(when((F.trim(F.regexp_extract(type_access, regex_letters, 0)).startswith("DDCSPP")),F.trim(F.regexp_extract(code, regex_letters_numbers, 0)))
        .otherwise(when((F.trim(F.regexp_extract(type_access, regex_letters, 0)).startswith("HOTEL")),F.trim(F.regexp_extract(code, regex_letters_numbers, 0)))
        .otherwise(when((F.trim(F.regexp_extract(type_access, regex_letters, 0)).startswith("DDSV")),F.trim(F.regexp_extract(code, regex_letters_numbers, 0)))
        #.otherwise("N/A")
        ))))))))))))))))))))

    return df_data
