import csv
import os
import sys
from datetime import datetime

from pyspark.sql.functions import col, when, lit
from pyspark.sql import functions as F


def construct_cols(df_data):
    print("Debut traitement du récap des codes présents")

    regex_letters = "(^[A-Za-z]*)"
    regex_letters_numbers = "(^[A-Za-z]*[0-9]*)"
    
    df_data = df_data.withColumn('Code_present',
     when((F.col("SIRET") != ""), "SIRET")
     .when((F.col("SIRET13") != ""), "SIRET13")
     .when((F.col("SIRET12") != ""), "SIRET12")
     .when((F.col("SIRET11") != ""), "SIRET11")
     .when((F.col("SIRET10") != ""), "SIRET10")
     .when((F.col("SIREN") != ""), "SIREN")
     .when((F.col("SIREN8") != ""), "SIREN8")
     .when((F.col("SIREN7") != ""), "SIREN7")
     .when((F.col("SIREN6") != ""), "SIREN6")
     .when((F.col("SIREN5") != ""), "SIREN5")
     .when((F.col("RNA") != ""), "RNA")
     .when((F.col("RNA8") != ""), "RNA8")
     .when((F.col("RNA7") != ""), "RNA7")
     .when((F.col("RNA6") != ""), "RNA6")
     .when((F.col("RNA5") != ""), "RNA5")
     .when((F.col("NUMAGRIT") != ""), "NUMAGRIT")
     .when((F.col("NUMAGRIT") != ""), "NUMAGRIT")
     .when(((F.regexp_extract("Code", regex_letters, 0)).startswith("MAI")), "MAI")
     .when(((F.regexp_extract("Code", regex_letters, 0)).startswith("ELV")), "ELV")
     .when(((F.regexp_extract("Code", regex_letters, 0)).startswith("AYD")), "AYD")
     .when(((F.regexp_extract("Code", regex_letters, 0)).startswith("TATOU")), "TATOU")
     .when(((F.regexp_extract("Code", regex_letters, 0)).startswith("POL")), "POL")
     .when(((F.regexp_extract("Code", regex_letters, 0)).startswith("PRO")), "PRO")
     .when(((F.regexp_extract("Code", regex_letters, 0)).startswith("DDPP")), "DDPP")
     .when(((F.regexp_extract("Code", regex_letters, 0)).startswith("DDCSPP")), "DDCSPP")
     .when(((F.regexp_extract("Code", regex_letters, 0)).startswith("HOTEL")), "HOTEL")
     .when(((F.regexp_extract("Code", regex_letters, 0)).startswith("DDSV")), "DDSV")
     .otherwise("N/A"))

    df_data = df_data.withColumn('Code_final',
     when((F.col("SIRET") != ""), F.col("SIRET"))
     .when((F.col("SIRET13") != ""), F.col("SIRET13"))
     .when((F.col("SIRET12") != ""), F.col("SIRET12"))
     .when((F.col("SIRET11") != ""), F.col("SIRET11"))
     .when((F.col("SIRET10") != ""), F.col("SIRET10"))
     .when((F.col("SIREN") != ""), F.col("SIREN"))
     .when((F.col("SIREN8") != ""), F.col("SIREN8"))
     .when((F.col("SIREN7") != ""), F.col("SIREN7"))
     .when((F.col("SIREN6") != ""), F.col("SIREN6"))
     .when((F.col("SIREN5") != ""), F.col("SIREN5"))
     .when((F.col("RNA") != ""), F.col("RNA"))
     .when((F.col("RNA8") != ""), F.col("RNA8"))
     .when((F.col("RNA7") != ""), F.col("RNA7"))
     .when((F.col("RNA6") != ""), F.col("RNA6"))
     .when((F.col("RNA5") != ""), F.col("RNA5"))
     .when((F.col("NUMAGRIT") != ""), F.col("NUMAGRIT"))
     .when((F.col("NUMAGRIT") != ""), F.col("NUMAGRIT"))
     .when((F.trim(F.regexp_extract("Code", regex_letters, 0)).startswith("MAI")),F.trim(F.regexp_extract("Code", regex_letters_numbers, 0)))
     .when((F.trim(F.regexp_extract("Code", regex_letters, 0)).startswith("ELV")),F.trim(F.regexp_extract("Code", regex_letters_numbers, 0)))
     .when((F.trim(F.regexp_extract("Code", regex_letters, 0)).startswith("AYD")),F.trim(F.regexp_extract("Code", regex_letters_numbers, 0)))
     .when((F.trim(F.regexp_extract("Code", regex_letters, 0)).startswith("TATOU")),F.trim(F.regexp_extract("Code", regex_letters_numbers, 0)))
     .when((F.trim(F.regexp_extract("Code", regex_letters, 0)).startswith("POL")),F.trim(F.regexp_extract("Code", regex_letters_numbers, 0)))
     .when((F.trim(F.regexp_extract("Code", regex_letters, 0)).startswith("PRO")),F.trim(F.regexp_extract("Code", regex_letters_numbers, 0)))
     .when((F.trim(F.regexp_extract("Code", regex_letters, 0)).startswith("DDPP")),F.trim(F.regexp_extract("Code", regex_letters_numbers, 0)))
     .when((F.trim(F.regexp_extract("Code", regex_letters, 0)).startswith("DDCSPP")),F.trim(F.regexp_extract("Code", regex_letters_numbers, 0)))
     .when((F.trim(F.regexp_extract("Code", regex_letters, 0)).startswith("HOTEL")),F.trim(F.regexp_extract("Code", regex_letters_numbers, 0)))
     .when((F.trim(F.regexp_extract("Code", regex_letters, 0)).startswith("DDSV")),F.trim(F.regexp_extract("Code", regex_letters_numbers, 0)))
     .otherwise("N/A"))
