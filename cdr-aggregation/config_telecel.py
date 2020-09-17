# Databricks notebook source
from pyspark.sql.types import *

# Set up schema based on file strcucture
schema = StructType([
  StructField("call_datetime", StringType(), True), #load as string, will be turned into datetime in standardize_csv_files()
  StructField("msisdn", IntegerType(), True),
  StructField("location_id", StringType(), True),
  StructField("province", StringType(), True),
  StructField("city", StringType(), True),
  StructField("longitude", StringType(), True),
  StructField("latitude", StringType(), True)
])

datasource_configs = {
#   "load_datemask":"yyyy-MM-dd HH:mm:ss",
  "load_datemask":"EEE MMM dd HH:mm:ss zzz yyyy",
  "load_header":"true",
  "base_path": "/mnt/COVID19Data/proof-of-concept",
  # Subfolders in outputs
  "country_code": "ZW",
  "telecom_alias": "telecel",
  # Schema
  "schema" : schema,
  # Folders containing data in base_path/
  "data_paths" : ["world_bank_cdr_new.csv"],
  "filestub":"full",
  # Select what type of environment, 'local', 'cluster' or 'hive'
  "spark_mode":"cluster",
  # Select files to be loaded. If you need to create the mappings and distances files, select just the first 3 files, these are not created by the code.
  "geofiles": { "tower_sites":"sites-lookup-edited.csv",
                "admin2":"zw_admin2_shapefile.csv",
                "admin3":"zw_admin3_shapefile.csv",
                "voronoi":"ZW_voronoi_shapefile.csv",
                "admin2_tower_map":"ZW_admin2_tower_map.csv",
                "admin3_tower_map":"ZW_admin3_tower_map.csv",
                "voronoi_tower_map":"ZW_voronoi_tower_map.csv",
                "distances" : "ZW_distances_pd_long.csv"
              },
  # Select levels to  
  "shapefiles": ['admin2','admin3'],
# "shapefiles": ['admin2','admin3', 'voronoi'],
  "dates": {'start_date' : dt.datetime(2016,12,1),
            'end_date' : dt.datetime(2019,2,28)}}