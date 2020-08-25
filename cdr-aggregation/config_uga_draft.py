from pyspark.sql.types import *

# Set up schema based on file strcucture
schema = StructType([
  StructField("msisdn", IntegerType(), True),
  StructField("call_datetime", StringType(), True), #load as string, will be turned into datetime in standardize_csv_files()
  StructField("data_type", StringType(), True), #load as string, will be turned into datetime in standardize_csv_files()
  StructField("location_id", StringType(), True)
])


datasource_configs = {
  "base_path": "/home/jovyan/work/data",
  # Subfolders in outputs
  "country_code": "UG",
  "telecom_alias": "mtn",
  # Schema
  "schema" : schema,
  # Folders containing data in base_path/
  "data_paths" : ["*.csv"],
  "filestub":"feb20",
  # Select what type of environment, 'local', 'cluster' or 'hive'
  "spark_mode":"local",
  # Select files to be loaded. If you need to create the mappings and distances files, select just the first 3 files, these are not created by the code.
  "geofiles": { "tower_sites":"ug_mtn_sites.csv",
                "admin2":"ug_admin2_shapefile.csv",
                "admin3":"ug_admin3_shapefile.csv",
                "voronoi":"ug_voronoi_shapefile.csv",
                "admin2_tower_map":"ug_admin2_tower_map.csv",
                "admin3_tower_map":"ug_admin3_tower_map.csv",
                "voronoi_tower_map":"ug_voronoi_tower_map.csv",
                "distances" : "ug_distances_pd_long.csv"
#                 "admin2_weight" : "ug_admin2_weight.csv",
#                 "admin3_weight" : "ug_admin3_weight.csv"
              },
  # Select levels to  
  "shapefiles": ['admin2','admin3'],
#   "shapefiles": ['admin2','admin3', 'voronoi'],
  "dates": {'start_date' : dt.datetime(2020,2,1),
            'end_date' : dt.datetime(2020,3,31)}}