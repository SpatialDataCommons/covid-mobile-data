from pyspark.sql.types import *
schema = StructType([
  StructField("msisdn", IntegerType(), True),
  StructField("call_datetime", StringType(), True), #load as string, will be turned into datetime in standardize_csv_files()
  StructField("location_id", StringType(), True)
])

datasource_configs = {
  "base_path": "path_to_folder/data", #folder path used in this docker env
  "hive_warehouse_location": "/apps/hive/warehouse",
  "hive_metastore_uris": "thrift://thrift://<IPaddress>:9083",
  "spark_mode": 'hive',
  "hive_vars":{ 'msisdn' : 'col1',        #change col1 to hive column that contain msisdn
                'call_datetime': 'col2',  #change col2 to hive column that contain call_datetime
                'location_id': 'col3',    #change col3 to hive column that contain location_id
                'calls': 'table'},        #change table to hive column that contain cdr data
  "country_code": "",
  "telecom_alias": "",
  "schema" : schema,
  "data_paths" : ["*.csv"],
  "filestub": "",
  "geofiles": {},
  "shapefiles": ['admin2','admin3', 'voronoi'],
  "dates": {'start_date' : dt.datetime(2020,2,1),
            'end_date' : dt.datetime(2020,3,31)}
}
