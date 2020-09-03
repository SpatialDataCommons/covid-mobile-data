import pyspark.sql.functions as F
from pyspark.sql.functions import to_timestamp
from pyspark.sql.types import *


# Set up schema based on file strcucture
schema = StructType([
  StructField("msisdn", IntegerType(), True),
  StructField("call_datetime", StringType(), True), #load as string, will be turned into datetime in standardize_csv_files()
  StructField("data_type", StringType(), True), #load as string, will be turned into datetime in standardize_csv_files()
  StructField("location_id", StringType(), True)
])


newfolder_data_paths = "data/new/UG/mtn/mtn_mock_sample_short.csv"


#--------------------------------------------
# orginal code
#Load csv file or files using load option and schema
raw_df = spark.read\
  .option("header", False)\
  .csv(newfolder_data_paths, schema=schema)

#convert call_datetime string to call_datetime timestamp
raw_df = raw_df.withColumn("call_datetime", to_timestamp("call_datetime","dd/MM/yyyy HH:mm:ss"))

#get call_date from call_datetime
raw_df = raw_df.withColumn('call_date', raw_df.call_datetime.cast('date'))

#--------------------------------------------
# 

from pyspark.sql.functions import ceil, col

split_col = F.split(raw_df['location_id'], ',')
raw_df = raw_df.withColumn(
    'LAT',
    F.round(
        split_col.getItem(0).cast(DoubleType()), 
        6)
    )
raw_df = raw_df.withColumn(
    'LNG',
    F.round(
        split_col.getItem(1).cast(DoubleType()), 
        6)
    )

raw_df = raw_df.withColumn('location_id', 
              F.concat(F.col('LAT'),
                          F.lit(' '), 
                          F.col('LNG')))

raw_df = raw_df.drop('LAT', 'LNG')

raw_df.toPandas().to_csv(newfolder_data_paths + 'foo', index = False)