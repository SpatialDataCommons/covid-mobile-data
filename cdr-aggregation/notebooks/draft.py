import os

from modules.aggregator import *
from modules.import_packages import *
from modules.utilities import *


from pyspark.sql.functions import to_timestamp
from pyspark.sql.types import *
from pyspark.sql import Window
from random import sample, seed

import datetime as dt
import pyspark.sql.functions as F



# parquet_path = 'C:/Users/wb519128/Downloads/sample1/'
parquet_path = '/home/jovyan/work/data/sample1/'

InputPath = [parquet_path + '*.parquet']
df = spark.read.parquet(*InputPath)

user_window = Window\
    .partitionBy('msisdn').orderBy('call_datetime')
#-----------------------------------------------------------------
# Basic processing

privacy_filter = 15
missing_value_code = 99999
cutoff_days = 7
max_duration = 21
dates = {'start_date' : dt.datetime(2020,2,1),
            'end_date' : dt.datetime(2020,3,31)}


df = df\
    .withColumnRenamed('location_id', 'region')\
    .orderBy('msisdn', 'call_datetime')\
    .withColumn('region_lag', F.lag('region').over(user_window))\
    .withColumn('region_lead', F.lead('region').over(user_window))\
    .withColumn('call_datetime_lag',
      F.lag('call_datetime').over(user_window))\
    .withColumn('call_datetime_lead',
      F.lead('call_datetime').over(user_window))\
    .withColumn('hour_of_day', F.hour('call_datetime').cast('byte'))\
    .withColumn('hour', F.date_trunc('hour', F.col('call_datetime')))\
    .withColumn('week', F.date_trunc('week', F.col('call_datetime')))\
    .withColumn('month', F.date_trunc('month', F.col('call_datetime')))\
    .withColumn('constant', F.lit(1).cast('byte'))\
    .withColumn('day', F.date_trunc('day', F.col('call_datetime')))\
    .na.fill({'region' : missing_value_code ,
              'region_lag' : missing_value_code ,
              'region_lead' : missing_value_code })

#### Indicator 1

def transactions(df, frequency):
    result = df\
        .groupby(frequency, 'region')\
        .count()
    return result

transactions(df, 'hour').show()


#### Indicator 10

    # result:
    # - apply sample period filter
    # - drop all observations that don't imply a region change from lag or lead
    # - create timestamp lead, recplaing missing values with end of sample period
    # - calculate duration
    # - constrain duration to max seven days
    # - get the lead duration
    # - sum durations for stops without lead switch
    # - set max duration to 21 days
    # - get lag duration
    # - drop all observations with lead rather than lag switch
    # - group by frequency and origin (lag) and destination (lead)
    # - calculate avg, std, sums and counts of o and d durations

def origin_destination_matrix_time(df, frequency):
            
    user_frequency_window = Window.partitionBy('msisdn').orderBy('call_datetime')
    
    result = df\
      .where((F.col('region_lag') != F.col('region')) | \
          (F.col('region_lead') != F.col('region')) | \
          (F.col('call_datetime_lead').isNull()))\
      .withColumn('call_datetime_lead',
          F.when(F.col('call_datetime_lead').isNull(),
          dates['end_date'] + dt.timedelta(1)).otherwise(F.col('call_datetime_lead')))\
      .withColumn('duration', (F.col('call_datetime_lead').cast('long') - \
          F.col('call_datetime').cast('long')))\
      .withColumn('duration', F.when(F.col('duration') <= \
          (cutoff_days * 24 * 60 * 60), F.col('duration')).otherwise(0))\
      .withColumn('duration_next', F.lead('duration').over(user_frequency_window))\
      .withColumn('duration_change_only', F.when(F.col('region') == \
          F.col('region_lead'), F.col('duration_next') + \
          F.col('duration')).otherwise(F.col('duration')))\
      .withColumn('duration_change_only',
          F.when(F.col('duration_change_only') > \
          (max_duration * 24 * 60 * 60),
          (max_duration * 24 * 60 * 60)).otherwise(F.col('duration_change_only')))\
      .withColumn('duration_change_only_lag',
          F.lag('duration_change_only').over(user_frequency_window))\
      .where(F.col('region_lag') != F.col('region'))\
      .groupby(frequency, 'region', 'region_lag')\
      .agg(F.sum('duration_change_only').alias('total_duration_destination'),
         F.avg('duration_change_only').alias('avg_duration_destination'),
         F.count('duration_change_only').alias('count_destination'),
         F.stddev_pop('duration_change_only').alias('stddev_duration_destination'),
         F.sum('duration_change_only_lag').alias('total_duration_origin'),
         F.avg('duration_change_only_lag').alias('avg_duration_origin'),
         F.count('duration_change_only_lag').alias('count_origin'),
         F.stddev_pop('duration_change_only_lag').alias('stddev_duration_origin'))
    
    return result

origin_destination_matrix_time(df, 'day')