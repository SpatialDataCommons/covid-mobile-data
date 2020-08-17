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


#-----------------------------------------------------------------

# parquet_path = 'C:/Users/wb519128/Downloads/sample1/'
data_path = '/home/jovyan/work/data/'
parquet_path = data_path + 'sample1/'

out_path = '/home/jovyan/work/out/'

# Load CDR data
InputPath = [parquet_path + '*.parquet']
df_raw = spark.read.parquet(*InputPath)

# Load tower mapping
cells = spark\
            .read.format('csv')\
            .options(header='true', inferSchema='true')\
            .load(data_path + 'zw_admin2_tower_map.csv')


# Create new object to backup raw data
df = df_raw

#-----------------------------------------------------------------
spark_ses = SparkSession.builder.master("local[*]") \
              .config("spark.driver.maxResultSize", "2g") \
              .config("spark.sql.shuffle.partitions", "16") \
              .config("spark.driver.memory", "8g") \
              .config("spark.sql.execution.arrow.enabled", "true")\
              .getOrCreate()

# Register the DataFrame as a SQL temporary view
# Using the same names in the original code 
df.createOrReplaceTempView('calls')
cells.createOrReplaceTempView("cells")

#-----------------------------------------------------------------
# Basic processing


# Globals definitions
user_window = Window\
    .partitionBy('msisdn').orderBy('call_datetime')

privacy_filter = 15
missing_value_code = 99999
cutoff_days = 7
max_duration = 21
dates = {'start_date' : dt.datetime(2020,2,1),
            'end_date' : dt.datetime(2020,3,31)}

# Process DF
df = df\
    .withColumn('region', df.location_id)\
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

i1 = transactions(df, 'hour')

i1.toPandas().to_csv(out_path + 'i1.csv', index = False)



#### Indicator 5

    # assert correct frequency

    # result (intra-day od):
    # - get intra-day od matrix using flowminder definition

    # prep (inter-day od):
    # - apply sample period filter
    # - create timestamp lag per user
    # - create day lag per user, with a max calue of 7 days
    # - filter for observations that involve a day change (cause we have intra-day already)
    # - also filter for region changes only, since we are computing od matrix
    # - groupby o(rigin) and (d)estination, and frequency
    # - count observations
    # - apply privacy filter

    # result (joining intra-day od (result) and inter-day od (prep)):
    # - join on o, d, and frequency
    # - fill columns for NA's that arose in merge, so that we have complete columns
    # - compute total od summing intra and inter od count


# Gettting original sql code from
# https://github.com/worldbank/covid-mobile-data/blob/master/cdr-aggregation/notebooks/modules/sql_code_aggregates.py
start_date = "\'2020-02-01\'",
sql_code = { 'directed_regional_pair_connections_per_day' :
"""
      WITH subscriber_locations AS (
          SELECT calls.msisdn,
              calls.call_date,
              cells.region,
              min(calls.call_datetime) AS earliest_visit,
              max(calls.call_datetime) AS latest_visit
          FROM calls
          INNER JOIN cells
              ON calls.location_id = cells.cell_id
          WHERE calls.call_date >= {}
              AND calls.call_date <= CURRENT_DATE
          GROUP BY msisdn, call_date, region
      )
      SELECT * FROM (
          SELECT connection_date,
              region_from,
              region_to,
              count(*) AS subscriber_count
          FROM (
              SELECT t1.call_date AS connection_date,
                  t1.msisdn AS msisdn,
                  t1.region AS region_from,
                  t2.region AS region_to
              FROM subscriber_locations t1
              FULL OUTER JOIN subscriber_locations t2
              ON t1.msisdn = t2.msisdn
                  AND t1.call_date = t2.call_date
              WHERE t1.region <> t2.region
                  AND t1.earliest_visit < t2.latest_visit
          ) AS pair_connections
          GROUP BY 1, 2, 3
      ) AS grouped
      WHERE grouped.subscriber_count >= 15
      """.format(start_date)}


def origin_destination_connection_matrix(df, frequency):
    
    assert frequency == 'day', 'This indicator is only defined for daily frequency'
    
    #  result = self.spark.sql(self.sql_code['directed_regional_pair_connections_per_day'])
    # result = spark_ses.sql(sql_code['directed_regional_pair_connections_per_day'])
    result = spark_ses.sql(sql_code['directed_regional_pair_connections_per_day'])
    
    # prep = df\
    #     .withColumn('call_datetime_lag', F.lag('call_datetime').over(user_window))\
    #     .withColumn('day_lag',
    #       F.when((F.col('call_datetime').cast('long') - \
    #       F.col('call_datetime_lag').cast('long')) <= (self.cutoff_days * 24 * 60 * 60),
    #       F.lag('day').over(user_window))\
    #       .otherwise(F.col('day')))\
    #     .where((F.col('region_lag') != F.col('region')) & ((F.col('day') > F.col('day_lag'))))\
    #     .groupby(frequency, 'region', 'region_lag')\
    #     .agg(F.count(F.col('msisdn')).alias('od_count'))\
    #     .where(F.col('od_count') > self.privacy_filter)
    
    # result = result.join(prep, (prep.region == result.region_to)\
    #                    & (prep.region_lag == result.region_from)\
    #                    & (prep.day == result.connection_date), 'full')\
    #     .withColumn('region_to', F.when(F.col('region_to').isNotNull(),
    #         F.col('region_to')).otherwise(F.col('region')))\
    #     .withColumn('region_from', F.when(F.col('region_from').isNotNull(),
    #         F.col('region_from')).otherwise(F.col('region_lag')))\
    #     .withColumn('connection_date', F.when(F.col('connection_date').isNotNull(),
    #         F.col('connection_date')).otherwise(F.col('day')))\
    #     .na.fill({'od_count' : 0, 'subscriber_count' : 0})\
    #     .withColumn('total_count', F.col('subscriber_count') + F.col('od_count'))\
    #     .drop('region').drop('region_lag').drop('day')
    
    return result

i5 = origin_destination_connection_matrix(df, 'day')

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

i10 = origin_destination_matrix_time(df, 'day')


i10.toPandas().to_csv(out_path + 'i10.csv', index = False)