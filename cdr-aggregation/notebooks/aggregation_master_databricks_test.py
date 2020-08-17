# Databricks notebook source
# MAGIC %md
# MAGIC Importing the necessary code:

# COMMAND ----------

# MAGIC %run COVID19DataAnalysis/modules/DataSource 

# COMMAND ----------

# MAGIC %run COVID19DataAnalysis/modules/import_packages

# COMMAND ----------

# MAGIC %run COVID19DataAnalysis/modules/utilities

# COMMAND ----------

# MAGIC %run COVID19DataAnalysis/modules/sql_code_aggregates

# COMMAND ----------

# MAGIC %run COVID19DataAnalysis/modules/outliers

# COMMAND ----------

# MAGIC %run COVID19DataAnalysis/modules/voronoi

# COMMAND ----------

# MAGIC %run COVID19DataAnalysis/modules/tower_clustering

# COMMAND ----------

# MAGIC %run COVID19DataAnalysis/modules/aggregator

# COMMAND ----------

# MAGIC %run COVID19DataAnalysis/modules/priority_aggregator

# COMMAND ----------

# MAGIC %run COVID19DataAnalysis/modules/flowminder_aggregator

# COMMAND ----------

# MAGIC %run COVID19DataAnalysis/modules/custom_aggregator

# COMMAND ----------

# MAGIC %run COVID19DataAnalysis/modules/scaled_aggregator

# COMMAND ----------

# MAGIC %md 
# MAGIC ## 1. Load CDR Data 
# MAGIC - Load from CSV files
# MAGIC - Standardize columns
# MAGIC - Save as parquet file

# COMMAND ----------

# DBTITLE 1,Load Config file for this datasource
# MAGIC %run COVID19DataAnalysis/datasource_config_files/datasource-configure-test

# COMMAND ----------

# DBTITLE 1,Configure and create DataSource object
#Set up the datasource object, and show the config settings
ds = DataSource(datasource_configs)
ds.show_config()

# COMMAND ----------

# #Standardize the csv and save as parque
# ds.standardize_csv_files(show=True)
# ds.save_as_parquet()

# COMMAND ----------

# ds.load_standardized_parquet_file()
# calls = ds.parquet_df

# COMMAND ----------

## Use this in case you want to sample the data and run the code on the sample

# ds.sample_and_save(number_of_ids=1,  filestub = 'sample_temp')
ds.load_sample('sample10')
ds.parquet_df = ds.sample_df


# COMMAND ----------

# ds.parquet_df.count()

# COMMAND ----------

# MAGIC %md ## 3. Load shapefiles of admin regions and tower locations

# COMMAND ----------

ds.load_geo_csvs()

# COMMAND ----------

## Use this in case you want to cluster the towers and create a distance matrix

# ds.create_gpds()
# clusterer = tower_clusterer(ds, 'admin2', 'ID_2')
# ds.admin2_tower_map, ds.distances = clusterer.cluster_towers()
# clusterer = tower_clusterer(ds, 'admin3', 'ADM3_PCODE')

# COMMAND ----------

## Use this in case you want to create a voronoi tesselation

# voronoi = voronoi_maker(ds, 'admin3', 'ADM3_PCODE')
# ds.voronoi = voronoi.make_voronoi()

# COMMAND ----------



# COMMAND ----------

# DBTITLE 1,Aggregation of priority indicators at admin2 level
agg_priority_admin2 = priority_aggregator(result_stub = '/admin2/priority',
                               datasource = ds,
                               re_create_vars  = True, # Needed to use sample parquet
                               regions = 'admin2_tower_map')


agg_priority_admin2.attempt_aggregation(indicators_to_produce = {
  'transactions_per_hour' : ['transactions', 'hour'],
  'origin_destination_connection_matrix_per_day' : ['origin_destination_connection_matrix', 'day'],
  'origin_destination_matrix_time_per_day' : ['origin_destination_matrix_time', 'day']})


# COMMAND ----------

# DBTITLE 1,Aggregation of priority indicators at admin3 level
# agg_priority_admin3 = priority_aggregator(result_stub = '/admin3/priority',
#                             datasource = ds,
#                             regions = 'admin3_tower_map')

# agg_priority_admin3.attempt_aggregation()

# COMMAND ----------

# agg_priority_admin2.calls.show()
# agg_priority_admin2.df.select('msisdn').distinct().show()