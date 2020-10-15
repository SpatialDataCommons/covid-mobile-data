# Aggregation of CDR Data (for running on HDP/Hive)

This code is an extension of cdr-aggregation under [worldbank/covid-mobile-data](https://github.com/worldbank/covid-mobile-data/tree/master/cdr-aggregation). It specifically customized to allow computing the indicators on the data resisted on Hive table under Hortonworks Data Platform (HDP).


### Table of contents

* [Required Software](#required-software)
* [Environment Preparation](#environment-preparation)
* [Create Work folder in HDFS](#create-work-folder-in-hdfs)
* [Prepare necessary data](#prepare-necessary-data)
* [Access to jupyter notebook](#access-to-jupyter-notebook)
* [Initial setup](#initial-setup)
* [Run script for indicators](#run-script-for-indicators)
* [Indicator results](#indicator-results)


## Required Software
* Hortonwork Data Platform (HDP) 2.6.5.1100
* Spark 2.3.0 (Build-in HDP)
* Spark client, hive client on the machine (one of slave node in cluster)
* Github: [covid-mobile-data-master/cdr-aggregation-hive] software
## Environment Preparation
* Create new user: `cdrspark`
  * Make cdrspark user as sudoer
  * Group: `hdfs`
  * Home path: /home/cdrspark
* Change user to “cdrspark”
  * su cdrspark
* Install related libraries
  * `sudo yum install sqlite-devel openssl-devel bzip2-devel libffi-devel xz-devel`
  * Download and install spatial index
    ```
    wget https://download-ib01.fedoraproject.org/pub/epel/7/x86_64/Packages/s/spatialindex-1.8.5-1.el7.x86_64.rpm
    sudo rpm -Uvh spatialindex-1.8.5-1.el7.x86_64.rpm
    ```
  * Download and install zlib
    ```
    wget http://mirror.centos.org/centos/7/os/x86_64/Packages/zlib-devel-1.2.7-18.el7.x86_64.rpm
    sudo rpm -Uvh zlib-devel-1.2.7-18.el7.x86_64.rpm
    ```
  * Install Python 3.7
    ```
    wget https://www.python.org/ftp/python/3.7.9/Python-3.7.9.tgz
    tar xvf Python-3.7.9.tgz
    cd Python-3.7*/
    ./configure --enable-loadable-sqlite-extensions --enable-optimizations
    make altinstall (python3.7, pip3.7)
    ```
  * Install, create and access virtual environment
    ```
    pip3.7 install virtualenv
    virtualenv virtualenv37 (/home/cdrspark)
    source virtualenv37/bin/activate
    ```
  * Install necessary python libraries under virtualenv
    ```
    pip3.7 install pandas sklearn pyspark findspark numpy shapely matplotlib seaborn  geopandas Rtree geovoronoi
    ```
  * Install jupyter notebook
    ```
    pip3.7 install jupyter
    ```
  * Export environment parameters
    ```
    export SPARK_HOME=/usr/hdp/current/spark2-client
    export PYTHONPATH=$SPARK_HOME/python/:$PYTHONPATH
    export PYTHONPATH=$SPARK_HOME/python/lib/py4j-0.10.6-src.zip:$PYTHONPATH
    ```
  * Extract cdr-aggregation-hive to /home/cdrspark
    ex: /home/cdrspark/covid-mobile-data-master/cdr-aggregation-hive

## Create Work folder in HDFS
  * Run the command to create **`/work`** in hdfs
    ```
    hdfs dfs -mkdir /work
    hdfs dfs -mkdir -p /work/data/results/country_code/telecom_alias
    hdfs dfs -mkdir -p /work/data/standardized/country_code/telecom_alias
    hdfs dfs -mkdir -p /work/data/support-data/country_code/telecom_alias/geofiles
    hdfs dfs -mkdir -p /work/data/tempfiles/country_code/telecom_alias
    hdfs dfs -mkdir -p /work/data/new/country_code/telecom_alias

    * change "country_code" to code of country such as JP, MZ, US
    * change "telecom_alias" to short code of mobile operator
    ```

## Prepare necessary data
  * **Cell tower** shape file (CSV with WKT format) 
    * CSV file header (geometry, cell_id, LAT, LNG)
    * File e.g: basestation_wkt.csv
    * cell_id must be digit and unique
  * **Admin** shape file (Admin 2 & Admin 3 in CSV with WKT format)
    * WKT column header name should be specified as `geometry` including `admin ID` and `admin name`.
    * File e.g: admin2_wkt.csv, admin3_wkt.csv
  * **CDR data** in Hive
    * create table calls (msisdn string,call_datetime string,latitude string,longitude string,location_id string, call_date string).
    * Make sure to have data in the table.
  * Copy Cell tower shape file and Admin shape file data to the following directory in HDFS 
    * HDFS directory: `/work/data/support-data/country_code/telecom_alias/geofiles/`
    * change `country_code` to code of country such as JP, MZ, US
    * change `telecom_alias` to short code of mobile operator such as mno1,

## Access to jupyter notebook
  * Goto `/home/cdrspark`
  * source virtualenv37/bin/activate
  * Goto “/home/cdrspark/covid-mobile-data-master/cdr-aggregation-hive”
  * Run command as ‘jupyter notebook’ to start notebook
  * Copy the jupyter notebook url to the web browser to access the python script

  ```
    cd /home/cdrspark
    source virtualenv37/bin/activate
    cd /home/cdrspark/covid-mobile-data-master/cdr-aggregation-hive
    jupyter notebook
  ```
## Initial setup 
  * Access to **config_file_hive.py**
    * **`hive_warehouse_location`** Specify path of hive warehouse. Normally `/apps/hive/warehouse`
    * **`hive_metastore_uris`** specify thrift service uris of the cluster. Ex: `thrift://<IPaddress>:9083`
    * **`spark_mode`** Change spark mode to hive
    * **`hive_vars`** Adjust table according to your structure.
    * **`country_code`** change country code => e.g. moz 
    * **`telecom_alias`** change  telecom alias => e.g. mno 
    * **`geofiles`** specify 'tower_sites': 'basestation_wkt.csv', 'admin2' : 'admin2_wkt.csv', 'admin3' : 'admin3_wkt.csv'
    * **`dates`** specify date range of data

      ```
      datasource_configs = {
        "base_path": "path_to_folder/data", #no need to change
        "hive_warehouse_location": "/apps/hive/warehouse",
        "hive_metastore_uris": "thrift://<IPaddress>:9083", 
        "spark_mode": 'hive',
        "hive_vars":{ 'msisdn' : 'col1',
                      'call_datetime': 'col2',
                      'location_id': 'col3',
                      'calls': 'table'},
        "country_code": "cc",
        "telecom_alias": "mno",
        "schema" : schema,
        "data_paths" : ["*.csv"],
        "filestub": "",
        "geofiles": {},
        "shapefiles": ['admin2','admin3', 'voronoi'],
        "dates": {'start_date' : dt.datetime(2020,2,1),
                  'end_date' : dt.datetime(2020,3,31)}
      }
      ```
  
  * Verify hive connection and check data
    * Open and run **`test_spark_hive.ipynb`** to check data connection to the hive. 


## Run script for indicators
  * Run **`aggregation_master_hive.ipynb`** script to compute the indicator at admin2, admin3 and voronoi level
    ```
    ~/cdr-aggregation-hdp-hive/notebooks/aggregation_master_hive.ipynb
    ```

## Indicators results
  * Indicators results are stored in the **`/work/data/results/country_code/telecom_alias`** directory in **HDFS**
  * Result for **admin2** level 
    ```
    /work/data/results/country_code/telecom_alias/admin2/flowminder
    /work/data/results/country_code/telecom_alias/admin2/priority
    ```
  * Result for **admin3** level 
       ```
      /work/data/results/country_code/telecom_alias/admin3/flowminder
      /work/data/results/country_code/telecom_alias/admin3/priority
      ```
  * Result for **voronoi** level 
       ```
      /work/data/results/country_code/telecom_alias/voronoi/priority
      ```
  * **`country_code`** and **`telecom_alias`** should be replaced with the one in **`config_file_hive.py`**

