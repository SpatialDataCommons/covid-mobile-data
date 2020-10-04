
############# Utility functions used throughout
import os
if os.environ['HOME'] != '/root':
    from modules.import_packages import *
    from modules.DataSource import *
    databricks = False
else:
    databricks = True
    
import subprocess, re

def save_and_load_parquet(df, filename, ds):
    # write parquet
    df.write.mode('overwrite').parquet(filename)
    #load parquet
    df = ds.spark.read.format("parquet").load(filename)
    return df



def run_cmd(args_list):
    
    print('Running the command')
    proc = subprocess.Popen(args_list, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    s_output, s_err = proc.communicate()
    s_return = proc.returncode
    return s_return, s_output, s_err

def get_file(file_list):
    csv_file = [re.search('(/.+)',i).group(1) for i in str(file_list).split("\\n") if re.search('(/.+)\.csv',i)]
    return csv_file


def save_csv(matrix, path, filename):
    # write to csv
    print(path)
    print(filename)
    matrix.repartition(1).write.mode('overwrite').format('com.databricks.spark.csv') \
        .save(os.path.join(path, filename), header = 'true')
    # move one folder up and rename to human-legible .csv name
    

    
    if databricks:
        dbutils.fs.mv(dbutils.fs.ls(path + '/' + filename)[-1].path,
                  path + '/' + filename + '.csv')
        # remove the old folder
        dbutils.fs.rm(path + '/' + filename + '/', recurse = True)

    else:
        print(os.path.join(path, filename))
        #print(glob.glob(os.path.join(path, filename + '/*.csv')))
        
        full_path = os.path.join(path, filename)
        print(full_path)
        ret, out, err = run_cmd(['hdfs', 'dfs', '-ls', os.path.join(path, filename)])
        csv_file = get_file(out)
        _, _, _ = run_cmd(['hdfs', 'dfs', '-mv', csv_file[0] , path + '/' + filename + '.csv'])
        _, _, _ = run_cmd(['hdfs', 'dfs', '-rm', '-r', '-skipTrash', full_path ])
        
        
        """
        os.rename(glob.glob(os.path.join(path, filename + '/*.csv'))[0],
                  os.path.join(path, filename + '.csv'))
        shutil.rmtree(os.path.join(path, filename))
        """
    

############# Windows for window functions

# window by cardnumber
user_window = Window\
    .partitionBy('msisdn').orderBy('call_datetime')

# window by cardnumber starting with last transaction
user_window_rev = Window\
    .partitionBy('msisdn').orderBy(F.desc('call_datetime'))

# user date window
user_date_window = Window\
    .partitionBy('msisdn', 'call_date').orderBy('call_datetime')

# user date window starting from last date
user_date_window_rev = Window\
    .partitionBy('msisdn', 'call_date').orderBy(F.desc('call_datetime'))


############# Plotting

def zero_to_nan(values):
    """Replace every 0 with 'nan' and return a copy."""
    values[ values==0 ] = np.nan
    return values

def fill_zero_dates(pd_df):
    pd_df = pd_df[~pd_df.index.isnull()].sort_index()
    msisdnx = pd.date_range(pd_df.index[0], pd_df.index[-1])
    pd_df = pd_df.reindex(msisdnx, fill_value= 0)
    return pd_df
