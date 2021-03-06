
#-----------------------------------------------------------------#
# Exploratory analysis
#-----------------------------------------------------------------#
#-----------------------------------------------------------------#
#### Settings

from globals import *

#-----------------------------------------------------------------#
#### Set file paths

DATA_GIS = DATA_path + 'proof-of-concept/geo_files/'
INDICATORS_path = DATA_path + "proof-of-concept/panel_indicators/clean/"

#-----------------------------------------------------------------#
#### Load data

i1 = pd.read_csv(INDICATORS_path + 'i1_3.csv') # Number of calls
i3 = pd.read_csv(INDICATORS_path + 'i3_3.csv') # number of users
i5 = pd.read_csv(INDICATORS_path + 'i5_3.csv') # orgin and destination 
i52 = pd.read_csv(INDICATORS_path + 'i5_2.csv') # orgin and destination 

i7 = pd.read_csv(INDICATORS_path + 'i7_3.csv') # distance travelled

#-----------------------------------------------------------------#
#### Aggregate data at the country level

i1_agg = i1\
        .groupby('date')\
        .agg({'count' : np.sum})\
        .reset_index()\
        .sort_values('date')

i3_agg = i3\
        .groupby('date')\
        .agg({'count' : np.sum})\
        .reset_index()\
        .sort_values('date')\
        .rename(columns = {'count': 'subs'})

# Add number of subscribers to indicator 1 aggregated data
i1_agg = i1_agg.merge(i3_agg, on = 'date')
i1_agg['calls_p'] = i1_agg['count']/i1_agg['subs']


# OD matrix aggregated data
i5_agg = i5\
        .groupby('date')\
        .agg({'subscriber_count' : np.mean,
              'total_count' : np.sum,
              'region_to': pd.Series.nunique,
              'region_from': pd.Series.nunique})\
        .reset_index()\
        .sort_values('date')

i5_agg = i5_agg.merge(i3_agg, on = 'date')
i5_agg['moves_p_sub'] = i5_agg['subscriber_count']/i5_agg['subs']


#-----------------------------------------------------------------#
# Comparisson between pre and post lockdown stats  

# Pre-post lockdown variables
lockdown_date = np.datetime64(dt.date(2020, 3, 27))

i1['post'] = (i1['date'].astype('datetime64') > lockdown_date).astype(int)
i3['post'] = (i3['date'].astype('datetime64') > lockdown_date).astype(int)
i5['post'] = (i5['date'].astype('datetime64') > lockdown_date).astype(int)
i7['post'] = (i7['date'].astype('datetime64') > lockdown_date).astype(int)

i1_agg['post'] = (i1_agg['date'].astype('datetime64') > lockdown_date).astype(int)
i5_agg['post'] = (i5_agg['date'].astype('datetime64') > lockdown_date).astype(int)
i7['post'] = (i7['date'].astype('datetime64') > lockdown_date).astype(int)

# Number of calls per user
i1_agg['calls_p'].mean()
i1_agg['calls_p'][i1_agg['post'] == 0].mean()
i1_agg['calls_p'][i1_agg['post'] == 1].mean()

# Number of districts visited?
i5_agg['moves_p_sub'].mean()
i5_agg['moves_p_sub'][i5_agg['post'] == 0].mean()
i5_agg['moves_p_sub'][i5_agg['post'] == 1].mean()

# Average distance travelled
i7['mean_distance'].mean()

i7['mean_distance'][i7['post'] == 0].mean()
i7['mean_distance'][i7['post'] == 1].mean()

# Number of wards
i5['subscriber_count'].mean()
i5['subscriber_count'][i5['post'] == 0].sum()
i5['subscriber_count'][i5['post'] == 1].mean()

# Distance travelled
i7['mean_distance'].mean()
i7['mean_distance'][i7['post'] == 0].mean()
i7['mean_distance'][i7['post'] == 1].mean()


#-----------------------------------------------------------------#
# Plot number of regions that received visitors per day  

import plotly.express as px

fig = px.line(i5_agg, x="date", y="region_to")
fig.show()


#-----------------------------------------------------------------#
# Compare regions that received and sent visitors  

import plotly.graph_objects as go

# set up plotly figure
fig = go.Figure()

# add line / trace 1 to figure
fig.add_trace(go.Scatter(
    x=i5_agg['date'],
    y=i5_agg['region_to'],
    marker=dict(
        color="blue"
    )))
fig.add_trace(go.Scatter(
    x=i5_agg['date'],
    y=i5_agg['region_from'],
    marker=dict(
        color="red"
    )))

fig.show()



