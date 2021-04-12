#!/usr/bin/env python
# coding: utf-8

# In[1]:


from google.cloud import bigquery
import os
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
import datetime
from google.cloud import bigquery
import pytz
from google.oauth2 import service_account
import numpy as np
import requests
from datetime import datetime as dt
from datetime import date


# In[58]:


os.listdir()


# In[2]:


os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r'.\creds\opendatadbt-f2e674527eb6.json'
client = bigquery.Client()

credentials = ServiceAccountCredentials.from_json_keyfile_name(r'.\creds\opendatadbt-f2e674527eb6.json') # Your json file here


# gc = gspread.authorize(credentials)

# wks = gc.open("NYTimes Mini Crossword times").sheet1
# 

# data = wks.get_all_values()
# headers = data.pop(0)
# 
# df = pd.DataFrame(data, columns=headers)
# 

# df = df.replace(np.NaN, -1)

# # saving a data frame to a buffer (same as with a regular file):
# df.to_csv('nytimescrossword.csv',index=False)

# # NYC

# #Initial Load
# nyc_table_id = "opendatadbt.311.nyc311"
# nyc311 = requests.get("https://data.cityofnewyork.us/resource/erm2-nwe9.json?$limit=10000&$where=created_date >= '2021-03-18T00:00:00.000' OR closed_date >= '2021-03-18T00:00:00.000'" )
# nyc311json = nyc311.json()
# nyc_df = pd.DataFrame(nyc311json)
# #nyc_df = nyc_df[['created_date','closed_date','agency','agency_name','descriptor','incident_zip','incident_address','city','status','latitude','longitude']]

# In[60]:


#Incremental load
nyc_table_id = "opendatadbt.311.nyc311"
current_datetime = dt.today().strftime("%Y-%m-%dT%H:%M:%S.000")
query_string = """
SELECT 
max(created_date), max(closed_date)
FROM `opendatadbt.311.nyc311`
limit 10
"""

nycmaxopendatedataframe = (
    client.query(query_string)
    .result()
    .to_dataframe()
)
nycmaxopendatedate = str(nycmaxopendatedataframe['f0_'][0])

nycmaxclosedatedataframe = (
    client.query(query_string)
    .result()
    .to_dataframe()
)
nycmaxclosedate = str(nycmaxclosedatedataframe['f1_'][0])


# In[61]:


nycmaxopendatedate


# In[62]:



nyc311 = requests.get("https://data.cityofnewyork.us/resource/erm2-nwe9.json?$limit=50000&$where=created_date>'"+nycmaxopendatedate+"' OR closed_date between '" +nycmaxclosedate+ "' AND '" + current_datetime + "'") 
nyc311json = nyc311.json()
cols = ['unique_key','created_date','closed_date','agency','agency_name','descriptor','incident_zip','incident_address','city','status','latitude','longitude']
nyc_df = pd.DataFrame(nyc311json, columns=cols)
nyc_df


# In[63]:


nyc_table_id = "opendatadbt.311.nyc311"
dataset_ref = client.dataset('311')
table_ref = dataset_ref.table('nyc311')
job_config = bigquery.LoadJobConfig(
    write_disposition="WRITE_TRUNCATE",
)

client.load_table_from_dataframe(
    nyc_df, table_ref, job_config=job_config
).result()  # Make an API request.


# In[3]:


nyc_table_id = "opendatadbt.311.nyc311"
current_datetime = dt.today().strftime("%Y-%m-%dT%H:%M:%S.000")
query_string = """
SELECT 
count(*), current_date(), "NYC"
FROM `opendatadbt.311.nyc311`

"""

nycdatacount = (
    client.query(query_string)
    .result()
    .to_dataframe()
)

nycdatacount = nycdatacount.rename(columns={'f0_':'count','f1_':'date','f2_':'city'})

nyc_table_id = "opendatadbt.311.extractrowcount"
dataset_ref = client.dataset('311')
table_ref = dataset_ref.table('extractrowcount')
job_config = bigquery.LoadJobConfig(
    write_disposition="WRITE_APPEND",
)

client.load_table_from_dataframe(
    nycdatacount, table_ref, job_config=job_config
).result()  # Make an API request.


# In[ ]:




