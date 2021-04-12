#!/usr/bin/env python
# coding: utf-8

# In[2]:


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


# In[3]:


os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r'.\creds\opendatadbt-f2e674527eb6.json'
client = bigquery.Client()

credentials = ServiceAccountCredentials.from_json_keyfile_name(r'.\creds\opendatadbt-f2e674527eb6.json') # Your json file here


# #Initial Load
# 
# austin311 = requests.get("https://data.austintexas.gov/resource/xwdj-i9he.json?$limit=50000&$where=sr_created_date>'2021-03-24'" ) 
# austin311json = austin311.json()
# cols = ['sr_number','sr_created_date','sr_closed_date','sr_type_desc','sr_location', 'sr_location_zip_code','sr_location_city','sr_status_desc','sr_location_lat','sr_location_long']
# austin_df = pd.DataFrame(austin311json, columns=cols)
# austin_df

# In[3]:


#Incremental load
austin_table_id = "opendatadbt.311.austin311"
current_datetime = dt.today().strftime("%Y-%m-%dT%H:%M:%S.000")
query_string = """
SELECT 
max(sr_created_date), max(sr_closed_date)
FROM `opendatadbt.311.austin311`
limit 10
"""

austinmaxopendatedataframe = (
    client.query(query_string)
    .result()
    .to_dataframe()
)
austinmaxopendatedate = str(austinmaxopendatedataframe['f0_'][0])

austinmaxclosedatedataframe = (
    client.query(query_string)
    .result()
    .to_dataframe()
)
austinmaxclosedate = str(austinmaxclosedatedataframe['f1_'][0])


# In[4]:



austin311 = requests.get("https://data.austintexas.gov/resource/xwdj-i9he.json?$limit=50000&$where=sr_created_date>'"+austinmaxopendatedate+"' OR sr_closed_date between '" + austinmaxclosedate+ "' AND '" + current_datetime + "'") 
austin311json = austin311.json()
cols = ['sr_number','sr_created_date','sr_closed_date','sr_type_desc','sr_location', 'sr_location_zip_code','sr_location_city','sr_status_desc','sr_location_lat','sr_location_long']
austin_df = pd.DataFrame(austin311json, columns=cols)
austin_df


# In[51]:


austin_table_id = "opendatadbt.311.austin311"
dataset_ref = client.dataset('311')
table_ref = dataset_ref.table('austin311')
job_config = bigquery.LoadJobConfig(
    write_disposition="WRITE_TRUNCATE",
)

client.load_table_from_dataframe(
    austin_df, table_ref, job_config=job_config
).result()  # Make an API request.


# In[14]:


austin_table_id = "opendatadbt.311.austin311"
current_datetime = dt.today().strftime("%Y-%m-%dT%H:%M:%S.000")
query_string = """
SELECT 
count(*), current_date(), "Austin"
FROM `opendatadbt.311.austin311`

"""

austindatacount = (
    client.query(query_string)
    .result()
    .to_dataframe()
)

austindatacount = austindatacount.rename(columns={'f0_':'count','f1_':'date','f2_':'city'})

austin_table_id = "opendatadbt.311.extractrowcount"
dataset_ref = client.dataset('311')
table_ref = dataset_ref.table('extractrowcount')
job_config = bigquery.LoadJobConfig(
    write_disposition="WRITE_APPEND",
)

client.load_table_from_dataframe(
    austindatacount, table_ref, job_config=job_config
).result()  # Make an API request.


# In[ ]:




