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


# In[2]:


os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r'.\creds\opendatadbt-f2e674527eb6.json'
client = bigquery.Client()

credentials = ServiceAccountCredentials.from_json_keyfile_name(r'.\creds\opendatadbt-f2e674527eb6.json') # Your json file here


# #initial load kc_table_id = "opendatadbt.311.kc311"
# 
# kc311 = requests.get("https://data.kcmo.org/resource/7at3-sxhp.json? limit=10000& where=creation_date >= '2021-03-17T00:00:00.000'" ) kc311json = kc311.json() kc_df = pd.DataFrame(kc311json) kc_df = kc_df[['creation_date', 'closed_date','department','work_group','request_type','zip_code','street_address','status','ycoordinate','xcoordinate']] kc_df

# In[51]:


#Incremental load
nyc_table_id = "opendatadbt.311.kc311"
current_datetime = dt.today().strftime("%Y-%m-%dT%H:%M:%S.000")
query_string = """
SELECT 
max(creation_date), max(closed_date)
FROM `opendatadbt.311.kc311`
limit 10
"""

kcmaxopendatedataframe = (
    client.query(query_string)
    .result()
    .to_dataframe()
)
kcmaxopendatedate = str(kcmaxopendatedataframe['f0_'][0])

kcmaxclosedatedataframe = (
    client.query(query_string)
    .result()
    .to_dataframe()
)
kcmaxclosedate = str(kcmaxclosedatedataframe['f1_'][0])


# In[52]:



kc311 = requests.get("https://data.kcmo.org/resource/7at3-sxhp.json?$limit=50000&$where=creation_date>'"+kcmaxopendatedate+"' OR closed_date between '" +kcmaxclosedate+ "' AND '" + current_datetime + "'") 
kc311json = kc311.json()


# In[53]:


cols = ['case_id','creation_date', 'closed_date','department','work_group','request_type','zip_code','street_address','status','ycoordinate','xcoordinate']
kc_df = pd.DataFrame(kc311json, columns=cols)
kc_df


# In[54]:


kc_table_id = "opendatadbt.311.kc311"
dataset_ref = client.dataset('311')
table_ref = dataset_ref.table('kc311')
job_config = bigquery.LoadJobConfig(
    write_disposition="WRITE_TRUNCATE",
)

client.load_table_from_dataframe(
    kc_df, table_ref, job_config=job_config
).result()  # Make an API request.


# In[4]:


kc_table_id = "opendatadbt.311.kc311"
current_datetime = dt.today().strftime("%Y-%m-%dT%H:%M:%S.000")
query_string = """
SELECT 
count(*), current_date(), "KC"
FROM `opendatadbt.311.kc311`

"""

kcdatacount = (
    client.query(query_string)
    .result()
    .to_dataframe()
)

kcdatacount = kcdatacount.rename(columns={'f0_':'count','f1_':'date','f2_':'city'})

kc_table_id = "opendatadbt.311.extractrowcount"
dataset_ref = client.dataset('311')
table_ref = dataset_ref.table('extractrowcount')
job_config = bigquery.LoadJobConfig(
    write_disposition="WRITE_APPEND",
)

client.load_table_from_dataframe(
    kcdatacount, table_ref, job_config=job_config
).result()  # Make an API request.


# In[ ]:




