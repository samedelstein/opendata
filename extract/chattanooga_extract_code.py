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


# chattanooga_table_id = "opendatadbt.311.chattanooga311"
# 
# #Inital Load
# chattanooga311 = requests.get("https://www.chattadata.org/resource/8qb9-5fja.json?$limit=10000&$where=created_date >= '2021-03-22'")
# chattanooga311json = chattanooga311.json()
# chattanooga_df = pd.json_normalize(chattanooga311json)
# chattanooga_df_parsed = chattanooga_df['publiclocation.coordinates'].apply(pd.Series)
# chattanooga_df_cleaned = pd.concat([chattanooga_df.drop(['publiclocation.coordinates'], axis=1), chattanooga_df_parsed], axis=1)
# mapping = {chattanooga_df_cleaned.columns[-1]:'latitude', chattanooga_df_cleaned.columns[-2]:'longitude',chattanooga_df_cleaned.columns[-3]:'publiclocationtype'}
# chattanooga_df_cleaned = chattanooga_df_cleaned.rename(columns=mapping)
# chattanooga_df_cleaned = chattanooga_df_cleaned.drop(['publiclocation.type'], axis=1)
# cols = ['service_request_key','created_date','completed_at','department','request_type','status_code','latitude','longitude']
# chattanooga_df_cleaned = pd.DataFrame(chattanooga_df_cleaned,columns = cols)
# chattanooga_df_cleaned

# In[43]:


#Incremental load
chattanooga_table_id = "opendatadbt.311.chattanooga311"
current_datetime = dt.today().strftime("%Y-%m-%dT%H:%M:%S.000")
query_string = """
SELECT 
max(created_date), max(completed_at)
FROM `opendatadbt.311.chattanooga311`
limit 10
"""

chattanoogamaxopendatedataframe = (
    client.query(query_string)
    .result()
    .to_dataframe()
)
chattanoogamaxopendatedate = str(chattanoogamaxopendatedataframe['f0_'][0])

chattanoogamaxclosedatedataframe = (
    client.query(query_string)
    .result()
    .to_dataframe()
)
chattanoogamaxclosedate = str(chattanoogamaxclosedatedataframe['f1_'][0])


# In[44]:


#Incremental Load

chattanooga311 = requests.get("https://www.chattadata.org/resource/8qb9-5fja.json?$limit=50000&$where=created_date>'"+chattanoogamaxopendatedate+"' OR completed_at between '" +chattanoogamaxclosedate+ "' AND '" + current_datetime + "'")
chattanooga311json = chattanooga311.json()
chattanooga_df = pd.json_normalize(chattanooga311json)
chattanooga_df_parsed = chattanooga_df['publiclocation.coordinates'].apply(pd.Series)
chattanooga_df_cleaned = pd.concat([chattanooga_df.drop(['publiclocation.coordinates'], axis=1), chattanooga_df_parsed], axis=1)
mapping = {chattanooga_df_cleaned.columns[-1]:'latitude', chattanooga_df_cleaned.columns[-2]:'longitude',chattanooga_df_cleaned.columns[-3]:'publiclocationtype'}
chattanooga_df_cleaned = chattanooga_df_cleaned.rename(columns=mapping)
chattanooga_df_cleaned = chattanooga_df_cleaned.drop(['publiclocationtype'], axis=1)
cols = ['service_request_key','created_date','completed_at','department','request_type','status_code','latitude','longitude']
chattanooga_df_cleaned = pd.DataFrame(chattanooga_df_cleaned,columns = cols)
chattanooga_df_cleaned


# In[ ]:





# In[45]:


chattanooga_table_id = "opendatadbt.311.chattanooga311"
dataset_ref = client.dataset('311')
table_ref = dataset_ref.table('chattanooga311')
job_config = bigquery.LoadJobConfig(
    write_disposition="WRITE_TRUNCATE",
)

client.load_table_from_dataframe(
    chattanooga_df_cleaned, table_ref, job_config=job_config
).result()  # Make an API request.


# In[4]:


chattanooga_table_id = "opendatadbt.311.chattanooga311"
current_datetime = dt.today().strftime("%Y-%m-%dT%H:%M:%S.000")
query_string = """
SELECT 
count(*), current_date(), "Chattanooga"
FROM `opendatadbt.311.chattanooga311`

"""

chattanoogadatacount = (
    client.query(query_string)
    .result()
    .to_dataframe()
)

chattanoogadatacount = chattanoogadatacount.rename(columns={'f0_':'count','f1_':'date','f2_':'city'})

chattanooga_table_id = "opendatadbt.311.extractrowcount"
dataset_ref = client.dataset('311')
table_ref = dataset_ref.table('extractrowcount')
job_config = bigquery.LoadJobConfig(
    write_disposition="WRITE_APPEND",
)

client.load_table_from_dataframe(
    chattanoogadatacount, table_ref, job_config=job_config
).result()  # Make an API request.


# In[ ]:




