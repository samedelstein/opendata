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


# #Initial load 
# littlerock_table_id = "opendatadbt.311.littlerock311"
# 
# littlerock311 = requests.get("https://data.littlerock.gov/resource/2x6n-j9fb.json?$limit=10000&$where=ticket_created_date_time>='2021-03-22T00:00:00.000'") 
# littlerockjson = littlerock311.json()
# littlerock_df_parsed = littlerock_df['geocoded_column'].apply(pd.Series)
# littlerock_df_parsed = littlerock_df_parsed.drop(littlerock_df_parsed.columns[[0,1]], axis=1)
# littlerock_df_cleaned = pd.concat([littlerock_df.drop(['geocoded_column'], axis=1), littlerock_df_parsed], axis=1)
# littlerock_df_cleaned
# cols = ['ticket_id','ticket_created_date_time','ticket_closed_date_time','issue_type','issue_sub_category','human_address','ticket_status','latitude','longitude']
# littlerock_df_cleaned = pd.DataFrame(littlerock_df_cleaned, columns = cols)

# In[59]:


#Incremental load
littlerock_table_id = "opendatadbt.311.littlerock311"
current_datetime = dt.today().strftime("%Y-%m-%dT%H:%M:%S.000")
query_string = """
SELECT 
max(ticket_created_date_time), max(ticket_closed_date_time)
FROM `opendatadbt.311.littlerock311`
limit 10
"""

littlerockmaxopendatedataframe = (
    client.query(query_string)
    .result()
    .to_dataframe()
)
littlerockmaxopendatedate = str(littlerockmaxopendatedataframe['f0_'][0])

littlerockmaxclosedatedataframe = (
    client.query(query_string)
    .result()
    .to_dataframe()
)
littlerockmaxclosedate = str(littlerockmaxclosedatedataframe['f1_'][0])


# In[60]:


littlerockmaxclosedate


# In[61]:



littlerock_table_id = "opendatadbt.311.littlerock311"

littlerock311 = requests.get("https://data.littlerock.gov/resource/2x6n-j9fb.json?$limit=10000&$where=ticket_created_date_time>'"+littlerockmaxopendatedate+"' OR ticket_closed_date_time between '" +littlerockmaxclosedate+ "' AND '" + current_datetime + "'") 
littlerockjson = littlerock311.json()
littlerock_df = pd.DataFrame(littlerockjson)
littlerock_df_parsed = littlerock_df['geocoded_column'].apply(pd.Series)
littlerock_df_parsed = littlerock_df_parsed.drop(littlerock_df_parsed.columns[[0,1]], axis=1)
littlerock_df_cleaned = pd.concat([littlerock_df.drop(['geocoded_column'], axis=1), littlerock_df_parsed], axis=1)
littlerock_df_cleaned
cols = ['ticket_id','ticket_created_date_time','ticket_closed_date_time','issue_type','issue_sub_category','human_address','ticket_status','latitude','longitude']
littlerock_df_cleaned = pd.DataFrame(littlerock_df_cleaned, columns = cols)


# In[62]:


littlerock_df_cleaned


# In[63]:


littlerock_table_id = "opendatadbt.311.littlerock311"
dataset_ref = client.dataset('311')
table_ref = dataset_ref.table('littlerock311')
job_config = bigquery.LoadJobConfig(
    write_disposition="WRITE_TRUNCATE",
)

client.load_table_from_dataframe(
    littlerock_df_cleaned, table_ref, job_config=job_config
).result()  # Make an API request.


# In[3]:


littlerock_table_id = "opendatadbt.311.littlerock311"
current_datetime = dt.today().strftime("%Y-%m-%dT%H:%M:%S.000")
query_string = """
SELECT 
count(*), current_date(), "Little Rock"
FROM `opendatadbt.311.littlerock311`

"""

littlerockdatacount = (
    client.query(query_string)
    .result()
    .to_dataframe()
)

littlerockdatacount = littlerockdatacount.rename(columns={'f0_':'count','f1_':'date','f2_':'city'})

littlerock_table_id = "opendatadbt.311.extractrowcount"
dataset_ref = client.dataset('311')
table_ref = dataset_ref.table('extractrowcount')
job_config = bigquery.LoadJobConfig(
    write_disposition="WRITE_APPEND",
)

client.load_table_from_dataframe(
    littlerockdatacount, table_ref, job_config=job_config
).result()  # Make an API request.


# In[ ]:




