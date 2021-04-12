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


# #Initial load chicago_table_id = "opendatadbt.311.chicago311" chicago311 = requests.get("https://data.cityofchicago.org/resource/v6vf-nfxy.json? limit=10000& where=created_date >= '2021-03-18T00:00:00.000'" ) chicago311json = chicago311.json() chicago_df = pd.DataFrame(chicago311json) chicago_df = chicago_df[['created_date','closed_date','owner_department','sr_type','zip_code','street_address','city','status','latitude','longitude']] chicago_df

# In[51]:


#Incremental load
chicago_table_id = "opendatadbt.311.chicago311"
current_datetime = dt.today().strftime("%Y-%m-%dT%H:%M:%S.000")
query_string = """
SELECT 
max(created_date), max(closed_date)
FROM `opendatadbt.311.chicago311`
limit 10
"""

chicagomaxopendatedataframe = (
    client.query(query_string)
    .result()
    .to_dataframe()
)
chicagomaxopendatedate = str(chicagomaxopendatedataframe['f0_'][0])

chicagomaxclosedatedataframe = (
    client.query(query_string)
    .result()
    .to_dataframe()
)
chicagomaxclosedate = str(chicagomaxclosedatedataframe['f1_'][0])


# In[52]:


chicagomaxclosedate


# In[53]:


chicago311 = requests.get("https://data.cityofchicago.org/resource/v6vf-nfxy.json?$limit=50000&$where=created_date > '" + chicagomaxopendatedate +"' OR closed_date between '" +chicagomaxclosedate+ "' AND '" + current_datetime + "'") 
chicago311json = chicago311.json()
cols = ['sr_number','created_date','closed_date','owner_department','sr_type','zip_code','street_address','city','status','latitude','longitude']
chicago_df = pd.DataFrame(chicago311json, columns = cols)

chicago_df


# In[54]:


chicago_table_id = "opendatadbt.311.chicago311"
dataset_ref = client.dataset('311')
table_ref = dataset_ref.table('chicago311')
job_config = bigquery.LoadJobConfig(
    write_disposition="WRITE_TRUNCATE",
)

client.load_table_from_dataframe(
    chicago_df, table_ref, job_config=job_config
).result()  # Make an API request.


# In[4]:


chicago_table_id = "opendatadbt.311.chicago311"
current_datetime = dt.today().strftime("%Y-%m-%dT%H:%M:%S.000")
query_string = """
SELECT 
count(*), current_date(), "Chicago"
FROM `opendatadbt.311.chicago311`

"""

chicagodatacount = (
    client.query(query_string)
    .result()
    .to_dataframe()
)

chicagodatacount = chicagodatacount.rename(columns={'f0_':'count','f1_':'date','f2_':'city'})

chicago_table_id = "opendatadbt.311.extractrowcount"
dataset_ref = client.dataset('311')
table_ref = dataset_ref.table('extractrowcount')
job_config = bigquery.LoadJobConfig(
    write_disposition="WRITE_APPEND",
)

client.load_table_from_dataframe(
    chicagodatacount, table_ref, job_config=job_config
).result()  # Make an API request.


# In[ ]:




