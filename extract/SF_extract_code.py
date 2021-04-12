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


# #Initial Load boston_table_id = "opendatadbt.311.boston311"
# 
# sf311 = requests.get("https://data.sfgov.org/resource/vw6y-z8j6.json?$limit=50000&$where=requested_datetime>'2021-03-24'" ) 
# sf311json = sf311.json()
# cols = ['service_request_id','requested_datetime','closed_date','agency_responsible','service_name','service_details','address','neighborhoods_sffind_boundaries','status_description','lat','long']
# sf_df = pd.DataFrame(sf311json, columns=cols)
# sf_df

# In[43]:


#Incremental load
sf_table_id = "opendatadbt.311.sf311"
current_datetime = dt.today().strftime("%Y-%m-%dT%H:%M:%S.000")
query_string = """
SELECT 
max(requested_datetime), max(closed_date)
FROM `opendatadbt.311.sf311`
limit 10
"""

sfmaxopendatedataframe = (
    client.query(query_string)
    .result()
    .to_dataframe()
)
sfmaxopendatedate = str(sfmaxopendatedataframe['f0_'][0])

sfmaxclosedatedataframe = (
    client.query(query_string)
    .result()
    .to_dataframe()
)
sfmaxclosedate = str(sfmaxclosedatedataframe['f1_'][0])


# In[44]:



sf311 = requests.get("https://data.sfgov.org/resource/vw6y-z8j6.json?$limit=50000&$where=requested_datetime>'"+sfmaxopendatedate+"' OR closed_date between '" + sfmaxclosedate+ "' AND '" + current_datetime + "'") 
sf311json = sf311.json()
cols = ['service_request_id','requested_datetime','closed_date','agency_responsible','service_name','service_details','address','neighborhoods_sffind_boundaries','status_description','lat','long']
sf_df = pd.DataFrame(sf311json, columns=cols)
sf_df


# In[45]:


sf_table_id = "opendatadbt.311.sf311"
dataset_ref = client.dataset('311')
table_ref = dataset_ref.table('sf311')
job_config = bigquery.LoadJobConfig(
    write_disposition="WRITE_TRUNCATE",
)

client.load_table_from_dataframe(
    sf_df, table_ref, job_config=job_config
).result()  # Make an API request.


# In[3]:


SF_table_id = "opendatadbt.311.sf311"
current_datetime = dt.today().strftime("%Y-%m-%dT%H:%M:%S.000")
query_string = """
SELECT 
count(*), current_date(), "SF"
FROM `opendatadbt.311.sf311`

"""

sfdatacount = (
    client.query(query_string)
    .result()
    .to_dataframe()
)

sfdatacount = sfdatacount.rename(columns={'f0_':'count','f1_':'date','f2_':'city'})

sf_table_id = "opendatadbt.311.extractrowcount"
dataset_ref = client.dataset('311')
table_ref = dataset_ref.table('extractrowcount')
job_config = bigquery.LoadJobConfig(
    write_disposition="WRITE_APPEND",
)

client.load_table_from_dataframe(
    sfdatacount, table_ref, job_config=job_config
).result()  # Make an API request.


# In[ ]:





# In[ ]:




