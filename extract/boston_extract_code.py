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


# #Initial Load boston_table_id = "opendatadbt.311.boston311"
# 
# boston311 = requests.get("https://data.boston.gov/api/3/action/datastore_search_sql?sql=SELECT%20*%20from%20%22f53ebccd-bc61-49f9-83db-625f209c95f5%22%20WHERE%20%22open_dt%22%20%3E=%20%272021-03-22%27" ) 
# boston311json = boston311.json() 
# boston_df = pd.json_normalize(boston311json['result']['records']) 
# boston_df
# 
# 

# In[41]:


#Incremental load
boston_table_id = "opendatadbt.311.boston311"
current_datetime = dt.today().strftime("%Y-%m-%dT%H:%M:%S.000")

query_string = """
SELECT 
max(open_dt), max(closed_dt)
FROM `opendatadbt.311.boston311`
limit 10
"""

bostonmaxopendatedataframe = (
    client.query(query_string)
    .result()
    .to_dataframe()
)
bostonmaxopendatedate = str(bostonmaxopendatedataframe['f0_'][0])

bostonmaxclosedatedataframe = (
    client.query(query_string)
    .result()
    .to_dataframe()
)
bostonmaxclosedate = str(bostonmaxclosedatedataframe['f1_'][0])


# In[42]:


boston311 = requests.get("https://data.boston.gov/api/3/action/datastore_search_sql?sql=SELECT%20*%20from%20%22f53ebccd-bc61-49f9-83db-625f209c95f5%22%20WHERE%20%22open_dt%22>%20%27" + bostonmaxopendatedate +"%27 OR closed_dt between '" +bostonmaxclosedate+ "' AND '" + current_datetime + "'") 
boston311json = boston311.json()
cols = ['case_enquiry_id','open_dt','closed_dt','department','subject','case_title','location_zipcode','location','neighborhood','case_status','latitude','longitude']
boston_df = pd.json_normalize(boston311json['result']['records'])
boston_df = pd.DataFrame(boston_df, columns = cols)
boston_df


# In[43]:



boston_table_id = "opendatadbt.311.boston311"
dataset_ref = client.dataset('311')
table_ref = dataset_ref.table('boston311')
job_config = bigquery.LoadJobConfig(
    write_disposition="WRITE_TRUNCATE",
)

client.load_table_from_dataframe(
    boston_df, table_ref, job_config=job_config
).result()  # Make an API request.


# In[5]:


boston_table_id = "opendatadbt.311.boston311"
current_datetime = dt.today().strftime("%Y-%m-%dT%H:%M:%S.000")
query_string = """
SELECT 
count(*), current_date(), "Boston"
FROM `opendatadbt.311.boston311`

"""

bostondatacount = (
    client.query(query_string)
    .result()
    .to_dataframe()
)

bostondatacount = bostondatacount.rename(columns={'f0_':'count','f1_':'date','f2_':'city'})

boston_table_id = "opendatadbt.311.extractrowcount"
dataset_ref = client.dataset('311')
table_ref = dataset_ref.table('extractrowcount')
job_config = bigquery.LoadJobConfig(
    write_disposition="WRITE_APPEND",
)

client.load_table_from_dataframe(
    bostondatacount, table_ref, job_config=job_config
).result()  # Make an API request.


# In[ ]:





# In[ ]:




