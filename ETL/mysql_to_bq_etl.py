#!/usr/bin/env python
# coding: utf-8

# In[4]:


import pandas as pd
import numpy as np
import datetime
import db_dtypes
from google.cloud import bigquery
from google.oauth2 import service_account


# In[5]:


import warnings
warnings.filterwarnings("ignore")


# In[52]:


import subprocess
import sys

def install(package):
    subprocess.check_call([sys.executable, "-m", "pip", "install", package])


# In[53]:


install("pymysql")


# In[6]:


import pymysql

# Connect to the database
connection = pymysql.connect(host='104.196.48.95',
                         user='root',
                         password='Nilotpal#810',
                         db='sales')

# create cursor
cursor=connection.cursor()


# In[7]:


df_consumer_master=pd.read_sql(f'''select * from customer_master''',connection)
df_product_master=pd.read_sql(f'''select * from product_master''',connection)
df_order_details=pd.read_sql(f'''select * from order_details''',connection)
df_order_items=pd.read_sql(f'''select * from order_items''',connection)


# In[29]:


df_order_items.head()


# In[8]:


def insert_dim_order(df_order_details):
    g = df_order_details
    x = g[["orderid","order_status_update_timestamp","order_status"]]
    dim_order = x.reset_index()
    del dim_order["index"]
    return dim_order


# In[9]:


def insert_dim_customer(df_consumer_master):
    df_consumer_master["address_id"] = list(range(1,1001))
    dim_customer =df_consumer_master[["customerid","name","address_id"]]
    dim_customer["start_date"]= (df_consumer_master['update_timestamp'].dt.date).astype('datetime64')
    dim_customer["end_date"] = np.nan
    dim_customer["end_date"]=pd.to_datetime(dim_customer["end_date"])
    return dim_customer


# In[10]:


def insert_dim_address(df_consumer_master):
    df_consumer_master["address_id"] = list(range(1,1001))
    dim_address = df_consumer_master[["address_id","address","city","state","pincode"]]
    return dim_address


# In[11]:


def insert_dim_product(df_product_master):
    dim_product = df_product_master
    dim_product["start_date"]=np.nan
    dim_product["start_date"]=pd.to_datetime(dim_product["start_date"])
    dim_product["end_date"]=np.nan
    dim_product["end_date"]=pd.to_datetime(dim_product["end_date"])
    return dim_product


# In[12]:


def insert_f_order_details(df_order_details,df_order_items):
    f_order_details = pd.merge(df_order_details.groupby("orderid").tail(1), df_order_items, how='inner')[["orderid","order_status_update_timestamp","productid","quantity"]]
    f_order_details.columns = ["orderid","order_delivery_timestamp","productid","quantity"]
    return f_order_details


# In[39]:


def insert_fact_daily_orders(df_consumer_master,df_order_details,df_order_items):
    x=df_order_details.groupby("orderid").head(1)[["customerid","orderid","order_status_update_timestamp"]].reset_index()
    del x["index"]
    y=df_order_details.groupby("orderid").tail(1)["order_status_update_timestamp"].reset_index()
    del y["index"]     
    fact_daily_orders = pd.concat([x,y],axis=1)
    fact_daily_orders.columns = ["customerid","orderid","order_received_timestamp","order_delivery_timestamp"]
    l=[]
    for i in fact_daily_orders["customerid"]:
             l.append(int(df_consumer_master.where(df_consumer_master["customerid"]==i).dropna()["pincode"]))
            # l is pincode column
    fact_daily_orders["pincode"] = l
        
    
    k=[]
    m=[]
    for i in df_order_items["productid"]:
        m=list(df_product_master.where(df_product_master["productid"]==i)["rate"].dropna() * df_order_items.iloc[i,2])
        k.append(m[0])
    df_order_items["Total"]=k    
    k = df_order_items.groupby('orderid')["Total"].sum()
    k1 = df_order_items.groupby('orderid')["quantity"].sum()
    k=k.tolist()
    fact_daily_orders["order_amount"]=k
    fact_daily_orders["item_count"]=list(k1)
    fact_daily_orders["order_delivery_time_seconds"] =fact_daily_orders["order_received_timestamp"]-fact_daily_orders["order_delivery_timestamp"] 
    return fact_daily_orders


# In[14]:


dim_order = insert_dim_order(df_order_details)


# In[15]:


dim_order.head()


# In[18]:


dim_customer = insert_dim_customer(df_consumer_master)


# In[19]:


dim_customer.head()


# In[20]:


dim_address = insert_dim_address(df_consumer_master)


# In[21]:


dim_product=insert_dim_product(df_product_master)


# In[22]:


f_order_details = insert_f_order_details(df_order_details,df_order_items)


# In[24]:


f_order_details = insert_f_order_details(df_order_details,df_order_items)


# In[40]:


fact_daily_orders = insert_fact_daily_orders(df_consumer_master,df_order_details,df_order_items)


# In[41]:


fact_daily_orders.head()


# In[42]:


credentials = service_account.Credentials.from_service_account_file('/home/airflow/gcs/dags/tasks/pax-4-366517-c08051ec910d.json')


# In[43]:


client = bigquery.Client(credentials=credentials)


# In[44]:


tableRef1 = client.dataset("star_schema").table("dim_order")
client.load_table_from_dataframe(dim_order,tableRef1)


# In[45]:


tableRef2 = client.dataset("star_schema").table("dim_customer")
client.load_table_from_dataframe(dim_customer,tableRef2)


# In[46]:


tableRef3 = client.dataset("star_schema").table("dim_address")
client.load_table_from_dataframe(dim_address,tableRef3)


# In[47]:


tableRef4 = client.dataset("star_schema").table("dim_product")
client.load_table_from_dataframe(dim_product,tableRef4)


# In[48]:


tableRef5 = client.dataset("star_schema").table("f_order_details")
client.load_table_from_dataframe(f_order_details,tableRef5)


# In[50]:


tableRef6 = client.dataset("star_schema").table("fact_daily_orders")
client.load_table_from_dataframe(fact_daily_orders,tableRef6)


# In[ ]:




