#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Apr 14 02:58:04 2020
Run this code to update the company details on the postgres sql server, based on companies in 
1) dbo.company_list where is_current = 1
@author: danielbartolomeo
"""

import pandas as pd
import intrinio_sdk
import json
import os
import time
import datetime as dt
import sqlalchemy

# get my password info
with open(os.path.expanduser('~')+'/git_repository/quantamentals/password.json') as pass_file:
  password_file = json.load(pass_file)
pass_file.close()
del pass_file



# prep the sql call
dbname='stockmaster'
host=password_file.get('postgres',{}).get('server',{}).get(dbname,{}).get('host','')
user=password_file.get('postgres',{}).get('server',{}).get(dbname,{}).get('user','')
password=password_file.get('postgres',{}).get('server',{}).get(dbname,{}).get('password','')
port=password_file.get('postgres',{}).get('server',{}).get(dbname,{}).get('port','')


# use sql alchemy conn string AND connect to database
conn = sqlalchemy.create_engine('postgresql://'+user+':'+password+'@'+host+':'+port+'/'+dbname).connect()
 
# get list of intrinio ids to pull the company info for
comp_list = pd.read_sql(sql = "select distinct company_id from dbo.company_list where is_current = 1", con = conn, parse_dates=['insert_date', 'update_date'], chunksize=None)
# companies currently in the details sql table
comp_dets = pd.read_sql(sql = "select * from dbo.company_details where is_current = 1", con = conn, parse_dates=['insert_date', 'update_date'], chunksize=None)

# connect to intrino
intrinio_sdk.ApiClient().configuration.api_key['api_key'] = password_file.get('intrinio', {}).get('API_sandbox','')


# loop thru company list and make api calls while updating database
for int_id in comp_list.loc[:,'company_id'].to_list():
    # make the api call to get the company info
    temp_dict = intrinio_sdk.CompanyApi().get_company(identifier = int_id).to_dict()
    # convert to dataframe and rename columns to match columns in database
    temp_pd = pd.DataFrame(temp_dict, index = [0]).rename(columns = {'id':'company_id', 'name':'company', 'template':'finstate_template'}).loc[:,comp_dets.columns[~comp_dets.columns.isin(['update_date', 'insert_date', 'is_current'])]]
    # if the company data is not in the database add it
    if len(comp_dets.loc[comp_dets['company_id']==int_id,:]) == 0:
        # add insert update dates and current flag & insert into database
        temp_pd.assign(insert_date = dt.date.today()).assign(update_date = dt.date.today()).assign(is_current = 1).to_sql('company_details', schema = 'dbo', con = conn, index = False, if_exists = 'append')
    # check to make sure there are no differences
    elif temp_pd.equals(comp_dets.loc[comp_dets['company_id']==int_id, comp_dets.columns[~comp_dets.columns.isin(['update_date', 'insert_date', 'is_current'])]].reset_index(drop = True)):
        # api pull = values in database; just update the update_date to reflect the last check
        conn.execute("update dbo.company_details set update_date = '"+dt.date.today().strftime("%Y-%m-%d")+"' where company_id = '"+int_id+"' and is_current = 1")
    else:
        # api pull != values in database;
        # set the is_current flag = 0
        conn.execute("update dbo.company_details set update_date = '"+dt.date.today().strftime("%Y-%m-%d")+"' , is_current = 0 where company_id = '"+int_id+"' and is_current = 1")
        # add the api pulled values
        temp_pd.assign(insert_date = comp_dets.loc[comp_dets['company_id']==int_id, 'insert_date']).assign(update_date = dt.date.today()).assign(is_current = 1).to_sql('company_details', schema = 'dbo', con = conn, index = False, if_exists = 'append')
    
    # ensure no more than 100 API calls per sec
    time.sleep(0.01)

# close the connection
conn.close()
