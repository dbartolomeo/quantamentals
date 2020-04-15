#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Apr 14 02:58:04 2020

@author: danielbartolomeo
"""

import pandas as pd
import numpy as np
import intrinio_sdk
from intrinio_sdk.rest import ApiException
import psycopg2
import json
import os
import time
import datetime
from sqlalchemy import create_engine

# get my password info
with open(os.path.expanduser('~')+'/git_repository/quantamentals/password.json') as pass_file:
  password_file = json.load(pass_file)
pass_file.close()
del pass_file

# connect to intrino
intrinio_sdk.ApiClient().configuration.api_key['api_key'] = password_file.get('intrinio', {}).get('API_sandbox','')

# make api calls
# create a dummy dict of the company list to store the 'next_page' key
colist = {'next_page':''}
# blank dataframe to append the data into
companies = pd.DataFrame()

# make api calls and loop until entire file is pulled
while colist.get('next_page') is not None:
    # api call
    colist = intrinio_sdk.CompanyApi().get_all_companies(page_size = 100, next_page = colist.get('next_page')).to_dict()
    try:
        # merge with previous
        companies = pd.concat([companies, pd.DataFrame(colist['companies'])], axis = 'index')
    except:
        # if there is an issue break out of loop
        break
    # if you can not reached the end of file sleep for 1 second as to not overload API
    if colist.get('next_page') is not None:
        time.sleep(1.0)

# reset the index
companies = companies.reset_index(drop = True).rename(columns = {'id':'intrinio_id', 'name':'company'})


# prep the sql call
dbname='stockmaster'
host=password_file.get('postgres',{}).get('server',{}).get(dbname,{}).get('host','')
user=password_file.get('postgres',{}).get('server',{}).get(dbname,{}).get('user','')
password=password_file.get('postgres',{}).get('server',{}).get(dbname,{}).get('password','')
port=password_file.get('postgres',{}).get('server',{}).get(dbname,{}).get('port','')


# connect to postgres db
conn_str = psycopg2.connect(dbname=dbname, host=host, port=port, user=user, password=password)
    
# get infor from db
comp_db = pd.read_sql(sql = "select * from dbo.company_list", con = conn_str, parse_dates=['insert_date', 'update_date'], chunksize=None)



# new companies -- need to be added to database
new_co = companies.loc[~companies['intrinio_id'].isin(comp_db.loc[comp_db['is_current'] == 1,'intrinio_id']),'intrinio_id']
# old companies -- need to check if there have been any changes
old_co = companies.loc[companies['intrinio_id'].isin(comp_db.loc[comp_db['is_current'] == 1,'intrinio_id']),'intrinio_id']
# companies no longer listed
obs_co = comp_db.loc[~comp_db['intrinio_id'].isin(companies.loc[:,'intrinio_id']) & (comp_db['is_current'] == 1),'intrinio_id']


# check old companies
for co in old_co:
    comp_db.loc[(comp_db['intrinio_id'] == co) & (comp_db['is_current'] == 1), 'update_date'] = datetime.date.today() 
    if companies.loc[companies['intrinio_id'] == co,:].reset_index(drop = True).equals(comp_db.loc[(comp_db['intrinio_id'] == co) & (comp_db['is_current'] == 1), ['intrinio_id', 'ticker', 'company', 'lei', 'cik']].reset_index(drop = True)):
        comp_db.loc[(comp_db['intrinio_id'] == co) & (comp_db['is_current'] == 1), 'update_date'] = datetime.date.today() 
    else:
        comp_db.loc[(comp_db['intrinio_id'] == co) & (comp_db['is_current'] == 1), 'is_current'] = 0
        comp_db = pd.concat([comp_db, companies.loc[companies['intrinio_id'] == co,:].assign(insert_date = comp_db.loc[(comp_db['intrinio_id'] == co) & (comp_db['is_current'] == 1), 'insert_date']).assign(update_date = datetime.date.today()).assign(is_current = 1)], axis = 'index')

# set is current flag for obsolete companies
for co in obs_co:
    comp_db.loc[(comp_db['intrinio_id'] == co) & (comp_db['is_current'] == 1), 'update_date'] = datetime.date.today() 
    comp_db.loc[(comp_db['intrinio_id'] == co) & (comp_db['is_current'] == 1), 'is_current'] = 0

# add new companies
comp_db = pd.concat([comp_db, companies.loc[companies['intrinio_id'].isin(new_co),:].assign(insert_date = datetime.date.today()).assign(update_date = datetime.date.today()).assign(is_current = 1)], axis = 'index')

# use sql alchemy to write back to postgres
engine = create_engine('postgresql://'+user+':'+password+'@'+host+':'+port+'/'+dbname)
# write data
comp_db.to_sql('company_list', schema = 'dbo', engine, index = False, if_exists = 'replace')

# close conn 
conn_str.close()
