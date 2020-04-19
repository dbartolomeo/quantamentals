#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Apr 14 02:58:04 2020
Run this code to update the company list on the postgres sql server
1) inserts new companies
2) updates old companies if information changes (i.e. ticker) by intrinio id
3) flags companies as not current if dropped from intrinio
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
    # if you can not reached the end of file sleep 
    if colist.get('next_page') is not None:
        # ensure no more than 100 API calls per sec
        time.sleep(0.01)

# reset the index
companies = companies.reset_index(drop = True).rename(columns = {'id':'company_id', 'name':'company'})


# prep the sql call
dbname='stockmaster'
host=password_file.get('postgres',{}).get('server',{}).get(dbname,{}).get('host','')
user=password_file.get('postgres',{}).get('server',{}).get(dbname,{}).get('user','')
password=password_file.get('postgres',{}).get('server',{}).get(dbname,{}).get('password','')
port=password_file.get('postgres',{}).get('server',{}).get(dbname,{}).get('port','')


# use sql alchemy conn string AND connect to database
conn = sqlalchemy.create_engine('postgresql://'+user+':'+password+'@'+host+':'+port+'/'+dbname).connect()
 
# get infor from db
comp_db = pd.read_sql(sql = "select * from dbo.company_list where is_current = 1", con = conn, parse_dates=['insert_date', 'update_date'], chunksize=None)


# LIST OF COMPANIES:
# new companies -- need to be added to database
new_co = companies.loc[~companies['company_id'].isin(comp_db.loc[:,'company_id']),'company_id']
# old companies -- need to check if there have been any changes
old_co = companies.loc[companies['company_id'].isin(comp_db.loc[:,'company_id']),'company_id']
# companies no longer listed
obs_co = comp_db.loc[~comp_db['company_id'].isin(companies.loc[:,'company_id']),'company_id']

# insert new companies
companies.loc[companies['company_id'].isin(new_co),:].assign(insert_date = dt.date.today()).assign(update_date = dt.date.today()).assign(is_current = 1).to_sql('company_list', schema = 'dbo', con = conn, index = False, if_exists = 'append')

# flag sql table entries that are no longer being feed into the api
conn.execute("update dbo.company_list set update_date = '"+dt.date.today().strftime("%Y-%m-%d")+"', is_current = 0 where company_id in ('"+"', '".join(obs_co.to_list())+"')")
#conn.close()


# check old companies
old_co_update = []
for co in old_co:
    # no change
    if not(companies.loc[companies['company_id'] == co,:].reset_index(drop = True).equals(comp_db.loc[(comp_db['company_id'] == co), ['company_id', 'ticker', 'company', 'lei', 'cik']].reset_index(drop = True))):
        old_co_update = old_co_update + companies.loc[companies['company_id'] == co,'company_id'].to_list()
# update table flagging as old    
conn.execute("update dbo.company_list set update_date = '"+dt.date.today().strftime("%Y-%m-%d")+"', is_current = 0 where company_id in ('"+"', '".join(old_co_update)+"')")
# insert the updated rows
companies.loc[companies['company_id'].isin(old_co_update),:].assign(insert_date = comp_db.loc[(comp_db['company_id'].isin(old_co_update)), 'insert_date'].to_list()).assign(update_date = dt.date.today()).assign(is_current = 1).to_sql('company_list', schema = 'dbo', con = conn, index = False, if_exists = 'append')

# close the connection
conn.close()
