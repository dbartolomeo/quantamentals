#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Apr 14 02:58:04 2020
Run this code to update the security list on the postgres sql server, based on companies in 
dbo.company_list where is_current = 1
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

# connect to intrino
intrinio_sdk.ApiClient().configuration.api_key['api_key'] = password_file.get('intrinio', {}).get('API_sandbox','')

# loop thru company list and make api calls while updating database
for int_id in comp_list.loc[:,'company_id'].to_list():
    # make the api call to get the company info
    temp = pd.DataFrame(intrinio_sdk.CompanyApi().get_company_securities(identifier = int_id).to_dict().get('securities',{})).rename(columns = {'id':'intrinio_id', 'name':'company'}).reindex(['intrinio_id', 'company_id', 'company', 'code', 'currency', 'ticker', 'composite_ticker', 'figi', 'composite_figi', 'share_class_figi'], axis = 'columns')
    # loop over the values in the API pull and check against what is stored in the database
    for sec in temp.loc[:,'intrinio_id']:
        # get the security from that database
        sec_list = pd.read_sql(sql = "select intrinio_id, company_id, company, code, currency, ticker, composite_ticker, figi, composite_figi, share_class_figi from dbo.security_list where is_current = 1 and intrinio_id = '"+sec+"'", con = conn, chunksize=None)        
        # Nothing found
        if len(sec_list) == 0:
            # insert the API pull
            temp.loc[temp['intrinio_id'] == sec,:].assign(insert_date = dt.date.today()).assign(update_date = dt.date.today()).assign(is_current = 1).to_sql('security_list', schema = 'dbo', con = conn, index = False, if_exists = 'append')
        # the API = data base
        elif temp.loc[temp['intrinio_id'] == sec,:].reset_index(drop = True).equals(sec_list):
            # update the update date
            conn.execute("update dbo.security_list set update_date = '"+dt.date.today().strftime("%Y-%m-%d")+"' where intrinio_id = '"+sec+"' and is_current = 1")
        # the API = data base
        else:
            # the update date
            insert_date = pd.read_sql(sql = "select insert_date from dbo.security_list where is_current = 1 and intrinio_id = '"+sec+"'", con = conn, chunksize=None).iloc[0,0]
            # set the is_current flag = 0
            conn.execute("update dbo.security_list set update_date = '"+dt.date.today().strftime("%Y-%m-%d")+"' , is_current = 0 where intrinio_id = '"+sec+"' and is_current = 1")
            # add the api pulled values
            temp.loc[temp['intrinio_id'] == sec,:].assign(insert_date = insert_date).assign(update_date = dt.date.today()).assign(is_current = 1).to_sql('security_list', schema = 'dbo', con = conn, index = False, if_exists = 'append')
    # ensure no more than 100 API calls per sec
    time.sleep(0.01)
# close the connection
conn.close()
