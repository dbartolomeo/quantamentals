#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Apr 14 02:58:04 2020
Run this code to update the security price on the postgres sql server, based on securites in 
dbo.security_list where the securities ater trades on US excahnges --> composit ticker "US:..."
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
 
# get list of intrinio ids to pull the security (US ONLY) price data for
sec_list = pd.read_sql(sql = "select distinct intrinio_id from dbo.security_list where is_current = 1 and composite_ticker like '%%:US'", con = conn, chunksize=None)

# connect to intrino
intrinio_sdk.ApiClient().configuration.api_key['api_key'] = password_file.get('intrinio', {}).get('API_sandbox','')

# a place to store errors from API CALLS
err = pd.DataFrame(columns = ['identifier', 'start_date', 'end_date', 'frequency', 'page_size', 'next_page', 'err_msg'])

# loop over the secuirties to get the proce info
for sec in sec_list.loc[:,'intrinio_id'].to_list():
    # all the dates in the table for the security // will be used to set the start date and to avoid duplicates being added into the database
    dates_in_table = pd.read_sql(sql = "select asofdate from dbo.security_price where intrinio_id = '"+sec+"'", parse_dates = ['asofdate'],con = conn).loc[:,'asofdate']
    # id comes for security list // intrinio_id
    identifier = sec
    # use the max in the database or the min in the API
    start_date = dt.datetime.strftime(dates_in_table.max() + dt.timedelta(days = 1), '%Y-%m-%d') if len(dates_in_table) != 0 else ''
    # set end date as most recent date
    end_date = dt.datetime.strftime(dt.date.today(),'%Y-%m-%d')
    # data freq --> 'daily' is the most granular
    frequency = 'daily'
    # records per page // larger page sizes are flaged by API as bulk d/l
    page_size = 100
    # initialize the next page in the api call // '' ==> get the most recent
    next_page = ''
    # security price storage
    store = pd.DataFrame(columns = ['intrinio_id', 'asofdate', 'intraperiod', 'frequency', 'open_px', 'high_px', 'low_px', 'close_px', 'volume', 'adj_open', 'adj_high', 'adj_low', 'adj_close', 'adj_volume'])
    # keep on making API calls per security until there are no more pages to pull from
    while next_page is not None:
        try:
            # API CALL
            res = intrinio_sdk.SecurityApi().get_security_stock_prices(identifier, start_date=start_date, end_date=end_date, frequency=frequency, page_size=page_size, next_page=next_page).to_dict()
            # get the prices from the call result
            temp = pd.DataFrame(res.get('stock_prices',[])).assign(intrinio_id = sec).rename(columns = {'date':'asofdate', 'open':'open_px', 'high':'high_px', 'low':'low_px', 'close':'close_px'}).reindex(['intrinio_id', 'asofdate', 'intraperiod', 'frequency', 'open_px', 'high_px', 'low_px', 'close_px', 'volume', 'adj_open', 'adj_high', 'adj_low', 'adj_close', 'adj_volume'], axis = 'columns')
            # remove all dates that are already in the database for the security
            temp = temp.loc[~pd.to_datetime(temp['asofdate']).isin(dates_in_table),:]
            # add to previous calls
            store = pd.concat([store, temp], axis = 'index')
            # set the next page for the next API call
            next_page = res.get('next_page','')
        except Exception as err_msg:
            # write original query into the error msg
            err = pd.concat([err, pd.DataFrame([[identifier, start_date, end_date, frequency, page_size, next_page, str(err_msg)]], columns = ['identifier', 'start_date', 'end_date', 'frequency', 'page_size', 'next_page', 'err_msg'])], axis = 'index').reset_index(drop = True)
            # set the next_page = None --> condition to break out of loop
            next_page = None
            
        # only sleep if there is more to pull
        if next_page is not None:
            # ensure no more than 100 API calls per sec
            time.sleep(0.01)
            
    # if something was retireved -- write it into the database
    if len(store) != 0:
        store.to_sql('security_price', schema = 'dbo', con = conn, index = False, if_exists = 'append')
    
# close the connection
conn.close()

        
