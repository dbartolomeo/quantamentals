#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Apr 12 01:14:52 2020

@author: danielbartolomeo
"""

import pandas as pd
import numpy as np
import sys
import intrinio_sdk
from intrinio_sdk.rest import ApiException
from pprint import pprint

sandbox_key = 'Ojg5Y2Q0M2Y1YjVlNjdlMWMzYjY4M2NkZDc2MTQ0ODM3'

colist = intrinio_sdk.CompanyApi().get_all_companies()

intrinio_sdk.ApiClient().configuration.api_key['api_key'] = sandbox_key

security_api = intrinio_sdk.SecurityApi()

identifier = 'AAPL' # str | A Security identifier (Ticker, FIGI, ISIN, CUSIP, Intrinio ID)
start_date = '2018-01-01' # date | Return prices on or after the date (optional)
end_date = '2019-01-01' # date | Return prices on or before the date (optional)
frequency = 'daily' # str | Return stock prices in the given frequency (optional) (default to daily)
page_size = 100 # int | The number of results to return (optional) (default to 100)
next_page = '' # str | Gets the next page of data from a previous API call (optional)

try:
  api_response = security_api.get_security_stock_prices(identifier, start_date=start_date, end_date=end_date, frequency=frequency, page_size=page_size, next_page=next_page)
  api_dict = api_response.__dict__
except ApiException as e:
  print("Exception when calling SecurityApi->get_security_stock_prices: %s\r\n" % e)
    


import psycopg2
myConnection = psycopg2.connect( host='localhost', user='dbartolomeo', password='ledzepplin', dbname='stockmaster' )
cur = myConnection.cursor()
cur.execute( "select * from dbo.test" )
# for asofdate in cur.fetchall() :
#     print(asofdate)
print(pd.read_sql(sql = "select * from dbo.company_list", con = myConnection, parse_dates=['asofdate'], chunksize=None).head())

myConnection.close()


import requests
from bs4 import BeautifulSoup

vgm_url = 'https://product.intrinio.com/developer-sandbox/coverage/us-fundamentals-financials-metrics-ratios-stock-prices'
html_text = requests.get(vgm_url).text
soup = BeautifulSoup(html_text, 'html.parser')

lis = [li.text.split('\r')[0] for li in soup.find('div', {'class':'body--container'}).findAll('li')]





