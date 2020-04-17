import json
import urllib.request
import pandas as pd
from pandas.io.json import json_normalize
import time

QUERY_URL = "https://www.alphavantage.co/query?function={REQUEST_TYPE}&apikey={KEY}&symbol={SYMBOL}"
API_KEY = 'RG3MY4ZIWNEBVF8F'

def _request(symbol, req_type):
    with urllib.request.urlopen(QUERY_URL.format(REQUEST_TYPE=req_type, KEY=API_KEY, SYMBOL=symbol)) as req:
        data = req.read().decode("UTF-8")
    return data

def get_monthly_data(symbol):
    return json.loads(_request(symbol, 'TIME_SERIES_MONTHLY'))

def get_daily_data(symbol):
    return json.loads(_request(symbol, 'TIME_SERIES_DAILY'))

def create_daily_stockprice_data(company_df, repeat_iterations):
    #create the first one
    
    company_list =  company_df.company_index.tolist()

    company_data_daily = pd.DataFrame(columns = ['yyyy-mm-dd'])
    for company in company_list:
        df = pd.DataFrame.from_dict(json_normalize(get_daily_data(company)))
        cols_to_use = [i for i in df.columns if 'Daily' in i and 'close' in i] #data at close
        cols_to_use_renamed = {i:i.replace('Time Series (Daily).', '')[:10] for i in cols_to_use}

        df = df.rename(columns = cols_to_use_renamed)[cols_to_use_renamed.values()].T
        data = pd.DataFrame(columns = ['yyyy-mm-dd', company])
        data[company] = df[0].tolist()
        data['yyyy-mm-dd'] = df.index.tolist()

        company_data_daily = company_data_daily.merge(data, how = 'outer', on = 'yyyy-mm-dd')

    comp_notnull = company_data_daily.loc[:,company_data_daily.isnull().sum() != company_data_daily.shape[0]].columns.tolist()
    company_list_remaining = [i for i in company_list if i not in comp_notnull]
    company_data_daily = company_data_daily.loc[:,comp_notnull]
    print('Total companies scraped: %d' %company_data_daily.shape[1])
    
    #wait 1 minute
    time.sleep(60)
    
    #repeat a few times
    for i in range(repeat_iterations):
        for company in company_list_remaining:
            df = pd.DataFrame.from_dict(json_normalize(get_daily_data(company)))
            cols_to_use = [i for i in df.columns if 'Daily' in i and 'close' in i] #data at close
            cols_to_use_renamed = {i:i.replace('Time Series (Daily).', '')[:10] for i in cols_to_use}

            df = df.rename(columns = cols_to_use_renamed)[cols_to_use_renamed.values()].T
            data = pd.DataFrame(columns = ['yyyy-mm-dd', company])
            data[company] = df[0].tolist()
            data['yyyy-mm-dd'] = df.index.tolist()

            company_data_daily = company_data_daily.merge(data, how = 'outer', on = 'yyyy-mm-dd')

        comp_notnull = company_data_daily.loc[:,company_data_daily.isnull().sum() != company_data_daily.shape[0]].columns.tolist()
        company_list_remaining = [i for i in company_list if i not in comp_notnull]
        company_data_daily = company_data_daily.loc[:,comp_notnull]
        print('Total companies scraped: %d' %company_data_daily.shape[1])
        
        #wait 1 minute
        time.sleep(60)
    
    return company_data_daily


def create_monthly_stockprice_data(company_df, repeat_iterations):
    #create the first one
    
    company_list =  company_df.company_index.tolist()

    company_data_monthly = pd.DataFrame(columns = ['yyyy-mm-dd'])
    for company in company_list:
        df = pd.DataFrame.from_dict(json_normalize(get_monthly_data(company)))
        cols_to_use = [i for i in df.columns if 'Monthly' in i and 'close' in i] #data at close
        cols_to_use_renamed = {i:i.replace('Monthly Time Series.', '')[:7] for i in cols_to_use}

        df = df.rename(columns = cols_to_use_renamed)[cols_to_use_renamed.values()].T
        data = pd.DataFrame(columns = ['yyyy-mm-dd', company])
        data[company] = df[0].tolist()
        data['yyyy-mm-dd'] = df.index.tolist()

        company_data_monthly = company_data_monthly.merge(data, how = 'outer', on = 'yyyy-mm-dd')

    comp_notnull = company_data_monthly.loc[:,company_data_monthly.isnull().sum() != company_data_monthly.shape[0]].columns.tolist()
    company_list_remaining = [i for i in company_list if i not in comp_notnull]
    company_data_monthly = company_data_monthly.loc[:,comp_notnull]
    print('Total companies scraped: %d' %company_data_monthly.shape[1])
    
    #wait 1 minute
    time.sleep(60)
    
    #repeat a few times
    for i in range(repeat_iterations):
        for company in company_list_remaining:
            df = pd.DataFrame.from_dict(json_normalize(get_monthly_data(company)))
            cols_to_use = [i for i in df.columns if 'Monthly' in i and 'close' in i] #data at close
            cols_to_use_renamed = {i:i.replace('Monthly Time Series.', '')[:7] for i in cols_to_use}

            df = df.rename(columns = cols_to_use_renamed)[cols_to_use_renamed.values()].T
            data = pd.DataFrame(columns = ['yyyy-mm-dd', company])
            data[company] = df[0].tolist()
            data['yyyy-mm-dd'] = df.index.tolist()

            company_data_monthly = company_data_monthly.merge(data, how = 'outer', on = 'yyyy-mm-dd')

        comp_notnull = company_data_monthly.loc[:,company_data_monthly.isnull().sum() != company_data_monthly.shape[0]].columns.tolist()
        company_list_remaining = [i for i in company_list if i not in comp_notnull]
        company_data_monthly = company_data_monthly.loc[:,comp_notnull]
        print('Total companies scraped: %d' %company_data_monthly.shape[1])
        
        #wait 1 minute
        time.sleep(60)
    
    return company_data_monthly