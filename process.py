# -*- coding: utf-8 -*-
"""
Created on Wed Nov  6 18:11:02 2019

@author: Satya
"""
import os
import pandas as pd

chunksize = 1000
os.chdir(r'C:\Users\Satya\Desktop\dviz\arcos_all_washpost.tsv')
df = pd.read_table('arcos_all_washpost.tsv', sep='\t',chunksize=chunksize, header=0)

df1 = next(df)

df1['TRANSACTION_DATE']

def fetch_date(df,row_number,col='TRANSACTION_DATE'):
    date = df[col].iloc[row_number]
    date = str(date)
    year = date[-4:]
    day = date[-6:-4]
    
    year_day_str_len = len(list(year)) + len(list(day))
    month_len = len(date) - year_day_str_len
    
    month = date[0:month_len]
    
    year = int(year)
    day = int(day)
    month = int(month)
    
    return year,month

def fetch_col(df,row_number,col,fetch_date=fetch_date):
    if col not in df.columns:
        raise Exception('Invalid Column')
    value = df[col].iloc[row_number]
    
    if col=='TRANSACTION_DATE':
        year,month = fetch_date(df,row_number,col)
        return year,month
    
    try:
        value = float(value)
        if np.isnan(value):
            # Contains NA
            return None
        else:
            value = int(value)
    except:
        value = str(value)
    return value

