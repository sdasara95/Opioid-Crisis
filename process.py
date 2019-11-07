# -*- coding: utf-8 -*-
"""
Created on Wed Nov  6 18:11:02 2019

@author: Satya
"""
import os
import pandas as pd
import numpy as np
import json

chunksize = 100000

def set_dir(path=r'C:\Users\Satya\Desktop\dviz\arcos_all_washpost.tsv'):
    try:    
        os.chdir(path)
        print('Path set successfully!')
    except:
        print('Path Error')

def read_pointer(file='arcos_all_washpost.tsv',chunksize=chunksize):
    pointer = pd.read_table('arcos_all_washpost.tsv', sep='\t',chunksize=chunksize, header=0)
    return pointer

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
    
    return [year,month]

def fetch_col(df,row_number,col,fetch_date=fetch_date):
    if col not in df.columns:
        raise Exception('Invalid Column')
    value = df[col].iloc[row_number]
    
    if col=='TRANSACTION_DATE':
        value = float(value)
        if np.isnan(value):
            return None
        year,month = fetch_date(df,row_number,col)
        return [year,month]
    
    try:
        value = float(value)
        if np.isnan(value):
            # Contains NA/ Do sanity check
            return None
        else:
            pass
    except:
        value = str(value)
    return value

def is_none(value):
    if value==None:
        return True
    else:
        return False

if __name__ == "main":
    
    date = 'TRANSACTION_DATE'     
    
    seller_name = 'Reporter_family'
    seller_state = 'REPORTER_STATE'
    seller_county = 'REPORTER_COUNTY'
    seller_type = 'REPORTER_BUS_ACT'
    
    buyer_name = 'BUYER_NAME'
    buyer_state = 'BUYER_STATE'
    buyer_county = 'BUYER_COUNTY'
    buyer_type = 'BUYER_BUS_ACT'
    
    manufacturer_name = 'Combined_Labeler_Name'
    
    quantity = 'QUANTITY'
    weight_gram = 'CALC_BASE_WT_IN_GM'
    
    set_dir()
    pt = read_pointer()

    pill_count = {}
    pill_count_gms = {}
    
    chunk_count = 1
    while True:
        try:
            df = next(pt)
            # For each row in the chunk dataframe
            for i in range(len(df)):
                row_num = i
                
                state1 = fetch_col(df,row_num,buyer_state)
                county1 = fetch_col(df,row_num,buyer_county)
                quantity1 = fetch_col(df,row_num,quantity)
                weight1 = fetch_col(df,row_num,weight_gram)
                year1,month1 = fetch_col(df,row_num,date)
                
                list_interested = [state1,county1,quantity1,weight1,year1,month1]
                contains_na = False
                
                for i in list_interested:
                    if is_none(i):
                        contains_na = True
                    
                if contains_na:
                    continue
                                
                total_gms = weight1*quantity1
                try:
                    pill_count[year1][state1][county1]+=quantity1
                    pill_count_gms[year1][state1][county1]+=total_gms
                except KeyError:
                    pill_count.setdefault(year1, {}).setdefault(state1, {})[county1] = quantity1                 
                    pill_count_gms.setdefault(year1, {}).setdefault(state1, {})[county1] = total_gms                 
            
            print('Chunk {} done!'.format(chunk_count))
            chunk_count+=1
        except StopIteration:
            break
    
    out_file = open('pill_count','w+')
    json.dump(pill_count,out_file)
    out_file.close()
    out_file = open('pill_count_gms','w+')
    json.dump(pill_count_gms,out_file)
    out_file.close()
            
            
            
            
    
    
    