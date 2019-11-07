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
