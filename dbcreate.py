# -*- coding: utf-8 -*-
"""
Created on Thu Feb 21 19:16:53 2019

@author: 43849407
"""

# Import the libraries

import pandas as pd
from sqlalchemy import create_engine
import datetime

# Create Connection to the database

engine = create_engine('postgres://postgres:DataAdmin@127.0.0.1:5432/Capstone')

def create_table():
    ''' Create the Tables - Transactions, Chemicals and Address. While tranactions 
    will be appended serially, Chemicals and Address will be unique and only new
    ones will be appended'''
    
    tran_create='''CREATE TABLE TRANSACTIONS(
            TRANID SERIAL PRIMARY KEY,
            TRANPRACTICE VARCHAR(10) NOT NULL, 
            TRANBNFCODE VARCHAR(15) NOT NULL,
            TRANBNFNAME VARCHAR(200),
            ITEMS BIGINT,
            NIC NUMERIC,
            ACTCOST NUMERIC,
            QUANTITY INTEGER
            );'''
    
    chem_create = '''CREATE TABLE CHEMICALS(
            CHEMBNFCODE VARCHAR (15) NOT NULL,
            CHEMBNFNAME VARCHAR(200) NOT NULL,
            UNIQUE(CHEMBNFCODE,CHEMBNFNAME)
            );'''
    
    addr_create='''CREATE TABLE ADDRESS(
            ADDID SERIAL PRIMARY KEY,
            ADDPRACTICE VARCHAR(10) NOT NULL UNIQUE,
            PRACNAME VARCHAR(200) NOT NULL),
            PRACADDR VARCHAR(200),
            STREETADDR VARCHAR(200),
            AREA VARCHAR(200),
            TOWN VARCHAR(200),
            ZIPCODE VARCHAR (10)
            );'''
    
    try:
        engine.execute(tran_create)
    except:
        print('The Transaction Table already exists')

    
    try:
        engine.execute(chem_create)
    except:
        print('The Chemical Table already exists')
        
    
    try:
        engine.execute(addr_create)
    except:
        print('The Address Table already exists')
        
    

# Get the filename
year = list(range(2011,2019))
year = [str(x) for x in year]

month= list(range(1,13))
month = ['0'+str(x) if x in range(1,10) else str(x) for x in month ]


pdpi_fullfile=[]
for i in range(len(year)):
    for j in range(len(month)):
        pdpi_fullfile.append('../Capstonedata/T'+year[i]+month[j]+'PDPI BNFT'+'.csv')

chem_fullfile=[]
for i in range(len(year)):
    for j in range(len(month)):
        chem_fullfile.append('../Capstonedata/T'+year[i]+month[j]+'CHEM SUBS'+'.csv')        

addr_fullfile=[]
for i in range(len(year)):
    for j in range(len(month)):
        addr_fullfile.append('../Capstonedata/T'+year[i]+month[j]+'ADDR BNFT'+'.csv')   




# Load the chemical File
df_chem = pd.read_csv(chem_fullfile[50])
df_chem.drop([df_chem.columns[2],df_chem.columns[3]],axis = 1,inplace =True)

# Load the Transaction File
df_trans = pd.read_csv(pdpi_fullfile[0])
df_trans.drop([df_trans.columns[0],df_trans.columns[1],df_trans.columns[-1]], 
              axis = 1,inplace =True)

# Find the data on 5-10 categories

categories = ['']
    
# Get 10 categories and 10 drugs from each category

''' Initial Time series modelling on one of the drugs'''

# Load the Address File


# Clean the File and bring to correct format

# save to chemical table in database

# Save the cleaned data in postgresql
#df.to_sql(name = 'Wikihow', con = engine, if_exists = 'append',index =True)
