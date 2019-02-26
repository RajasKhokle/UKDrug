# -*- coding: utf-8 -*-
"""
Created on Sat Feb 23 18:50:15 2019

@author: 43849407
"""


# Import the libraries

import pandas as pd
from sqlalchemy import create_engine


# Create Connection to the database

engine = create_engine('postgres://postgres:DataAdmin@127.0.0.1:5432/Capstone')


# Form the filename
year = list(range(2010,2019))
year = [str(x) for x in year]

month= list(range(1,13))
month = ['0'+str(x) if x in range(1,10) else str(x) for x in month ]


pdpi_fullfile=[]
for i in range(len(year)):
    for j in range(len(month)):
        pdpi_fullfile.append('../Capstonedata/T'+year[i]+month[j]+'PDPI BNFT'+'.csv')



# Load the Transaction File, aggregate by drug and store in dB.
trans_columns = ['tranpractice','tranbnfcode','tranbnfname','items','nic',
                    'actcost','quantity','period']

for i in range(len(pdpi_fullfile)):
    try:
        df_trans = pd.read_csv(pdpi_fullfile[i])
    except:
        continue
    df_trans.drop([df_trans.columns[0],df_trans.columns[1],df_trans.columns[-1]], 
                  axis = 1,inplace =True)
    df_trans.columns = trans_columns
    print(f'Processing {i} th file')
    
    # Aggregate by drugname
    # X now contains information aggregated by drugcode (drugname). The information 
    # about the practice is losti in this process, but this will be useful for 
    # making the time series analysis
        
    x = df_trans.groupby(['tranbnfcode','tranbnfname']).sum()
    x.period = x.period.apply(lambda x: df_trans.period[0])
#    Activate below lines only when index = false in the x.to_sql command below.
#    x['ind'] = x.index   
#    x[['bnfcode', 'bnfname']] = pd.DataFrame(x['ind'].tolist(), index=x.index)
#    x.drop('ind',axis=1,inplace =True)
      
    
    # Save this in the database
    
    x.to_sql('aggregate',engine,if_exists = 'append',index = True) 
    # Index = True ensures that the bnfcode and bnfname is written in the database
    # If this is set to false uncomment x['ind'] = x.index .... code above
    del x






  
    




