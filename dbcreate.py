# -*- coding: utf-8 -*-
"""
Created on Thu Feb 21 19:16:53 2019

@author: 43849407
"""

# Import the libraries

import pandas as pd
from sqlalchemy import create_engine


# Create Connection to the database

engine = create_engine('postgres://postgres:DataAdmin@127.0.0.1:5432/Capstone')


''' Create the Tables - Transactions, Chemicals and Address. While tranactions 
will be appended serially, Chemicals will be unique and only new
ones will be appended'''

tran_create='''CREATE TABLE DIABETES(
        TRANID SERIAL PRIMARY KEY,
        TRANPRACTICE VARCHAR(10) NOT NULL, 
        TRANBNFCODE VARCHAR(15) NOT NULL,
        TRANBNFNAME VARCHAR(200),
        ITEMS BIGINT,
        NIC NUMERIC,
        ACTCOST NUMERIC,
        QUANTITY INTEGER,
        PERIOD VARCHAR(10)
        );'''

chem_create = '''CREATE TABLE CHEMICALS(
        CHEMID SERIAL PRIMARY KEY,
        CHEMBNFCODE VARCHAR (15) NOT NULL,
        CHEMBNFNAME VARCHAR(200) NOT NULL
        );'''

addr_create='''CREATE TABLE ADDRESS(
        ADDID SERIAL PRIMARY KEY,
        ADDRDATE VARCHAR(10),
        ADDPRACTICE VARCHAR(10) NOT NULL,
        PRACNAME VARCHAR(200) NOT NULL,
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
    print('The Chemical Table already exists.')
    

try:
    engine.execute(addr_create)
except:
    print('The Address Table already exists')
        
    

# Get the filename
year = list(range(2010,2019))
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

for i in range(len(chem_fullfile)):
    try:
        df_chem = pd.read_csv(chem_fullfile[i])
    except:
        continue
    
    df_chem.drop([df_chem.columns[2],df_chem.columns[3]],axis = 1,inplace =True) # Drop Unnecessary columns
    df_chem.columns=['chembnfcode','chembnfname']                                # Rename the columns
    
    # load to the database
    df_chem.to_sql('chemicals',engine,if_exists='append',index =False)


    # Remove the duplicates from the chemicals columns

del_dup = '''DELETE  FROM
    chemicals a
        USING chemicals b
WHERE
    a.chemid > b.chemid
    AND a.chembnfname = b.chembnfname;'''

engine.execute(del_dup)

# Load the Address File
addr_columns = ['addrdate','addpractice','pracname','pracaddr','streetaddr','area',
                'town','zipcode']
for i in range(len(chem_fullfile)):
    try:
        df_addr = pd.read_csv(addr_fullfile[i],header = None)
    except:
        continue
    if len(df_addr.columns)>8:                                # To handle some files for which extra null column at end is imported
        df_addr.drop(df_addr.columns[-1],axis =1,inplace =True)
    
    df_addr.columns = addr_columns
    
    # load to the database
    df_addr.to_sql('address',engine,if_exists='append',index =False)


# Load the Transaction File
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
    ''' Get the data for diabetes related drug only 
        Diabetic Monitoring Agents =060106 - There are 5 types of drugs in BNF. 
        This gives how people many were tested.
        Insulin = 060101 (There are 14 drugs in BNF which are labeled as insulin). 
        This gives how many had high Sugar.
        Hypoglycemia = 060104 - 3 Types of Drugs. This gives how many had low sugar.'''

    insulin = df_trans[df_trans['tranbnfcode'].str.match('060101')]
    hypoglycemia = df_trans[df_trans['tranbnfcode'].str.match('060104')]
    diabetic_test = df_trans[df_trans['tranbnfcode'].str.match('060106')]
    
    frames = [insulin,hypoglycemia,diabetic_test]
    df_total = pd.concat(frames)
    
    df_total.to_sql('diabetes',engine,if_exists='append',index =False)


# Run these lines only once after creating the databaser. Then commengt them 
# for any updates.    
# To increase the speed of querying an index on BNFCODE columns is created.
code_index_create = 'CREATE INDEX CODE ON DIABETES(TRANBNFCODE)'
engine.execute(code_index_create)

practice_index_create = 'CREATE INDEX PRACTICE ON DIABETES(TRANPRACTICE)'
engine.execute(practice_index_create)

# Create a denormalized view / table with drugs and address combined 

create_address_view = '''CREATE VIEW UKDRUG AS
SELECT * FROM DIABETES JOIN ADDRESS ON 
(DIABETES.TRANPRACTICE = ADDRESS.ADDPRACTICE AND 
DIABETES.PERIOD = ADDRDATE) '''
engine.execute(create_address_view)
       
# Get 5 categories and all drugs from each category
#    df1 = df_trans[df_trans['tranbnfcode'].str.match('01')]
#    df2 = df_trans[df_trans['tranbnfcode'].str.match('02')]
#    df3 = df_trans[df_trans['tranbnfcode'].str.match('03')]
#    df4 = df_trans[df_trans['tranbnfcode'].str.match('04')]
#    df5 = df_trans[df_trans['tranbnfcode'].str.match('05')]
   
#    frames = [df1,df2,df3,df4,df5]
#    df_total = pd.concat(frames)
    
    # Save to database
#    df_total.to_sql('transactions',engine,if_exists='append',index =False)
    # delete the dataframe to save space
#    del df_trans,df1,df2,df3,df4,df5,df_total
 







''' Initial Time series modelling on one of the drugs'''

# Load the Address File


# Clean the File and bring to correct format

# save to chemical table in database

# Save the cleaned data in postgresql
#df.to_sql(name = 'Wikihow', con = engine, if_exists = 'append',index =True)
