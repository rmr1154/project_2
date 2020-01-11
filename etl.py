# Dependencies and Setup
#%matplotlib inline
#import matplotlib.pyplot as plt
import pandas as pd
#import numpy as np
#from pandas_profiling import ProfileReport
import sqlite3 as sq
import json

def load_db(dict_in,dbname):
    table_list = [table for table in dict_in.keys()]
#export to sqlite
    sql_data = f'assets/data/{dbname}.db'
    
    conn = sq.connect(sql_data)
    cur = conn.cursor()
    for table in table_list:
        dropstring = '''drop table if exists "{0}"'''.format(table)
        print(dropstring)
        cur.execute(dropstring)
        print(f'create and load table - {table}')
        dict_in[table].to_sql(table,conn, if_exists='replace', index=True)
    conn.commit()
    conn.close() 
    print('SQLite Load Complete')
    
#schema_dict = {'mortality':{c:c for i, c in enumerate(mortality.columns)}}

def dict_to_json(dict_in,filename):
    json_data = json.dumps(dict_in,indent=4,sort_keys=True)
    f = open(filename+'.json','w')
    f.write(json_data)
    f.close()    

#import our csv into df
mortality_src = "assets/data/mort.csv"
mortality = pd.read_csv(mortality_src)

#   ETL STEPS to clean the data for export to SQLite
#remove the (min) and (max) for each year col
mortality = mortality.filter([col for col in mortality if '(' not in col])

#clean col names
mortality.rename(columns={"Mortality Rate, 1980*": "1980",
        "Mortality Rate, 1985*": "1985",
        "Mortality Rate, 1990*": "1990",
        "Mortality Rate, 1995*": "1995",
        "Mortality Rate, 2000*": "2000",
        "Mortality Rate, 2005*": "2005",
        "Mortality Rate, 2010*": "2010",
        "Mortality Rate, 2014*": "2014",
        "% Change in Mortality Rate, 1980-2014": "%_Change_1980-2014"},inplace=True)

#dict to hold our dataframes
df_dict = {}

#split out County dataframe
df_dict['mortality_county'] = mortality.query('FIPS > 1000').copy().reset_index(drop=True)
df_dict['mortality_county'][['County','State']] = df_dict['mortality_county']['Location'].str.rsplit(',',expand=True)
df_dict['mortality_county']['FIPS'] = df_dict['mortality_county']['FIPS'].astype('int')
df_dict['mortality_county'].drop(columns='Location',inplace=True)

#Split out state dataframe
df_dict['mortality_state'] = mortality.query('FIPS < 1000').copy().reset_index(drop=True)
df_dict['mortality_state']['FIPS'] = df_dict['mortality_state']['FIPS'].astype('int')
df_dict['mortality_state'].rename(columns={"Location":"State"},inplace=True)

#split out us dataframe
df_dict['mortality_us'] = mortality[mortality['FIPS'].isnull()].copy().reset_index(drop=True)
df_dict['mortality_us'].drop(columns='FIPS',inplace=True)

#add any other transforms here

#load sqlite db from df_dict
load_db(df_dict,'mortality')

