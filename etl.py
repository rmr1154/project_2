# Dependencies and Setup
#%matplotlib inline
#import matplotlib.pyplot as plt
import pandas as pd
#import numpy as np
#from pandas_profiling import ProfileReport
import sqlite3 as sq
import json
import re
from sqlalchemy import create_engine
from sqlalchemy import MetaData
from sqlalchemy import Table
from sqlalchemy import Column
from sqlalchemy import Integer, String, Float
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base


dbname = 'mortality.db'
dbpath = 'assets/data/'
db_string = f'{dbpath}{dbname}'

Base = declarative_base()

class Mortality_County(Base):
    __tablename__ = 'mortality_county'
    
    index = Column(Integer, primary_key = True)
    FIPS = Column(String)
    Category = Column(String)
    Change_1980_2014 = Column(Float)
    Date = Column(String)
    Value = Column(Float)
    County = Column(String)
    State = Column(String)

class Mortality_State(Base):
    __tablename__ = 'mortality_state'
    
    index = Column(Integer, primary_key = True)
    FIPS = Column(String)
    Category = Column(String)
    Change_1980_2014 = Column(Float)
    Date = Column(String)
    Value = Column(Float)
    State = Column(String)
    
class Mortality_US(Base):
    __tablename__ = 'mortality_us'
    
    index = Column(Integer, primary_key = True)
    Category = Column(String)
    Change_1980_2014 = Column(Float)
    Date = Column(String)
    Location = Column(String)
    Value = Column(Float)
    

engine = create_engine(f'sqlite:///{db_string}', echo=True)

def create_all_tables():
    Base.metadata.create_all(engine)


#metadata.reflect()
def truncate_all_tables():
    metadata = MetaData(engine)
    metadata.reflect()
    metadata.delete_all()

#metadata.reflect()
def drop_all_tables():
    metadata = MetaData(engine)
    metadata.reflect()
    metadata.drop_all()    

#function to load the database from the dict of df's 
def load_db(class_dict,df_dict,db_string):
    table_list = [table for table in df_dict.keys()]
    Session = sessionmaker(bind=engine)
    session = Session()
    drop_all_tables()
    create_all_tables()
    for table in table_list:
        #session.execute(f'DELETE FROM {table}')
        mapper = class_dict.get(table)
        session.bulk_insert_mappings(mapper, df_dict[table].to_dict(orient="records"))
    session.commit()
    session.close()


def process_etl():
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
            "% Change in Mortality Rate, 1980-2014": "Change_1980_2014"},inplace=True)

    #melt year columns into rows
    mortality = mortality.melt(id_vars=['Location', 'FIPS', 'Category', 'Change_1980_2014'], 
            var_name="Date", 
            value_name="Value")
    
    #replace vals to shorten categories
    new_vals = {"Neonatal disorders":"Neonatal disorders",
                "HIV/AIDS and tuberculosis":"HIV/AIDS and TB",
                "Musculoskeletal disorders":"Musculoskeletal disorders",
                "Diabetes, urogenital, blood, and endocrine diseases":"Diabetes",
                "Digestive diseases":"Digestive diseases",
                "Chronic respiratory diseases":"Chronic resp",
                "Neurological disorders":"Neurological disorders",
                "Cirrhosis and other chronic liver diseases":"Chronic liver",
                "Mental and substance use disorders":"Mental disorders",
                "Forces of nature, war, and legal intervention":"Non Natural",
                "Unintentional injuries":"Unintentional injuries",
                "Nutritional deficiencies":"Nutritional deficiencies",
                "Other communicable, maternal, neonatal, and nutritional diseases":"Other communicable",
                "Cardiovascular diseases":"Cardiovascular",
                "Diarrhea, lower respiratory, and other common infectious diseases":"Diarrhea",
                "Maternal disorders":"Maternal disorders",
                "Other non-communicable diseases":"Other non-communicable diseases",
                "Self-harm and interpersonal violence":"Violence",
                "Neoplasms":"Neoplasms",
                "Transport injuries":"Transport injuries",
                "Neglected tropical diseases and malaria":"Tropical diseases"}
    
    mortality = mortality.replace(new_vals)

    #dict to hold our dataframes
    df_dict = {}

    #split out County dataframe
    df_dict['mortality_county'] = mortality.query('FIPS > 1000').copy().reset_index(drop=True)
    df_dict['mortality_county'][['County','State']] = df_dict['mortality_county']['Location'].str.rsplit(',',expand=True)
    df_dict['mortality_county']['FIPS'] = df_dict['mortality_county']['FIPS'].astype('int')
    df_dict['mortality_county']['FIPS'] = df_dict['mortality_county']['FIPS'].apply(lambda x: '{0:0>5}'.format(x)).astype('str')
    df_dict['mortality_county'].drop(columns='Location',inplace=True)

    #Split out state dataframe
    df_dict['mortality_state'] = mortality.query('FIPS < 1000').copy().reset_index(drop=True)
    df_dict['mortality_state']['FIPS'] = df_dict['mortality_state']['FIPS'].astype('int')
    df_dict['mortality_state']['FIPS'] = df_dict['mortality_state']['FIPS'].apply(lambda x: '{0:0>2}'.format(x)).astype('str')
    df_dict['mortality_state'].rename(columns={"Location":"State"},inplace=True)

    #split out us dataframe
    df_dict['mortality_us'] = mortality[mortality['FIPS'].isnull()].copy().reset_index(drop=True)
    df_dict['mortality_us'].drop(columns='FIPS',inplace=True)

    #build a dict of classes so that we can class_dict.get(key) and call our mapper class when doing the bulk insert for each dataframe / class
    class_dict = {}
    class_dict['mortality_county'] = Mortality_County
    class_dict['mortality_state'] = Mortality_State
    class_dict['mortality_us'] = Mortality_US

    #add any other transforms here

    #load sqlite db from df_dict
    load_db(class_dict,df_dict,db_string)
    return 'ETL Processing Completed'




