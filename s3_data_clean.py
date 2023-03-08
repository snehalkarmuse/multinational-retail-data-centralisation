import pandas as pd
import psycopg2
from yaml import safe_load
import db_connect as dbc
from sqlalchemy import text
import tabula
import requests
import boto3
import re
def extract_from_s3(filename):
    s3_df = pd.read_csv(filename)
    s3_df.drop(columns=s3_df.columns[0], axis=1,  inplace=True)
    # print(s3_df.info())
    print(s3_df.shape[0])
    print(s3_df.head(3))
    return s3_df

def clean_product_data(s3_df):
    s3_df.drop_duplicates()

    print(s3_df.info())
    s3_df['product_price'] = s3_df['product_price'].str.replace(r'[£]', '')
    
    s3_df['date_added']= pd.to_datetime(s3_df['date_added'], infer_datetime_format=True, errors='coerce')
    s3_df = s3_df.dropna()
    return s3_df

def convert_product_weights(s3_df):
    s3_df.drop(s3_df.loc[s3_df['weight'].str.match(r'(\d*\.?\d+)(kg|g|l|ml)') == False].index, axis = 0, inplace = True)
    s3_df.loc[:, 'weight'] = s3_df['weight'].apply(lambda x: weight_conversion(x))
    print(s3_df.head(5))
    
    # This method takes the weight from s3_data dataframe. compiles the expression. search for it. If it gets dicides it into 2 groups
    # group 1 is converted into float. and group 2 is for unit check. If unit is g, ml then divide the weight by 1000.       
def weight_conversion(weight_str):
    weight_rgx = re.compile(r'(\d*\.?\d+)(kg|g|l|ml)')
    mo = weight_rgx.search(weight_str)
    wn = float(mo.group(1)) 
    if mo.group(2)=='g' or mo.group(2)=='ml':
        wn = wn/1000
    return wn



db_connector = dbc.DatabaseConnector()
db_connector.init_db_engine()
s3 = boto3.client("s3")
s3.download_file("data-handling-public","products.csv","products.csv")
s3_df = extract_from_s3("products.csv")
product_data = clean_product_data(s3_df)
convert_product_weights(product_data)