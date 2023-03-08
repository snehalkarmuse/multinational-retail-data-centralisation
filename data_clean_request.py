import pandas as pd
import psycopg2
import db_connect as dbc
import data_extraction as dbe
import data_cleaning as dbcl
import datetime as dt
from sqlalchemy import text
import numpy as np
import re
import boto3

# API url and header info.

data_retrieve_link = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/"
store_api_config = {"x-api-key":"yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX"} 

# instance for our classes. initlizing the engine and connect it.
db_connector = dbc.DatabaseConnector()
db_connector.init_db_engine()
db_extractor = dbe.DataExtractor()
db_cleaning = dbcl.DataCleaning()

# clean user table data. converted it into dim_user.
db_cleaning.clean_user_data()
db_connector.upload_to_db(db_cleaning.df,'dim_users_table')

# clean card detail data from pdf file. take data from each page conver it into dataframe. All dataframe is added into dim_card_detail table.
list_df = db_extractor.retrieve_pdf_data("https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf")
list_of_df = db_cleaning.clean_card_data(list_df)
print("list_of_df:", list_of_df.shape[0])
for i in list_of_df:
    db_connector.upload_to_db(i,'dim_card_detail')

# clean store data from API url. take each store and collects it details and convert it into dataframe. Upload it into dim_store_details.
no_of_stores = db_extractor.list_number_of_stores('https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores',store_api_config)
store_dataframe= db_extractor.add_data_to_dataframe(data_retrieve_link, no_of_stores,store_api_config)
clean_store_dataframe = db_cleaning.called_clean_store_data(store_dataframe)
db_connector.upload_to_db(db_cleaning.store_dataframe,'dim_store_details')

# clean the data from AWS s3. it is csv file converted it into json and finally it into dataframe.
#  Here clean the weight column into single unit and in float. Uploaded data in dim_store_details.

product_data = db_extractor.extract_from_s3("s3://data-handling-public/products.csv")
product_data = db_cleaning.clean_product_data(product_data)
db_cleaning.convert_product_weights(product_data)
db_connector.upload_to_db(db_cleaning.product_df,'dim_products')

# clean order table. used read db method. converted it into dataframe. Uploaded as fact_order_table.
order_df = db_cleaning.clean_order_data()
db_connector.upload_to_db(order_df,'orders_table')

# clean date data which is in json link. upload it into dim_date_times.
json_link = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json'
date_df = db_extractor.extract_from_json_file(json_link)
clean_date_df = db_cleaning.clean_date_data(date_df)
db_connector.upload_to_db(clean_date_df,'dim_date_times')