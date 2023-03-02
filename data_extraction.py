import pandas as pd
import psycopg2
from yaml import safe_load
import db_connect as dbc
from sqlalchemy import text
import tabula
import requests
import boto3

class DataExtractor:
    dict_to_df = pd.DataFrame()
    def read_rds_table(self, db_connector,table_name):
        with db_connector.init_db_engine().connect() as connection:
            df = pd.read_sql_query(text(f'''select * from {table_name}'''), connection)
            
            df = pd.DataFrame(df)
            
        return df
    
    def retrieve_pdf_data(self, link):
        pdf_to_df = tabula.read_pdf(link, stream = True, pages = 'all')
        return pdf_to_df

    def list_number_of_stores(self, api_end_point,store_api_config):
        no_of_store_response = requests.get(api_end_point, headers = store_api_config)
        
        s = no_of_store_response.json()
       
        store_no = s['number_stores']
        return store_no
    
    def retrieve_stores_data(self, data_retrieve_link, store_number, store_api_config):
        responce = requests.get(f"{data_retrieve_link}{store_number}", headers = store_api_config)
        store_detail = responce.json()
        return store_detail
    
    def add_data_to_dataframe(self,data_retrieve_link, no_of_stores, store_api_config):
        
        for store in range(no_of_stores):
            store_detail = self.retrieve_stores_data(data_retrieve_link, store, store_api_config)
            self.dict_to_df = self.dict_to_df.append(store_detail, ignore_index = True)
        return self.dict_to_df 

    def extract_from_s3(self,link):
        responce = requests.get(f"{link}")
        date_data =responce.json()
        return date_data
    

s3 = boto3.client("s3")
#s3.download_file("s3://data-handling-public/products.csv")
#print(s3)
data_retrieve_link = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/"
store_api_config = {"x-api-key":"yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX"}
pdf_path = "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"
dbconnector = dbc.DatabaseConnector()
db_extractor = DataExtractor()
#db_extractor.read_rds_table(dbconnector,"legacy_users")
#db_extractor.retrieve_pdf_data(pdf_path)
#no_of_stores = db_extractor.list_number_of_stores('https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores',store_api_config)
#data = db_extractor.add_data_to_dataframe(data_retrieve_link, no_of_stores, store_api_config)

#db_extractor.extract_from_s3("s3://data-handling-public/products.csv")

db_extractor.read_rds_table(dbconnector,'orders_table')


json_link = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json'

response = requests.get(json_link)
r = response.json()
print(type(r))
s_df = pd.DataFrame(r)
print(s_df.shape[0])