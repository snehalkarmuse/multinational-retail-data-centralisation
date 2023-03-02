import pandas as pd
import psycopg2
from yaml import safe_load
import db_connect as dbc
from sqlalchemy import text
import tabula
import requests
import boto3

class DataExtractor:
    # created new dataframe which will be needed when converting store detail.
    dict_to_df = pd.DataFrame()
    # This method initialzes and connects to the engine. Getting records from the tablename and converting it into dataframe.
    def read_rds_table(self, db_connector,table_name):
        # 'with' is context manager which alocates and releases the resources.
        with db_connector.init_db_engine().connect() as connection:
            df = pd.read_sql_query(text(f'''select * from {table_name}'''), connection)
            df = pd.DataFrame(df)
        return df
    # In this method, used tabula, which converts pdf file into csv, json or in pandas dataframe. Imported tabula.
    #There might be number of pages so include pages = all. it creates new dataframe for each page.  
    def retrieve_pdf_data(self, link):
        pdf_to_df = tabula.read_pdf(link, stream = True, pages = 'all')
        return pdf_to_df
    # This method created for taking teh data from the API. Which is interface which takes request and gives response. 
    # To get the data, needs url and header info or authentication key. Here we call json method on response and converted it into dataframe.
    # This method gives number of stores are in the database. 
    def list_number_of_stores(self, api_end_point,store_api_config):
        no_of_store_response = requests.get(api_end_point, headers = store_api_config)
        s = no_of_store_response.json()
        store_no = s['number_stores']
        return store_no
    # This method created for taking teh data from the API. Which is interface which takes request and gives response. 
    # To get the data, needs url and header info or authentication key. Here we call json method on response and converted it into dataframe
    # This method takes the specific store number and converts store details into dataframe. 
    def retrieve_stores_data(self, data_retrieve_link, store_number, store_api_config):
        responce = requests.get(f"{data_retrieve_link}{store_number}", headers = store_api_config)
        store_detail = responce.json()
        return store_detail
    # This method created for taking teh data from the API. Which is interface which takes request and gives response. 
    # This method is taking all store in for loop and retrives the details. appending it in one dataframe which created above.
    def add_data_to_dataframe(self,data_retrieve_link, no_of_stores, store_api_config):
        for store in range(no_of_stores):
            store_detail = self.retrieve_stores_data(data_retrieve_link, store, store_api_config)
            self.dict_to_df = self.dict_to_df.append(store_detail, ignore_index = True)
        return self.dict_to_df 
    # This methods takes data from the AWS S3. which is data pool where files are stored. For this created user on AWS s3. Important information is 
    # username, password, region (selected US) which supoorts all data. After that downloaded awscli, aws configure and boto. imported boto in program.
    # The link gets the file on the aws s3. Created s3 object. convered json response into dataframe.
    def extract_from_s3(self,link):
        responce = requests.get(f"{link}")
        date_data =responce.json()
        return date_data
    def extract_from_json_file(self,link):
        response = requests.get(json_link)
        r = response.json()
        s_df = pd.DataFrame(r)
        return s_df
    
# for aws s3 data.
s3 = boto3.client("s3")
#s3.download_file("s3://data-handling-public/products.csv")

# API link and header info.(data_retrieve_link and store_api_config variable)
data_retrieve_link = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/"
store_api_config = {"x-api-key":"yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX"}

# PDF url to get data.
pdf_path = "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"

# Created instance of Database Connector class. to initialize the engine, connect it.
dbconnector = dbc.DatabaseConnector()
db_extractor = DataExtractor()
# This call id for method read db which takes data from 'legacy_user' abd converts it into dataframe.
db_extractor.read_rds_table(dbconnector,"legacy_users")
# This is a call for retrieve pdf data and giving pdf file path.
db_extractor.retrieve_pdf_data(pdf_path)
# This is a call to get the number of stores which on API. url and header as an argument.
no_of_stores = db_extractor.list_number_of_stores('https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores',store_api_config)
# This method call is for adding store details into one dataframe.
data = db_extractor.add_data_to_dataframe(data_retrieve_link, no_of_stores, store_api_config)
# This method call is for AWS s3 data. Which is product details.
db_extractor.extract_from_s3("s3://data-handling-public/products.csv")
# This is a call for read order table. which is main table.
db_extractor.read_rds_table(dbconnector,'orders_table')

# Json link to get the data which date.
json_link = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json'

