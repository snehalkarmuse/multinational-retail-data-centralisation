import pandas as pd
import psycopg2
import db_connect as dbc
import data_extraction as dbe
import datetime as dt
from sqlalchemy import text
import numpy as np
import re
class DataCleaning:
    # method to clean user data which is coming from legacy_user table. uploaded in dim_user
    def clean_user_data(self):
        
        self.df = db_extractor.read_rds_table(db_connector,"legacy_users")
         # check any null value
        print("Any Null values:", self.df.isnull().values.any())
        # drop null 
        self.df = self.df.dropna()
        # null count in whole table
        na_count = self.df.isna().sum().sum()
        # any duplicates in table. count rows
        self.df = self.df.drop_duplicates()
        no_duplicate_row_count = self.df.shape[0]
        print("No Duplicate:", no_duplicate_row_count)    

        self.df.drop(self.df.loc[self.df['first_name'].str.contains(r'[0-9]') == True].index,axis = 0,inplace = True)
        self.df.drop(self.df.loc[self.df['last_name'].str.contains(r'[0-9]') == True].index,axis = 0,inplace = True)
        
        self.df.drop(self.df.loc[self.df['email_address'].str.contains('@') == False].index,axis = 0,inplace = True)
        
        self.df.drop(self.df.loc[self.df['country'].str.contains(r'[0-9]') == True].index,axis = 0,inplace = True)
        
        self.df.drop(self.df.loc[self.df['country_code'].str.len() != 2].index,axis = 0,inplace = True)
       
        self.df.drop(self.df.loc[self.df['phone_number'].str.contains(r'[a-z]') == True].index,axis = 0,inplace = True)
        self.df['phone_number']= self.df['phone_number'].replace(['(',')',"+",".","-"]," ",inplace = True)
        #self.df['phone_number'] = self.df['phone_number'].str.strip()
        print(self.df.shape[0])
        #self.df = self.df[['date_of_birth','join_date']].dt.strftime('%y-%m-%d')
        self.df.drop(self.df.loc[self.df['date_of_birth'].str.contains('NULL', case = False)].index, axis = 0,inplace=True)
        self.df.drop(self.df.loc[self.df['date_of_birth'].str.contains(r'[a-z]', case = False)].index,axis = 0,inplace=True)
        self.df.drop(self.df.loc[self.df['date_of_birth'].str.contains('-') == False].index,axis = 0,inplace = True)
        self.df['date_of_birth'] = self.df['date_of_birth'].astype('datetime64[ns]')
        self.df['date_of_birth'] = pd.to_datetime(self.df["date_of_birth"]).dt.normalize()
        self.df['date_of_birth'] = pd.to_datetime(self.df['date_of_birth']).dt.date
        self.df.drop(self.df.loc[self.df['join_date'].str.contains('-') == False].index,axis = 0,inplace = True)
        self.df['join_date'] = self.df['join_date'].astype('datetime64[ns]')
        self.df['join_date'] = pd.to_datetime(self.df["join_date"]).dt.normalize()
        self.df['join_date'] = pd.to_datetime(self.df['join_date']).dt.date

    # This method is cleaning card data pdf file called card_details.pdf. uploading it into dim_card_details.
    def clean_card_data(self, dataframe):
        list_of_df = []
        for card_df in dataframe:
            local_card_dataframe = card_df
            local_card_dataframe = local_card_dataframe.dropna()
            local_card_dataframe.drop_duplicates()
            #change the datatype from int to string and then delete records whose card number is not equal to 16.
            local_card_dataframe['card_number'] = local_card_dataframe['card_number'].apply(str)
            local_card_dataframe.drop(local_card_dataframe.loc[local_card_dataframe['card_number'].str.len() != 16].index, axis = 0, inplace= True)
            # converting date_payment_confirmed first into date datatype andformating it. infer_datetime parameter will delete previous formating
            local_card_dataframe['date_payment_confirmed'] = pd.to_datetime(local_card_dataframe['date_payment_confirmed'],infer_datetime_format=True, format='%Y%m%d')
            local_card_dataframe['date_payment_confirmed'] = local_card_dataframe['date_payment_confirmed'].apply(str)
            list_of_df.append(local_card_dataframe)
            return list_of_df
        
    # This method is clean store data getting it from API. uploading it into dim_store_details.
    def called_clean_store_data(self, store_dataframe):
        self.store_dataframe = store_dataframe
        self.store_dataframe.pop('lat')
        self.store_dataframe = self.store_dataframe.dropna()
        self.store_dataframe.drop_duplicates()   
        self.store_dataframe.drop(self.store_dataframe.loc[self.store_dataframe['longitude'].str.contains(r'[0-9]') != True].index,axis = 0,inplace = True)
        self.store_dataframe.drop(self.store_dataframe.loc[self.store_dataframe['staff_numbers'].str.contains(r'[0-9]') != True].index,axis = 0,inplace = True)
        self.store_dataframe.drop(self.store_dataframe.loc[self.store_dataframe['latitude'].str.contains(r'[0-9]') != True].index,axis = 0,inplace = True)
        self.store_dataframe['continent'] = self.store_dataframe['continent'].str.replace('eeEurope','Europe')
        self.store_dataframe.drop(self.store_dataframe.loc[self.store_dataframe['continent'].str.contains(r'[a-z]') == False].index,axis = 0,inplace = True)
        self.store_dataframe.drop(self.store_dataframe.loc[self.store_dataframe['locality'].str.contains(r'[a-z]') == False].index,axis = 0,inplace = True)
        self.store_dataframe.drop(self.store_dataframe.loc[self.store_dataframe['store_type'].str.contains(r'[a-z]') == False].index,axis = 0,inplace = True)
        self.store_dataframe['opening_date']= pd.to_datetime(self.store_dataframe['opening_date'], infer_datetime_format=True)
        return self.store_dataframe

    # This method is clean product data aws s3. uploading it into dim_products.
    def clean_product_data(self,product_df):
        self.product_df = product_df
        self.product_df.drop_duplicates()
        self.product_df['date_added']= pd.to_datetime(self.product_df['date_added'], infer_datetime_format=True, errors='coerce')
        self.product_df = self.product_df.dropna()
        return self.product_df
    # This method calls weight_conversion method which checks the regular expression and converts the weight column into kg or in float.
    # This column is in product table.
    def convert_product_weights(self,product_df):
        s3_data.drop(s3_data.loc[s3_data['weight'].str.match(r'(\d*\.?\d+)(kg|g|l|ml)') == False].index,axis = 0,inplace = True)
        
        s3_data['weight'] = s3_data['weight'].apply(lambda x: self.weight_conversion(x))
    
    # This method takes the weight from s3_data dataframe. compiles the expression. search for it. If it gets dicides it into 2 groups
    # group 1 is converted into float. and group 2 is for unit check. If unit is g, ml then divide the weight by 1000.       
    def weight_conversion(self,weight_str):
        weight_rgx = re.compile(r'(\d*\.?\d+)(kg|g|l|ml)')
        mo = weight_rgx.search(weight_str)
        wn = float(mo.group(1)) 
        if mo.group(2)=='g' or mo.group(2)=='ml':
            wn = wn/1000
        return wn

    # This method cleans order data. deleted 3 columns which were null.
    def clean_order_data(self):
        self.order_df = db_extractor.read_rds_table(db_connector,"orders_table")
        self.order_df = self.order_df.drop(['first_name','last_name','1'], axis = 1)
        self.order_df['card_number'] = self.order_df['card_number'].apply(str)
        #self.order_df = self.order_df.drop(self.order_df.loc[self.order_df['card_number'].str.len() != 16].index, inplace= True, axis = 0)
        print(self.order_df.shape[0])
        self.order_df = self.order_df.dropna()
        self.order_df.drop_duplicates()
        return self.order_df

# API url and header info.
data_retrieve_link = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/"
store_api_config = {"x-api-key":"yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX"}

# instance for our classes. initlizing the engine and connect it.
db_connector = dbc.DatabaseConnector()
db_connector.init_db_engine()
db_extractor = dbe.DataExtractor()
db_cleaning = DataCleaning()

# clean user table data. converted it into dim_user.
db_cleaning.clean_user_data()
db_connector.upload_to_db(db_cleaning.df,'dim_user')

# clean card detail data from pdf file. take data from each page conver it into dataframe. All dataframe is added into dim_card_detail table.
list_of_df = db_cleaning.clean_card_data(db_extractor.retrieve_pdf_data("https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"))
for i in list_of_df:
    db_connector.upload_to_db(i,'dim_card_detail')

# clean store data from API url. take each store and collects it details and convert it into dataframe. Upload it into dim_store_details.
no_of_stores = db_extractor.list_number_of_stores('https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores',store_api_config)
store_dataframe= db_extractor.add_data_to_dataframe(data_retrieve_link, no_of_stores,store_api_config)
clean_store_dataframe = db_cleaning.called_clean_store_data(store_dataframe)
db_connector.upload_to_db(db_cleaning.store_dataframe,'dim_store_details')

# clean the data from AWS s3. it is csv file converted it into json and finally it into dataframe.
#  Here clean the weight column into single unit and in float. Uploaded data in dim_store_details.
s3_data = db_extractor.extract_from_s3("s3://data-handling-public/products.csv")
s3_data = db_cleaning.clean_product_data(s3_data)
db_cleaning.convert_product_weights(s3_data)
db_connector.upload_to_db(db_cleaning.product_df,'dim_products')

# clean order table. used read db method. converted it into dataframe. Uploaded as fact_order_table.
order_df = db_cleaning.clean_order_data()
db_connector.upload_to_db(order_df,'fact_orders_table')




