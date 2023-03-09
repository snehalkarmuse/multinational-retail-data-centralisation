
import db_connect as dbc
import data_extraction as dbe
import data_cleaning as dbcl
import validate as vld
import pandas as pd


db_connector = dbc.DatabaseConnector()
db_extractor =dbe.DataExtractor()
db_cleanser = dbcl.DataCleaning()
db_connector.init_db_engine()

data_retrieve_link = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/"
store_api_config = {"x-api-key":"yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX"} 

'''clean user table data. converted it into dim_user.'''
df = db_extractor.read_rds_table(db_connector,"legacy_users")
df = db_cleanser.clean_user_data(df)
db_connector.upload_to_db(df,'dim_users_table')

'''clean store data from API url. take each store and collects it details and convert it into dataframe. Upload it into dim_store_details.'''
no_of_stores = db_extractor.list_number_of_stores('https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores',store_api_config)
store_df= db_extractor.add_data_to_dataframe(data_retrieve_link, no_of_stores,store_api_config)
print(store_df.shape[0])
clean_store_df = db_cleanser.called_clean_store_data(store_df)
print(clean_store_df.shape[0])
db_connector.upload_to_db(clean_store_df,'dim_store_details')


product_data = db_extractor.extract_from_s3("s3://data-handling-public/products.csv")
product_df = db_cleanser.clean_product_data(product_data)
db_cleanser.convert_product_weights(product_df)
db_cleanser.add_product_weight_classifier(product_df)
db_connector.upload_to_db(product_df,'dim_products')


'''clean date data which is in json link. upload it into dim_date_times.'''
json_link = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json'
date_df = db_extractor.extract_from_json_file(json_link)
clean_date_df = db_cleanser.clean_date_data(date_df)
db_connector.upload_to_db(clean_date_df,'dim_date_times')

'''clean card detail data from pdf file. take data from each page conver it into dataframe. All dataframe is added into dim_card_detail table.'''
list_df = db_extractor.retrieve_pdf_data("https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf")
list_of_df = db_cleanser.clean_card_data(list_df)
print("x", len(list_df))
new_df = pd.DataFrame()
for i in list_of_df:
    new_df = new_df.append(i)
db_connector.upload_to_db(new_df,'dim_card_detail')

''' clean order table. used read db method. converted it into dataframe. Uploaded as fact_order_table.'''
order_df = db_extractor.read_rds_table(db_connector,"legacy_orders_table")
order_df = db_cleanser.clean_order_data(order_df)
db_connector.upload_to_db(order_df,'orders_table')
