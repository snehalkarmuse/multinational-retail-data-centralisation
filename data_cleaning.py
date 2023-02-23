import pandas as pd
import psycopg2
import db_connect as dbc
import data_extraction as dbe

class DataCleaning:
    def clean_user_data(self):
        
        self.df = db_extractor.read_rds_table(db_connector,"legacy_user")
        
        # check any null value
        print("Any Null values:", self.df.isnull().values.any())
        # drop null 
        self.df.dropna()
        # null count in whole table
        na_count = self.df.isna().sum().sum()
        print("Null Values:", na_count)
        # any duplicates in table. count rows
        self.df = self.df.drop_duplicates()
        duplicate_row_count = self.df.shape[0]
        print("duplicate row ", duplicate_row_count)       

        self.df.drop(self.df.loc[self.df['first_name'].str.contains(r'[0-9]') == True].index,axis = 0,inplace = True)
        self.df.drop(self.df.loc[self.df['last_name'].str.contains(r'[0-9]') == True].index,axis = 0,inplace = True)
        
        self.df.drop(self.df.loc[self.df['email_address'].str.contains('@') == False].index,axis = 0,inplace = True)
        
        self.df.drop(self.df.loc[self.df['country'].str.contains(r'[0-9]') == True].index,axis = 0,inplace = True)
        
        self.df.drop(self.df.loc[self.df['country_code'].str.len() != 2].index,axis = 0,inplace = True)
       
        self.df.drop(self.df.loc[self.df['phone_number'].str.contains(r'[a-z]') == True].index,axis = 0,inplace = True)
        self.df= self.df.replace(['(',')',"+",".","-"]," ",inplace = True)
        self.df['phone_number'] = self.df['phone_number'].str.strip()

        #self.df = self.df[['date_of_birth','join|_date']].dt.strftime('%y-%m-%d')
        self.df.drop(self.df.loc[self.df['date_of_birth'].str.contains('NULL',case = False)].index, axis = 0,inplace=True)
        self.df.drop(self.df.loc[self.df['date_of_birth'].str.contains('%a%',case = False)].index,axis = 0,inplace = True)
        self.df.drop(self.df.loc[self.df['date_of_birth'].str.contains('-') == False].index,axis = 0,inplace = True)
        self.df['date_of_birth'] = self.df['date_of_birth'].astype('datetime64[ns]')
        self.df['date_of_birth'] = pd.to_datetime(self.df["date_of_birth"]).dt.normalize()
        #self.df['date_of_birth'] = pd.to_datetime(self.df['date_of_birth']).dt.date

        self.df.drop(self.df.loc[self.df['join_date'].str.contains('-') == False].index,axis = 0,inplace = True)
        self.df['join_date'] = self.df['join_date'].astype('datetime64[ns]')
        self.df['join_date'] = pd.to_datetime(self.df["join_date"]).dt.normalize()
        #self.df['join_date'] = pd.to_datetime(self.df['join_date']).dt.date

        

db_connector = dbc.DatabaseConnector()
db_extractor = dbe.DataExtractor()
data_cleaning = DataCleaning()
data_cleaning.clean_user_data()
db_connector.upload_to_db(data_cleaning.df,'dim_user')
