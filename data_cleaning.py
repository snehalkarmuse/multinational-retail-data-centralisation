import pandas as pd
import validate as vld
import numpy as np
import re


class DataCleaning:

    validator = vld.Validate()

    '''method to clean user data which is coming from legacy_user table. uploaded in dim_user'''
    def clean_user_data(self,df):
        self.validator.check_null(df)
        self.validator.validate_string(df, 'first_name')
        self.validator.validate_string(df, 'last_name')
        df.drop(df.loc[df['country'].str.contains('NULL') == True].index, inplace =True, axis = 0)
        df.drop(df.loc[df['country'].str.contains(r'[0-9]') == True].index, inplace =True, axis = 0)
        df.drop(df.loc[df['email_address'].str.contains('NULL') == True].index, inplace =True, axis = 0)
        self.validator.validate_email(df, 'email_address')
        df['phone_number'].replace(['(',')',"+",".","-"]," ",inplace = True)
        df.drop(df.loc[df['phone_number'].str.contains('NULL') == True].index, inplace =True, axis = 0)
        df.drop(df.loc[df['phone_number'].str.contains(r'[a-z]|[A-Z]') == True].index, inplace =True, axis = 0)
        #df.drop(df.loc[df['join_date'].str.contains('-') == False].index,axis = 0,inplace = True)
        self.validator.validate_date(df, 'join_date')
        self.validator.validate_date(df, 'date_of_birth')
        df.drop(df.loc[df['country_code'].str.len() != 2].index,axis = 0,inplace = True)
        return df

        
        
    ''' This method is cleaning card data pdf file called card_details.pdf. uploading it into dim_card_details.'''
    def clean_card_data(self, dataframe):
        list_of_df = []
        for card_df in dataframe:
            self.validator.check_null(card_df)
            card_df['card_number']= card_df['card_number'].astype(str)
            self.validator.validate_number(card_df,'card_number')
            card_df.drop(card_df.loc[card_df['expiry_date'].str.contains(r'[a-z]|[A-Z]') == True].index, inplace =True, axis = 0)
            self.validator.validate_date(card_df,'date_payment_confirmed')
            #card_df['date_payment_confirmed'] = card_df['date_payment_confirmed'].apply(lambda x: self.check_valid_date(x) )
            list_of_df.append(card_df)
        return list_of_df
    

    def check_valid_date(self, dt):
        dt_rgx = re.compile(r'\d\d\d\d-\d\d-\d\d')
        date_result = dt_rgx.search(dt)
        return date_result
    
    '''This method is clean store data getting it from API. uploading it into dim_store_details.'''
    def called_clean_store_data(self, store_df):
        store_df.pop('lat')
        #store_df['longitude'].replace('N/A','null',inplace = True)
        #self.validator.check_null(store_df)
        #store_df.drop(store_df.loc[store_df['longitude'].str.contains(r'[a-z]|[A-Z]') == True].index, inplace =True, axis = 0)
        #store_df.drop(store_df.loc[store_df['latitude'].str.contains(r'[a-z]|[A-Z]') == True].index, inplace =True, axis = 0)
        store_df.drop(store_df.loc[store_df['store_code'].str.contains('NULL') == True].index, inplace =True, axis = 0)
        self.validator.validate_number(store_df, 'staff_numbers')
        #self.validator.validate_string(store_df, 'continent')
        self.validator.validate_date(store_df, 'opening_date')  
        #store_df.drop(store_df.loc[store_df['country_code'].str.len() != 2].index,axis = 0,inplace = True)
        #store_df.drop(store_df.loc[store_df['country_code'].str.contains(r'[0-9]') == True].index, inplace =True, axis = 0)
        #store_df.drop(store_df.loc[store_df['continent'].str.contains(r'[0-9]') == True].index, inplace =True, axis = 0)
        store_df['continent'].replace('eeEurope','Europe',inplace = True)
        store_df['continent'].replace('eeAmerica','America',inplace = True)
        return store_df

    '''This method is clean product data aws s3. uploading it into dim_products.'''
    def clean_product_data(self,product_df):
        self.validator.check_null(product_df)
        product_df['product_price'] = product_df['product_price'].str.lstrip('£')
        self.validator.validate_date(product_df, 'date_added')
        self.validator.validate_number(product_df, 'EAN')
        self.validator.validate_string(product_df, 'category')
        self.validator.validate_string(product_df, 'removed')
        product_df['removed'].replace('Still_avaliable','True', inplace = True)
        product_df['removed'].replace('Removed','False', inplace = True)
        return product_df
    
    
    def add_product_weight_classifier(self,product_df):
        product_df ['weight_class'] = product_df ['weight'].apply(lambda x: self.weight_classifier(x) )
        

    def weight_classifier(self,x):
        x = int(x)
        if x < 2:
            classification = 'Light'
        elif 2 < x < 40:
            classification = 'Mid-sized'
        elif 40 < x < 140:
            classification = 'Heavy'
        else:
            classification = 'Truck_required'
        return classification
    
    ''' This method calls weight_conversion method which checks the regular expression and converts the weight column into kg or in float.
     This column is in product table.'''
    def convert_product_weights(self,product_df):
        product_df.drop(product_df.loc[product_df['weight'].str.match(r'(\d*\.?\d+)(kg|g|l|ml)') == False].index,axis = 0,inplace = True)
        product_df['weight'] = product_df['weight'].apply(lambda x: self.weight_conversion(x))
    
    ''' This method takes the weight from s3_data dataframe. compiles the expression. search for it. If it gets dicides it into 2 groups
     group 1 is converted into float. and group 2 is for unit check. If unit is g, ml then divide the weight by 1000. '''      
    def weight_conversion(self,weight_str):
        weight_rgx = re.compile(r'(\d*\.?\d+)(kg|g|l|ml)')
        mo = weight_rgx.search(weight_str)
        wn = float(mo.group(1)) 
        if mo.group(2)=='g' or mo.group(2)=='ml':
            wn = wn/1000
        return wn

    ''' This method cleans order data. deleted 3 columns which were null.'''
    def clean_order_data(self,order_df):
        order_df = order_df.drop(['first_name','last_name','1'], axis = 1)
        self.validator.check_null(order_df)
        return order_df

    ''' This method cleans the date data which got it from url.'''
    def clean_date_data(self,date_data):
        ''' cleans data. check valid date and month is entered.'''
        self.validator.check_null(date_data)
        self.validator.validate_number(date_data,'month')
        self.validator.validate_number(date_data,'year')
        self.validator.validate_number(date_data,'day')
        self.validator.validate_string(date_data,'time_period')
        #date_data['timestamp'] = date_data['timestamp'].apply(lambda x:self.clean_timestamp(x))
        #date_data['month'] = date_data['month'].apply(lambda x: self.check_valid(x,0,13))
        #date_data['day'] = date_data['day'].apply(lambda x: self.check_valid(x,0,32))
        return date_data
    
    def clean_timestamp(self,tm):
        tm_rgx = re.compile(r'\d\d:\d\d:\d\d')
        time_result = tm_rgx.search(tm)
        return time_result

    def check_valid(self,s,start,end):
        s = int(s)
        if start < s < end:
            return s





