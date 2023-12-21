import yaml
import pandas as pd
import tabula
import random
import data_extraction
import database_utils
import numpy as np

class DataCleaning:
    '''
    
    Methods:
    '''
    def __init__(self):        
        pass

    def convert_product_weights(self, products_df):
        self.products_df = products_df
        return products_df
    
    def clean_user_data(self, df):
        self.df = df
        df = df.dropna(how = 'any')
        false_emails = []
        for email in df['email_address'].tolist():
            if not '@' in email or not '.' in email:
                false_emails.append(email)
        for email in false_emails:
            df['email_address'] = df['email_address'].replace(email, np.nan)
        df = df.replace('NULL', np.nan)
        df = df.dropna()
        def invalid_email(email):
             if not '@' in email or not '.' in email:
                    return email
             
        #df.loc[df['email_address'].apply(invalid_email()) 'GGB', 'country_code'] = 'GB'
        df.loc[df['country_code'] == 'GGB', 'country_code'] = 'GB'
        print("clean user data")
        return df

    def clean_card_data(self, df):
        df['card_number'] = pd.to_numeric(df['card_number'], errors='coerce')
        df = df.replace('NULL', np.nan)
        df = df.dropna(how = 'any')
        return df
    
    def clean_store_data(self, df):        
        df = df[df['country_code'].str.len() == 2]
        print("clean store data")
        return df

    def clean_products_data(self, df):
        df.loc[df['weight'] == '77g .', 'weight'] = '77g'
        df.loc[df['weight'] == '16oz', 'weight'] = '0.454kg'
        def weight_conversion(weight):
                if 'na' in str(weight):
                        str_1 = str(weight).split('na')[0]
                        str_2 = str(weight).split('na')[1]
                        weight = str_1 + str_2
                if str(weight)[-2:] == 'ml':  
                        str(weight).replace('ml', 'g')
                if ' x ' in str(weight):  
                        a =  float((str(weight).split(' x '))[0])
                        b = (str(weight).split(' x '))[1]
                        if b[-2] == 'k':
                                unit = 'kg'
                                b = float(b[:-2])
                        else:
                                unit = 'g'
                                b = float(b[:-1])
                        weight = a * b
                #if str(weight)[-2] != 'k':
                #        gram_weight = float(str(weight)[:-1])
                #        weight = gram_weight / 100
                return f"{weight}g"
              #weight = self.weight
              #weight = float(weight) / 100
              #return weight
        df['weight'] = df['weight'].apply(weight_conversion)
        #df['weight'].loc[:, 'weight'] = df['weight'].loc[:,'weight'].astype('str').apply(lambda x : weight_conversion(x))
        df.loc[str(df['weight'])[-1] != 'g', 'weight'] = np.nan
        df = df.dropna()
        return df
        """def is_ml(weight):
              if str(weight)[-2:] == 'ml':
                   return weight

        df.loc[df['weight'].apply(is_ml()), 'weight'] = df[df['weight'] == is_ml(df['weight']), 'weight'] = df[df['weight'].str.replace('ml', 'g')]"""
        
        """def has_x(weight):
              if ' x ' in str(weight):
                    return weight
        df.loc[df['weight'].apply(has_x), 'weight'] = df[df['weight'].apply(has_x)]['weight'].float(str.split(' x ')[0] * str.split(' x ')[1])"""
        
        
        
    def clean_orders_data(self, df):
        df = df.replace('NULL', np.nan)
        year_list = df['year'].tolist()
        """false_years = []
        for year in year_list:
                if not str(year).isdigit():
                        false_years.append(year)
        for year in false_years:
              df['year'] = df['year'].replace(year, np.nan)"""
        df = df.dropna(how = 'any')
        #print(df['time_period'].describe())
        #print(df['timestamp'].value_counts())
        return df

de = data_extraction.DataExtractor()
dc = DataCleaning()

header = {"x-api-key": "yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX"}
pdf_link = "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"
retrieve_store_api = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/"
number_store_api = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores"
number_stores_extracted = de.list_number_of_stores(number_store_api, header)
address = "s3://data-handling-public/products.csv"
json_address = "https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json"

#rough_user_df = de.reads_rds_table()
#clean_user_df = dc.clean_user_data(rough_user_df)
#rough_card_df = de.retrieve_pdf_data(pdf_link)
#clean_card_df = dc.clean_card_data(rough_card_df)
#rough_store_df = de.retrieve_stores_data(retrieve_store_api, header, number_stores_extracted)
#clean_store_df = dc.clean_store_data(rough_store_df)
rough_products_df = de.extract_from_s3(address)
clean_products_df = dc.clean_products_data(rough_products_df)
#rough_orders_df = de.extract_from_json(json_address)
#clean_orders_df = dc.clean_orders_data(rough_orders_df)

du = database_utils.DatabaseConnector()
#du.upload_to_db(clean_user_df, 'dim_users')
#du.upload_to_db(clean_card_df, 'dim_card_details')
#print("upload dim_card_details")
#du.upload_to_db(clean_store_df, 'dim_store_details')
du.upload_to_db(clean_products_df, 'dim_products')
#du.upload_to_db(clean_orders_df, 'orders_table')
#dc.clean_user_data(user_df)