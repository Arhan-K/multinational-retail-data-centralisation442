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
    
    def clean_user_data_new(self, df):
        self.df = df
        df = df.dropna(how = 'any')
        """false_emails = []
        for email in df['email_address'].tolist():
            if not '@' in email or not '.' in email:
                false_emails.append(email)
        for email in false_emails:
            df['email_address'] = df['email_address'].replace(email, np.nan)"""
        def is_valid(email):
              if not '@' in email or not '.' in email:
                    return False
              else:
                    return True
        df = df[is_valid(str(df['email_address']))]
        df = df.replace('NULL', np.nan)
        df = df.dropna()     
        #df.loc[df['email_address'].apply(invalid_email()) 'GGB', 'country_code'] = 'GB'
        
        
        print("clean user data")
        return df
    
    def clean_user_data(self, df):
        false_emails = []
        for email in df['email_address'].tolist():
            if not '@' in email or not '.' in email:
                false_emails.append(email)
        for email in false_emails:
            df['email_address'] = df['email_address'].replace(email, np.nan)
        df = df.replace('NULL', np.nan)
        df = df.dropna()
        for country in df['country_code'].tolist():
            if country == 'GGB':
                df.loc[df['country_code'] == country, 'country_code'] = 'GB'
        print("clean user data")
        return df

    def clean_card_data(self, df):
        df['card_number'] = pd.to_numeric(df['card_number'], errors='coerce')
        df = df.replace('NULL', np.nan)
        df = df.dropna(how = 'any')
        return df
    
    def clean_store_data(self, df):
        df.loc[df['country_code'] == 'GGB', 'country_code'] = 'GB'        
        df = df[df['country_code'].str.len() == 2]
        df.loc[df['latitude'] == 'NULL', 'latitude'] = np.nan
        df.loc[df['lat'] == 'NULL', 'lat'] = np.nan
        #df['latitude'] = df['latitude'].replace('NULL', np.nan)
        #df = df.replace('NULL', np.nan)
        #df['lat'] = df['lat'].dropna()
        #df['latitude'] = df['latitude'].dropna()
        df = df['lat'].dropna()
        df = df['latitude'].dropna()
        print("clean store data")
        return df

    def clean_products_data_new(self, df):
        df.loc[df['weight'] == '77g .', 'weight'] = '77g'
        df.loc[df['weight'] == '16oz', 'weight'] = '0.454kg'
        df = df['EAN'].dropna(how = 'all')
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
                if str(weight)[-2] != 'k':
                        gram_weight = float(str(weight)[:-1])
                        weight = gram_weight / 100
                return f"{weight}g"
        df = df['weight'].apply(weight_conversion)
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
        
    def clean_products_data(self, df):
        df.loc[df['weight'] == '77g .', 'weight'] = '77g'
        df.loc[df['weight'] == '16oz', 'weight'] = '0.454kg'
        
        ml_weights = []
        for weight in df['weight'].tolist():
                if str(weight)[-2:] == 'ml':
                        ml_weights.append(weight)        
        for ml_weight in ml_weights:
                df.loc[df['weight'] == ml_weight, 'weight'] = str(ml_weight).replace('ml', 'g')
        
        false_weights = []
        for weight in df['weight'].tolist():
                if str(weight)[-1] != 'g':
                        false_weights.append(weight)
                #if not str(weight).isdigit():
                #        false_weights.append(weight)
        for weight in false_weights:
                df['weight'] = df['weight'].replace(weight, np.nan)
        df = df.dropna()

        for weight in df['weight'].tolist():
                if ' x ' in (str(weight)):
                        #weight_1 = str(weight).slice[0 : ' '.index()]
                        weight_1 = float(str(weight).split(' x ')[0])
                        weight_2 = str(weight).split(' x ')[1]
                        if weight_2[-2] == 'k':
                                unit = 'kg'
                                weight_2 = float(weight_2[:-2])
                        else:
                                unit = 'g'
                                weight_2 = float(weight_2[:-1])
                        new_weight = weight_1 * weight_2
                        df.loc[df['weight'] == weight, 'weight'] = str(new_weight) + unit

        for weight in df['weight'].tolist():
                if 'na' in str(weight):
                        str_1 = str(weight).split('na')[0]
                        str_2 = str(weight).split('na')[1]
                        weight = str_1 + str_2
                        df.loc[df['weight'] == weight, 'weight'] = str(weight)

        for weight in df['weight'].tolist():
                if str(weight)[-2] != 'k':
                        gram_weight = float(str(weight)[:-1])
                        kg_weight = gram_weight / 100
                        df.loc[df['weight'] == weight, 'weight'] = str(weight).replace('g', 'kg')
                        df.loc[df['weight'] == weight, 'weight'] = str(weight).replace(str(weight)[:-2], str(kg_weight))
        
        return df
        
    def clean_orders_data(self, df):
          df = df.drop(columns=['first_name'])
          df = df.drop(columns=['last_name'])
          #df = df.drop(columns=['1'])
          return df

    def clean_dates_data(self, df):
        df = df.replace('NULL', np.nan)
        false_years = []
        for year in df['year'].tolist():
                if not str(year).isdigit():
                        false_years.append(year)
        for year in false_years:
              df['year'] = df['year'].replace(year, np.nan)
        df = df.dropna(how = 'any')
        #print(df['time_period'].describe())
        #print(df['timestamp'].value_counts())
        return df

de = data_extraction.DataExtractor()
dc = DataCleaning()
du = database_utils.DatabaseConnector()

header = {"x-api-key": "yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX"}
pdf_link = "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"
retrieve_store_api = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/"
number_store_api = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores"
number_stores_extracted = de.list_number_of_stores(number_store_api, header)
address = "s3://data-handling-public/products.csv"
json_address = "https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json"

"""rough_user_df = de.reads_rds_table()[0]
clean_user_df = dc.clean_user_data(rough_user_df)
du.upload_to_db(clean_user_df, 'dim_users')
print(clean_user_df.columns)
print("upload dim_users")

rough_card_df = de.retrieve_pdf_data(pdf_link)
clean_card_df = dc.clean_card_data(rough_card_df)
du.upload_to_db(clean_card_df, 'dim_card_details')
print(clean_card_df.columns)
print("upload dim_card_details")"""

rough_store_df = de.retrieve_stores_data(retrieve_store_api, header, number_stores_extracted)
clean_store_df = dc.clean_store_data(rough_store_df)
du.upload_to_db(clean_store_df, 'dim_store_details')
print(clean_store_df.columns)
print("upload dim_store_details")

"""rough_products_df = de.extract_from_s3(address)
clean_products_df = dc.clean_products_data(rough_products_df)
du.upload_to_db(clean_products_df, 'dim_products')
print(clean_products_df.columns)
print("upload dim_products")"""

"""rough_orders_df = de.reads_rds_table()[2]
clean_orders_df = dc.clean_orders_data(rough_orders_df)
du.upload_to_db(clean_orders_df, 'orders_table')
print(clean_orders_df.columns)
print("upload orders_table")

rough_dates_df = de.extract_from_json(json_address)
clean_dates_df = dc.clean_dates_data(rough_dates_df)
du.upload_to_db(clean_dates_df, 'dim_date_times')
print(clean_dates_df.columns)
print("upload dim_date_times")"""