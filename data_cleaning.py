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
        #for country in df['country_code'].tolist():
        #    if country == 'GGB':
        #        df.loc[df['country_code'] == country, 'country_code'] = 'GB'
        df.loc[df['country_code'] == 'GGB', 'country_code'] = 'GB'
        print("clean user data")
        return df

    def clean_card_data(self, df):
        #print(f"total card numbers = {len(df['card_number'])}")
        #print(f"unique card numbers before cleaning = {df['card_number'].nunique()}")
        string_cards = [str(card_number) for card_number in df['card_number'].tolist()]
        """false_cards = []
        for card in string_cards:
            if not card.isdigit():
                false_cards.append(card)
        for card in false_cards:
            df['card_number'] = df['card_number'].replace(card, np.nan)"""
        df['card_number'] = pd.to_numeric(df['card_number'], errors='coerce')
        df = df.replace('NULL', np.nan)
        df = df.dropna(how = 'any')
        #print(f"unique card numbers = {df['card_number'].nunique()}")
        #print(f"valid card numbers = {len(df['card_number'])}")
        #print(df['card_number'].describe())
        #print(df['card_number'].value_counts())
        #print("clean card data")
        return df
    
    def clean_store_data(self, df):
        country_code_list = df['country_code'].tolist()
        #for code in false_codes:
        #    df['country_code'] = df['country_code'].replace(code, np.nan)
        #df = df.replace('NULL', np.nan)
        #df['country_code'] = df['country_code'].dropna()#(how = 'all')
        #df  = df.dropna(axis = 0, how  = 'any')
        df = df[df['country_code'].str.len() == 2]
        print("clean store data")
        return df

    def clean_products_data(self, df):
        df.loc[df['weight'] == '77g .', 'weight'] = '77g'
        df.loc[df['weight'] == '16oz', 'weight'] = '0.454kg'
        weight_list = df['weight'].tolist()
        
        ml_weights = []
        for weight in weight_list:
                if str(weight)[-2:] == 'ml':
                        ml_weights.append(weight)        
        for ml_weight in ml_weights:
                df.loc[df['weight'] == ml_weight, 'weight'] = str(ml_weight).replace('ml', 'g')
        
        false_weights = []
        for weight in weight_list:
                if str(weight)[-1] != 'g':
                        false_weights.append(weight)
                #if not str(weight).isdigit():
                #        false_weights.append(weight)
        for weight in false_weights:
                df['weight'] = df['weight'].replace(weight, np.nan)
        df = df.dropna()
        
        weight_list = df['weight'].tolist()

        for weight in weight_list:
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
        weight_list = df['weight'].tolist()
        for weight in weight_list:
                if 'na' in str(weight):
                        str_1 = str(weight).split('na')[0]
                        str_2 = str(weight).split('na')[1]
                        weight = str_1 + str_2
                        df.loc[df['weight'] == weight, 'weight'] = str(weight)
        weight_list = df['weight'].tolist()

        for weight in weight_list:
                if str(weight)[-2] != 'k':
                        gram_weight = float(str(weight)[:-1])
                        kg_weight = gram_weight / 100
                        df.loc[df['weight'] == weight, 'weight'] = str(weight).replace('g', 'kg')
                        df.loc[df['weight'] == weight, 'weight'] = str(weight).replace(str(weight)[:-2], str(kg_weight))
        
        return df

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

rough_user_df = de.reads_rds_table()
clean_user_df = dc.clean_user_data(rough_user_df)
#rough_card_df = de.retrieve_pdf_data(pdf_link)
#clean_card_df = dc.clean_card_data(rough_card_df)
rough_store_df = de.retrieve_stores_data(retrieve_store_api, header, number_stores_extracted)
clean_store_df = dc.clean_store_data(rough_store_df)
rough_products_df = de.extract_from_s3(address)
clean_products_df = dc.clean_products_data(rough_products_df)
rough_orders_df = de.extract_from_json(json_address)
clean_orders_df = dc.clean_orders_data(rough_orders_df)

du = database_utils.DatabaseConnector()
du.upload_to_db(clean_user_df, 'dim_users')
print("upload dim_users")
#du.upload_to_db(clean_card_df, 'dim_card_details')
#print("upload dim_card_details")
du.upload_to_db(clean_store_df, 'dim_store_details')
print("upload dim_store_details")
du.upload_to_db(clean_products_df, 'dim_products')
print("upload dim_products")
du.upload_to_db(clean_orders_df, 'orders_table')
print("upload orders_table")
#dc.clean_user_data(user_df)