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

    def clean_user_data(self, df):
        '''
        Cleans the user data by removing or correcting NULL values, errors with dates, 
        incorrectly typed values and rows filled with the wrong information.

        Parameters:
        df: pandas dataframe

        Returns:
        df: pandas dataframe after cleaning

        '''
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
        '''
        Cleans the card data by removing or correcting NULL values, errors with dates, 
        incorrectly typed values and rows filled with the wrong information.

        Parameters:
        df: pandas dataframe

        Returns:
        df: pandas dataframe after cleaning

        '''
        df['card_number'] = pd.to_numeric(df['card_number'], errors='coerce')
        df = df.replace('NULL', np.nan)
        df = df.dropna(how = 'any')
        return df
    
    def clean_store_data(self, df):
        '''
        Cleans the store data by removing or correcting NULL values, errors with dates, 
        incorrectly typed values and rows filled with the wrong information.
        Corrects spelling mistakes and imprecise values for in the country_code and continent 
        columns.
        Uses the locality of stores to add the correct country_code.
        Gets rid of the erroneous lat column

        Parameters:
        df: pandas dataframe

        Returns:
        df: pandas dataframe after cleaning

        '''
        df['country_code'] = df['country_code'].replace('America', 'US')
        df.loc[df['continent'] == 'eeEurope', 'continent'] = 'Europe'
        df.loc[df['continent'] == 'eeAmerica', 'continent'] = 'America'
        
        europe_df =  df[df['country_code'] == 'Europe']
        GB_list = ["High Wycombe", "Lymington", "Newbury", "Lancing", "Gainsborough", "Viewpark", 
                   "Kingston upon Hull", "Penzance", "Chapeltown", "Westbury"]
        
        DE_list = ["Schleswig", "Pfullingen", "Friedberg", "Rutherglen", "Belper", "Gauting", 
                   "Reutlingen", "Trossingen", "Bad Segeberg", "Gronau", "Bad Honnef"]
        euro_cities = []
        for country in europe_df['address'].tolist():
              euro_cities.append(country.split(', ')[-1])
        
        
        for city in df['locality'].tolist():
              if city in GB_list:
                df['country_code'] = df['country_code'].replace('Europe', 'GB')
                #df.loc[df['address'] == address, 'country_code'] = 'GB'
              if city in DE_list:
                df['country_code'] = df['country_code'].replace('Europe', 'DE')
        
        for num in df['staff_numbers'].tolist():
                for char in str(num):
                      if not char.isdigit():
                            new_num = str(num).replace(char, '')
                            df.loc[df['staff_numbers'] == num, 'staff_numbers'] = new_num
        df = df[df['country_code'].str.len() == 2]
        df.loc[df['latitude'] == 'NULL', 'latitude'] = np.nan
        df = df.replace('NULL', np.nan)
        df = df.drop(columns=['lat'])
        df = df.dropna(how='all')
        #print(df['staff_numbers'].describe())
        #print(df['staff_numbers'].value_counts())
        return df

    def clean_products_data(self, df):
        '''
        Cleans the store data by removing or correcting NULL values, erroneous weights, 
        incorrectly typed values and rows filled with the wrong information.
        Converts all weights into kg after making appropriate conversions.
        Uses the locality of stores to add the correct country_code.
        Gets rid of the erroneous lat column.

        Parameters:
        df: pandas dataframe

        Returns:
        df: pandas dataframe after cleaning

        '''
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
        for weight in false_weights:
                df['weight'] = df['weight'].replace(weight, np.nan)
        df = df.dropna()

        for weight in df['weight'].tolist():
                if ' x ' in (str(weight)):
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
          '''
        Cleans the orders by getting rid of the first_name, last_name, and 1 columns

        Parameters:
        df: pandas dataframe

        Returns:
        df: pandas dataframe after cleaning

        '''
          df = df.drop(columns=['first_name'])
          df = df.drop(columns=['last_name'])
          df = df.drop(columns=['1'])
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

rough_user_df = de.reads_rds_table()[0]
clean_user_df = dc.clean_user_data(rough_user_df)
du.upload_to_db(clean_user_df, 'dim_users')

rough_card_df = de.retrieve_pdf_data(pdf_link)
clean_card_df = dc.clean_card_data(rough_card_df)
du.upload_to_db(clean_card_df, 'dim_card_details')

rough_store_df = de.retrieve_stores_data(retrieve_store_api, header, number_stores_extracted)
clean_store_df = dc.clean_store_data(rough_store_df)
du.upload_to_db(clean_store_df, 'dim_store_details')


rough_products_df = de.extract_from_s3(address)
clean_products_df = dc.clean_products_data(rough_products_df)
du.upload_to_db(clean_products_df, 'dim_products')

rough_orders_df = de.reads_rds_table()[2]
clean_orders_df = dc.clean_orders_data(rough_orders_df)
du.upload_to_db(clean_orders_df, 'orders_table')

rough_dates_df = de.extract_from_json(json_address)
clean_dates_df = dc.clean_dates_data(rough_dates_df)
du.upload_to_db(clean_dates_df, 'dim_date_times')