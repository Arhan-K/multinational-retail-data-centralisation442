import yaml
import pandas as pd
import tabula
import requests
import boto3
import io
import database_utils

class DataExtractor:
    '''
    
    Methods:
    '''
    def __init__(self):
        # attributes
        a = 10

    # methods
    def reads_rds_table(self):
        '''
        Uses an instance of the DatabaseConnector class and uses Python's tabula-py library to read 
        the pdf returns a pandas DataFrame.
        '''
        dc = database_utils.DatabaseConnector()
        rds_engine = dc.init_db_engine()
        table_list = dc.list_db_tables
        store_details_query = "SELECT * FROM legacy_store_details;"
        user_query = "SELECT * FROM legacy_users;"
        orders_query = "SELECT * FROM orders_table;"
        store_df = pd.read_sql(store_details_query, rds_engine)
        user_df = pd.read_sql(user_query, rds_engine)
        orders_df = pd.read_sql(orders_query, rds_engine)
        return [user_df, store_df, orders_df]
    
    def retrieve_pdf_data(self, pdf_link):  
        '''
        Uses Python's tabula-py library to read the pdf

        Parameters:
        ----------
        pdf_link: str
            The link to the pdf to obtain data from

        '''
        tabular_data = tabula.read_pdf(pdf_link, pages = 'all', multiple_tables = True)
        df = pd.concat(tabular_data, ignore_index=True)
        return df
    
    def list_number_of_stores(self,store_number_endpoint, header_dict):
        response = requests.get(store_number_endpoint, headers = header_dict)
        if response.status_code == 200:
            data = response.json()
            number_extracted_stores = data['number_stores']
            return number_extracted_stores  
        else:
            print(f"Request failed with status code: {response.status_code}")
            print(f"Response Text: {response.text}")
    
    def retrieve_stores_data(self, retrieve_store_endpoint, header_dict, number_stores):
        '''
        Take the endpoint to retrieve a store as an argument and extracts all the stores from the API
        saving them in a pandas DataFrame.

        Parameters:
        retrieve_store_endpoint: str
            API endpoint to retrieve a store
        header_dict: dictionary
        '''
        store_data_list = []
        for i in range(0, number_stores):
            response = requests.get(f"{retrieve_store_endpoint}{i}", headers = header_dict)
            if response.status_code == 200:
                data = response.json()
                store_data_list.append(data)
            else:
                print(f"Request failed with status code: {response.status_code}")
                print(f"Response Text: {response.text}")
        store_data_df = pd.DataFrame(store_data_list)
        return store_data_df
    
    def extract_from_s3(self, s3_address):
        '''
        Uses the boto3 package to download and extract the information returning a pandas DataFrame.

        Parameters:
        retrieve_store_endpoint: str
            API endpoint to retrieve a store
        header_dict: dictionary
        '''
        access_key = 'AKIAUW2XISLH2YEKUQ4L'
        secret_key = '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'        
        bucket_name = "data-handling-public"
        object_key = "products.csv"
        s3 = boto3.client('s3')#, access_key, secret_key)
        response = s3.get_object(Bucket=bucket_name, Key=object_key)
        df = pd.read_csv(io.BytesIO(response['Body'].read()))
        df.to_csv('s3_csv.csv', index = False)
        return df

    def extract_from_json(self, json_link):
        '''
        Uses pandas to extract the information from the json link a pandas DataFrame.

        Parameters:
        json_link: str
            link to the json file
        '''
        df = pd.read_json(json_link)
        return df