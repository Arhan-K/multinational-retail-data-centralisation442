import requests
import pandas as pd
import numpy as np
def list_number_of_stores(store_number_endpoint, header_dict):
    response = requests.get(store_number_endpoint, headers = header_dict)

    if response.status_code == 200:
        data = response.json()
        number_extracted_stores = data['number_stores']
        return number_extracted_stores  
    else:
        print(f"Request failed with status code: {response.status_code}")
        print(f"Response Text: {response.text}")

def retrieve_stores_data(retrieve_store_endpoint, header_dict, number_stores):
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
    
header = {"x-api-key": "yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX"}
retrieve_store_api = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/"
number_store_api = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores"
number_stores_extracted = list_number_of_stores(number_store_api, header)

def clean_store_data(data_store_rough):
    #print(f"total card numbers = {len(df['card_number'])}")
    #print(f"unique card numbers before cleaning = {df['card_number'].nunique()}")
    df = data_store_rough
    country_code_list = df['country_code'].tolist()
    false_codes = []
    for code in country_code_list:
        if len(code) != 2:
            false_codes.append(code)
    #for code in false_codes:
    #    df['country_code'] = df['country_code'].replace(code, np.nan)
    #df = df.replace('NULL', np.nan)
    #df['country_code'] = df['country_code'].dropna()#(how = 'all')
    #df  = df.dropna(axis = 0, how  = 'any')
    df = df[df['country_code'].str.len() == 2]
    return df

rough_data = retrieve_stores_data(retrieve_store_api, header, number_stores_extracted)
clean_data = clean_store_data(rough_data)
print(clean_data)
print(clean_data['address'].describe())
print(clean_data['address'].value_counts())
#cleaned_df = clean_store_data(data_store)
data = response['Body'].read().decode('utf-8')