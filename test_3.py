import pandas as pd
import boto3
import io
import numpy as np
import database_utils
#import awswrangler as wr

def extract_from_s3(s3_address):
        access_key = 'AKIAUW2XISLH2YEKUQ4L'
        secret_key = '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'        
        bucket_name = "data-handling-public"
        object_key = "products.csv"
        s3 = boto3.client('s3')#, access_key, secret_key)
        response = s3.get_object(Bucket=bucket_name, Key=object_key)
        #session = boto3.session.Session(aws_access_key_id=access_key,  aws_secret_access_key=secret_key)
        #df = wr.s3.read_csv(path=address, boto3_session=session)
        df = pd.read_csv(io.BytesIO(response['Body'].read()))
        #print(df)
        
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

        #df.loc[df['weight'] == weight for 'na' in str(weight), 'weight'] = str(str(weight).split('na')[0] + str(weight).split('na')[1])

        for weight in weight_list:
                if str(weight)[-2] != 'k':
                        gram_weight = float(str(weight)[:-1])
                        kg_weight = gram_weight / 100
                        df.loc[df['weight'] == weight, 'weight'] = str(weight).replace('g', 'kg')
                        df.loc[df['weight'] == weight, 'weight'] = str(weight).replace(str(weight)[:-2], str(kg_weight))
        #new_weight_list = df['weight'].tolist()
        #print(new_weight_list)
        #df = df[df['weight'].str[-1] != 'g']
        #print(df['weight'].describe())
        #print(df['weight'].value_counts())
        #print(df['weight'])
        #print(df['weight'].tolist())


        #df = pd.DataFrame(response)
        #print(df)
        #print(response)
        #s3.download_file(bucket_name, object_key, "C:/Users/arhan/OneDrive/Documents/AI_Core/MRDC/s3_file.csv", "s3_data_file.csv")
        
        #df = pd.read_csv(s3_data_file.csv)
        return df

address = "s3://data-handling-public/products.csv"
#s3_df = extract_from_s3(address)
clean_df = extract_from_s3(address)
du = database_utils.DatabaseConnector()
du.upload_to_db(clean_df, 'dim_products')
