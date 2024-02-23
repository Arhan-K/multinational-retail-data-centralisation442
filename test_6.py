import yaml
import pandas as pd
import tabula
import random
import data_extraction
import database_utils
import numpy as np

def reads_rds_table():
        dc = database_utils.DatabaseConnector()
        rds_engine = dc.init_db_engine()
        table_list = dc.list_db_tables
        store_details_query = "SELECT * FROM legacy_store_details;"
        user_query = "SELECT * FROM legacy_users;"
        orders_query = "SELECT * FROM orders_table;"
        store_df = pd.read_sql(store_details_query, rds_engine)
        user_df = pd.read_sql(user_query, rds_engine)
        orders_df = pd.read_sql(orders_query, rds_engine)
        return user_df #store_df, user_df, orders_df

def clean_user_data(df):
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
        return df

rough_user_df = reads_rds_table()
clean_user_df = clean_user_data(rough_user_df)
du = database_utils.DatabaseConnector()
du.upload_to_db(clean_user_df, 'dim_users')
