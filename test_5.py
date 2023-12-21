import pandas as pd
import numpy as np
import database_utils

def extract_from_json(json_link):
        df = pd.read_json(json_link)
        df = df.replace('NULL', np.nan)
        df['year'] = pd.to_numeric(df['year'], errors='coerce')
        df.loc[df['year'].isna(), 'year'] = np.nan
        """for year in df['year'].tolist():
                if not str(year).isdigit():
                        df['year'] = df['year'].replace(year, np.nan)
        """
        #print(df['time_period'].describe())
        #print(df['timestamp'].value_counts())
        #print(df)
        df = df.dropna(how = 'any')
        return df
address = "https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json"
clean_df = extract_from_json(address)
du = database_utils.DatabaseConnector()
du.upload_to_db(clean_df, 'orders_table')