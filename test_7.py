import yaml
import pandas as pd
import tabula
import random
import data_extraction
import database_utils
import numpy as np
def clean_orders_data(df):
          print(df.columns)
          df = df.drop(columns=['first_name'])
          df = df.drop(columns=['last_name'])
          #df = df.drop(columns=['1'])
          print(df.columns)

de = data_extraction.DataExtractor()
du = database_utils.DatabaseConnector()
rough_orders_df = de.reads_rds_table()[2]
clean_orders_data(rough_orders_df)