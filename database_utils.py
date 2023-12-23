import yaml
from sqlalchemy import create_engine, MetaData
import psycopg2

class DatabaseConnector:
    def __init__(self):
            pass
    def read_db_creds(self):
        file_path = "C:/Users/arhan/OneDrive/Documents/AI_Core/MRDC/db_creds.yaml"
        with open(file_path, 'r') as file:
            creds = yaml.safe_load(file)
        #print("read creds")
        return creds
    
    def init_db_engine(self):
        credentials = self.read_db_creds()
        from sqlalchemy import create_engine
        username = credentials['RDS_USER']
        password = credentials['RDS_PASSWORD']
        host = credentials['RDS_HOST']
        port = credentials['RDS_PORT']
        database = credentials['RDS_DATABASE']
        url = f"postgresql://{username}:{password}@{host}:{port}/{database}"
        db_engine = create_engine(url)
        db_engine.execution_options(isolation_level='AUTOCOMMIT').connect()
        db_engine.connect()
        #print("init db engine")
        return db_engine

    def list_db_tables(self):
        engine = self.init_db_engine()
        metadata = MetaData()
        metadata.reflect(bind=engine)
        table_names = metadata.tables.keys()
        table_list = ['legacy_store_details', 'legacy_users', 'orders_table']
        #print("list db tables")
        return table_list
    
    def upload_to_db(self, df, table_name):
        engine = self.init_db_engine()
        metadata = MetaData()
        metadata.reflect(bind=engine)
        table_names = metadata.tables.keys()
        username = 'postgres'
        password = 'FlatEarth[00]'
        host = 'localhost'
        port = '5433'
        database = 'sales_data'
        url = f"postgresql://{username}:{password}@{host}:{port}/{database}"
        df_engine = create_engine(url)
        #db_engine.execution_options(isolation_level='AUTOCOMMIT').connect()
        #db_engine.connect()
        #print("upload")
        df.to_sql(table_name, df_engine, index = False, if_exists = 'replace')